//! Finite or explicitly continuous observation with one ordered decision path.
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use aos_core::{resolve_qos, Context, EndpointOptions};
use serde_json::{json, Map, Value};

use super::hooks::{self, Condition, Decision, HookError, HookErrorKind, HookRunner};
use super::statistics::{system_time_ns, Statistics};
use super::{
    configuration::InvocationConfig, event, full_name, named_entities, publication, text,
    unique_type, CliError, Result,
};

struct Options {
    discovery: Duration,
    duration: Option<Duration>,
    idle: Option<Duration>,
    count: Option<u64>,
    filter: Option<Condition>,
    hook: Option<HookRunner>,
    fields: Option<Vec<String>>,
    window: usize,
    report_interval: Duration,
}

fn hook_error(error: HookError) -> CliError {
    CliError {
        code: error.code(),
        message: error.message,
        exit: if error.kind == HookErrorKind::InvalidArgument {
            2
        } else {
            1
        },
    }
}

impl Options {
    fn parse(request: &Value) -> Result<Self> {
        let once = request["once"].as_bool().unwrap_or(false);
        let count = super::optional_u64(request, "count")?;
        if once && count.is_some_and(|count| count != 1) {
            return Err(CliError::input(
                "once=true conflicts with count other than 1",
            ));
        }
        let fields = request["fields"]
            .as_array()
            .map(|fields| {
                fields
                    .iter()
                    .map(|field| {
                        let field = field
                            .as_str()
                            .ok_or_else(|| CliError::input("fields must contain JSON Pointers"))?;
                        hooks::validate_pointer(field).map_err(hook_error)?;
                        Ok(field.to_owned())
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;
        Ok(Self {
            discovery: publication::seconds(request, "discovery_timeout", 30.0, false)?,
            duration: if request["follow"].as_bool() == Some(true)
                && request.get("timeout").is_none()
            {
                None
            } else {
                Some(publication::seconds(request, "timeout", 30.0, false)?)
            },
            idle: request
                .get("idle_timeout")
                .map(|_| publication::seconds(request, "idle_timeout", 30.0, false))
                .transpose()?,
            count: if once { Some(1) } else { count },
            filter: request
                .get("filter")
                .map(Condition::parse)
                .transpose()
                .map_err(hook_error)?,
            hook: request
                .get("hook")
                .map(HookRunner::from_json)
                .transpose()
                .map_err(hook_error)?,
            fields,
            window: super::optional_u64(request, "window")?.unwrap_or(1000) as usize,
            report_interval: publication::seconds(request, "report_interval", 1.0, false)?,
        })
    }
}

pub fn validate(request: &Value) -> Result<()> {
    Options::parse(request).map(|_| ())
}

fn project(message: &Value, fields: Option<&[String]>) -> (Value, u64) {
    let Some(fields) = fields else {
        return (message.clone(), 0);
    };
    let mut selected = Map::new();
    let mut missing = 0;
    for path in fields {
        if let Some(value) = message.pointer(path) {
            selected.insert(path.clone(), value.clone());
        } else {
            missing += 1;
        }
    }
    (Value::Object(selected), missing)
}

pub fn execute(
    ctx: &Context,
    verb: &str,
    request: &Value,
    configuration: &InvocationConfig,
) -> Result<Option<Value>> {
    let mut options = Options::parse(request)?;
    let interrupted = publication::interrupt()?;
    let invocation_started = Instant::now();
    let resolution = configuration.target(text(request, "topic_name"), "topic")?;
    let name = resolution.resolved_name.clone();
    let expected_type = request["message_type"].as_str().map(str::to_owned);
    let discovery = options.discovery;
    let discovery_result = publication::interruptible(ctx, &interrupted, move |context| {
        let deadline = Instant::now() + discovery;
        let found = named_entities(&context, "topic", &name, discovery)?;
        let (type_name, hash) = unique_type(&found)?;
        if expected_type.is_some_and(|expected| expected != type_name) {
            return Err(CliError::input(
                "message_type does not match discovered topic",
            ));
        }
        Ok(context.get_type_description(
            &full_name(&found[0]),
            &type_name,
            Some(&hash),
            deadline.saturating_duration_since(Instant::now()),
        )?)
    });
    let interface = match discovery_result {
        Ok(interface) => interface,
        Err(error) => {
            event(
                "summary",
                json!({
                    "operation":format!("topic {verb}"),"topic_name":resolution.resolved_name,
                    "received":0,"filtered":0,"phase":"discovery",
                    "stop_reason":if error.exit == 130 {"interrupted"} else {"error"},
                    "elapsed_seconds":invocation_started.elapsed().as_secs_f64(),
                    "provider_stopped":false,
                }),
            )?;
            return Err(error);
        }
    };
    let node = configuration.create_node(ctx)?;
    let qos = resolve_qos(&request["qos"], "default", false)?;
    let subscriber = node.create_subscription(
        &interface,
        &resolution.resolved_name,
        qos.clone(),
        EndpointOptions::default(),
    )?;
    if options.hook.is_some() && subscriber.queue_stats().is_none() {
        return Err(CliError {
            code: "UNSUPPORTED",
            message: "hook observation requires queue overflow accounting".into(),
            exit: 2,
        });
    }
    let started = Instant::now();
    let deadline = options.duration.map(|duration| started + duration);
    let mut last_received = started;
    let mut next_report = started + options.report_interval;
    let mut received = 0_u64;
    let mut filtered = 0_u64;
    let mut filter_missing = 0_u64;
    let mut hook_missing = 0_u64;
    let mut projection_missing = 0_u64;
    let mut matched = None;
    let mut statistics = Statistics::new(options.window);
    let mut stop_reason = "error";
    let outcome = (|| {
        loop {
            super::flush_logs()?;
            if interrupted.load(Ordering::Acquire) {
                stop_reason = "interrupted";
                return Err(publication::interrupted_error());
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                stop_reason = "total_timeout";
                return if verb == "echo" {
                    Err(aos_core::Error::Timeout.into())
                } else {
                    Ok(())
                };
            }
            if options.hook.is_some()
                && subscriber
                    .queue_stats()
                    .is_some_and(|stats| stats.dropped_total > 0)
            {
                stop_reason = "hook_overflow";
                return Err(CliError {
                    code: "HOOK_OVERFLOW",
                    message: "observation queue evicted messages; hook matching is incomplete"
                        .into(),
                    exit: 3,
                });
            }
            if verb != "echo" && now >= next_report {
                event(
                    "statistics",
                    json!({
                        "operation":format!("topic {verb}"),"topic_name":resolution.resolved_name,
                        "received":received,"filtered":filtered,"statistics":statistics.report(),
                        "queue":subscriber.queue_stats(),"elapsed_seconds":started.elapsed().as_secs_f64(),
                    }),
                )?;
                next_report = Instant::now() + options.report_interval;
            }
            let mut wait = Duration::from_millis(20);
            if let Some(deadline) = deadline {
                wait = wait.min(deadline.saturating_duration_since(Instant::now()));
            }
            if let Some(idle) = options.idle {
                wait = wait.min((last_received + idle).saturating_duration_since(Instant::now()));
            }
            let sample = match subscriber.recv_sample_timeout(wait) {
                Ok(sample) => sample,
                Err(aos_core::Error::Timeout) => {
                    if options
                        .idle
                        .is_some_and(|idle| last_received.elapsed() >= idle)
                    {
                        stop_reason = "idle_timeout";
                        return Err(aos_core::Error::Timeout.into());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            // A dequeued message does not get a hook call after the total limit.
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                stop_reason = "total_timeout";
                return if verb == "echo" {
                    Err(aos_core::Error::Timeout.into())
                } else {
                    Ok(())
                };
            }
            received += 1;
            last_received = Instant::now();
            if let Some(filter) = &options.filter {
                let decision = filter.evaluate(&sample.payload);
                filter_missing += decision.missing_fields;
                if !decision.matched {
                    continue;
                }
            }
            filtered += 1;
            statistics.record(&sample);
            let receive_time = sample.received.map(|received| json!({
                "time_domain":"system","time_ns":system_time_ns(received.system),
                "scope":received.scope,
                "steady_since_subscription_ns":received.steady.checked_duration_since(started).map(|elapsed| elapsed.as_nanos().to_string()),
            })).unwrap_or_else(|| json!({
                "time_domain":null,"time_ns":null,"status":"unavailable",
                "reason":"backend did not expose delivery time",
            }));
            let envelope = json!({
                "message":sample.payload,"topic":resolution.resolved_name,
                "sequence":received,"received_at":receive_time,
            });
            let mut hook_match = None;
            if let Some(hook) = &mut options.hook {
                let result = hook.evaluate(&envelope, &interrupted, deadline);
                match result {
                    Ok(result) => {
                        hook_missing += result.missing_fields;
                        if subscriber
                            .queue_stats()
                            .is_some_and(|stats| stats.dropped_total > 0)
                        {
                            stop_reason = "hook_overflow";
                            return Err(CliError {
                                code: "HOOK_OVERFLOW",
                                message: "observation queue overflowed while evaluating the hook"
                                    .into(),
                                exit: 3,
                            });
                        }
                        if result.decision == Decision::Matched {
                            hook_match = Some(json!({
                                "reason":result.reason,"consecutive_matches":result.consecutive_matches,
                                "stderr":result.stderr,
                            }));
                        }
                    }
                    Err(error) if error.kind == HookErrorKind::Interrupted => {
                        stop_reason = "interrupted";
                        return Err(publication::interrupted_error());
                    }
                    Err(error) if error.kind == HookErrorKind::OverallDeadline => {
                        stop_reason = "total_timeout";
                        return if verb == "echo" {
                            Err(aos_core::Error::Timeout.into())
                        } else {
                            Ok(())
                        };
                    }
                    Err(error) => {
                        stop_reason = "hook_error";
                        event(
                            "hook_error",
                            json!({"code":error.code(),"message":error.message,"stderr":error.stderr}),
                        )?;
                        return Err(hook_error(error));
                    }
                }
            }
            let (projection, missing) = project(&sample.payload, options.fields.as_deref());
            projection_missing += missing;
            if verb == "echo" {
                event(
                    "message",
                    json!({
                        "topic_name":resolution.resolved_name,"name_resolution":resolution,
                        "qos":qos.effective_value(),"message":projection,
                        "received":received,"filtered":filtered,"sequence":received,
                        "received_at":receive_time,"source":sample.source,"payload_size_bytes":sample.payload_size_bytes,
                    }),
                )?;
            }
            // Hook takes precedence over count for the same completed message.
            if let Some(hook) = hook_match {
                stop_reason = "hook_matched";
                matched = Some(
                    json!({"message":sample.payload,"projection":projection,"hook":hook,"sequence":received}),
                );
                return Ok(());
            }
            if options.count.is_some_and(|count| filtered >= count) {
                stop_reason = if request["once"].as_bool() == Some(true) {
                    "once"
                } else {
                    "count"
                };
                return Ok(());
            }
        }
    })();
    let close_result = subscriber.close().map_err(CliError::from);
    event(
        "summary",
        json!({
            "operation":format!("topic {verb}"),"topic_name":resolution.resolved_name,
            "name_resolution":resolution,"stop_reason":stop_reason,"received":received,"filtered":filtered,
            "filter_missing_fields":filter_missing,"hook_missing_fields":hook_missing,
            "projection_missing_fields":projection_missing,"matched":matched,
            "elapsed_seconds":started.elapsed().as_secs_f64(),
            "invocation_seconds":invocation_started.elapsed().as_secs_f64(),
            "timeout_includes":"observation and hook evaluation; discovery has its own budget",
            "idle_basis":"each valid dequeued message, including filtered-out messages; queued messages are checked before idle termination",
            "statistics":statistics.report(),"queue":subscriber.queue_stats(),
            "provider_stopped":false,
        }),
    )?;
    outcome?;
    close_result?;
    Ok(None)
}
