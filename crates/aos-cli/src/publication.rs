//! Explicit CLI publication scheduling; Publisher::publish remains one message.
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::time::{Duration, Instant};

use aos_core::{resolve_qos, Context, EndpointOptions, EntityKind, Interface, TypedPublisher};
use serde_json::{json, Value};

use super::{configuration::InvocationConfig, event, resolve_interface, text, CliError, Result};

pub fn seconds(request: &Value, field: &str, default: f64, allow_zero: bool) -> Result<Duration> {
    let value = request
        .get(field)
        .and_then(Value::as_f64)
        .unwrap_or(default);
    let duration = Duration::try_from_secs_f64(value)
        .map_err(|_| CliError::input(format!("{field} exceeds the supported duration range")))?;
    if (!allow_zero && duration.is_zero()) || Instant::now().checked_add(duration).is_none() {
        return Err(CliError::input(format!(
            "{field} exceeds the steady clock range"
        )));
    }
    Ok(duration)
}

pub fn interrupt() -> Result<Arc<AtomicBool>> {
    let interrupted = Arc::new(AtomicBool::new(false));
    let flag = interrupted.clone();
    ctrlc::set_handler(move || flag.store(true, Ordering::Release)).map_err(|error| CliError {
        code: "PROCESS_SETUP_ERROR",
        message: format!("cannot install signal handler: {error}"),
        exit: 1,
    })?;
    Ok(interrupted)
}

pub fn sleep_until(deadline: Instant, interrupted: &AtomicBool) -> bool {
    loop {
        if interrupted.load(Ordering::Acquire) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(20)));
    }
}

pub fn interrupted_error() -> CliError {
    CliError {
        code: "INTERRUPTED",
        message: "operation interrupted".into(),
        exit: 130,
    }
}

/// Discovery/introspection can block in the backend. The worker owns only this
/// CLI Context; closing it cancels its waits without stopping a remote provider.
pub fn interruptible<T: Send + 'static>(
    context: &Context,
    interrupted: &AtomicBool,
    operation: impl FnOnce(Context) -> Result<T> + Send + 'static,
) -> Result<T> {
    let owned = context.clone();
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = sender.send(operation(owned));
    });
    let result = loop {
        if let Err(error) = super::flush_logs() {
            let _ = context.shutdown();
            break Err(error);
        }
        if interrupted.load(Ordering::Acquire) {
            let _ = context.shutdown();
            break Err(interrupted_error());
        }
        match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(result) => break result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break Err(CliError {
                    code: "INTERNAL_ERROR",
                    message: "discovery worker disconnected".into(),
                    exit: 1,
                });
            }
        }
    };
    let _ = worker.join();
    result
}

struct Options {
    times: Option<u64>,
    period: Duration,
    discovery: Duration,
    matching: Duration,
    subscribers: u64,
    linger: Duration,
}

impl Options {
    fn parse(request: &Value) -> Result<Self> {
        let once = request.get("once");
        let times = request.get("times");
        let follow = request["follow"].as_bool().unwrap_or(false);
        let modes = usize::from(once == Some(&Value::Bool(true)))
            + usize::from(times.is_some())
            + usize::from(follow);
        if modes > 1 || once == Some(&Value::Bool(false)) {
            return Err(CliError::input(
                "choose once=true, times, or follow=true; once=false is not a publication mode",
            ));
        }
        if request.get("timeout").is_some()
            && (request.get("discovery_timeout").is_some()
                || request.get("match_timeout").is_some())
        {
            return Err(CliError::input(
                "timeout conflicts with discovery_timeout or match_timeout",
            ));
        }
        let alias = request["timeout"].as_f64().unwrap_or(30.0);
        let rate = request["rate"].as_f64().unwrap_or(1.0);
        let period = Duration::try_from_secs_f64(1.0 / rate)
            .ok()
            .filter(|value| !value.is_zero() && Instant::now().checked_add(*value).is_some())
            .ok_or_else(|| CliError::input("rate is outside the supported steady clock range"))?;
        Ok(Self {
            times: if follow {
                None
            } else {
                Some(super::optional_u64(request, "times")?.unwrap_or(1))
            },
            period,
            discovery: seconds(request, "discovery_timeout", alias, false)?,
            matching: seconds(request, "match_timeout", alias, false)?,
            subscribers: super::optional_u64(request, "wait_for_subscribers")?.unwrap_or(0),
            linger: seconds(request, "linger", 0.0, true)?,
        })
    }
}

pub fn validate(request: &Value) -> Result<()> {
    Options::parse(request).map(|_| ())
}

fn wait_for_subscribers(
    ctx: &Context,
    publisher: &TypedPublisher,
    interface: &Interface,
    name: &str,
    options: &Options,
    interrupted: &AtomicBool,
) -> Result<usize> {
    if options.subscribers == 0 {
        return Ok(0);
    }
    let deadline = Instant::now() + options.matching;
    let watch = ctx.watch_graph()?;
    loop {
        super::flush_logs()?;
        if interrupted.load(Ordering::Acquire) {
            return Err(interrupted_error());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(aos_core::Error::Timeout.into());
        }
        let graph = match &watch {
            Some(watch) => watch.snapshot()?,
            None => ctx.graph(remaining)?,
        };
        let found: BTreeSet<_> = graph
            .into_iter()
            .filter(|entity| {
                entity.kind == EntityKind::Subscription
                    && entity.endpoint.as_ref().is_some_and(|endpoint| {
                        endpoint.name == name
                            && endpoint.type_name == interface.type_name()
                            && endpoint.type_hash == interface.type_hash()
                    })
            })
            .map(|entity| entity.id)
            .collect();
        if found.len() as u64 >= options.subscribers {
            // Graph supplies the count; Zenoh's boolean matching confirms at
            // least one data route. Neither proves consumer acknowledgement.
            match publisher.wait_for_subscribers(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            ) {
                Ok(()) => return Ok(found.len()),
                Err(aos_core::Error::Timeout) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let poll = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(20));
        // This bounds only local interrupt checks. The history subscriber stays
        // alive across every poll, including RTTs longer than this interval.
        match &watch {
            Some(watch) => match watch.wait_for_change(poll) {
                Ok(()) | Err(aos_core::Error::Timeout) => {}
                Err(error) => return Err(error.into()),
            },
            None => {
                sleep_until(Instant::now() + poll, interrupted);
            }
        }
    }
}

pub fn execute(
    ctx: &Context,
    request: &Value,
    configuration: &InvocationConfig,
) -> Result<Option<Value>> {
    let options = Options::parse(request)?;
    let interrupted = interrupt()?;
    let began = Instant::now();
    let mut submitted = 0_u64;
    let mut observed_subscribers = None;
    let mut phase = "type_discovery";
    let outcome = (|| {
        let type_name = text(request, "message_type").to_owned();
        let discovery = options.discovery;
        let interface = interruptible(ctx, &interrupted, move |context| {
            resolve_interface(&context, &type_name, discovery)
        })?;
        interface.validate("message", &request["values"])?;
        let node = configuration.create_node(ctx)?;
        let qos = resolve_qos(&request["qos"], "default", false)?;
        let resolution = configuration.target(text(request, "topic_name"), "topic")?;
        let publisher = node.create_publisher(
            &interface,
            &resolution.resolved_name,
            qos.clone(),
            EndpointOptions::default(),
        )?;
        phase = "subscriber_matching";
        if options.subscribers > 0 {
            observed_subscribers = Some(wait_for_subscribers(
                ctx,
                &publisher,
                &interface,
                &resolution.resolved_name,
                &options,
                &interrupted,
            )?);
        }
        phase = "publication";
        let mut scheduled = Instant::now();
        loop {
            if !sleep_until(scheduled, &interrupted) {
                return Err(interrupted_error());
            }
            publisher.publish(&request["values"])?;
            submitted += 1;
            event(
                "published",
                json!({
                    "sequence":submitted,"topic_name":resolution.resolved_name,
                    "name_resolution":resolution,"qos":qos.effective_value(),"submitted":true
                }),
            )?;
            if options.times.is_some_and(|limit| submitted >= limit) {
                break;
            }
            scheduled = scheduled.checked_add(options.period).ok_or_else(|| {
                CliError::input("publication schedule exceeds the steady clock range")
            })?;
            if scheduled < Instant::now() {
                // Do not burst to catch up after a blocked publication/output.
                scheduled = Instant::now() + options.period;
            }
        }
        phase = "linger";
        if !options.linger.is_zero() && !sleep_until(Instant::now() + options.linger, &interrupted)
        {
            return Err(interrupted_error());
        }
        Ok(())
    })();
    let stop_reason = match &outcome {
        Ok(()) if options.times == Some(1) && request.get("times").is_none() => "once",
        Ok(()) => "count",
        Err(error) if error.exit == 130 => "interrupted",
        Err(error) if error.code == "TIMEOUT" && phase == "subscriber_matching" => "match_timeout",
        Err(_) => "error",
    };
    event(
        "summary",
        json!({
            "operation":"topic pub","submitted_count":submitted,"stop_reason":stop_reason,
            "phase":phase,"elapsed_seconds":began.elapsed().as_secs_f64(),
            "requested_subscribers":options.subscribers,"observed_subscribers":observed_subscribers,
            "matching_basis":"Graph subscription entity IDs with exact resolved name, type and hash, plus Zenoh boolean data-route matching. QoS compatibility is diagnostic only.",
            "acknowledgement":"local submission only"
        }),
    )?;
    outcome?;
    Ok(None)
}
