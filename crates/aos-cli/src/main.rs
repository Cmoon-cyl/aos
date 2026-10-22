mod actions;
mod bag_commands;
mod bag_schema;
mod configuration;
mod doctor;
mod hooks;
#[cfg(any(unix, windows, test))]
mod launch_probe;
mod node_management;
mod observation;
mod process_commands;
mod publication;
mod schema;
mod security_commands;
mod service_calls;
mod statistics;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use aos_core::transport::{EntityKind, GraphEntity};
use aos_core::{
    qos_compatibility, resolve_qos, ActionQos, Context, EndpointOptions, Interface,
    TelemetryComponent, TelemetryEvent, TelemetryOutcome,
};
use aos_zenoh::ZenohTransport;
use configuration::InvocationConfig;
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, CliError>;

/// JSON Schema treats `3.0` as an integer, but serde preserves its float
/// representation. Supplied counts must never silently become defaults.
fn optional_u64(request: &Value, field: &str) -> Result<Option<u64>> {
    request
        .get(field)
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                CliError::input(format!(
                    "{field} must use an unsigned JSON integer representation (for example 3, not 3.0)"
                ))
            })
        })
        .transpose()
}

#[cfg(unix)]
fn prepare_child_reaping() -> Result<()> {
    // The CLI owns its child processes. An inherited SIG_IGN would auto-reap
    // their leaders and invalidate waitid(WNOWAIT)'s PID/PGID ownership guard.
    // This runs before opening the Context or creating any worker/child.
    let result = unsafe { libc::signal(libc::SIGCHLD, libc::SIG_DFL) };
    if result == libc::SIG_ERR {
        return Err(CliError {
            code: "PROCESS_SETUP_ERROR",
            message: format!("cannot reset SIGCHLD: {}", io::Error::last_os_error()),
            exit: 1,
        });
    }
    Ok(())
}
// A single CLI invocation selects its output shape after parsing the request.
// Action/Topic streaming can also be identified before input has been parsed.
static REPEATED_SERVICE_OUTPUT: AtomicBool = AtomicBool::new(false);
static OPERATION_CONTEXT: OnceLock<Mutex<Value>> = OnceLock::new();
static LOG_CONTEXT: OnceLock<Context> = OnceLock::new();

fn flush_logs() -> Result<()> {
    if let Some(context) = LOG_CONTEXT.get() {
        context.flush_logs_to_stderr().map_err(|_| CliError {
            code: "LOG_OUTPUT_ERROR",
            message: "Cannot write structured diagnostics to stderr".into(),
            exit: 1,
        })?;
    }
    Ok(())
}

fn operation_event(ctx: &Context, name: &str, outcome: TelemetryOutcome, elapsed: Duration) {
    let mut event = TelemetryEvent::new(TelemetryComponent::Context, name);
    event.outcome = outcome;
    event.duration_ns = Some(elapsed.as_nanos().min(u64::MAX as u128) as u64);
    if let Some(operation) = OPERATION_CONTEXT.get() {
        let operation = operation.lock().unwrap_or_else(|error| error.into_inner());
        event.endpoint = operation["endpoint_name"].as_str().map(str::to_owned);
        event.goal_id = operation["goal_id"].as_str().map(str::to_owned);
    }
    ctx.telemetry().record(event);
}

fn finish_logging() -> Result<()> {
    flush_logs()?;
    if let Some(context) = LOG_CONTEXT.get() {
        let mut stderr = io::stderr().lock();
        serde_json::to_writer(
            &mut stderr,
            &json!({"schema_version":1,"event":"telemetry_summary",
                    "scope":"this CLI process and Context","telemetry":context.telemetry_snapshot()}),
        )
        .and_then(|_| stderr.write_all(b"\n").map_err(serde_json::Error::io))
        .map_err(|_| CliError {
            code: "LOG_OUTPUT_ERROR",
            message: "Cannot write diagnostic summary to stderr".into(),
            exit: 1,
        })?;
    }
    Ok(())
}

fn operation_field(key: &str, value: Value) {
    if let Some(context) = OPERATION_CONTEXT.get() {
        context.lock().unwrap_or_else(|error| error.into_inner())[key] = value;
    }
}

fn phase(name: &str) {
    operation_field("phase", json!(name));
}

#[derive(Debug)]
struct CliError {
    code: &'static str,
    message: String,
    exit: i32,
}

impl CliError {
    fn input(message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_ARGUMENT",
            message: message.into(),
            exit: 2,
        }
    }
    fn discovery(message: impl Into<String>) -> Self {
        Self {
            code: "NOT_FOUND",
            message: message.into(),
            exit: 3,
        }
    }
    fn ambiguous(message: impl Into<String>) -> Self {
        Self {
            code: "AMBIGUOUS",
            message: message.into(),
            exit: 3,
        }
    }
}

impl From<aos_core::Error> for CliError {
    fn from(error: aos_core::Error) -> Self {
        use aos_core::Error::*;
        let (code, exit) = match &error {
            Timeout => ("TIMEOUT", 4),
            InvalidArgument(_) | Validation(_) => ("INVALID_ARGUMENT", 2),
            ResourceLimit(_) => ("RESOURCE_LIMIT", 2),
            Closed | Transport(_) => ("COMMUNICATION_ERROR", 3),
            QueueOverflow(_) => ("QUEUE_OVERFLOW", 3),
            NoResponse(_) => ("NO_RESPONSE", 3),
            Unsupported(_) => ("UNSUPPORTED", 2),
            Remote(_) => ("REMOTE_ERROR", 1),
            Inactive(_) => ("INACTIVE", 1),
        };
        Self {
            code,
            message: error.to_string(),
            exit,
        }
    }
}

fn print_json(value: &Value) -> Result<()> {
    flush_logs()?;
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)
        .and_then(|_| output.write_all(b"\n").map_err(serde_json::Error::io))
        .map_err(|error| CliError {
            code: "OUTPUT_ERROR",
            message: error.to_string(),
            exit: 1,
        })?;
    output.flush().map_err(|error| CliError {
        code: "OUTPUT_ERROR",
        message: error.to_string(),
        exit: 1,
    })
}

fn event(name: &str, data: Value) -> Result<()> {
    print_json(&json!({"event":name,"data":data}))
}

fn response(data: &Value) -> Result<()> {
    print_json(&json!({"ok":true,"data":data}))
}

fn main() {
    #[cfg(any(unix, windows))]
    {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if args.len() > 1
            && args.first().is_some_and(|group| {
                (group == "security" && args.get(1).is_some_and(|verb| verb == "serve"))
                    || (cfg!(any(unix, windows))
                        && (matches!(group.as_str(), "run" | "launch")
                            || (group == "bag"
                                && args.get(1).is_some_and(|verb| {
                                    matches!(verb.as_str(), "record" | "play")
                                }))))
            })
            && !args.iter().any(|arg| arg == "--help")
        {
            if let Err(error) = process_commands::begin_output() {
                std::process::exit(error.exit);
            }
            // security serve installs into this owner after request/profile
            // validation. Keep its handlers through the terminal output drain.
            let mut security_signals = security_commands::SignalGuard::default();
            let result = run(&mut security_signals);
            let context = OPERATION_CONTEXT
                .get()
                .map(|context| {
                    context
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .clone()
                })
                .unwrap_or_else(|| json!({}));
            let output = process_commands::finish_output(result.as_ref().err(), context);
            let exit = result
                .err()
                .map(|error| error.exit)
                .unwrap_or_else(|| output.err().map(|error| error.exit).unwrap_or(0));
            // A permanently blocked private writer is retired by this process
            // exit, after the runtime has finished owned-child cleanup.
            std::process::exit(exit);
        }
    }
    let mut security_signals = security_commands::SignalGuard::default();
    if let Err(error) = run(&mut security_signals) {
        let args: Vec<_> = std::env::args().skip(1).collect();
        let streaming = (REPEATED_SERVICE_OUTPUT.load(Ordering::Relaxed)
            || (args.first().is_some_and(|g| g == "topic")
                && args.get(1).is_some_and(|v| {
                    matches!(v.as_str(), "pub" | "echo" | "hz" | "bw" | "delay")
                }))
            || (args.first().is_some_and(|g| g == "action")
                && args.get(1).is_some_and(|v| v == "send_goal"))
            || args
                .first()
                .is_some_and(|g| matches!(g.as_str(), "run" | "launch"))
            || (args.first().is_some_and(|g| g == "bag")
                && args
                    .get(1)
                    .is_some_and(|v| matches!(v.as_str(), "record" | "play"))))
            && !args.iter().any(|a| a == "--help");
        let mut data = OPERATION_CONTEXT
            .get()
            .map(|context| {
                context
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone()
            })
            .unwrap_or_else(|| json!({}));
        data["code"] = json!(error.code);
        data["message"] = json!(error.message);
        if error.exit == 130 && streaming {
            std::process::exit(130);
        }
        let _ = if streaming {
            event("error", data)
        } else {
            print_json(&json!({"ok":false,"error":data}))
        };
        std::process::exit(error.exit);
    }
}

fn run(security_signals: &mut security_commands::SignalGuard) -> Result<()> {
    #[cfg(unix)]
    prepare_child_reaping()?;
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args == ["--help"] || args == ["help"] {
        return response(&schema::help(None, None).unwrap());
    }
    if args[0] == "schema" {
        if args.len() != 3 {
            return Err(CliError::input("usage: aos schema <group> <verb>"));
        }
        return response(
            &schema::describe(&args[1], &args[2])
                .ok_or_else(|| CliError::input("unknown command; see aos --help"))?,
        );
    }
    if args[0] == "help" {
        args.remove(0);
        args.push("--help".into());
    }
    if args.iter().any(|a| a == "--help") || (args.len() == 1 && args[0] != "doctor") {
        args.retain(|a| a != "--help");
        if args.len() > 2 {
            return Err(CliError::input("usage: aos <group> [<verb>] --help"));
        }
        return response(
            &schema::help(
                args.first().map(String::as_str),
                args.get(1).map(String::as_str),
            )
            .ok_or_else(|| CliError::input("unknown command; see aos --help"))?,
        );
    }
    if args[0] == "doctor" && (args.len() == 1 || args.get(1).is_some_and(|a| a == "--request")) {
        args.insert(1, "report".into());
    }
    if matches!(args[0].as_str(), "run" | "launch") && args.get(1).is_some_and(|a| a == "--request")
    {
        args.insert(1, "start".into());
    }
    if args.len() < 2 || !schema::known(&args[0], &args[1]) {
        return Err(CliError::input("unknown command; see aos --help"));
    }
    let _ = OPERATION_CONTEXT.set(Mutex::new(json!({
        "command":format!("aos {} {}",args[0],args[1]),
        "phase":"request","operation_id":uuid::Uuid::new_v4().simple().to_string(),
        "operation_id_scope":"this CLI invocation; wire request IDs are separate",
    })));
    let request = match &args[2..] {
        [] => json!({}),
        [flag, path] if flag == "--request" => read_request(path)?,
        _ => return Err(CliError::input("business inputs use --request PATH|- only")),
    };
    REPEATED_SERVICE_OUTPUT.store(
        args[0] == "service" && args[1] == "call" && request.get("times").is_some(),
        Ordering::Relaxed,
    );
    let request_schema = schema::request_schema(&args[0], &args[1]).unwrap();
    let validator =
        jsonschema::validator_for(&request_schema).map_err(|e| CliError::input(e.to_string()))?;
    if let Some(error) = validator.iter_errors(&request).next() {
        return Err(CliError::input(if args[0] == "security" {
            // Even masked schema errors may contain caller-written property
            // names. No malformed credential input is echoed here.
            format!(
                "invalid security request; see aos schema security {}",
                args[1]
            )
        } else {
            error.to_string()
        }));
    }
    if args[0] == "security" {
        if args[1] == "serve" {
            return security_commands::serve(&request, security_signals);
        }
        return response(&security_commands::execute(&args[1], &request)?);
    }
    if args[0] == "bag" {
        if request["control_input"] == "stdin" && args.get(3).is_some_and(|path| path == "-") {
            return Err(CliError::input(
                "stdin controls require a request file, not --request -",
            ));
        }
        if let Some(data) = bag_commands::execute(&args[1], &request)? {
            response(&data)?;
        }
        return Ok(());
    }
    if matches!(args[0].as_str(), "package" | "run" | "launch") {
        if let Some(data) = process_commands::execute(&args[0], &args[1], &request)? {
            response(&data)?;
        }
        return Ok(());
    }
    let log_capacity = optional_u64(&request["logging"], "capacity")?.unwrap_or(1024) as usize;
    let offline = args[0] == "config" && args[1] == "resolve";
    let uses_endpoint = ["topic_name", "service_name", "action_name"]
        .iter()
        .any(|key| request.get(key).is_some());
    phase("configuration");
    let configuration = InvocationConfig::parse(&request, offline, uses_endpoint)?;
    configuration.validate_client_identity(&args[0], &args[1])?;
    for (field, kind) in [
        ("topic_name", "topic"),
        ("service_name", "service"),
        ("action_name", "action"),
    ] {
        if let Some(name) = request[field].as_str() {
            operation_field(
                "endpoint_name",
                json!(configuration.target(name, kind)?.resolved_name),
            );
        }
    }
    for field in ["goal_id", "node_name"] {
        if let Some(value) = request.get(field) {
            operation_field(field, value.clone());
        }
    }
    if offline {
        return response(&configuration.report(&request)?);
    }
    if request.get("timeout").is_some() {
        publication::seconds(&request, "timeout", 30.0, true)?;
    }
    // Reject unsupported policies before opening a session or discovering peers.
    if args[0] == "action" {
        ActionQos::from_json(&request["qos"])?;
    } else if args[0] == "service" {
        resolve_qos(&request["qos"], "services_default", true)?;
        if args[1] == "call" {
            service_calls::validate(&request)?;
        }
    } else if args[0] == "topic" {
        resolve_qos(&request["qos"], "default", false)?;
        if args[1] == "pub" {
            publication::validate(&request)?;
        } else if matches!(args[1].as_str(), "echo" | "hz" | "bw" | "delay") {
            observation::validate(&request)?;
        }
    }
    if args[0] == "doctor" {
        doctor::validate(&request)?;
    }
    let management = if matches!(args[0].as_str(), "param" | "lifecycle") {
        Some(node_management::prepare(&request, &args[0], &args[1])?)
    } else {
        None
    };
    phase("session_open");
    let (ctx, router_ids) = open_context(&configuration)?;
    operation_field("telemetry_context_id", json!(ctx.telemetry().context_id()));
    if request["logging"]["stderr"] == true {
        ctx.enable_log_capture(log_capacity)?;
        let _ = LOG_CONTEXT.set(ctx.clone());
    }
    let operation_started = Instant::now();
    operation_event(
        &ctx,
        "cli.operation.started",
        TelemetryOutcome::Ok,
        Duration::ZERO,
    );
    phase("execute");
    let result = if let Some(prepared) = management {
        // These operations own one shared deadline and must not enter the
        // ordinary `list` Graph pre-query before that deadline starts.
        node_management::execute_prepared(&ctx, prepared, &configuration)
    } else {
        execute(
            &ctx,
            router_ids.as_deref(),
            &args[0],
            &args[1],
            &request,
            &configuration,
        )
    };
    if result.is_ok() {
        phase("shutdown");
    }
    let closed = ctx.shutdown().map_err(CliError::from);
    let outcome = match result.as_ref().err().or(closed.as_ref().err()) {
        None => TelemetryOutcome::Ok,
        Some(error) => match error.code {
            "TIMEOUT" | "QUERY_TIMEOUT" => TelemetryOutcome::Timeout,
            "QUEUE_OVERFLOW" | "HOOK_OVERFLOW" => TelemetryOutcome::QueueOverflow,
            "NO_RESPONSE" => TelemetryOutcome::NoResponse,
            "INVALID_ARGUMENT" => TelemetryOutcome::InvalidArgument,
            "RESOURCE_LIMIT" => TelemetryOutcome::ResourceLimit,
            "UNSUPPORTED" => TelemetryOutcome::Unsupported,
            "INTERRUPTED" => TelemetryOutcome::Closed,
            "REMOTE_ERROR" => TelemetryOutcome::Remote,
            "INACTIVE" => TelemetryOutcome::Inactive,
            _ => TelemetryOutcome::Transport,
        },
    };
    operation_event(
        &ctx,
        "cli.operation.finished",
        outcome,
        operation_started.elapsed(),
    );
    let logged = finish_logging();
    // Preserve the operation's original error if cleanup also failed.
    let data = result?;
    closed?;
    logged?;
    if let Some(data) = data {
        print_json(&json!({"ok":true,"data":data}))?;
    }
    Ok(())
}

fn read_request(path: &str) -> Result<Value> {
    let mut text = String::new();
    if path == "-" {
        io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| CliError::input(e.to_string()))?;
    } else {
        text =
            std::fs::read_to_string(path).map_err(|e| CliError::input(format!("{path}: {e}")))?;
    }
    Ok(aos_core::parse_config_json(&text)?)
}

fn open_context(configuration: &InvocationConfig) -> Result<(Context, Option<Vec<String>>)> {
    if let Some(profile) = &configuration.security {
        return Ok((
            aos_zenoh::security::open_resolved_secure_context(
                configuration.context.clone(),
                configuration.bootstrap.clone(),
                profile,
            )?,
            None,
        ));
    }
    let config = match &configuration.context.zenoh_config {
        Some(path) => zenoh::Config::from_file(PathBuf::from(path)),
        None => zenoh::Config::from_json5(include_str!("../../../config/zenoh-session.json5")),
    }
    .map_err(|e| CliError::input(format!("session configuration: {e}")))?;
    let transport = ZenohTransport::open(config, configuration.context.domain_id)?;
    let router_ids = transport.router_zids();
    Ok((
        Context::from_configured_transport(
            transport,
            configuration.context.clone(),
            configuration.bootstrap.clone(),
        )?,
        Some(router_ids),
    ))
}

fn timeout(request: &Value) -> Duration {
    Duration::from_secs_f64(
        request
            .get("timeout")
            .and_then(Value::as_f64)
            .unwrap_or(30.0),
    )
}

fn text<'a>(request: &'a Value, name: &str) -> &'a str {
    request.get(name).and_then(Value::as_str).unwrap_or("")
}

fn full_name(entity: &GraphEntity) -> String {
    format!(
        "{}/{}",
        entity.namespace.trim_end_matches('/'),
        entity.node_name
    )
}

fn hidden(name: &str) -> bool {
    name.split('/').any(|segment| segment.starts_with('_'))
}

fn discovery_snapshot(ctx: &Context, wait: Duration) -> Result<Vec<GraphEntity>> {
    let started = Instant::now();
    let deadline = started + wait;
    let window = Duration::from_secs_f64(schema::LIST_DISCOVERY_SECONDS).min(wait / 2);
    // ros2cli's DirectNode actively spins during its 0.5 s discovery window.
    // Zenoh's first Current-interest snapshot can finish before discovery has
    // settled. Start that bounded read now, count it toward the window, and
    // query again at the end. Only the final view is returned, so entities
    // removed during the window are not revived by merging earlier snapshots.
    let _ = ctx.graph(window)?;
    std::thread::sleep(window.saturating_sub(started.elapsed()));
    Ok(ctx.graph(query_remaining(deadline)?)?)
}

fn query_remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(CliError {
            code: "QUERY_TIMEOUT",
            message: "The shared Graph/description query budget expired".into(),
            exit: 4,
        })
    } else {
        Ok(remaining)
    }
}

fn entities(graph: Vec<GraphEntity>, group: &str, request: &Value) -> Vec<GraphEntity> {
    let include_hidden = request
        .get("include_hidden")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    graph
        .into_iter()
        .filter(|entity| {
            let relevant = match group {
                "topic" => matches!(
                    entity.kind,
                    EntityKind::Publisher | EntityKind::Subscription
                ),
                "service" => entity.kind == EntityKind::Service,
                "action" => matches!(
                    entity.kind,
                    EntityKind::ActionClient | EntityKind::ActionServer
                ),
                "node" => entity.kind == EntityKind::Node,
                _ => true,
            };
            relevant
                && (include_hidden || entity.endpoint.as_ref().is_none_or(|e| !hidden(&e.name)))
        })
        .collect()
}

fn named_entities(
    ctx: &Context,
    group: &str,
    name: &str,
    wait: Duration,
) -> Result<Vec<GraphEntity>> {
    let deadline = Instant::now() + wait;
    loop {
        let graph = ctx.graph(deadline.saturating_duration_since(Instant::now()))?;
        let found: Vec<_> = graph
            .into_iter()
            .filter(|e| {
                let kind = match group {
                    "topic" => matches!(e.kind, EntityKind::Publisher | EntityKind::Subscription),
                    "service" => e.kind == EntityKind::Service,
                    "service_all" => matches!(e.kind, EntityKind::Service | EntityKind::Client),
                    "action" => {
                        matches!(e.kind, EntityKind::ActionClient | EntityKind::ActionServer)
                    }
                    _ => e.kind == EntityKind::Node,
                };
                kind && if group == "node" {
                    full_name(e) == name
                } else {
                    e.endpoint
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.name == name)
                }
            })
            .collect();
        if !found.is_empty() {
            return Ok(found);
        }
        if Instant::now() >= deadline {
            return Err(CliError::discovery(format!(
                "{group} {name:?} was not discovered"
            )));
        }
        std::thread::sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn unique_type(entities: &[GraphEntity]) -> Result<(String, String)> {
    let types: BTreeSet<_> = entities
        .iter()
        .filter_map(|e| e.endpoint.as_ref())
        .map(|e| (e.type_name.clone(), e.type_hash.clone()))
        .collect();
    if types.len() != 1 {
        return Err(CliError::ambiguous(
            "name has multiple interface types or hashes",
        ));
    }
    Ok(types.into_iter().next().unwrap())
}

fn ensure_unique_provider(entities: &[GraphEntity]) -> Result<()> {
    let providers: BTreeSet<_> = entities.iter().map(|e| &e.node_id).collect();
    if providers.len() != 1 {
        return Err(CliError::ambiguous(
            "multiple providers use this name; use distinct namespaces",
        ));
    }
    Ok(())
}

fn local_interfaces() -> Result<Vec<Interface>> {
    fn visit(path: &Path, result: &mut Vec<Interface>) -> Result<()> {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).map_err(|e| CliError::input(e.to_string()))? {
                visit(
                    &entry.map_err(|e| CliError::input(e.to_string()))?.path(),
                    result,
                )?;
            }
        } else if path.extension().is_some_and(|ext| ext == "json") {
            result.push(Interface::load(path)?);
        }
        Ok(())
    }
    let mut result = Vec::new();
    if let Some(paths) = std::env::var_os("AOS_INTERFACE_PATH") {
        for path in std::env::split_paths(&paths) {
            visit(&path, &mut result)?;
        }
    }
    Ok(result)
}

fn resolve_interface(ctx: &Context, name: &str, wait: Duration) -> Result<Interface> {
    let deadline = Instant::now() + wait;
    let mut definitions: BTreeMap<String, Interface> = local_interfaces()?
        .into_iter()
        .filter(|i| i.type_name() == name)
        .map(|i| (i.type_hash().to_owned(), i))
        .collect();
    loop {
        for entity in ctx.graph(deadline.saturating_duration_since(Instant::now()))? {
            if let Some(endpoint) = &entity.endpoint {
                if endpoint.type_name == name && !definitions.contains_key(&endpoint.type_hash) {
                    let interface = ctx.get_type_description(
                        &full_name(&entity),
                        name,
                        Some(&endpoint.type_hash),
                        deadline.saturating_duration_since(Instant::now()),
                    )?;
                    definitions.insert(interface.type_hash().to_owned(), interface);
                }
            }
        }
        if !definitions.is_empty() || Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(
            Duration::from_millis(20).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
    if definitions.len() > 1 {
        return Err(CliError::ambiguous(format!(
            "type {name:?} has multiple hashes"
        )));
    }
    definitions.into_values().next().ok_or_else(|| {
        CliError::discovery(format!("interface {name:?} is not installed or discovered"))
    })
}

fn execute(
    ctx: &Context,
    router_ids: Option<&[String]>,
    group: &str,
    verb: &str,
    request: &Value,
    configuration: &InvocationConfig,
) -> Result<Option<Value>> {
    let wait = timeout(request);
    let query_started = Instant::now();
    let query_deadline = query_started + wait;
    // Begin discovery immediately on entry after opening the Context. This
    // finite CLI window is not a guarantee of global network readiness.
    let graph = if verb == "list" || (group == "doctor" && verb == "report") {
        discovery_snapshot(ctx, wait)?
    } else {
        Vec::new()
    };
    match (group, verb) {
        ("doctor", "report") => {
            let graph_elapsed = query_started.elapsed();
            let python = doctor::selected_python(request, configuration, query_deadline)?;
            Ok(Some(json!({
                "version":env!("CARGO_PKG_VERSION"),"core":"native Rust","router_ids":router_ids,
                "graph_entities":graph.len(),
                "status":if matches!(python["status"].as_str(),Some("error"|"timeout")) {"error"} else {"ok"},
                "python_native_module":python,
                "telemetry":{"scope":"this CLI process and Context; not remote aggregation",
                    "snapshot":ctx.telemetry_snapshot()},
                "config_source":configuration.context.zenoh_config.as_deref().unwrap_or(
                    if configuration.security.is_some() {"explicit secure client profile"} else {"built-in loopback peer"}),
                "router_ids_scope":if router_ids.is_some() {"observed by the local transport"} else {"not exposed by the secure Context opener"},
                "effective_config":configuration.context.effective_value(),
                "query":{"timeout_seconds":wait.as_secs_f64(),"elapsed_seconds":query_started.elapsed().as_secs_f64(),
                    "graph_elapsed_seconds":graph_elapsed.as_secs_f64(),
                    "scope":"shared Graph and selected-Python budget after session open"},
                "security":if configuration.security.is_some() {
                    json!({"profile":"mtls","authentication":"not independently observed; a peer session may open before its Router connects",
                        "encryption":"TLS-only validated configuration","authorization":"unknown",
                        "reason":"Session open does not prove connectivity or endpoint grants. Verify remote business effects and denial evidence."})
                } else {
                    json!({"authentication":"unknown","authorization":"unknown","encryption":"unknown",
                        "reason":"Discovery does not establish identity, ACL or encryption."})
                },
            })))
        }
        ("node", "list") => {
            let graph = entities(graph, group, request);
            let mut counts = BTreeMap::new();
            for node in &graph {
                *counts.entry(full_name(node)).or_insert(0_usize) += 1;
            }
            let mut nodes = Vec::new();
            let mut complete = true;
            for node in graph {
                let fqn = full_name(&node);
                let mut summary = json!({"name":fqn,"node_id":node.node_id});
                if counts[&fqn] > 1 {
                    complete = false;
                    summary["description_error"] = json!({
                        "code":"AMBIGUOUS_NODE","message":"ambiguous node name"
                    });
                    nodes.push(summary);
                    continue;
                }
                match query_remaining(query_deadline).and_then(|remaining| {
                    ctx.get_node_card(&fqn, remaining).map_err(CliError::from)
                }) {
                    Ok(card) => {
                        for field in ["display_name", "summary", "tags"] {
                            summary[field] = card.get(field).cloned().unwrap_or(Value::Null);
                        }
                    }
                    Err(error) => {
                        complete = false;
                        summary["description_error"] = json!({
                            "code":error.code,"message":error.message
                        });
                    }
                }
                nodes.push(summary);
            }
            Ok(Some(json!({"nodes":nodes,"descriptions_complete":complete,
                "query":{"timeout_seconds":wait.as_secs_f64(),
                    "elapsed_seconds":query_started.elapsed().as_secs_f64(),
                    "scope":"shared budget for Graph and all NodeCard requests after session open"}})))
        }
        ("node", "info") => {
            let name = text(request, "node_name");
            ensure_unique_provider(&named_entities(
                ctx,
                "node",
                name,
                query_remaining(query_deadline)?,
            )?)?;
            Ok(Some(
                ctx.get_node_card(name, query_remaining(query_deadline)?)?,
            ))
        }
        ("topic" | "service" | "action", "list") => {
            let mut list: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
            for entity in entities(graph, group, request) {
                if let Some(endpoint) = entity.endpoint {
                    list.entry(endpoint.name)
                        .or_default()
                        .insert((endpoint.type_name, endpoint.type_hash));
                }
            }
            let items: Vec<_> = list.into_iter().map(|(name, types)| json!({
                "name":name,"types":types.into_iter().map(|(name,hash)|json!({"name":name,"hash":hash})).collect::<Vec<_>>()
            })).collect();
            let key = match group {
                "topic" => "topics",
                "action" => "actions",
                _ => "services",
            };
            Ok(Some(json!({key:items})))
        }
        ("topic" | "service" | "action", "type" | "info") => {
            let name = text(
                request,
                match group {
                    "topic" => "topic_name",
                    "action" => "action_name",
                    _ => "service_name",
                },
            );
            let resolution = configuration.target(name, group)?;
            let name = &resolution.resolved_name;
            let found = named_entities(
                ctx,
                if group == "service" && verb == "info" {
                    "service_all"
                } else {
                    group
                },
                name,
                query_remaining(query_deadline)?,
            )?;
            let (type_name, type_hash) = unique_type(&found)?;
            if verb == "info" {
                let qos_endpoints = if group == "action" {
                    let owners: BTreeSet<_> = found.iter().map(|e| &e.node_id).collect();
                    let channels: BTreeSet<_> = [
                        "send_goal",
                        "get_result",
                        "cancel_goal",
                        "feedback",
                        "status",
                    ]
                    .iter()
                    .map(|channel| format!("{name}/_action/{channel}"))
                    .collect();
                    ctx.graph(query_remaining(query_deadline)?)?
                        .into_iter()
                        .filter(|entity| {
                            owners.contains(&entity.node_id)
                                && entity
                                    .endpoint
                                    .as_ref()
                                    .is_some_and(|endpoint| channels.contains(&endpoint.name))
                        })
                        .collect()
                } else {
                    found.clone()
                };
                Ok(Some(
                    json!({"name":name,"name_resolution":resolution,"type":type_name,"hash":type_hash,
                        "endpoints":found,"qos_endpoints":qos_endpoints,
                        "qos_compatibility":qos_diagnostics(&qos_endpoints),
                        "qos_diagnostic_scope":"Observed matching endpoint pairs only; an empty list is not proof of compatibility. Unknown policies remain unknown. Diagnostics do not enforce DDS matching."}),
                ))
            } else {
                Ok(Some(json!({"type":type_name,"hash":type_hash})))
            }
        }
        ("interface", "list") => {
            let mut types: BTreeSet<(String, String)> = local_interfaces()?
                .iter()
                .map(|i| (i.type_name().to_owned(), i.type_hash().to_owned()))
                .collect();
            for entity in graph {
                if let Some(endpoint) = entity.endpoint {
                    types.insert((endpoint.type_name, endpoint.type_hash));
                }
            }
            Ok(Some(
                json!({"interfaces":types.into_iter().map(|(name,hash)|json!({"type":name,"hash":hash})).collect::<Vec<_>>()}),
            ))
        }
        ("interface", "show") => Ok(Some(
            resolve_interface(ctx, text(request, "type"), wait)?
                .definition()
                .clone(),
        )),
        ("service", "call") => {
            if request.get("times").is_some() {
                return service_calls::execute(ctx, request, configuration);
            }
            let resolution = configuration.target(text(request, "service_name"), "service")?;
            let name = &resolution.resolved_name;
            let found = named_entities(ctx, group, name, wait)?;
            ensure_unique_provider(&found)?;
            let (type_name, type_hash) = unique_type(&found)?;
            if type_name != text(request, "service_type") {
                return Err(CliError::input(
                    "service_type does not match the discovered provider",
                ));
            }
            let interface = ctx.get_type_description(
                &full_name(&found[0]),
                &type_name,
                Some(&type_hash),
                wait,
            )?;
            let node = configuration.create_node(ctx)?;
            let qos = resolve_qos(&request["qos"], "services_default", true)?;
            let client = node.create_client_with_qos(
                &interface,
                name,
                qos.clone(),
                EndpointOptions::default(),
            )?;
            let response = client.call(&request["values"], wait)?;
            Ok(Some(
                json!({"response":response,"qos":qos.effective_value(),"name_resolution":resolution}),
            ))
        }
        ("topic", "pub") => publication::execute(ctx, request, configuration),
        ("topic", "echo" | "hz" | "bw" | "delay") => {
            observation::execute(ctx, verb, request, configuration)
        }
        ("action", "send_goal" | "get_result" | "cancel") => {
            actions::execute(ctx, verb, request, configuration)
        }
        _ => Err(CliError::input("command not implemented")),
    }
}

fn qos_diagnostics(found: &[GraphEntity]) -> Vec<Value> {
    let mut diagnostics = Vec::new();
    for offered in found
        .iter()
        .filter(|e| matches!(e.kind, EntityKind::Publisher | EntityKind::Service))
    {
        for requested in found
            .iter()
            .filter(|e| matches!(e.kind, EntityKind::Subscription | EntityKind::Client))
        {
            if offered.endpoint != requested.endpoint {
                continue;
            }
            diagnostics.push(json!({
                "offered_entity":offered.id,"requested_entity":requested.id,
                "offered_qos":offered.qos,"requested_qos":requested.qos,
                "diagnosis":qos_compatibility(offered.qos.as_ref(),requested.qos.as_ref())
            }));
        }
    }
    diagnostics
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use aos_core::transport::{PublisherBackend, SubscriptionBackend, Transport};

    use super::*;

    #[test]
    fn resource_refusals_have_a_distinct_input_exit_category() {
        let resource = CliError::from(aos_core::Error::ResourceLimit("payload work".into()));
        assert_eq!((resource.code, resource.exit), ("RESOURCE_LIMIT", 2));
        assert!(resource.message.contains("payload work"));
        for error in [
            aos_core::Error::InvalidArgument("bad argument".into()),
            aos_core::Error::Validation("wrong type".into()),
        ] {
            let invalid = CliError::from(error);
            assert_eq!((invalid.code, invalid.exit), ("INVALID_ARGUMENT", 2));
        }
    }

    struct DiscoverySequence {
        snapshots: Mutex<VecDeque<Vec<GraphEntity>>>,
        calls: Arc<Mutex<Vec<Duration>>>,
    }

    impl Transport for DiscoverySequence {
        fn create_publisher(&self, _: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
            panic!("discovery must not publish business data");
        }

        fn create_subscription(&self, _: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
            panic!("discovery must not create business subscriptions");
        }

        fn graph(&self, timeout: Duration) -> aos_core::Result<Vec<GraphEntity>> {
            self.calls.lock().unwrap().push(timeout);
            Ok(self.snapshots.lock().unwrap().pop_front().unwrap())
        }

        fn close(&self) -> aos_core::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn discovery_warms_and_returns_only_the_final_snapshot() {
        let node = GraphEntity {
            id: "node1".into(),
            node_id: "node1".into(),
            node_name: "capabilities".into(),
            namespace: "/lab".into(),
            kind: EntityKind::Node,
            endpoint: None,
            qos: None,
        };
        // A cold first query may miss a live node. Conversely, a node observed
        // at the start may disappear before the final query.
        for (first, last) in [(Vec::new(), vec![node.clone()]), (vec![node], Vec::new())] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let ctx = Context::new(DiscoverySequence {
                snapshots: Mutex::new(VecDeque::from([first, last.clone()])),
                calls: calls.clone(),
            });
            let began = Instant::now();
            let window = Duration::from_secs_f64(schema::LIST_DISCOVERY_SECONDS);
            let query_timeout = Duration::from_secs(3);
            assert_eq!(discovery_snapshot(&ctx, query_timeout).unwrap(), last);
            assert!(began.elapsed() >= window);
            let recorded = calls.lock().unwrap();
            assert_eq!(recorded.len(), 2);
            assert_eq!(recorded[0], window);
            assert!(recorded[1] <= query_timeout - window);
            assert!(recorded[1] > Duration::ZERO);
            drop(recorded);
            ctx.shutdown().unwrap();
        }
    }

    #[test]
    fn node_descriptions_share_one_budget_instead_of_waiting_per_node() {
        struct SlowCards {
            nodes: Vec<GraphEntity>,
            calls: Arc<Mutex<Vec<Duration>>>,
        }
        impl Transport for SlowCards {
            fn create_publisher(&self, _: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
                panic!("read-only query");
            }
            fn create_subscription(
                &self,
                _: &str,
            ) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
                panic!("read-only query");
            }
            fn graph(&self, _: Duration) -> aos_core::Result<Vec<GraphEntity>> {
                Ok(self.nodes.clone())
            }
            fn call(
                &self,
                _: &aos_core::Endpoint,
                _: &[u8],
                timeout: Duration,
            ) -> aos_core::Result<Vec<u8>> {
                self.calls.lock().unwrap().push(timeout);
                std::thread::sleep(timeout);
                Err(aos_core::Error::Timeout)
            }
            fn close(&self) -> aos_core::Result<()> {
                Ok(())
            }
        }
        let nodes = (0..20)
            .map(|index| GraphEntity {
                id: format!("n{index}"),
                node_id: format!("n{index}"),
                node_name: format!("worker{index}"),
                namespace: "/".into(),
                kind: EntityKind::Node,
                endpoint: None,
                qos: None,
            })
            .collect();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let context = Context::new(SlowCards {
            nodes,
            calls: calls.clone(),
        });
        let request = json!({"timeout":0.2});
        let config = InvocationConfig::parse(&request, false, false).unwrap();
        let began = Instant::now();
        let result = execute(&context, Some(&[]), "node", "list", &request, &config)
            .unwrap()
            .unwrap();
        assert!(began.elapsed() < Duration::from_secs(1));
        assert_eq!(result["nodes"].as_array().unwrap().len(), 20);
        assert_eq!(result["descriptions_complete"], false);
        assert_eq!(calls.lock().unwrap().len(), 1);
        assert_eq!(
            result["nodes"][1]["description_error"]["code"],
            "QUERY_TIMEOUT"
        );
        context.shutdown().unwrap();
    }
}
