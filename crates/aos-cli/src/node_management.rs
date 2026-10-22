//! Parameter and lifecycle commands use the provider's ordinary core services.
//! Parsing happens before Context creation; every network operation shares one
//! steady deadline. Neither an observation timeout nor SIGINT cancels remote work.
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use aos_core::{
    Context, EntityKind, GraphEntity, Interface, LifecycleClient, LifecycleServiceKind,
    LifecycleState, LifecycleTransitionId, LifecycleTransitionSelector, Parameter, ParameterClient,
    ParameterError, ParameterQueryReply, ParameterServiceKind, ParameterSetResult,
    ParameterSnapshot, ParameterValue,
};
use serde_json::{json, Value};

use super::{
    configuration::InvocationConfig, full_name, operation_field, optional_u64, phase, publication,
    CliError, Result,
};

pub struct Prepared {
    node_name: String,
    timeout: Duration,
    command: Command,
}

enum Command {
    Get(Vec<String>),
    Describe(Vec<String>),
    List {
        prefixes: Vec<String>,
        depth: u64,
    },
    Set(Vec<Parameter>),
    Dump {
        startup: bool,
        path: Option<PathBuf>,
    },
    Load {
        parameters: Vec<Parameter>,
        path: PathBuf,
    },
    State,
    LifecycleList(LifecycleServiceKind),
    Change {
        transition: LifecycleTransitionSelector,
        drain: Option<Duration>,
    },
}

fn input(message: impl Into<String>) -> CliError {
    CliError::input(message)
}

fn file_error(path: &Path, error: impl std::fmt::Display) -> CliError {
    CliError {
        code: "FILE_IO_ERROR",
        message: format!("{}: {error}", path.display()),
        exit: 1,
    }
}

fn field<'a>(request: &'a Value, name: &str) -> Result<&'a str> {
    request
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| input(format!("{name} must be a nonempty string")))
}

fn strings(request: &Value, name: &str, required: bool) -> Result<Vec<String>> {
    let values = match request.get(name) {
        Some(value) => value
            .as_array()
            .ok_or_else(|| input(format!("{name} must be an array of strings")))?,
        None if !required => return Ok(Vec::new()),
        None => return Err(input(format!("{name} is required"))),
    };
    if required && values.is_empty() {
        return Err(input(format!("{name} must not be empty")));
    }
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| input(format!("{name} entries must be nonempty strings")))?;
            if !seen.insert(value) {
                return Err(input(format!("{name} contains a duplicate: {value:?}")));
            }
            Ok(value.to_owned())
        })
        .collect()
}

fn parameters(value: &Value, allow_empty: bool) -> Result<Vec<Parameter>> {
    let values = value
        .as_object()
        .filter(|values| allow_empty || !values.is_empty())
        .ok_or_else(|| {
            input(if allow_empty {
                "parameters must be an object"
            } else {
                "parameters must be a nonempty object"
            })
        })?;
    values
        .iter()
        .map(|(name, value)| {
            if name.is_empty() {
                return Err(input("parameter names must not be empty"));
            }
            Ok(Parameter::new(
                name,
                ParameterValue::from_json(value)
                    .map_err(|error| input(format!("{name}: {error}")))?,
            ))
        })
        .collect()
}

fn load(path: &Path) -> Result<Vec<Parameter>> {
    // Read once before opening a Context. The prepared owned values, rather
    // than the filename, are moved into the one atomic remote request.
    if !fs::metadata(path)
        .map_err(|error| file_error(path, error))?
        .is_file()
    {
        return Err(file_error(path, "input must be a regular file"));
    }
    let contents = fs::read_to_string(path).map_err(|error| file_error(path, error))?;
    let value = aos_core::parse_config_json(&contents)?;
    if value.get("format_version").is_some() {
        let snapshot: ParameterSnapshot = serde_json::from_value(value)
            .map_err(|error| input(format!("parameter snapshot: {error}")))?;
        if snapshot.format_version != 1 {
            return Err(input("unsupported parameter snapshot version"));
        }
        let mut seen = BTreeSet::new();
        let parameters = snapshot
            .parameters
            .into_iter()
            .map(|record| {
                if record.name.is_empty() || !seen.insert(record.name.clone()) {
                    return Err(input(
                        "snapshot parameter names must be nonempty and unique",
                    ));
                }
                let value = ParameterValue::from_json(&json!(record.value))
                    .map_err(|error| input(error.to_string()))?;
                Ok(Parameter::new(record.name, value))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(parameters)
    } else {
        let object = value
            .as_object()
            .ok_or_else(|| input("startup configuration must be an object"))?;
        if object.len() != 1 || !object.contains_key("parameters") {
            return Err(input(
                "parameter load expects only {\"parameters\": {...}} or a version 1 snapshot",
            ));
        }
        parameters(&value["parameters"], true)
    }
}

fn check_output(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !fs::metadata(parent)
        .map_err(|error| file_error(path, error))?
        .is_dir()
    {
        return Err(file_error(path, "parent is not a directory"));
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => Err(
            file_error(path, "output must be a regular file or an unused path"),
        ),
        Ok(metadata) if metadata.permissions().readonly() => {
            Err(file_error(path, "output file is read-only"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(file_error(path, error)),
    }
}

/// Call before opening a Context. This consumes file input only once.
pub fn prepare(request: &Value, group: &str, verb: &str) -> Result<Prepared> {
    let node_name = field(request, "node_name")?.to_owned();
    aos_core::names::validate_absolute_name(&node_name)?;
    let timeout = publication::seconds(request, "timeout", 30.0, false)?;
    let command = match (group, verb) {
        ("param", "get") => Command::Get(strings(request, "names", true)?),
        ("param", "describe") => Command::Describe(strings(request, "names", true)?),
        ("param", "list") => Command::List {
            prefixes: strings(request, "prefixes", false)?,
            depth: optional_u64(request, "depth")?.unwrap_or(0),
        },
        ("param", "set") => Command::Set(parameters(&request["parameters"], false)?),
        ("param", "dump") => {
            let startup = match request
                .get("format")
                .and_then(Value::as_str)
                .unwrap_or("startup")
            {
                "startup" => true,
                "snapshot" => false,
                _ => return Err(input("format must be startup or snapshot")),
            };
            let path = request
                .get("path")
                .map(|_| field(request, "path").map(PathBuf::from))
                .transpose()?;
            if let Some(path) = &path {
                check_output(path)?;
            }
            Command::Dump { startup, path }
        }
        ("param", "load") => {
            let path = PathBuf::from(field(request, "path")?);
            Command::Load {
                parameters: load(&path)?,
                path,
            }
        }
        ("lifecycle", "get") => Command::State,
        ("lifecycle", "list") => Command::LifecycleList(
            match request
                .get("view")
                .and_then(Value::as_str)
                .unwrap_or("transitions")
            {
                "transitions" => LifecycleServiceKind::GetAvailableTransitions,
                "states" => LifecycleServiceKind::GetAvailableStates,
                "graph" => LifecycleServiceKind::GetTransitionGraph,
                _ => return Err(input("view must be transitions, states, or graph")),
            },
        ),
        ("lifecycle", "set") => {
            let transition = match &request["transition"] {
                Value::String(label) => {
                    // Validate spelling through core; the provider selects the
                    // shutdown ID from its authoritative current state.
                    LifecycleTransitionId::from_label(label, LifecycleState::Unconfigured)?;
                    LifecycleTransitionSelector {
                        id: 0,
                        label: label.clone(),
                    }
                }
                _ => {
                    let id = u8::try_from(
                        optional_u64(request, "transition")?
                            .ok_or_else(|| input("transition is required"))?,
                    )
                    .map_err(|_| input("transition must be a public integer ID or label"))?;
                    LifecycleTransitionId::from_id(id)?;
                    LifecycleTransitionSelector {
                        id,
                        label: String::new(),
                    }
                }
            };
            let drain = request
                .get("drain_timeout")
                .map(|_| {
                    let duration = publication::seconds(request, "drain_timeout", 0.0, true)?;
                    u64::try_from(duration.as_millis())
                        .map_err(|_| input("drain_timeout overflows milliseconds"))?;
                    Ok::<Duration, CliError>(duration)
                })
                .transpose()?;
            Command::Change { transition, drain }
        }
        _ => return Err(input("unknown Node management command")),
    };
    Ok(Prepared {
        node_name,
        timeout,
        command,
    })
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(aos_core::Error::Timeout.into())
    } else {
        Ok(remaining)
    }
}

impl Command {
    fn services(&self) -> Vec<(&'static str, Interface)> {
        use LifecycleServiceKind as L;
        use ParameterServiceKind as P;
        let parameter = match self {
            Self::Get(_) => vec![P::GetParameters],
            Self::Describe(_) => vec![P::DescribeParameters],
            Self::List { .. } => vec![P::ListParameters],
            Self::Set(_) | Self::Load { .. } => vec![P::SetParametersAtomically],
            Self::Dump { .. } => vec![P::ListParameters, P::DescribeParameters],
            _ => Vec::new(),
        };
        if !parameter.is_empty() {
            return parameter
                .into_iter()
                .map(|kind| (kind.suffix(), kind.interface()))
                .collect();
        }
        let kind = match self {
            Self::State => L::GetState,
            Self::LifecycleList(kind) => *kind,
            Self::Change { .. } => L::ChangeState,
            _ => unreachable!(),
        };
        vec![(kind.suffix(), kind.interface())]
    }
}

fn discover(
    context: &Context,
    name: &str,
    command: &Command,
    deadline: Instant,
) -> Result<GraphEntity> {
    let services = command.services();
    let watch = context.watch_graph()?;
    phase("node_discovery");
    loop {
        let budget = remaining(deadline)?;
        let graph = match &watch {
            Some(watch) => watch.snapshot()?,
            None => context.graph(budget)?,
        };
        let nodes: Vec<_> = graph
            .iter()
            .filter(|entity| entity.kind == EntityKind::Node && full_name(entity) == name)
            .collect();
        if nodes.len() > 1 {
            return Err(CliError::ambiguous(
                "multiple Nodes use this name; choose distinct namespaces",
            ));
        }
        if let Some(node) = nodes.first() {
            phase("service_discovery");
            let mut ready = true;
            for (suffix, interface) in &services {
                let endpoint_name = format!("{name}/{suffix}");
                let providers: Vec<_> = graph
                    .iter()
                    .filter(|entity| {
                        entity.kind == EntityKind::Service
                            && entity
                                .endpoint
                                .as_ref()
                                .is_some_and(|endpoint| endpoint.name == endpoint_name)
                    })
                    .collect();
                if providers.len() > 1 {
                    return Err(CliError::ambiguous(format!(
                        "multiple providers offer {endpoint_name}"
                    )));
                }
                match providers.first() {
                    None => ready = false,
                    Some(provider) => {
                        let endpoint = provider.endpoint.as_ref().unwrap();
                        if provider.node_id != node.node_id {
                            return Err(CliError::ambiguous(format!(
                                "{endpoint_name} belongs to a different observed Node"
                            )));
                        }
                        if endpoint.type_name != interface.type_name()
                            || endpoint.type_hash != interface.type_hash()
                        {
                            return Err(input(format!(
                                "{endpoint_name} does not offer the built-in management interface"
                            )));
                        }
                    }
                }
            }
            if ready {
                return Ok((*node).clone());
            }
        }
        let poll = remaining(deadline)?.min(Duration::from_millis(20));
        match &watch {
            Some(watch) => match watch.wait_for_change(poll) {
                Ok(()) | Err(aos_core::Error::Timeout) => {}
                Err(error) => return Err(error.into()),
            },
            None => std::thread::sleep(poll),
        }
    }
}

fn parameter_code(code: &str) -> &'static str {
    match code {
        "closed" => "PARAMETER_CLOSED",
        "invalid_argument" => "PARAMETER_INVALID_ARGUMENT",
        "already_declared" => "PARAMETER_ALREADY_DECLARED",
        "not_declared" => "PARAMETER_NOT_DECLARED",
        "uninitialized" => "PARAMETER_UNINITIALIZED",
        "read_only" => "PARAMETER_READ_ONLY",
        "state_not_allowed" => "PARAMETER_STATE_NOT_ALLOWED",
        "wrong_type" => "PARAMETER_WRONG_TYPE",
        "out_of_range" => "PARAMETER_OUT_OF_RANGE",
        "conflict" => "PARAMETER_CONFLICT",
        "expired" => "PARAMETER_EXPIRED",
        "validation" => "PARAMETER_VALIDATION",
        "unavailable" => "PARAMETER_UNAVAILABLE",
        _ => "PARAMETER_ERROR",
    }
}

fn parameter_error(error: ParameterError) -> CliError {
    operation_field("parameter_error", json!(error));
    CliError {
        code: parameter_code(error.code()),
        message: error.to_string(),
        exit: 1,
    }
}

fn query<T>(reply: ParameterQueryReply<T>) -> Result<(u64, T)> {
    if let Some(error) = reply.error {
        return Err(parameter_error(error));
    }
    match (reply.version, reply.value) {
        (Some(version), Some(value)) => Ok((version, value)),
        _ => Err(aos_core::Error::Validation(
            "parameter response omitted its version or value".into(),
        )
        .into()),
    }
}

fn committed(result: ParameterSetResult) -> Result<Value> {
    let value = json!(result);
    if !result.successful {
        operation_field("parameter_result", value);
        return Err(CliError {
            code: parameter_code(result.code.as_deref().unwrap_or("")),
            message: result.reason,
            exit: 1,
        });
    }
    Ok(json!({"result":value,"atomic":true}))
}

fn write_dump(path: &Path, value: &Value) -> Result<aos_core::AtomicWriteReport> {
    check_output(path)?;
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|error| file_error(path, error))?;
    bytes.push(b'\n');
    aos_core::write_private_atomic(path, &bytes).map_err(|error| file_error(path, error))
}

fn run(
    context: &Context,
    local: &aos_core::Node,
    prepared: Prepared,
    deadline: Instant,
) -> Result<Value> {
    let remote = discover(context, &prepared.node_name, &prepared.command, deadline)?;
    operation_field("node_id", json!(remote.node_id));
    phase("request");
    let mut data = match prepared.command {
        Command::State | Command::LifecycleList(_) | Command::Change { .. } => {
            let client = LifecycleClient::new(local, &prepared.node_name)?;
            match prepared.command {
                Command::State => json!(client.state(remaining(deadline)?)?),
                Command::LifecycleList(kind) => {
                    client.call(kind, &json!({}), remaining(deadline)?)?
                }
                Command::Change { transition, drain } => {
                    operation_field("remote_execution", json!("unknown until the provider replies; timeout or interruption does not stop the transition"));
                    phase("transition_wait");
                    let reply = client.change_state(transition, drain, remaining(deadline)?)?;
                    if !reply.success {
                        operation_field("lifecycle_result", json!(reply));
                        return Err(CliError {
                            code: if reply
                                .report
                                .as_ref()
                                .is_some_and(|report| report.drain_timed_out)
                            {
                                "LIFECYCLE_DRAIN_TIMEOUT"
                            } else {
                                "LIFECYCLE_TRANSITION_FAILED"
                            },
                            message: reply
                                .error
                                .as_ref()
                                .map(|e| format!("{}: {}", e.code, e.message))
                                .or_else(|| reply.report.as_ref().map(|r| r.reason.clone()))
                                .unwrap_or_else(|| {
                                    "provider rejected the lifecycle transition".into()
                                }),
                            exit: 1,
                        });
                    }
                    json!(reply)
                }
                _ => unreachable!(),
            }
        }
        command => {
            let client = ParameterClient::new(local, &prepared.node_name)?;
            match command {
                Command::Get(names) => {
                    let (version, values) = query(client.get(&names, remaining(deadline)?)?)?;
                    let parameters: Vec<_> = names
                        .into_iter()
                        .zip(values)
                        .map(|(name, value)| Parameter::new(name, value))
                        .collect();
                    json!({"version":version,"parameters":parameters})
                }
                Command::Describe(names) => {
                    let (version, records) = query(client.describe(&names, remaining(deadline)?)?)?;
                    json!({"version":version,"parameters":records})
                }
                Command::List { prefixes, depth } => {
                    let (version, result) =
                        query(client.list(&prefixes, depth, remaining(deadline)?)?)?;
                    json!({"version":version,"names":result.names,"prefixes":result.prefixes})
                }
                Command::Set(parameters) => {
                    operation_field("remote_execution", json!("unknown until the provider replies; timeout or interruption does not roll back an accepted transaction"));
                    phase("atomic_set_wait");
                    committed(client.set_atomically(&parameters, remaining(deadline)?)?)?
                }
                Command::Load { parameters, .. } => {
                    operation_field("remote_execution", json!("unknown until the provider replies; timeout or interruption does not roll back an accepted transaction"));
                    phase("atomic_load_wait");
                    committed(client.load_atomically(&parameters, remaining(deadline)?)?)?
                }
                Command::Dump { startup, path } => {
                    phase("parameter_snapshot");
                    let (list_version, listed) =
                        query(client.list(&[], 0, remaining(deadline)?)?)?;
                    let described = client.describe(&listed.names, remaining(deadline)?)?;
                    if matches!(described.error, Some(ParameterError::NotDeclared(_))) {
                        return Err(parameter_error(ParameterError::Conflict(
                            "a listed parameter was removed before describe; no snapshot was written"
                                .into(),
                        )));
                    }
                    let (version, records) = query(described)?;
                    if list_version != version {
                        return Err(parameter_error(ParameterError::Conflict(
                            "parameters changed between list and describe; no snapshot was written"
                                .into(),
                        )));
                    }
                    let snapshot = ParameterSnapshot {
                        format_version: 1,
                        node: prepared.node_name.clone(),
                        node_id: remote.node_id.clone(),
                        version,
                        parameters: records,
                    };
                    let document = if startup {
                        snapshot.startup_config()
                    } else {
                        json!(snapshot)
                    };
                    let file_write = if let Some(path) = &path {
                        phase("file_write");
                        operation_field(
                            "remote_execution",
                            json!("read-only parameter snapshot received"),
                        );
                        Some(write_dump(path, &document)?)
                    } else {
                        None
                    };
                    json!({"version":version,"format":if startup {"startup"} else {"snapshot"},
                        "document":document,"path":path,"file_write":file_write,
                        "consistent_versions":true})
                }
                _ => unreachable!(),
            }
        }
    };
    data["node_name"] = json!(prepared.node_name);
    data["node_id"] = json!(remote.node_id);
    data["identity_basis"] =
        json!("observed Graph owner; management responses are not authenticated");
    Ok(data)
}

/// The main entry point moves the preflight object across Context creation.
pub fn execute_prepared(
    ctx: &Context,
    prepared: Prepared,
    config: &InvocationConfig,
) -> Result<Option<Value>> {
    let interrupted = publication::interrupt()?;
    let began = Instant::now();
    let timeout = prepared.timeout;
    let deadline = began
        .checked_add(timeout)
        .ok_or_else(|| input("timeout exceeds the steady clock range"))?;
    operation_field("node_name", json!(prepared.node_name));
    operation_field("retry", json!(false));
    operation_field("timeout_seconds", json!(timeout.as_secs_f64()));
    if let Command::Load { path, .. } = &prepared.command {
        operation_field("source_path", json!(path));
    }
    let local = config.create_node(ctx)?;
    let result = publication::interruptible(ctx, &interrupted, move |context| {
        run(&context, &local, prepared, deadline)
    });
    let timing = json!({"timeout_seconds":timeout.as_secs_f64(),"elapsed_seconds":began.elapsed().as_secs_f64(),
        "scope":"one steady budget for discovery and all management requests after Context creation"});
    operation_field("query", timing.clone());
    let mut data = result?;
    data["query"] = timing;
    data["retry"] = json!(false);
    Ok(Some(data))
}

#[allow(dead_code)]
pub fn execute(
    ctx: &Context,
    request: &Value,
    group: &str,
    verb: &str,
    config: &InvocationConfig,
) -> Result<Option<Value>> {
    execute_prepared(ctx, prepare(request, group, verb)?, config)
}
