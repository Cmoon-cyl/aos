//! Installed package lookup and the CLI shell around the one launch runtime.
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use aos_launch::{
    resolve_package_prefixes, EventSink, LaunchError, LaunchErrorCode, LaunchEvent,
    LaunchOverrides, LaunchSpec, PackageIndex, ProcessLog, ResourceKind,
};
#[cfg(any(unix, windows, test))]
use aos_launch::{RunOutcome, StopReason};
#[cfg(any(unix, windows))]
use aos_launch::{RunProgress, Supervisor};
use serde_json::{json, Value};

#[cfg(any(unix, windows))]
use crate::launch_probe::CoreProbeFactory;
use crate::{operation_field, phase, CliError, Result};

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}

fn definitions() -> Value {
    let string = json!({"type":"string","minLength":1});
    let strings = json!({"type":"array","items":{"type":"string"}});
    let context = object(
        json!({
            "domain_id":{"type":"integer","minimum":0,"maximum":4294967295u64},
            "zenoh_config":{"type":["string","null"],"minLength":1}
        }),
        &["domain_id", "zenoh_config"],
    );
    let resource = object(
        json!({"package":string,"resource":string}),
        &["package", "resource"],
    );
    let mut lifecycle = object(
        json!({
            "target":{"enum":["unconfigured","inactive","active"]},
            "autostart":{"type":"boolean"}
        }),
        &["target"],
    );
    lifecycle["allOf"] = json!([{
        "if":{"properties":{"autostart":{"const":true}},"required":["autostart"]},
        "then":{"properties":{"target":{"const":"active"}}}
    }]);
    let readiness = object(
        json!({
            "node":string, "timeout":{"type":"number","exclusiveMinimum":0,"maximum":86400},
            "endpoints":{"type":"array","items":object(json!({
                "kind":{"enum":["publisher","subscription","service","action"]},
                "name":string,"interface":resource
            }), &["kind","name","interface"])},
            "lifecycle":lifecycle
        }),
        &["node"],
    );
    let restart = object(
        json!({
            "policy":{"enum":["never","on_failure","always"]},
            "delay":{"type":"number","minimum":0,"maximum":86400},
            "max_restarts":{"type":"integer","minimum":0,"maximum":4294967294u64}
        }),
        &[],
    );
    let shutdown = object(
        json!({
            "sigint_timeout":{"type":"number","minimum":0,"maximum":86400},
            "sigterm_timeout":{"type":"number","minimum":0,"maximum":86400}
        }),
        &[],
    );
    let process = object(
        json!({
            "id":string,"package":string,"entry":string,"package_version":string,
            "args":strings,"env":{"type":"object","additionalProperties":{"type":"string"}},
            "cwd":string,"config_files":strings,"node":{"type":"object"},
            "depends_on":strings,"readiness":readiness,"restart":restart
        }),
        &["id", "package", "entry"],
    );
    let overrides = object(
        json!({
            "context":object(json!({
                "domain_id":{"type":"integer","minimum":0,"maximum":4294967295u64},
                "zenoh_config":{"type":["string","null"],"minLength":1}
            }), &[]),
            "nodes":{"type":"object","additionalProperties":{"type":"object"}}
        }),
        &[],
    );
    json!({"context":context,"process":process,"readiness":readiness,"restart":restart,
        "shutdown":shutdown,"overrides":overrides,"launch":object(json!({
            "schema_version":{"const":1},"context":context,
            "processes":{"type":"array","minItems":1,"maxItems":128,"items":process},
            "shutdown":shutdown
        }), &["schema_version","processes"])})
}

fn output_schema() -> Value {
    let nullable_string = json!({"type":["string","null"]});
    let nullable_integer = json!({"type":["integer","null"]});
    let event = object(
        json!({
            "schema_version":{"const":1},"launch_id":{"type":"string"},
            "process_id":nullable_string,"generation":nullable_integer,"pid":nullable_integer,
            "pgid":nullable_integer,"elapsed_ms":{"type":"integer","minimum":0},
            "event":{"enum":["process_started","discovered","lifecycle_transition","ready",
                "process_exited","restart_scheduled","restart_exhausted","dependency_unavailable",
                "dependency_ready","process_stopping","process_signal","process_stdout",
                "output_truncated","output_dropped","launch_failed","launch_finished"]},
            "details":{"type":"object"}
        }),
        &[
            "schema_version",
            "launch_id",
            "process_id",
            "generation",
            "pid",
            "pgid",
            "elapsed_ms",
            "event",
            "details",
        ],
    );
    let summary = object(
        json!({
            "schema_version":{"const":1},"event":{"const":"summary"},
            "data":object(json!({
                "launch_id":{"type":"string"},"outcome":{"enum":["running","completed","stopped","failed"]},
                "stop_reason":{"type":["object","null"]},
                "processes":{"type":"array","items":object(json!({
                    "process_id":{"type":"string"},"generations":{"type":"integer","minimum":0},
                    "restarts":{"type":"integer","minimum":0},"ready":{"type":"boolean"},
                    "exit":{"type":["object","null"],"properties":{"code":nullable_integer,"signal":nullable_integer}}
                }), &["process_id","generations","restarts","ready","exit"])},
                "cleanup_errors":{"type":"array","items":{"type":"object"}}
            }), &["launch_id","outcome","stop_reason","processes","cleanup_errors"])
        }),
        &["schema_version", "event", "data"],
    );
    let error = object(
        json!({
            "schema_version":{"const":1},"event":{"const":"error"},
            "data":{"type":"object","required":["code","message"],
                "properties":{"code":{"type":"string"},"message":{"type":"string"}},
                "additionalProperties":true}
        }),
        &["schema_version", "event", "data"],
    );
    json!({"oneOf":[event,summary,error]})
}

pub fn request_schema(group: &str, verb: &str) -> Option<Value> {
    let defs = definitions();
    let string = json!({"type":"string","minLength":1});
    let mut properties = json!({
        "prefixes":{"type":"array","items":{"type":"string","minLength":1}}
    });
    let required: &[&str] = match (group, verb) {
        ("package", "list") => &[],
        ("package", "info" | "resource") => {
            properties["package"] = string.clone();
            properties["package_version"] = string.clone();
            if verb == "resource" {
                properties["resource"] = string.clone();
                &["package", "resource"]
            } else {
                &["package"]
            }
        }
        ("run", "start") => {
            properties
                .as_object_mut()?
                .extend(defs["process"]["properties"].as_object()?.clone());
            properties.as_object_mut()?.remove("depends_on");
            for name in ["context", "shutdown", "overrides"] {
                properties[name] = defs[name].clone();
            }
            &["package", "entry"]
        }
        ("launch", "start") => {
            properties["plan"] = defs["launch"].clone();
            for name in ["path", "package", "resource", "package_version"] {
                properties[name] = string.clone();
            }
            properties["overrides"] = defs["overrides"].clone();
            &[]
        }
        _ => return None,
    };
    let mut schema = object(properties, required);
    if matches!(group, "run" | "launch") {
        schema["properties"]["security_profile"] = json!({"type":"string","minLength":1,
            "description":"Absolute TLS profile for the supervisor control session; otherwise AOS_SECURITY_PROFILE. Each child must explicitly set its own AOS_SECURITY_PROFILE in env."});
        schema["properties"]["client_node"] = json!({"type":"object","additionalProperties":false,
            "required":["name","namespace"],"properties":{"name":string,"namespace":string},
            "description":"Policy-authorized supervisor Node identity; required with a secure profile."});
    }
    schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
    if group == "launch" {
        schema["oneOf"] = json!([
            {"required":["plan"],"not":{"anyOf":[{"required":["path"]},{"required":["package"]},
                {"required":["resource"]},{"required":["package_version"]}]}},
            {"required":["path"],"not":{"anyOf":[{"required":["plan"]},{"required":["package"]},
                {"required":["resource"]},{"required":["package_version"]}]}},
            {"required":["package","resource"],"not":{"anyOf":[{"required":["plan"]},{"required":["path"]}]}}
        ]);
    }
    Some(schema)
}

pub fn describe(group: &str, verb: &str) -> Option<Value> {
    let request = request_schema(group, verb)?;
    let streaming = group != "package";
    Some(json!({
        "command":format!("aos {group} {verb}"),
        "description":match (group,verb) {
            ("package","list")=>"List installed package candidates.",
            ("package","info")=>"Resolve one installed package without executing it.",
            ("package","resource")=>"Resolve a declared resource to an absolute path.",
            ("run",_)=>"Run one installed entry with optional readiness.",
            _=>"Supervise a validated local process dependency plan."
        },
        "input":"--request PATH|-; run and launch also accept the start verb implicitly.",
        "request_schema":request,
        "output_format":if streaming {"jsonl"} else {"json"},
        "output_schema":if streaming {
            output_schema()
        } else {
            json!({"type":"object","required":["ok"],"properties":{"ok":{"type":"boolean"},"data":{},"error":{"type":"object"}}})
        },
        "exit_codes":{"0":"completed or explicitly stopped without a signal",
            "1":"process, readiness, cleanup or output failure","2":"invalid input or unsupported platform",
            "3":"package, entry or resource not found or ambiguous","4":"readiness deadline",
            "129":"SIGHUP after owned-group cleanup","130":"SIGINT or Windows Ctrl-Break after owned-process cleanup",
            "131":"SIGQUIT after owned-group cleanup","143":"SIGTERM after owned-group cleanup"},
        "prefixes":"Explicit prefixes are exclusive. Otherwise AOS_PREFIX_PATH plus the installed CLI prefix is used; deployed processes with AOS_PROCESS_CONFIG use only the fixed environment prefixes. A wholly empty environment value is unset; empty list entries are invalid. The cwd is never searched for packages.",
        "paths":"Relative paths in file/resource plans use the document directory; inline plans and overrides use the invocation cwd. Every path is fixed before any child starts.",
        "protocol":{
            "ownership":"The aos-launch runtime owns child processes, restart limits and cleanup; provider core owns lifecycle and Action state.",
            "readiness":"Started is not ready. A matching Graph Node and NodeCard must prove launch/process/generation/PID; declared endpoints and lifecycle target must also match.",
            "readiness_latch":"Ready is retained for the current running provider generation; later lifecycle changes are not continuously monitored. A new provider generation must prove readiness again.",
            "run_without_readiness":"No control Context is opened and no ready event is claimed.",
            "lifecycle":"Autostart submits configure and activate at most once per generation. A timeout never proves a transition was not executed.",
            "output":"Runtime events are JSON objects with an event field; the terminal summary uses event=summary and data=RunSummary. Child JSON is wrapped in process_stdout; other output is diagnostic JSON on stderr.",
            "stop":"Unix SIGHUP/SIGINT/SIGQUIT/SIGTERM or Windows Ctrl-Break stop this invocation, cancel local readiness waits and perform bounded cleanup. Windows sends Ctrl-Break to the owned console group and escalates to termination of its Job Object.",
            "limits":"readiness/restart/shutdown waits are at most 86400 seconds; combined shutdown INT+TERM must also be at most 86400. Dependency DAG, package identities, Node configuration and interface compatibility receive additional preflight checks.",
            "output_backpressure":"stdout has a 4 MiB queue. An enqueue-blocked queue must make no write progress continuously for 30 seconds before the launch stops; every successful write resets this limit, and pending bytes below queue capacity alone do not trigger it. A stop also sets an absolute INT+TERM+32 second deadline (2 seconds for group cleanup plus 30 seconds for output); pending output is then discarded with OUTPUT_ERROR even after an earlier output failure. At leader reap, each stream snapshots kernel-readable bytes with FIONREAD on Unix or PeekNamedPipe on Windows, plus locally buffered bytes. This fixed residue can drain across byte/record quotas and process rotation; later writes cannot replenish it. After residue and a tail check, each process has a 2 second pipe-close window that new bytes do not extend. Pending events or sink backpressure may extend that window, but never the absolute stop deadline. Each stream also has a raw-read limit of pipe capacity plus 4 KiB (Linux F_GETPIPE_SZ; macOS/Windows conservative 1 MiB capacity); exceeding either post-reap bound produces CLEANUP_FAILED without signaling reaped or escaped groups. stderr has a 1 MiB best-effort queue and dropped diagnostics are counted, including unfinished records after a 100 ms final drain. Terminal summary/error use one separate shared 2 second deadline; summary is retained even when polling returns an error, and a failed output channel may prevent its delivery. Keep stdout and stderr separate.",
            "startup_signals":"Before the control Context has opened, no children exist and terminal signals retain their OS default behavior. Handlers are installed immediately after setup and before the first child can spawn.",
            "readiness_network":"A null zenoh_config uses the same built-in local-router profile as the SDK.",
            "restart_graph":"Only after the preceding owned generation exited, overlapping Node or Service/Action provider entries wait until the new generation deadline; initial external conflicts fail immediately.",
            "probe_close":"Client, Context, Graph, Node and worker cleanup share one 1 second caller deadline. Overrun produces a bounded best-effort stderr diagnostic.",
            "entry":"Unix requires the SDK Node in the owned leader PID; wrappers must exec. Windows verifies the Node PID belongs to the live Job Object for that exact generation, including Python virtual-environment redirectors. An explicit interpreter can run an installed script without a command shell.",
            "platform":"macOS/Linux process groups; Windows 10/11 Job Objects with atomic assignment and kill-on-close. On Windows pgid records the console-group leader PID; forced termination uses the Job handle."
        }
    }))
}

fn io_error(path: &Path, error: impl std::fmt::Display) -> CliError {
    CliError {
        code: "FILE_IO_ERROR",
        message: format!("{}: {error}", path.display()),
        exit: 1,
    }
}

pub(crate) fn runtime_error(error: LaunchError) -> CliError {
    use LaunchErrorCode::*;
    let (code, exit) = match error.code {
        InvalidArgument => ("INVALID_ARGUMENT", 2),
        InvalidManifest => ("INVALID_MANIFEST", 2),
        PackageNotFound => ("PACKAGE_NOT_FOUND", 3),
        AmbiguousPackage => ("AMBIGUOUS_PACKAGE", 3),
        EntryNotFound => ("ENTRY_NOT_FOUND", 3),
        ResourceNotFound => ("RESOURCE_NOT_FOUND", 3),
        VersionMismatch => ("VERSION_MISMATCH", 2),
        Unsupported => ("UNSUPPORTED", 2),
        Io => ("FILE_IO_ERROR", 1),
        SpawnFailed => ("SPAWN_FAILED", 1),
        ReadinessFailed => ("READINESS_FAILED", 1),
        ReadinessTimeout => ("READINESS_TIMEOUT", 4),
        ProcessFailed => ("PROCESS_FAILED", 1),
        RestartExhausted => ("RESTART_EXHAUSTED", 1),
        CleanupFailed => ("CLEANUP_FAILED", 1),
        OutputIoError => ("OUTPUT_ERROR", 1),
        Closed => ("CLOSED", 1),
    };
    operation_field("launch_error", json!(error));
    CliError {
        code,
        message: error.to_string(),
        exit,
    }
}

fn absolute(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    }
}

fn prefixes(request: &Value, cwd: &Path) -> Result<Vec<PathBuf>> {
    let explicit = request
        .get("prefixes")
        .map(|paths| {
            paths
                .as_array()
                .ok_or_else(|| CliError::input("prefixes must be an array"))?
                .iter()
                .map(|path| {
                    path.as_str()
                        .filter(|s| !s.is_empty())
                        .map(PathBuf::from)
                        .ok_or_else(|| CliError::input("prefixes must contain nonempty strings"))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    // Only an installed <prefix>/bin/aos contributes an implicit prefix.
    // A development binary in target/release must not discover a nearby repo.
    let installed = std::env::current_exe()
        .and_then(fs::canonicalize)
        .ok()
        .and_then(|executable| {
            if let Some(bin) = executable
                .parent()
                .filter(|p| p.file_name().is_some_and(|n| n == "bin"))
            {
                if let Some(prefix) = bin.parent() {
                    if prefix.join("share/aos/resource_index/packages").is_dir() {
                        return Some(prefix.to_owned());
                    }
                }
            }
            None
        });
    resolve_package_prefixes(
        explicit.as_deref(),
        std::env::var_os("AOS_PREFIX_PATH").as_deref(),
        installed.as_deref(),
        cwd,
        std::env::var_os(aos_launch::PROCESS_CONFIG_ENV).is_some(),
    )
    .map_err(runtime_error)
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| CliError::input(format!("{field} must be a nonempty string")))
}

fn read_plan(path: &Path) -> Result<(LaunchSpec, PathBuf)> {
    let path = fs::canonicalize(path).map_err(|e| io_error(path, e))?;
    if !fs::metadata(&path)
        .map_err(|e| io_error(&path, e))?
        .is_file()
    {
        return Err(io_error(&path, "launch input must be a regular file"));
    }
    let mut contents = String::new();
    fs::File::open(&path)
        .map_err(|e| io_error(&path, e))?
        .take(aos_launch::MAX_DOCUMENT_BYTES as u64 + 1)
        .read_to_string(&mut contents)
        .map_err(|e| io_error(&path, e))?;
    let spec = aos_launch::parse_launch(&contents).map_err(runtime_error)?;
    let document: Value =
        serde_json::from_str(&contents).map_err(|error| CliError::input(error.to_string()))?;
    let schema = definitions()["launch"].clone();
    let validator =
        jsonschema::validator_for(&schema).map_err(|error| CliError::input(error.to_string()))?;
    if let Some(error) = validator.iter_errors(&document).next() {
        return Err(CliError::input(format!("launch document: {error}")));
    }
    Ok((spec, path.parent().unwrap().to_owned()))
}

fn prepare_plan(
    group: &str,
    request: &Value,
    index: &PackageIndex,
    cwd: &Path,
) -> Result<aos_launch::LaunchPlan> {
    let (mut spec, base) = if group == "run" {
        let mut process = request.clone();
        for name in [
            "prefixes",
            "context",
            "shutdown",
            "overrides",
            "security_profile",
            "client_node",
        ] {
            process.as_object_mut().unwrap().remove(name);
        }
        if process.get("id").is_none() {
            process["id"] = json!("run");
        }
        let mut spec = json!({"schema_version":1,"processes":[process]});
        for name in ["context", "shutdown"] {
            if let Some(value) = request.get(name) {
                spec[name] = value.clone();
            }
        }
        (
            aos_launch::parse_launch(&spec.to_string()).map_err(runtime_error)?,
            cwd.to_owned(),
        )
    } else if let Some(plan) = request.get("plan") {
        (
            aos_launch::parse_launch(&plan.to_string()).map_err(runtime_error)?,
            cwd.to_owned(),
        )
    } else if let Some(path) = request.get("path") {
        read_plan(&absolute(Path::new(path.as_str().unwrap()), cwd))?
    } else {
        let package = required(request, "package")?;
        index
            .package(package, request["package_version"].as_str())
            .map_err(runtime_error)?;
        let resource = index
            .resource(package, required(request, "resource")?)
            .map_err(runtime_error)?;
        if resource.kind != ResourceKind::Launch {
            return Err(CliError::input(
                "the selected package resource is not a launch document",
            ));
        }
        read_plan(&resource.path)?
    };
    if let Some(context) = &mut spec.context {
        if let Some(path) = &mut context.zenoh_config {
            *path = absolute(path, &base);
        }
    }
    for process in &mut spec.processes {
        if let Some(path) = &mut process.cwd {
            *path = absolute(path, &base);
        }
        for path in &mut process.config_files {
            *path = absolute(path, &base);
        }
        if let Some(value) = process.env.get_mut("AOS_ZENOH_CONFIG") {
            if !value.is_empty() {
                *value = absolute(Path::new(value), &base)
                    .to_string_lossy()
                    .into_owned();
            }
        }
        if let Some(value) = process.env.get_mut("AOS_INTERFACE_PATH") {
            if !value.is_empty() {
                let paths = std::env::split_paths(value.as_str())
                    .map(|path| {
                        if path.as_os_str().is_empty() {
                            Err(CliError::input(
                                "AOS_INTERFACE_PATH must not contain empty entries",
                            ))
                        } else {
                            Ok(absolute(&path, &base))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                *value = std::env::join_paths(paths)
                    .map_err(|error| CliError::input(error.to_string()))?
                    .into_string()
                    .map_err(|_| CliError::input("AOS_INTERFACE_PATH must be UTF-8"))?;
            }
        }
    }
    let mut overrides: LaunchOverrides = serde_json::from_value(
        request
            .get("overrides")
            .cloned()
            .unwrap_or_else(|| json!({})),
    )
    .map_err(|e| CliError::input(format!("overrides: {e}")))?;
    if let Some(path) = overrides
        .context
        .get("zenoh_config")
        .and_then(Value::as_str)
    {
        overrides.context["zenoh_config"] = json!(absolute(Path::new(path), cwd));
    }
    aos_launch::prepare(index, spec, overrides).map_err(runtime_error)
}

/// Dispatch before InvocationConfig parsing: package commands never open a
/// session; a run without readiness also does not create a control session.
pub fn execute(group: &str, verb: &str, request: &Value) -> Result<Option<Value>> {
    let schema =
        request_schema(group, verb).ok_or_else(|| CliError::input("unknown process command"))?;
    let validator =
        jsonschema::validator_for(&schema).map_err(|e| CliError::input(e.to_string()))?;
    if let Some(error) = validator.iter_errors(request).next() {
        return Err(CliError::input(error.to_string()));
    }
    let cwd = std::env::current_dir().map_err(|e| io_error(Path::new("."), e))?;
    phase("package_index");
    let paths = prefixes(request, &cwd)?;
    let index = PackageIndex::discover(&paths).map_err(runtime_error)?;
    if group == "package" {
        let data = match verb {
            "list" => json!({"prefixes":index.prefixes(),"packages":index.packages()}),
            "info" => json!(index
                .package(
                    required(request, "package")?,
                    request["package_version"].as_str()
                )
                .map_err(runtime_error)?),
            "resource" => {
                let package = required(request, "package")?;
                index
                    .package(package, request["package_version"].as_str())
                    .map_err(runtime_error)?;
                json!(index
                    .resource(package, required(request, "resource")?)
                    .map_err(runtime_error)?)
            }
            _ => unreachable!(),
        };
        return Ok(Some(data));
    }
    phase("launch_preflight");
    let plan = prepare_plan(group, request, &index, &cwd)?;
    #[cfg(not(any(unix, windows)))]
    let _ = &plan;
    #[cfg(not(any(unix, windows)))]
    return Err(CliError {
        code: "UNSUPPORTED",
        message: "process groups require macOS or Linux".into(),
        exit: 2,
    });
    #[cfg(any(unix, windows))]
    {
        begin_output()?;
        let mut sink = OUTPUT
            .get()
            .unwrap()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut probes = CoreProbeFactory::from_request(request)?;
        probes.validate_children(&plan)?;
        let mut supervisor = Supervisor::start(plan, Some(&mut probes)).map_err(runtime_error)?;
        // Setup may wait for a network session, but it cannot spawn children.
        // Preserve default signal termination during that wait; install our
        // cleanup handlers before the first poll is permitted to spawn.
        let _signals = Signals::install()?;
        phase("supervision");
        let result = loop {
            let signal = RECEIVED_SIGNAL.load(std::sync::atomic::Ordering::Acquire);
            if signal != 0 {
                supervisor.request_stop(StopReason::Signal(signal));
            }
            if let Err(error) = sink.stdout.check() {
                supervisor.request_stop(StopReason::Failure(LaunchError::new(
                    LaunchErrorCode::OutputIoError,
                    "output",
                    error.to_string(),
                )));
            }
            match supervisor.poll(Duration::from_millis(10), &mut *sink) {
                Ok(RunProgress::Finished) => break Ok(()),
                Ok(_) => {}
                Err(error) => break Err(error),
            }
        };
        let summary = supervisor.summary();
        // Drop also closes the probe if a future poll error takes an early
        // path. Drain its diagnostics before propagating any output failure.
        drop(supervisor);
        sink.complete_supervision(result, probes.take_diagnostics(), &summary)?;
        if summary.outcome == RunOutcome::Failed {
            return Err(match summary.stop_reason {
                Some(StopReason::Failure(error)) => runtime_error(error),
                _ => CliError {
                    code: "LAUNCH_FAILED",
                    message: "launch failed; inspect summary".into(),
                    exit: 1,
                },
            });
        }
        if let Some(StopReason::Signal(signal)) = summary.stop_reason {
            return Err(CliError {
                code: "INTERRUPTED",
                message: format!("stopped by signal {signal}"),
                exit: 128 + signal,
            });
        }
        Ok(None)
    }
}

#[cfg(any(unix, windows))]
static RECEIVED_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// Read the first signal independently of the guard's owner. A command may
/// return while main still needs its handlers during bounded terminal output.
#[cfg(any(unix, windows))]
pub(crate) fn received_signal() -> i32 {
    RECEIVED_SIGNAL.load(std::sync::atomic::Ordering::Acquire)
}

#[cfg(unix)]
extern "C" fn receive_signal(signal: libc::c_int) {
    let _ = RECEIVED_SIGNAL.compare_exchange(
        0,
        signal,
        std::sync::atomic::Ordering::AcqRel,
        std::sync::atomic::Ordering::Acquire,
    );
}

#[cfg(unix)]
pub(crate) struct Signals(Vec<(libc::c_int, libc::sigaction)>);

#[cfg(unix)]
impl Signals {
    pub(crate) fn install() -> Result<Self> {
        let mut guard = Self(Vec::new());
        RECEIVED_SIGNAL.store(0, std::sync::atomic::Ordering::Release);
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT] {
            // Install before spawning children. The handler only stores a
            // lock-free atomic; all process and IO work stays in the main loop.
            let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
            let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
            action.sa_sigaction = receive_signal as *const () as usize;
            unsafe {
                libc::sigemptyset(&mut action.sa_mask);
            }
            if unsafe { libc::sigaction(signal, &action, &mut old) } != 0 {
                return Err(CliError {
                    code: "PROCESS_SETUP_ERROR",
                    message: io::Error::last_os_error().to_string(),
                    exit: 1,
                });
            }
            guard.0.push((signal, old));
        }
        Ok(guard)
    }

    pub(crate) fn received(&self) -> i32 {
        received_signal()
    }
}

#[cfg(unix)]
impl Drop for Signals {
    fn drop(&mut self) {
        for (signal, action) in self.0.iter().rev() {
            unsafe {
                libc::sigaction(*signal, action, std::ptr::null_mut());
            }
        }
    }
}

#[cfg(windows)]
pub(crate) struct Signals;

#[cfg(windows)]
impl Signals {
    pub(crate) fn received(&self) -> i32 {
        received_signal()
    }
    pub(crate) fn install() -> Result<Self> {
        aos_launch::windows_process::ensure_console().map_err(|e| CliError {
            code: "PROCESS_SETUP_ERROR",
            message: e.to_string(),
            exit: 1,
        })?;
        RECEIVED_SIGNAL.store(0, std::sync::atomic::Ordering::Release);
        ctrlc::set_handler(|| {
            let _ = RECEIVED_SIGNAL.compare_exchange(
                0,
                2,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
        })
        .map_err(|_| CliError {
            code: "PROCESS_SETUP_ERROR",
            message: "could not install the Windows console interrupt handler".into(),
            exit: 1,
        })?;
        Ok(Self)
    }
}

// Only this CLI module owns blocking writer threads. A stalled kernel write
// cannot pin the supervisor: bounded queues and deadlines stay on the caller.
// Normal drain joins; an irrecoverably blocked writer dies with this CLI process.
#[cfg(any(unix, windows))]
const OUTPUT_CAPACITY: usize = 4 * 1024 * 1024;
#[cfg(any(unix, windows))]
const OUTPUT_STALL: Duration = Duration::from_secs(30);
#[cfg(any(unix, windows))]
const TERMINAL_OUTPUT_BUDGET: Duration = Duration::from_secs(2);
#[cfg(any(unix, windows))]
static OUTPUT: std::sync::OnceLock<std::sync::Mutex<JsonSink>> = std::sync::OnceLock::new();

#[cfg(any(unix, windows))]
struct WriterState {
    queue: std::collections::VecDeque<Vec<u8>>,
    bytes: usize,
    records: usize,
    offset: usize,
    full_since: Option<std::time::Instant>,
    closing: bool,
    abort: bool,
    error: Option<(io::ErrorKind, String)>,
}

#[cfg(any(unix, windows))]
struct BoundedWriter {
    shared: std::sync::Arc<(std::sync::Mutex<WriterState>, std::sync::Condvar)>,
    worker: Option<std::thread::JoinHandle<()>>,
    capacity: usize,
    stall_budget: Duration,
}

#[cfg(any(unix, windows))]
impl BoundedWriter {
    fn new(fd: i32, capacity: usize) -> io::Result<Self> {
        Self::with_stall_budget(fd, capacity, OUTPUT_STALL)
    }
    fn with_stall_budget(fd: i32, capacity: usize, stall_budget: Duration) -> io::Result<Self> {
        #[cfg(unix)]
        let mut output = {
            use std::os::fd::FromRawFd;
            // dup preserves the descriptor flags without modifying the shared open
            // file description. No O_NONBLOCK is ever set on stdout/stderr.
            let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
            if copy < 0 {
                return Err(io::Error::last_os_error());
            }
            unsafe { std::fs::File::from_raw_fd(copy) }
        };
        // On Windows only the private writer thread touches this handle. A
        // blocked OS write cannot delay Router shutdown or terminal deadlines.
        #[cfg(windows)]
        let mut output = aos_launch::windows_process::duplicate_standard_output(fd)?;
        let shared = std::sync::Arc::new((
            std::sync::Mutex::new(WriterState {
                queue: std::collections::VecDeque::new(),
                bytes: 0,
                records: 0,
                offset: 0,
                full_since: None,
                closing: false,
                abort: false,
                error: None,
            }),
            std::sync::Condvar::new(),
        ));
        let state = shared.clone();
        let worker = std::thread::spawn(move || {
            let (mutex, available) = &*state;
            loop {
                let line = {
                    let mut locked = mutex.lock().unwrap_or_else(|e| e.into_inner());
                    while locked.queue.is_empty() && !locked.closing && !locked.abort {
                        locked = available.wait(locked).unwrap_or_else(|e| e.into_inner());
                    }
                    if locked.abort || (locked.queue.is_empty() && locked.closing) {
                        return;
                    }
                    locked.offset = 0;
                    locked.queue.pop_front().unwrap()
                };
                let mut offset = 0;
                while offset < line.len() {
                    let write = output.write(&line[offset..(offset + 4096).min(line.len())]);
                    let mut locked = mutex.lock().unwrap_or_else(|e| e.into_inner());
                    if locked.abort {
                        return;
                    }
                    match write {
                        Ok(0) => {
                            locked.error = Some((
                                io::ErrorKind::WriteZero,
                                "output stream made no progress".into(),
                            ));
                            available.notify_all();
                            return;
                        }
                        Ok(count) => {
                            offset += count;
                            locked.offset = offset;
                            locked.bytes -= count;
                            if offset == line.len() {
                                locked.records -= 1;
                            }
                            locked.full_since = None;
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            // Respect a descriptor that was already nonblocking
                            // when inherited, without changing its shared flags.
                            drop(locked);
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => {
                            locked.error = Some((error.kind(), error.to_string()));
                            available.notify_all();
                            return;
                        }
                    }
                    available.notify_all();
                }
            }
        });
        Ok(Self {
            shared,
            worker: Some(worker),
            capacity,
            stall_budget,
        })
    }
    fn health(&self, locked: &WriterState) -> io::Result<()> {
        if let Some((kind, message)) = &locked.error {
            return Err(io::Error::new(*kind, message.clone()));
        }
        if locked
            .full_since
            .is_some_and(|full| full.elapsed() >= self.stall_budget)
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "output queue remained full without write progress for {} milliseconds (current line offset {})",
                    self.stall_budget.as_millis(),
                    locked.offset
                ),
            ));
        }
        Ok(())
    }
    fn check(&self) -> io::Result<()> {
        self.health(&self.shared.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
    fn enqueue(&self, line: Vec<u8>) -> io::Result<()> {
        let mut locked = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        self.health(&locked)?;
        if line.len() > self.capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "JSONL frame exceeds output capacity",
            ));
        }
        if locked.bytes + line.len() > self.capacity {
            locked
                .full_since
                .get_or_insert_with(std::time::Instant::now);
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "bounded output queue is full",
            ));
        }
        locked.bytes += line.len();
        locked.records += 1;
        locked.queue.push_back(line);
        self.shared.1.notify_one();
        Ok(())
    }
    fn enqueue_until(&self, line: &[u8], deadline: std::time::Instant) -> io::Result<()> {
        loop {
            match self.enqueue(line.to_vec()) {
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "terminal output deadline expired",
                    ));
                }
                result => return result,
            }
        }
    }
    fn finish(&mut self, deadline: std::time::Instant) -> io::Result<()> {
        {
            let mut locked = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
            locked.closing = true;
            self.shared.1.notify_one();
        }
        while self
            .worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
        {
            self.check()?;
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "terminal output deadline expired",
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        self.check()?;
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("output worker panicked"))?;
        }
        Ok(())
    }
    fn abort(&mut self) -> usize {
        let mut locked = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        locked.abort = true;
        let unfinished = std::mem::take(&mut locked.records);
        locked.queue.clear();
        self.shared.1.notify_one();
        // Do not join a thread inside a blocking OS write. This private CLI
        // worker has no external child/process and ends at immediate CLI exit.
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.is_finished())
        {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
        unfinished
    }
}

#[cfg(any(unix, windows))]
impl Drop for BoundedWriter {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(any(unix, windows))]
struct JsonSink {
    stdout: BoundedWriter,
    stderr: Option<BoundedWriter>,
    dropped_logs: u64,
    launch_id: Option<String>,
    elapsed_ms: u64,
    summary: Option<Vec<u8>>,
}

fn json_line(value: &impl serde::Serialize) -> io::Result<Vec<u8>> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    Ok(line)
}

fn output_error(error: io::Error) -> CliError {
    CliError {
        code: "OUTPUT_ERROR",
        message: error.to_string(),
        exit: 1,
    }
}

#[cfg(any(unix, windows))]
impl JsonSink {
    fn new() -> Result<Self> {
        Ok(Self {
            stdout: BoundedWriter::new(1, OUTPUT_CAPACITY).map_err(output_error)?,
            stderr: BoundedWriter::new(2, 1024 * 1024).ok(),
            dropped_logs: 0,
            launch_id: None,
            elapsed_ms: 0,
            summary: None,
        })
    }
    fn dropped_line(&self) -> io::Result<Vec<u8>> {
        json_line(&json!({"schema_version":1,"launch_id":self.launch_id,
            "process_id":null,"generation":null,"pid":null,"pgid":null,
            "event":"output_dropped","elapsed_ms":self.elapsed_ms,
            "details":{"stream":"stderr","lines":self.dropped_logs,"reason":"diagnostic_backpressure"}}))
    }
    #[cfg(any(unix, windows))]
    fn diagnostic(&mut self, diagnostic: Value) -> Result<()> {
        let line =
            json_line(&json!({"schema_version":1,"event":"probe_diagnostic","data":diagnostic}))
                .map_err(output_error)?;
        if self
            .stderr
            .as_ref()
            .is_none_or(|writer| writer.enqueue(line).is_err())
        {
            self.dropped_logs = self.dropped_logs.saturating_add(1);
        }
        Ok(())
    }
    #[cfg(any(unix, windows))]
    fn complete_supervision(
        &mut self,
        result: aos_launch::LaunchResult<()>,
        diagnostics: Vec<Value>,
        summary: &aos_launch::RunSummary,
    ) -> Result<()> {
        // Preserve the final state before propagating poll or diagnostic
        // errors. finish_output still attempts summary/error delivery within
        // its existing shared two-second terminal deadline.
        operation_field("launch_summary", json!(summary));
        self.summary = Some(
            json_line(&json!({"schema_version":1,"event":"summary","data":summary}))
                .map_err(output_error)?,
        );
        for diagnostic in diagnostics {
            self.diagnostic(diagnostic)?;
        }
        result.map_err(runtime_error)
    }
    fn report_dropped(&mut self) -> io::Result<()> {
        if self.dropped_logs > 0 {
            self.stdout.enqueue(self.dropped_line()?)?;
            self.dropped_logs = 0;
        }
        Ok(())
    }
    fn finish(&mut self, error: Option<&CliError>, mut context: Value) -> Result<()> {
        let deadline = std::time::Instant::now() + TERMINAL_OUTPUT_BUDGET;
        if let Some(stderr) = &mut self.stderr {
            // Leave almost all of the shared deadline for terminal control
            // output. Count accepted diagnostic records that never completed.
            if stderr
                .finish(std::time::Instant::now() + Duration::from_millis(100))
                .is_err()
            {
                self.dropped_logs = self.dropped_logs.saturating_add(stderr.abort() as u64);
            }
        }
        let result = (|| -> io::Result<()> {
            if self.dropped_logs > 0 {
                self.stdout.enqueue_until(&self.dropped_line()?, deadline)?;
                self.dropped_logs = 0;
            }
            if let Some(summary) = self.summary.take() {
                self.stdout.enqueue_until(&summary, deadline)?;
            }
            if let Some(error) = error {
                context["code"] = json!(error.code);
                context["message"] = json!(error.message);
                self.stdout.enqueue_until(
                    &json_line(&json!({"schema_version":1,"event":"error","data":context}))?,
                    deadline,
                )?;
            }
            self.stdout.finish(deadline)
        })();
        if result.is_err() {
            self.stdout.abort();
        }
        result.map_err(output_error)
    }
}

#[cfg(any(unix, windows))]
impl EventSink for JsonSink {
    fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
        self.launch_id = Some(event.launch_id.clone());
        // Use the supervisor's event clock, including during final drain.
        // The writer's startup/network-open time is not the launch time base.
        self.elapsed_ms = event.elapsed_ms;
        self.report_dropped()?;
        // Preserve RawValue numeric precision. Never pass a LaunchEvent through Value.
        self.stdout.enqueue(json_line(event)?)
    }
    fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
        self.launch_id = Some(record.identity.launch_id.clone());
        let line = json_line(&json!({"schema_version":1,"event":"process_log","data":record}))?;
        if self
            .stderr
            .as_ref()
            .is_none_or(|writer| writer.enqueue(line).is_err())
        {
            self.dropped_logs = self.dropped_logs.saturating_add(1);
        }
        Ok(())
    }
}

#[cfg(any(unix, windows))]
pub fn begin_output() -> Result<()> {
    if OUTPUT.get().is_none() {
        let _ = OUTPUT.set(std::sync::Mutex::new(JsonSink::new()?));
    }
    Ok(())
}

#[cfg(any(unix, windows))]
pub fn finish_output(error: Option<&CliError>, context: Value) -> Result<()> {
    OUTPUT
        .get()
        .ok_or_else(|| CliError::input("process output was not initialized"))?
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .finish(error, context)
}

/// Share the bounded JSONL writer with bag record/play. False is backpressure:
/// the caller retains this event and continues servicing stop/worker state.
#[cfg(any(unix, windows))]
pub fn try_stream_event(value: &Value) -> Result<bool> {
    let sink = OUTPUT
        .get()
        .ok_or_else(|| CliError::input("stream output was not initialized"))?
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    match sink.stdout.enqueue(json_line(value).map_err(output_error)?) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(output_error(error)),
    }
}

/// Check permanent writer failure while a long-running command has no events.
#[cfg(any(unix, windows))]
pub fn check_stream_output() -> Result<()> {
    OUTPUT
        .get()
        .ok_or_else(|| CliError::input("stream output was not initialized"))?
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .stdout
        .check()
        .map_err(output_error)
}

#[cfg(any(unix, windows))]
pub fn set_stream_summary(value: &Value) -> Result<()> {
    OUTPUT
        .get()
        .ok_or_else(|| CliError::input("stream output was not initialized"))?
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .summary = Some(json_line(value).map_err(output_error)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aos_launch::{LaunchEventKind, ProcessExit, ProcessSummary, RunSummary};

    #[cfg(unix)]
    fn filled_output() -> (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ) {
        let (reader, mut writer) = std::os::unix::net::UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        loop {
            match writer.write(&[b'x'; 4096]) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("{error}"),
            }
        }
        (reader, writer)
    }

    #[test]
    #[cfg(unix)]
    fn pending_output_needs_full_queue_and_continuous_stall_before_failure() {
        use std::os::fd::AsRawFd;
        let (_reader, stream) = filled_output();
        let writer =
            BoundedWriter::with_stall_budget(stream.as_raw_fd(), 16384, Duration::from_millis(40))
                .unwrap();
        writer.enqueue(vec![b'a'; 4096]).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        writer.check().unwrap();
        assert_eq!(
            writer.enqueue(vec![b'b'; 16384]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        writer.check().unwrap();
        std::thread::sleep(Duration::from_millis(60));
        let error = writer.check().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(error.to_string().contains("queue remained full"));
    }

    #[test]
    #[cfg(unix)]
    fn successful_output_progress_resets_the_full_queue_deadline() {
        use std::os::fd::AsRawFd;
        let (mut reader, stream) = filled_output();
        reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let writer =
            BoundedWriter::with_stall_budget(stream.as_raw_fd(), 16384, Duration::from_millis(120))
                .unwrap();
        writer.enqueue(vec![b'a'; 8192]).unwrap();
        assert_eq!(
            writer.enqueue(vec![b'b'; 16384]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(80));
        let mut buffer = [0; 8192];
        reader.read_exact(&mut buffer).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let state = writer.shared.0.lock().unwrap();
            if state.bytes == 0 {
                assert!(state.full_since.is_none());
                break;
            }
            drop(state);
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        writer.enqueue(vec![b'b'; 16384]).unwrap();
        assert_eq!(
            writer.enqueue(vec![b'c'; 16384]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(80));
        writer.check().unwrap();
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(writer.check().unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    #[cfg(unix)]
    fn synthetic_dropped_records_use_the_supervisors_clock_and_launch_id() {
        use std::os::fd::AsRawFd;
        let (_reader, stream) = std::os::unix::net::UnixStream::pair().unwrap();
        let mut sink = JsonSink {
            stdout: BoundedWriter::new(stream.as_raw_fd(), OUTPUT_CAPACITY).unwrap(),
            stderr: None,
            dropped_logs: 1,
            launch_id: None,
            elapsed_ms: 0,
            summary: None,
        };
        let event = LaunchEvent {
            schema_version: 1,
            launch_id: "owned-launch".into(),
            process_id: Some("worker".into()),
            generation: Some(1),
            event: LaunchEventKind::ProcessStarted,
            elapsed_ms: 73,
            pid: Some(42),
            pgid: Some(42),
            details: serde_json::value::RawValue::from_string("{}".into()).unwrap(),
        };
        sink.emit(&event).unwrap();
        sink.dropped_logs = 2;
        let line: Value = serde_json::from_slice(&sink.dropped_line().unwrap()).unwrap();
        assert_eq!(line["elapsed_ms"], 73);
        assert_eq!(line["launch_id"], "owned-launch");
        assert_eq!(line["details"]["lines"], 2);
    }

    #[test]
    #[cfg(unix)]
    fn output_failure_still_emits_probe_close_diagnostics_to_stderr() {
        use std::io::BufRead;
        use std::os::fd::AsRawFd;
        let (reader, stream) = std::os::unix::net::UnixStream::pair().unwrap();
        drop(reader);
        let (stderr_reader, stderr_stream) = std::os::unix::net::UnixStream::pair().unwrap();
        stderr_reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut sink = JsonSink {
            stdout: BoundedWriter::new(stream.as_raw_fd(), OUTPUT_CAPACITY).unwrap(),
            stderr: Some(BoundedWriter::new(stderr_stream.as_raw_fd(), 4096).unwrap()),
            dropped_logs: 0,
            launch_id: None,
            elapsed_ms: 0,
            summary: None,
        };
        let error = sink
            .complete_supervision(
                Err(LaunchError::new(
                    LaunchErrorCode::OutputIoError,
                    "output",
                    "original broken stdout",
                )),
                vec![json!({"code":"PROBE_CLOSE_TIMEOUT","budget_seconds":1})],
                &failed_summary(),
            )
            .unwrap_err();
        assert_eq!(error.code, "OUTPUT_ERROR");
        assert_eq!(error.message, "output: original broken stdout");
        sink.stderr
            .as_mut()
            .unwrap()
            .finish(std::time::Instant::now() + Duration::from_secs(1))
            .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(stderr_reader)
            .read_line(&mut line)
            .unwrap();
        let record: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(record["event"], "probe_diagnostic");
        assert_eq!(record["data"]["code"], "PROBE_CLOSE_TIMEOUT");
    }

    #[cfg(unix)]
    fn failed_summary() -> RunSummary {
        RunSummary {
            launch_id: "failed-launch".into(),
            outcome: RunOutcome::Failed,
            stop_reason: Some(StopReason::Signal(libc::SIGTERM)),
            processes: vec![ProcessSummary {
                process_id: "writer".into(),
                generations: 1,
                restarts: 0,
                ready: false,
                exit: Some(ProcessExit {
                    code: Some(7),
                    signal: None,
                }),
            }],
            cleanup_errors: vec![LaunchError::new(
                LaunchErrorCode::CleanupFailed,
                "cleanup",
                "pipe did not close",
            )],
        }
    }

    #[test]
    #[cfg(unix)]
    fn poll_output_error_still_delivers_summary_when_stdout_is_writable() {
        use std::io::BufRead;
        use std::os::fd::AsRawFd;
        let (reader, stream) = std::os::unix::net::UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut sink = JsonSink {
            stdout: BoundedWriter::new(stream.as_raw_fd(), OUTPUT_CAPACITY).unwrap(),
            stderr: None,
            dropped_logs: 0,
            launch_id: None,
            elapsed_ms: 0,
            summary: None,
        };
        let summary = failed_summary();
        let error = sink
            .complete_supervision(
                Err(LaunchError::new(
                    LaunchErrorCode::OutputIoError,
                    "output",
                    "absolute shutdown output-drain deadline exceeded",
                )),
                vec![],
                &summary,
            )
            .unwrap_err();
        assert_eq!(error.code, "OUTPUT_ERROR");
        sink.finish(Some(&error), json!({})).unwrap();
        let mut reader = std::io::BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let record: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(record["event"], "summary");
        assert_eq!(record["data"], json!(summary));
        line.clear();
        reader.read_line(&mut line).unwrap();
        let record: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(record["event"], "error");
        assert_eq!(record["data"]["code"], "OUTPUT_ERROR");
    }

    #[test]
    fn described_output_validates_real_events_summary_and_errors() {
        let schema = output_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let event = LaunchEvent {
            schema_version: 1,
            launch_id: "launch".into(),
            process_id: Some("worker".into()),
            generation: Some(1),
            event: LaunchEventKind::ProcessStdout,
            elapsed_ms: 12,
            pid: Some(42),
            pgid: Some(42),
            details: serde_json::value::RawValue::from_string(
                r#"{"value":{"message":"business"}}"#.into(),
            )
            .unwrap(),
        };
        let summary = RunSummary {
            launch_id: "launch".into(),
            outcome: RunOutcome::Stopped,
            stop_reason: Some(StopReason::Signal(libc::SIGINT)),
            processes: vec![ProcessSummary {
                process_id: "worker".into(),
                generations: 1,
                restarts: 0,
                ready: false,
                exit: Some(ProcessExit {
                    code: None,
                    signal: Some(libc::SIGINT),
                }),
            }],
            cleanup_errors: vec![],
        };
        for actual in [
            serde_json::to_value(event).unwrap(),
            json!({"schema_version":1,"event":"summary","data":summary}),
            json!({"schema_version":1,"event":"error","data":{
                "code":"INTERRUPTED","message":"stopped","phase":"supervision"
            }}),
        ] {
            assert!(validator.is_valid(&actual), "{actual}");
        }
        assert!(!validator.is_valid(&json!({"event":"process_started"})));
        assert!(!validator.is_valid(&json!({"schema_version":1,"event":"summary","data":{}})));
    }

    #[test]
    fn described_request_limits_match_the_runtime_contract() {
        let schema = request_schema("run", "start").unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        for invalid in [
            json!({"package":"demo","entry":"worker","restart":{"max_restarts":4294967295u64}}),
            json!({"package":"demo","entry":"worker","readiness":{"node":"/worker",
                "lifecycle":{"target":"inactive","autostart":true}}}),
            json!({"package":"demo","entry":"worker","readiness":{"node":"/worker","timeout":86401}}),
            json!({"package":"demo","entry":"worker","context":{"domain_id":0}}),
        ] {
            assert!(!validator.is_valid(&invalid), "{invalid}");
        }
        assert!(
            validator.is_valid(&json!({"package":"demo","entry":"worker",
            "context":{"domain_id":0,"zenoh_config":null},
            "restart":{"max_restarts":4294967294u64},
            "readiness":{"node":"/worker","timeout":86400,
                "lifecycle":{"target":"active","autostart":true}}}))
        );
    }
}
