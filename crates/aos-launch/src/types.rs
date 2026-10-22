use std::{collections::BTreeMap, io, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{LaunchError, LaunchResult};

pub const SCHEMA_VERSION: u32 = 1;
pub const RUNTIME_ABI: &str = "aos.process.v1";
pub const PROCESS_CONFIG_ENV: &str = "AOS_PROCESS_CONFIG";
pub const PREFIX_PATH_ENV: &str = "AOS_PREFIX_PATH";
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
pub const MAX_OUTPUT_LINE_BYTES: usize = 64 * 1024;
/// Per-readiness/backoff and combined shutdown ceiling, in steady seconds.
pub const MAX_WAIT_SECONDS: f64 = 86_400.0;

fn object() -> Value {
    json!({})
}
fn readiness_timeout() -> f64 {
    30.0
}
fn shutdown_timeout() -> f64 {
    5.0
}
fn restart_delay() -> f64 {
    1.0
}
fn restart_limit() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchSpec {
    pub schema_version: u32,
    /// A shared explicit network selection is required when readiness is used.
    #[serde(default)]
    pub context: Option<LaunchContextSpec>,
    pub processes: Vec<ProcessSpec>,
    #[serde(default)]
    pub shutdown: ShutdownSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchContextSpec {
    pub domain_id: u32,
    pub zenoh_config: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSpec {
    pub id: String,
    pub package: String,
    pub entry: String,
    #[serde(default)]
    pub package_version: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub config_files: Vec<PathBuf>,
    #[serde(default = "object")]
    pub node: Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub readiness: Option<ReadinessSpec>,
    #[serde(default)]
    pub restart: RestartSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LaunchOverrides {
    /// Explicit CLI network overrides, above the shared launch context.
    pub context: Value,
    /// Explicit CLI node overrides, selected by process id.
    pub nodes: BTreeMap<String, Value>,
}

impl Default for LaunchOverrides {
    fn default() -> Self {
        Self {
            context: object(),
            nodes: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessSpec {
    pub node: String,
    #[serde(default = "readiness_timeout")]
    pub timeout: f64,
    #[serde(default)]
    pub endpoints: Vec<EndpointRequirement>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleReadiness>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointRequirement {
    pub kind: ReadyEndpointKind,
    pub name: String,
    pub interface: ResourceRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    pub package: String,
    pub resource: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadyEndpointKind {
    Publisher,
    Subscription,
    Service,
    Action,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleReadiness {
    pub target: ReadyLifecycleState,
    #[serde(default)]
    pub autostart: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadyLifecycleState {
    Unconfigured,
    Inactive,
    Active,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    #[default]
    Never,
    OnFailure,
    Always,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartSpec {
    #[serde(default)]
    pub policy: RestartPolicy,
    #[serde(default = "restart_delay")]
    pub delay: f64,
    #[serde(default = "restart_limit")]
    pub max_restarts: u32,
}

impl Default for RestartSpec {
    fn default() -> Self {
        Self {
            policy: RestartPolicy::Never,
            delay: restart_delay(),
            max_restarts: restart_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownSpec {
    #[serde(default = "shutdown_timeout")]
    pub sigint_timeout: f64,
    #[serde(default = "shutdown_timeout")]
    pub sigterm_timeout: f64,
}

impl Default for ShutdownSpec {
    fn default() -> Self {
        Self {
            sigint_timeout: shutdown_timeout(),
            sigterm_timeout: shutdown_timeout(),
        }
    }
}

/// The v1 file written for each generation. No program or environment layer
/// is folded into these deployment sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessBootstrap {
    pub schema_version: u32,
    pub launch_id: String,
    pub process_id: String,
    pub generation: u32,
    pub context: DeploymentLayers,
    pub node: DeploymentLayers,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeploymentLayers {
    pub config_files: Vec<String>,
    pub launch: Value,
    pub cli: Value,
}

impl Default for DeploymentLayers {
    fn default() -> Self {
        Self {
            config_files: Vec::new(),
            launch: object(),
            cli: object(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedLaunchContext {
    pub domain_id: u32,
    pub zenoh_config: Option<PathBuf>,
    pub effective_config: Value,
}

#[derive(Debug, Clone)]
pub struct ResolvedReadiness {
    pub node: String,
    pub timeout: Duration,
    pub endpoints: Vec<ResolvedReadyEndpoint>,
    pub lifecycle: Option<LifecycleReadiness>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedReadyEndpoint {
    pub kind: ReadyEndpointKind,
    pub name: String,
    pub type_name: String,
    pub type_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub launch_id: String,
    pub process_id: String,
    pub generation: u32,
    pub pid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessPhase {
    Waiting,
    Discovered,
    Configuring,
    Activating,
    Ready,
}

#[derive(Debug, Clone)]
pub struct ReadinessProgress {
    pub phase: ReadinessPhase,
    pub node_id: Option<String>,
}

/// Nonblocking progress over an owned generation. Long network requests must
/// run in the probe's bounded control worker, not in this method.
pub trait ReadinessProbe: Send {
    #[cfg(windows)]
    fn register_owned_job(
        &mut self,
        _identity: &ProcessIdentity,
        _job: crate::windows_process::OwnedJob,
    ) {
    }
    fn poll(
        &mut self,
        expected: &ProcessIdentity,
        requirement: &ResolvedReadiness,
        remaining: Duration,
    ) -> LaunchResult<ReadinessProgress>;

    /// Discard outstanding observations/operations for an old generation.
    fn forget(&mut self, expected: &ProcessIdentity);

    fn close(&mut self);
}

pub trait ProbeFactory {
    fn create(&mut self, context: &ResolvedLaunchContext) -> LaunchResult<Box<dyn ReadinessProbe>>;
}

/// The caller chooses stdout and stderr writers. `emit` is JSONL control/data;
/// `log` is only the separate diagnostic stream. Neither may be unbounded.
/// `emit(WouldBlock)` retains the event for a later poll; the sink must enforce
/// its own finite stall deadline. Other emit errors stop the launch. A log error
/// drops that diagnostic line and records `output_dropped`, without stopping.
pub trait EventSink {
    fn emit(&mut self, event: &LaunchEvent) -> io::Result<()>;
    fn log(&mut self, record: &ProcessLog) -> io::Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchEventKind {
    ProcessStarted,
    Discovered,
    LifecycleTransition,
    Ready,
    ProcessExited,
    RestartScheduled,
    RestartExhausted,
    DependencyUnavailable,
    DependencyReady,
    ProcessStopping,
    ProcessSignal,
    ProcessStdout,
    OutputTruncated,
    OutputDropped,
    LaunchFailed,
    LaunchFinished,
}

#[derive(Debug, Clone, Serialize)]
pub struct LaunchEvent {
    pub schema_version: u32,
    pub launch_id: String,
    pub process_id: Option<String>,
    pub generation: Option<u32>,
    pub event: LaunchEventKind,
    pub elapsed_ms: u64,
    pub pid: Option<u32>,
    pub pgid: Option<i32>,
    /// Raw JSON preserves child stdout number spellings and precision.
    /// Serialize this event directly; converting it through `Value` may lose
    /// numbers outside serde_json's ordinary integer/floating-point range.
    pub details: Box<serde_json::value::RawValue>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessLog {
    pub identity: ProcessIdentity,
    pub stream: OutputStream,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StopReason {
    Requested,
    Signal(i32),
    Failure(LaunchError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunProgress {
    Running,
    Stopping,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    Running,
    Completed,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessExit {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl ProcessExit {
    pub fn success(&self) -> bool {
        self.code == Some(0) && self.signal.is_none()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessSummary {
    pub process_id: String,
    pub generations: u32,
    pub restarts: u32,
    pub ready: bool,
    pub exit: Option<ProcessExit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub launch_id: String,
    pub outcome: RunOutcome,
    pub stop_reason: Option<StopReason>,
    pub processes: Vec<ProcessSummary>,
    pub cleanup_errors: Vec<LaunchError>,
}
