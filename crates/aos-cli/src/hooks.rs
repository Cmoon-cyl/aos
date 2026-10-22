//! Filters and observation hooks, called by the observation worker, never a
//! transport callback. Conditions inspect the unprojected message. Script
//! stdin receives its whole envelope:
//! `{message, topic, sequence, received_at: {time_domain, time_ns}}`.
//!
//! Missing fields do not equal null. All comparisons, including `ne`, return
//! false for different JSON types; numbers compare numerically without turning
//! large integers into rounded floats. Ordered comparisons require numbers.
//! Every condition leaf is visited so missing-field diagnostics are complete.
//!
//! Scripts run directly in a Unix process group or a Windows Job Object. Nonblocking pipes share
//! one worker thread; there are no detached reader/writer threads. The group is
//! terminated and the direct child reaped before returning, including success.
//! Unix hooks must not daemonize or leave their group. Windows descendants
//! remain in the owned Job even if they start another console group.
//! This controls the hook's local process lifetime, never provider execution.

use std::cmp::Ordering as Comparison;
use std::fmt;
#[cfg(any(unix, windows))]
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Map, Number, Value};

#[cfg(any(unix, windows))]
pub const MAX_STDERR_BYTES: usize = 64 * 1024;
const DEFAULT_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_CONDITION_DEPTH: usize = 64;

pub type Result<T> = std::result::Result<T, HookError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    not(any(unix, windows)),
    expect(
        dead_code,
        reason = "retain the shared hook error contract; script execution requires Unix"
    )
)]
pub enum HookErrorKind {
    InvalidArgument,
    Interrupted,
    OverallDeadline,
    HookTimeout,
    Spawn,
    Io,
    Exit,
    OutputLimit,
    InvalidOutput,
    Reported,
    Cleanup,
    #[cfg(not(any(unix, windows)))]
    UnsupportedPlatform,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookError {
    pub kind: HookErrorKind,
    pub message: String,
    /// Bounded script diagnostics. Never includes a copy of its stdin message.
    pub stderr: String,
}

impl HookError {
    fn new(kind: HookErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            stderr: String::new(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(HookErrorKind::InvalidArgument, message)
    }

    pub fn code(&self) -> &'static str {
        match self.kind {
            HookErrorKind::InvalidArgument => "INVALID_ARGUMENT",
            HookErrorKind::Interrupted => "HOOK_INTERRUPTED",
            HookErrorKind::OverallDeadline => "OVERALL_DEADLINE",
            HookErrorKind::HookTimeout => "HOOK_TIMEOUT",
            HookErrorKind::Spawn => "HOOK_SPAWN_ERROR",
            HookErrorKind::Io => "HOOK_IO_ERROR",
            HookErrorKind::Exit => "HOOK_EXIT_ERROR",
            HookErrorKind::OutputLimit => "HOOK_OUTPUT_LIMIT",
            HookErrorKind::InvalidOutput => "HOOK_INVALID_OUTPUT",
            HookErrorKind::Reported => "HOOK_REPORTED_ERROR",
            HookErrorKind::Cleanup => "HOOK_CLEANUP_ERROR",
            #[cfg(not(any(unix, windows)))]
            HookErrorKind::UnsupportedPlatform => "HOOK_UNSUPPORTED_PLATFORM",
        }
    }
}

impl fmt::Display for HookError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for HookError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Continue,
    Matched,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookOutcome {
    pub decision: Decision,
    pub reason: Option<String>,
    /// Current streak for a condition hook; zero for a script hook.
    pub consecutive_matches: u64,
    /// Missing comparison fields in this message, including all/any branches.
    /// A missing `exists` operand is an ordinary false result, not a diagnostic.
    pub missing_fields: u64,
    /// Bounded stderr from a completed script; empty for a condition hook.
    pub stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConditionOutcome {
    pub matched: bool,
    pub missing_fields: u64,
}

#[derive(Debug, Clone)]
pub struct Condition(ConditionNode);

#[derive(Debug, Clone)]
enum ConditionNode {
    Compare {
        path: String,
        operation: Operation,
        expected: Value,
    },
    Exists(String),
    All(Vec<Condition>),
    Any(Vec<Condition>),
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Condition {
    pub fn parse(value: &Value) -> Result<Self> {
        Self::parse_at(value, 0)
    }

    fn parse_at(value: &Value, depth: usize) -> Result<Self> {
        if depth >= MAX_CONDITION_DEPTH {
            return Err(HookError::invalid("condition nesting exceeds 64 levels"));
        }
        let fields = object(value, "condition")?;
        if fields.contains_key("all") || fields.contains_key("any") {
            let key = if fields.contains_key("all") {
                "all"
            } else {
                "any"
            };
            only_fields(fields, &[key], "condition")?;
            let children = fields[key]
                .as_array()
                .filter(|children| !children.is_empty())
                .ok_or_else(|| HookError::invalid(format!("{key} must be a nonempty array")))?;
            let children = children
                .iter()
                .map(|child| Self::parse_at(child, depth + 1))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Self(if key == "all" {
                ConditionNode::All(children)
            } else {
                ConditionNode::Any(children)
            }));
        }
        let path = fields
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| HookError::invalid("condition.path must be a JSON Pointer string"))?;
        validate_pointer(path)?;
        let operation = fields
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| HookError::invalid("condition.op is required"))?;
        if operation == "exists" {
            only_fields(fields, &["path", "op"], "exists condition")?;
            return Ok(Self(ConditionNode::Exists(path.into())));
        }
        only_fields(fields, &["path", "op", "value"], "comparison condition")?;
        let expected = fields
            .get("value")
            .filter(|value| !value.is_array() && !value.is_object())
            .ok_or_else(|| HookError::invalid("condition.value must be a JSON scalar"))?
            .clone();
        let operation = match operation {
            "eq" => Operation::Eq,
            "ne" => Operation::Ne,
            "gt" => Operation::Gt,
            "gte" => Operation::Gte,
            "lt" => Operation::Lt,
            "lte" => Operation::Lte,
            other => {
                return Err(HookError::invalid(format!(
                    "unsupported condition operator {other:?}"
                )));
            }
        };
        if !matches!(operation, Operation::Eq | Operation::Ne) && !expected.is_number() {
            return Err(HookError::invalid(
                "gt/gte/lt/lte require a numeric condition.value",
            ));
        }
        Ok(Self(ConditionNode::Compare {
            path: path.into(),
            operation,
            expected,
        }))
    }

    /// Evaluates the raw message, before fields are projected for display.
    pub fn evaluate(&self, message: &Value) -> ConditionOutcome {
        match &self.0 {
            ConditionNode::Compare {
                path,
                operation,
                expected,
            } => match message.pointer(path) {
                None => ConditionOutcome {
                    matched: false,
                    missing_fields: 1,
                },
                Some(actual) => ConditionOutcome {
                    matched: compare(actual, expected, *operation),
                    missing_fields: 0,
                },
            },
            ConditionNode::Exists(path) => ConditionOutcome {
                matched: message.pointer(path).is_some(),
                missing_fields: 0,
            },
            ConditionNode::All(children) | ConditionNode::Any(children) => {
                let is_all = matches!(&self.0, ConditionNode::All(_));
                let mut outcome = ConditionOutcome {
                    matched: is_all,
                    missing_fields: 0,
                };
                for child in children {
                    let next = child.evaluate(message);
                    outcome.matched = if is_all {
                        outcome.matched && next.matched
                    } else {
                        outcome.matched || next.matched
                    };
                    outcome.missing_fields =
                        outcome.missing_fields.saturating_add(next.missing_fields);
                }
                outcome
            }
        }
    }
}

fn compare(actual: &Value, expected: &Value, operation: Operation) -> bool {
    let ordering = match (actual, expected) {
        (Value::Number(actual), Value::Number(expected)) => number_cmp(actual, expected),
        (Value::String(actual), Value::String(expected)) => actual.cmp(expected),
        (Value::Bool(actual), Value::Bool(expected)) => actual.cmp(expected),
        (Value::Null, Value::Null) => Comparison::Equal,
        // `ne` deliberately also returns false for different JSON types.
        _ => return false,
    };
    match operation {
        Operation::Eq => ordering == Comparison::Equal,
        Operation::Ne => ordering != Comparison::Equal,
        Operation::Gt => ordering == Comparison::Greater,
        Operation::Gte => ordering != Comparison::Less,
        Operation::Lt => ordering == Comparison::Less,
        Operation::Lte => ordering != Comparison::Greater,
    }
}

fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

fn number_cmp(left: &Number, right: &Number) -> Comparison {
    match (integer(left), integer(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(left), None) => integer_float_cmp(left, right.as_f64().unwrap()),
        (None, Some(right)) => integer_float_cmp(right, left.as_f64().unwrap()).reverse(),
        (None, None) => left
            .as_f64()
            .unwrap()
            .partial_cmp(&right.as_f64().unwrap())
            .expect("serde_json numbers are finite"),
    }
}

fn integer_float_cmp(integer: i128, float: f64) -> Comparison {
    // Every JSON integer fits [-2^63, 2^64-1]. Test bounds before truncation;
    // casting the integer to f64 would conflate distinct values above 2^53.
    if float >= 18_446_744_073_709_551_616.0 {
        return Comparison::Less;
    }
    if float < -9_223_372_036_854_775_808.0 {
        return Comparison::Greater;
    }
    match integer.cmp(&(float.trunc() as i128)) {
        Comparison::Equal => 0.0_f64.partial_cmp(&float.fract()).unwrap(),
        ordering => ordering,
    }
}

pub fn validate_pointer(path: &str) -> Result<()> {
    if !path.is_empty() && !path.starts_with('/') {
        return Err(HookError::invalid("field path must be a JSON Pointer"));
    }
    let mut characters = path.chars();
    while let Some(character) = characters.next() {
        if character == '~' && !matches!(characters.next(), Some('0' | '1')) {
            return Err(HookError::invalid(
                "JSON Pointer only permits ~0 and ~1 escapes",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
#[cfg_attr(
    not(any(unix, windows)),
    expect(
        dead_code,
        reason = "validate and retain script options on every platform; execution requires Unix"
    )
)]
struct Script {
    executable: String,
    argv: Vec<String>,
    timeout: Duration,
    max_output_bytes: usize,
}

#[derive(Debug, Clone)]
enum Hook {
    Condition {
        condition: Condition,
        required: u64,
        consecutive: u64,
    },
    Script(Script),
}

#[derive(Debug, Clone)]
pub struct HookRunner(Hook);

/// Reuse the bounded process/pipe lifetime for the explicitly selected doctor
/// interpreter. This does not apply the observation hook result contract.
pub fn run_json_command(
    executable: &str,
    argv: Vec<String>,
    input: &Value,
    cancel: &AtomicBool,
    deadline: Instant,
) -> Result<Value> {
    let control = Control {
        cancel,
        deadline: Some(deadline),
        timeout_kind: HookErrorKind::OverallDeadline,
    };
    control.check()?;
    let script = Script {
        executable: executable.into(),
        argv,
        timeout: deadline.saturating_duration_since(Instant::now()),
        max_output_bytes: 1024 * 1024,
    };
    #[cfg(any(unix, windows))]
    {
        let mut bytes =
            serde_json::to_vec(input).map_err(|error| HookError::invalid(error.to_string()))?;
        bytes.push(b'\n');
        let (output, _) = process::capture(&script, bytes, &control)?;
        serde_json::from_slice(&output).map_err(|_| {
            HookError::new(
                HookErrorKind::InvalidOutput,
                "doctor stdout was not one JSON value",
            )
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (script, input);
        Err(HookError::new(
            HookErrorKind::UnsupportedPlatform,
            "selected-interpreter doctor requires Unix process group cleanup",
        ))
    }
}

impl HookRunner {
    pub fn from_json(value: &Value) -> Result<Self> {
        let fields = object(value, "hook")?;
        match fields.get("kind").and_then(Value::as_str) {
            Some("condition") => {
                only_fields(
                    fields,
                    &["kind", "condition", "consecutive_matches"],
                    "condition hook",
                )?;
                let condition = Condition::parse(
                    fields
                        .get("condition")
                        .ok_or_else(|| HookError::invalid("hook.condition is required"))?,
                )?;
                let required = match fields.get("consecutive_matches") {
                    None => 1,
                    Some(value) => positive_integer(value, "consecutive_matches")?,
                };
                Ok(Self(Hook::Condition {
                    condition,
                    required,
                    consecutive: 0,
                }))
            }
            Some("script") => {
                only_fields(
                    fields,
                    &["kind", "executable", "argv", "timeout", "max_output_bytes"],
                    "script hook",
                )?;
                let executable = fields
                    .get("executable")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty() && !value.contains('\0'))
                    .ok_or_else(|| HookError::invalid("hook.executable must be a nonempty path"))?;
                #[cfg(windows)]
                {
                    let path = std::path::Path::new(executable);
                    if !path.is_absolute() {
                        return Err(HookError::invalid(
                            "Windows hook.executable must be an absolute native executable path",
                        ));
                    }
                    if path.exists() {
                        aos_launch::windows_process::validate_executable(path).map_err(|e| {
                            HookError::invalid(format!("invalid Windows hook executable: {e}"))
                        })?;
                    }
                }
                let argv = match fields.get("argv") {
                    None => Vec::new(),
                    Some(Value::Array(arguments)) => arguments
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .filter(|argument| !argument.contains('\0'))
                                .map(str::to_owned)
                                .ok_or_else(|| {
                                    HookError::invalid("hook.argv entries must be strings")
                                })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    Some(_) => return Err(HookError::invalid("hook.argv must be an array")),
                };
                let timeout = match fields.get("timeout") {
                    None => Duration::from_secs(1),
                    Some(value) => {
                        let seconds = value
                            .as_f64()
                            .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                            .ok_or_else(|| {
                                HookError::invalid("hook.timeout must be positive and finite")
                            })?;
                        Duration::try_from_secs_f64(seconds)
                            .ok()
                            .filter(|duration| !duration.is_zero())
                            .ok_or_else(|| {
                                HookError::invalid("hook.timeout is outside the supported range")
                            })?
                    }
                };
                if Instant::now().checked_add(timeout).is_none() {
                    return Err(HookError::invalid(
                        "hook.timeout exceeds the steady clock range",
                    ));
                }
                let max_output_bytes = match fields.get("max_output_bytes") {
                    None => DEFAULT_OUTPUT_BYTES,
                    Some(value) => usize::try_from(positive_integer(value, "max_output_bytes")?)
                        .map_err(|_| {
                            HookError::invalid("max_output_bytes exceeds this platform's range")
                        })?,
                };
                Ok(Self(Hook::Script(Script {
                    executable: executable.into(),
                    argv,
                    timeout,
                    max_output_bytes,
                })))
            }
            _ => Err(HookError::invalid("hook.kind must be condition or script")),
        }
    }

    /// Invoke once for each message that passed the filter, before projection.
    /// The caller owns the observation queue and must report its own overflow.
    /// The first elapsed deadline is preserved: OverallDeadline maps to the
    /// observation's total_timeout; HookTimeout is a hook_error.
    pub fn evaluate(
        &mut self,
        envelope: &Value,
        cancel: &AtomicBool,
        overall_deadline: Option<Instant>,
    ) -> Result<HookOutcome> {
        let message = envelope
            .get("message")
            .ok_or_else(|| HookError::invalid("hook input envelope requires message"))?;
        match &mut self.0 {
            Hook::Condition {
                condition,
                required,
                consecutive,
            } => {
                let control = Control {
                    cancel,
                    deadline: overall_deadline,
                    timeout_kind: HookErrorKind::OverallDeadline,
                };
                control.check()?;
                let outcome = condition.evaluate(message);
                control.check()?;
                *consecutive = if outcome.matched {
                    consecutive.saturating_add(1)
                } else {
                    0
                };
                Ok(HookOutcome {
                    decision: if *consecutive >= *required {
                        Decision::Matched
                    } else {
                        Decision::Continue
                    },
                    reason: None,
                    consecutive_matches: *consecutive,
                    missing_fields: outcome.missing_fields,
                    stderr: String::new(),
                })
            }
            Hook::Script(script) => {
                let own_deadline = Instant::now().checked_add(script.timeout).ok_or_else(|| {
                    HookError::invalid("hook.timeout exceeds the steady clock range")
                })?;
                let (deadline, timeout_kind) = match overall_deadline {
                    Some(overall) if overall <= own_deadline => {
                        (overall, HookErrorKind::OverallDeadline)
                    }
                    _ => (own_deadline, HookErrorKind::HookTimeout),
                };
                let control = Control {
                    cancel,
                    deadline: Some(deadline),
                    timeout_kind,
                };
                run_script(script, envelope, &control)
            }
        }
    }
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| HookError::invalid(format!("{name} must be an object")))
}

fn only_fields(fields: &Map<String, Value>, allowed: &[&str], name: &str) -> Result<()> {
    for key in fields.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(HookError::invalid(format!("unknown {name} field {key:?}")));
        }
    }
    Ok(())
}

fn positive_integer(value: &Value, name: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| HookError::invalid(format!("{name} must be a positive integer")))
}

struct Control<'a> {
    cancel: &'a AtomicBool,
    deadline: Option<Instant>,
    timeout_kind: HookErrorKind,
}

impl Control<'_> {
    fn check(&self) -> Result<()> {
        if self.cancel.load(Ordering::Acquire) {
            return Err(HookError::new(
                HookErrorKind::Interrupted,
                "hook interrupted by observation shutdown",
            ));
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HookError::new(
                self.timeout_kind,
                if self.timeout_kind == HookErrorKind::OverallDeadline {
                    "observation deadline expired during hook execution"
                } else {
                    "script hook exceeded its execution timeout"
                },
            ));
        }
        Ok(())
    }

    #[cfg(any(unix, windows))]
    fn poll_timeout_ms(&self) -> i32 {
        self.deadline
            .map(|deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis()
            })
            .unwrap_or(10)
            .clamp(1, 10) as i32
    }
}

#[cfg(any(unix, windows))]
fn parse_script_result(stdout: &[u8]) -> Result<HookOutcome> {
    let text = std::str::from_utf8(stdout).map_err(|error| {
        HookError::new(
            HookErrorKind::InvalidOutput,
            format!("hook stdout must be UTF-8 JSON: {error}"),
        )
    })?;
    // Reuse core's strict JSON parser so duplicate decisions cannot silently
    // turn an earlier error into a later matched result.
    let value = aos_core::parse_config_json(text).map_err(|error| {
        HookError::new(
            HookErrorKind::InvalidOutput,
            format!("hook stdout must contain exactly one unambiguous JSON object: {error}"),
        )
    })?;
    let fields = value.as_object().ok_or_else(|| {
        HookError::new(
            HookErrorKind::InvalidOutput,
            "hook result must be an object",
        )
    })?;
    if fields
        .keys()
        .any(|key| !matches!(key.as_str(), "decision" | "reason"))
    {
        return Err(HookError::new(
            HookErrorKind::InvalidOutput,
            "hook result only permits decision and reason",
        ));
    }
    let reason = match fields.get("reason") {
        None => None,
        Some(Value::String(reason)) => Some(reason.clone()),
        Some(_) => {
            return Err(HookError::new(
                HookErrorKind::InvalidOutput,
                "hook result reason must be a string",
            ));
        }
    };
    let decision = match fields.get("decision").and_then(Value::as_str) {
        Some("continue") => Decision::Continue,
        Some("matched") => Decision::Matched,
        Some("error") => {
            return Err(HookError::new(
                HookErrorKind::Reported,
                reason.unwrap_or_else(|| "script reported an error".into()),
            ));
        }
        _ => {
            return Err(HookError::new(
                HookErrorKind::InvalidOutput,
                "hook decision must be continue, matched, or error",
            ));
        }
    };
    Ok(HookOutcome {
        decision,
        reason,
        consecutive_matches: 0,
        missing_fields: 0,
        stderr: String::new(),
    })
}

#[cfg(any(unix, windows))]
struct InputWriter<'a, 'b> {
    bytes: Vec<u8>,
    control: &'a Control<'b>,
}

#[cfg(any(unix, windows))]
impl io::Write for InputWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.control.check().map_err(io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.control.check().map_err(io::Error::other)
    }
}

#[cfg(unix)]
#[path = "hooks_process.rs"]
mod process;
#[cfg(windows)]
#[path = "hooks_process_windows.rs"]
mod process;

#[cfg(any(unix, windows))]
fn run_script(script: &Script, envelope: &Value, control: &Control<'_>) -> Result<HookOutcome> {
    control.check()?;
    let mut input = InputWriter {
        bytes: Vec::new(),
        control,
    };
    if let Err(error) = serde_json::to_writer(&mut input, envelope) {
        control.check()?;
        return Err(HookError::new(
            HookErrorKind::Io,
            format!("encode hook input: {error}"),
        ));
    }
    input.bytes.push(b'\n');
    control.check()?;
    process::run(script, input.bytes, control)
}

#[cfg(not(any(unix, windows)))]
fn run_script(_script: &Script, _envelope: &Value, control: &Control<'_>) -> Result<HookOutcome> {
    control.check()?;
    Err(HookError::new(
        HookErrorKind::UnsupportedPlatform,
        "script hooks require Unix process groups on this platform",
    ))
}
