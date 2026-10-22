// Copyright 2016 Open Source Robotics Foundation, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
//
// Type, descriptor, range and atomic-set behavior follows ROS 2 Jazzy rclpy
// 4e607c09d624debc39a4ae5405680c06d87fac35 (parameter.py, node.py).
// Optimistic versions, finite JSON numbers and expiring transactions are AOS
// adaptations. User validators run between prepare and commit, outside locks.
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    clock::ShutdownResource, Clock, ClockType, Error, Lifecycle, LifecycleState, TimePoint,
    TypedPublisher,
};

/// AOS transaction lease; this timeout is not a ROS parameter-service timeout.
pub const DEFAULT_PARAMETER_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(5);
pub type ParameterResult<T> = std::result::Result<T, ParameterError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "reason", rename_all = "snake_case")]
pub enum ParameterError {
    Closed,
    InvalidArgument(String),
    AlreadyDeclared(String),
    NotDeclared(String),
    Uninitialized(String),
    ReadOnly(String),
    StateNotAllowed(String),
    WrongType(String),
    OutOfRange(String),
    Conflict(String),
    Expired,
    Validation(String),
    Unavailable(String),
}
impl ParameterError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::InvalidArgument(_) => "invalid_argument",
            Self::AlreadyDeclared(_) => "already_declared",
            Self::NotDeclared(_) => "not_declared",
            Self::Uninitialized(_) => "uninitialized",
            Self::ReadOnly(_) => "read_only",
            Self::StateNotAllowed(_) => "state_not_allowed",
            Self::WrongType(_) => "wrong_type",
            Self::OutOfRange(_) => "out_of_range",
            Self::Conflict(_) => "conflict",
            Self::Expired => "expired",
            Self::Validation(_) => "validation",
            Self::Unavailable(_) => "unavailable",
        }
    }
}
impl fmt::Display for ParameterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Closed => "parameter owner is closed",
            Self::Expired => "parameter transaction expired",
            Self::InvalidArgument(s)
            | Self::AlreadyDeclared(s)
            | Self::NotDeclared(s)
            | Self::Uninitialized(s)
            | Self::ReadOnly(s)
            | Self::StateNotAllowed(s)
            | Self::WrongType(s)
            | Self::OutOfRange(s)
            | Self::Conflict(s)
            | Self::Validation(s)
            | Self::Unavailable(s) => s,
        };
        write!(f, "{}: {reason}", self.code())
    }
}
impl std::error::Error for ParameterError {}
impl From<ParameterError> for Error {
    fn from(value: ParameterError) -> Self {
        match value {
            ParameterError::Closed => Self::Closed,
            other => Self::InvalidArgument(other.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    NotSet,
    Bool,
    Integer,
    Double,
    String,
    ByteArray,
    BoolArray,
    IntegerArray,
    DoubleArray,
    StringArray,
}

/// Canonical wire values retain empty-array and integer/byte distinctions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ParameterValue {
    NotSet,
    Bool(bool),
    Integer(i64),
    Double(f64),
    String(String),
    ByteArray(Vec<u8>),
    BoolArray(Vec<bool>),
    IntegerArray(Vec<i64>),
    DoubleArray(Vec<f64>),
    StringArray(Vec<String>),
}
impl ParameterValue {
    pub fn parameter_type(&self) -> ParameterType {
        match self {
            Self::NotSet => ParameterType::NotSet,
            Self::Bool(_) => ParameterType::Bool,
            Self::Integer(_) => ParameterType::Integer,
            Self::Double(_) => ParameterType::Double,
            Self::String(_) => ParameterType::String,
            Self::ByteArray(_) => ParameterType::ByteArray,
            Self::BoolArray(_) => ParameterType::BoolArray,
            Self::IntegerArray(_) => ParameterType::IntegerArray,
            Self::DoubleArray(_) => ParameterType::DoubleArray,
            Self::StringArray(_) => ParameterType::StringArray,
        }
    }
    /// Accept canonical typed JSON or infer homogeneous nonempty raw values.
    /// Empty arrays require a canonical type; mixed arrays are rejected.
    pub fn from_json(value: &Value) -> ParameterResult<Self> {
        let parsed = match value {
            Value::Object(_) => serde_json::from_value(value.clone())
                .map_err(|e| ParameterError::InvalidArgument(e.to_string()))?,
            Value::Null => Self::NotSet,
            Value::Bool(v) => Self::Bool(*v),
            Value::String(v) => Self::String(v.clone()),
            Value::Number(v) if v.is_i64() || v.is_u64() => {
                Self::Integer(v.as_i64().ok_or_else(|| {
                    ParameterError::InvalidArgument("integer exceeds signed 64-bit range".into())
                })?)
            }
            Value::Number(v) => Self::Double(v.as_f64().ok_or_else(|| {
                ParameterError::InvalidArgument("invalid floating-point value".into())
            })?),
            Value::Array(items) => {
                let Some(first) = items.first() else {
                    return Err(ParameterError::InvalidArgument(
                        "empty parameter arrays require an explicit type".into(),
                    ));
                };
                let items = items
                    .iter()
                    .map(Self::from_json)
                    .collect::<ParameterResult<Vec<_>>>()?;
                let kind = Self::from_json(first)?.parameter_type();
                if items.iter().any(|v| v.parameter_type() != kind) {
                    return Err(ParameterError::InvalidArgument(
                        "parameter arrays must contain a single scalar type".into(),
                    ));
                }
                match kind {
                    ParameterType::Bool => Self::BoolArray(
                        items
                            .into_iter()
                            .map(|v| match v {
                                Self::Bool(v) => v,
                                _ => unreachable!(),
                            })
                            .collect(),
                    ),
                    ParameterType::Integer => Self::IntegerArray(
                        items
                            .into_iter()
                            .map(|v| match v {
                                Self::Integer(v) => v,
                                _ => unreachable!(),
                            })
                            .collect(),
                    ),
                    ParameterType::Double => Self::DoubleArray(
                        items
                            .into_iter()
                            .map(|v| match v {
                                Self::Double(v) => v,
                                _ => unreachable!(),
                            })
                            .collect(),
                    ),
                    ParameterType::String => Self::StringArray(
                        items
                            .into_iter()
                            .map(|v| match v {
                                Self::String(v) => v,
                                _ => unreachable!(),
                            })
                            .collect(),
                    ),
                    _ => {
                        return Err(ParameterError::InvalidArgument(
                            "parameter arrays must contain scalar values".into(),
                        ))
                    }
                }
            }
        };
        parsed.validate()?;
        Ok(parsed)
    }
    /// Common binding/CLI parser. An explicit type removes empty-array
    /// ambiguity while serde still checks every element and numeric boundary.
    pub fn from_json_with_type(
        value: &Value,
        parameter_type: Option<ParameterType>,
    ) -> ParameterResult<Self> {
        match parameter_type {
            None => Self::from_json(value),
            Some(ParameterType::NotSet) if value.is_null() => Ok(Self::NotSet),
            Some(ParameterType::NotSet) => {
                Err(ParameterError::WrongType("not_set requires null".into()))
            }
            Some(parameter_type) => Self::from_json(&json!({
                "type": parameter_type,
                "value": value,
            })),
        }
    }
    fn validate(&self) -> ParameterResult<()> {
        let finite = match self {
            Self::Double(v) => v.is_finite(),
            Self::DoubleArray(v) => v.iter().all(|v| v.is_finite()),
            _ => true,
        };
        if finite {
            Ok(())
        } else {
            Err(ParameterError::InvalidArgument(
                "parameter JSON numbers must be finite".into(),
            ))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    pub value: ParameterValue,
}
impl Parameter {
    pub fn new(name: impl Into<String>, value: ParameterValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegerRange {
    pub from_value: i64,
    pub to_value: i64,
    pub step: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FloatingPointRange {
    pub from_value: f64,
    pub to_value: f64,
    pub step: f64,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ParameterDescriptor {
    /// Omit to infer from the default. A static uninitialized declaration must
    /// specify a type other than NotSet.
    pub parameter_type: Option<ParameterType>,
    pub description: String,
    pub additional_constraints: String,
    pub read_only: bool,
    pub dynamic_typing: bool,
    pub integer_range: Option<IntegerRange>,
    pub floating_point_range: Option<FloatingPointRange>,
    /// Empty permits updates in every state. Otherwise only these primary
    /// Lifecycle states permit set/delete/load. Declaration remains available
    /// so startup defaults and overrides can be installed before activation.
    pub allowed_states: Vec<LifecycleState>,
}
impl ParameterDescriptor {
    fn validate(&self) -> ParameterResult<()> {
        for (index, state) in self.allowed_states.iter().enumerate() {
            if !matches!(
                state,
                LifecycleState::Unconfigured
                    | LifecycleState::Inactive
                    | LifecycleState::Active
                    | LifecycleState::Finalized
            ) || self.allowed_states[..index].contains(state)
            {
                return Err(ParameterError::InvalidArgument(
                    "allowed_states must contain unique primary lifecycle states".into(),
                ));
            }
        }
        if self.integer_range.is_some() && self.floating_point_range.is_some() {
            return Err(ParameterError::InvalidArgument(
                "integer and floating-point ranges are mutually exclusive".into(),
            ));
        }
        if let Some(range) = &self.floating_point_range {
            if !range.from_value.is_finite()
                || !range.to_value.is_finite()
                || !range.step.is_finite()
            {
                return Err(ParameterError::InvalidArgument(
                    "floating-point range values must be finite".into(),
                ));
            }
        }
        Ok(())
    }
    fn check_value(&self, name: &str, value: &ParameterValue) -> ParameterResult<()> {
        value.validate()?;
        if !self.dynamic_typing && self.parameter_type != Some(value.parameter_type()) {
            return Err(ParameterError::WrongType(format!(
                "{name:?} expects {:?}, received {:?}",
                self.parameter_type,
                value.parameter_type(),
            )));
        }
        if let (Some(range), ParameterValue::Integer(value)) = (&self.integer_range, value) {
            let low = range.from_value.min(range.to_value);
            let high = range.from_value.max(range.to_value);
            let aligned = range.step == 0
                || (i128::from(*value) - i128::from(low)) % i128::from(range.step) == 0;
            if *value < low || *value > high || (*value != low && *value != high && !aligned) {
                return Err(ParameterError::OutOfRange(format!(
                    "{name:?} violates integer range"
                )));
            }
        }
        if let (Some(range), ParameterValue::Double(value)) = (&self.floating_point_range, value) {
            let low = range.from_value.min(range.to_value);
            let high = range.from_value.max(range.to_value);
            // Jazzy rclpy uses math.isclose(rel_tol=1e-6, abs_tol=0) and
            // Python's ties-to-even round for the closest step.
            let endpoint = is_close(*value, low) || is_close(*value, high);
            let aligned = range.step == 0.0
                || is_close(
                    *value,
                    low + ((*value - low) / range.step.abs()).round_ties_even() * range.step.abs(),
                );
            if !endpoint && (*value < low || *value > high || !aligned) {
                return Err(ParameterError::OutOfRange(format!(
                    "{name:?} violates floating-point range"
                )));
            }
        }
        Ok(())
    }
}
fn is_close(a: f64, b: f64) -> bool {
    a.is_finite() && b.is_finite() && (a - b).abs() <= 1e-6 * a.abs().max(b.abs())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterOverride {
    pub value: ParameterValue,
    pub source: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterRecord {
    pub name: String,
    pub value: ParameterValue,
    pub default_value: ParameterValue,
    pub descriptor: ParameterDescriptor,
    /// Version of this record's last successful declaration or update.
    pub version: u64,
    pub source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterSnapshot {
    pub format_version: u32,
    pub node: String,
    pub node_id: String,
    pub version: u64,
    pub parameters: Vec<ParameterRecord>,
}
impl ParameterSnapshot {
    /// Reusable C03 startup configuration. Loading does not auto-declare names.
    pub fn startup_config(&self) -> Value {
        let values = self
            .parameters
            .iter()
            .map(|p| (p.name.clone(), json!(p.value)))
            .collect::<serde_json::Map<_, _>>();
        json!({"parameters": values})
    }
    pub fn write(&self, path: impl AsRef<Path>) -> ParameterResult<()> {
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| ParameterError::InvalidArgument(e.to_string()))?;
        bytes.push(b'\n');
        crate::write_private_atomic(path, &bytes)
            .map(|_| ())
            .map_err(|e| ParameterError::Unavailable(e.to_string()))
    }
    pub fn read(path: impl AsRef<Path>) -> ParameterResult<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ParameterError::Unavailable(e.to_string()))?;
        Self::from_json(&text)
    }
    pub fn from_json(text: &str) -> ParameterResult<Self> {
        let value = crate::parse_config_json(text)
            .map_err(|e| ParameterError::InvalidArgument(e.to_string()))?;
        let snapshot: Self = serde_json::from_value(value)
            .map_err(|e| ParameterError::InvalidArgument(e.to_string()))?;
        if snapshot.format_version != 1 {
            return Err(ParameterError::InvalidArgument(
                "unsupported parameter snapshot version".into(),
            ));
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterEvent {
    pub node: String,
    pub node_id: String,
    pub version: u64,
    pub transaction_id: String,
    pub stamp: TimePoint,
    pub new_parameters: Vec<Parameter>,
    pub changed_parameters: Vec<Parameter>,
    pub deleted_parameters: Vec<Parameter>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterCommit {
    pub version: u64,
    pub transaction_id: String,
    pub event: ParameterEvent,
    /// A committed update is never rolled back if publication subsequently fails.
    pub event_published: bool,
    pub event_publish_error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParametersResult {
    pub names: Vec<String>,
    pub prefixes: Vec<String>,
}

/// Clones share the Node's sole parameter state. No user callback executes
/// while the store lock or commit-order lock is held.
#[derive(Clone)]
pub struct ParameterStore {
    inner: Arc<ParameterStoreInner>,
}
type SimTimeSource = Arc<dyn Fn() -> crate::Result<()> + Send + Sync>;
struct ParameterStoreInner {
    node: String,
    node_id: String,
    overrides: BTreeMap<String, ParameterOverride>,
    state: Mutex<ParameterState>,
    commit_order: Mutex<()>,
    publisher: Mutex<Option<Arc<TypedPublisher>>>,
    clock: Clock,
    lifecycle: Option<Lifecycle>,
    owns_ros_clock: bool,
    sim_time_source: Mutex<Option<SimTimeSource>>,
}
struct ParameterState {
    closed: bool,
    version: u64,
    entries: BTreeMap<String, ParameterRecord>,
}
enum Mutation {
    Upsert(Box<ParameterRecord>),
    Delete(String),
}
/// Single-use optimistic transaction. Drop/abort discards the candidate.
/// Applications validate candidate() in their own Executor/caller thread.
pub struct ParameterTransaction {
    owner: Weak<ParameterStoreInner>,
    id: String,
    base_version: u64,
    lifecycle_epoch: Option<u64>,
    deadline: Instant,
    candidate: Vec<Parameter>,
    mutations: Vec<Mutation>,
}
impl ParameterTransaction {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn base_version(&self) -> u64 {
        self.base_version
    }
    pub fn candidate(&self) -> &[Parameter] {
        &self.candidate
    }
    pub fn abort(self) {}
    pub fn commit(self) -> ParameterResult<ParameterCommit> {
        let owner = self.owner.upgrade().ok_or(ParameterError::Closed)?;
        owner.commit(self)
    }
}

impl ParameterStore {
    pub fn new(node: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self::with_overrides(node, node_id, BTreeMap::new())
    }
    pub fn with_overrides(
        node: impl Into<String>,
        node_id: impl Into<String>,
        overrides: BTreeMap<String, ParameterOverride>,
    ) -> Self {
        Self {
            inner: Arc::new(ParameterStoreInner {
                node: node.into(),
                node_id: node_id.into(),
                overrides,
                state: Mutex::new(ParameterState {
                    closed: false,
                    version: 0,
                    entries: BTreeMap::new(),
                }),
                commit_order: Mutex::new(()),
                publisher: Mutex::new(None),
                clock: Clock::new(ClockType::System),
                lifecycle: None,
                owns_ros_clock: false,
                sim_time_source: Mutex::new(None),
            }),
        }
    }
    pub(crate) fn for_node(
        node: String,
        node_id: String,
        overrides: BTreeMap<String, ParameterOverride>,
        clock: Clock,
        lifecycle: Option<Lifecycle>,
    ) -> crate::Result<Self> {
        let initial = overrides
            .get("use_sim_time")
            .cloned()
            .unwrap_or(ParameterOverride {
                value: ParameterValue::Bool(false),
                source: "default".into(),
            });
        let ParameterValue::Bool(enabled) = initial.value else {
            return Err(Error::InvalidArgument(
                "use_sim_time must be a boolean parameter".into(),
            ));
        };
        clock.set_use_sim_time_direct(enabled)?;
        let store = Self {
            inner: Arc::new(ParameterStoreInner {
                node,
                node_id,
                overrides,
                state: Mutex::new(ParameterState {
                    closed: false,
                    version: 1,
                    entries: BTreeMap::from([(
                        "use_sim_time".into(),
                        ParameterRecord {
                            name: "use_sim_time".into(),
                            value: ParameterValue::Bool(enabled),
                            default_value: ParameterValue::Bool(false),
                            descriptor: ParameterDescriptor {
                                parameter_type: Some(ParameterType::Bool),
                                description: "Use the Node ROS clock's /clock time source".into(),
                                ..Default::default()
                            },
                            version: 1,
                            source: initial.source,
                        },
                    )]),
                }),
                commit_order: Mutex::new(()),
                publisher: Mutex::new(None),
                clock: clock.clone(),
                lifecycle,
                owns_ros_clock: true,
                sim_time_source: Mutex::new(None),
            }),
        };
        let owner = Arc::downgrade(&store.inner);
        clock.bind_sim_time_setter(Arc::new(move |enabled| {
            let inner = owner.upgrade().ok_or(Error::Closed)?;
            ParameterStore { inner }.set_atomically(vec![Parameter::new(
                "use_sim_time",
                ParameterValue::Bool(enabled),
            )])?;
            Ok(())
        }))?;
        Ok(store)
    }
    pub(crate) fn set_sim_time_source(&self, source: SimTimeSource) {
        *self.inner.sim_time_source.lock().unwrap() = Some(source);
    }
    pub fn version(&self) -> ParameterResult<u64> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(state.version)
    }
    pub fn snapshot(&self) -> ParameterResult<ParameterSnapshot> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(ParameterSnapshot {
            format_version: 1,
            node: self.inner.node.clone(),
            node_id: self.inner.node_id.clone(),
            version: state.version,
            parameters: state.entries.values().cloned().collect(),
        })
    }
    pub fn get(&self, name: &str) -> ParameterResult<ParameterValue> {
        self.get_parameters(&[name.to_owned()])
            .map(|mut values| values.remove(0))
    }
    pub fn get_parameters(&self, names: &[String]) -> ParameterResult<Vec<ParameterValue>> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        names
            .iter()
            .map(|name| {
                let entry = state
                    .entries
                    .get(name)
                    .ok_or_else(|| ParameterError::NotDeclared(name.clone()))?;
                if entry.value == ParameterValue::NotSet && !entry.descriptor.dynamic_typing {
                    return Err(ParameterError::Uninitialized(name.clone()));
                }
                Ok(entry.value.clone())
            })
            .collect()
    }
    pub fn describe(&self, names: &[String]) -> ParameterResult<Vec<ParameterRecord>> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        names
            .iter()
            .map(|name| {
                state
                    .entries
                    .get(name)
                    .cloned()
                    .ok_or_else(|| ParameterError::NotDeclared(name.clone()))
            })
            .collect()
    }
    pub fn list(&self, prefixes: &[String], depth: u64) -> ParameterResult<ListParametersResult> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        let names: Vec<_> = state
            .entries
            .keys()
            .filter(|name| {
                if prefixes.is_empty() {
                    depth == 0 || name.matches('.').count() < depth as usize
                } else {
                    prefixes.iter().any(|prefix| {
                        name.as_str() == prefix
                            || name
                                .strip_prefix(&format!("{prefix}."))
                                .is_some_and(|tail| {
                                    depth == 0 || tail.matches('.').count() < depth as usize
                                })
                    })
                }
            })
            .cloned()
            .collect();
        let prefixes = names
            .iter()
            .filter_map(|name| name.rsplit_once('.').map(|(prefix, _)| prefix.to_owned()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(ListParametersResult { names, prefixes })
    }
    pub fn get_types(&self, names: &[String]) -> ParameterResult<Vec<ParameterType>> {
        self.describe(names).map(|records| {
            records
                .into_iter()
                .map(|record| record.value.parameter_type())
                .collect()
        })
    }
    pub fn declare(
        &self,
        name: &str,
        default_value: ParameterValue,
        descriptor: ParameterDescriptor,
        ignore_override: bool,
    ) -> ParameterResult<ParameterCommit> {
        self.prepare_declare(name, default_value, descriptor, ignore_override)?
            .commit()
    }
    pub fn prepare_declare(
        &self,
        name: &str,
        default_value: ParameterValue,
        descriptor: ParameterDescriptor,
        ignore_override: bool,
    ) -> ParameterResult<ParameterTransaction> {
        self.prepare_declare_with_timeout(
            name,
            default_value,
            descriptor,
            ignore_override,
            DEFAULT_PARAMETER_TRANSACTION_TIMEOUT,
        )
    }
    pub fn prepare_declare_with_timeout(
        &self,
        name: &str,
        default_value: ParameterValue,
        mut descriptor: ParameterDescriptor,
        ignore_override: bool,
        timeout: Duration,
    ) -> ParameterResult<ParameterTransaction> {
        validate_name(name)?;
        descriptor.validate()?;
        if !descriptor.allowed_states.is_empty() && self.inner.lifecycle.is_none() {
            return Err(ParameterError::Unavailable(
                "allowed_states requires a LifecycleNode".into(),
            ));
        }
        default_value.validate()?;
        if descriptor.parameter_type.is_none() {
            descriptor.parameter_type = Some(default_value.parameter_type());
        }
        if !descriptor.dynamic_typing && descriptor.parameter_type == Some(ParameterType::NotSet) {
            return Err(ParameterError::WrongType(
                "a static declaration needs a concrete type or default".into(),
            ));
        }
        // Infer the static type from the default, then range-check the
        // effective override, matching Jazzy declaration order.
        if default_value != ParameterValue::NotSet
            && !descriptor.dynamic_typing
            && descriptor.parameter_type != Some(default_value.parameter_type())
        {
            return Err(ParameterError::WrongType(format!(
                "{name:?} default differs from its declared type"
            )));
        }
        let (value, source) = if !ignore_override {
            self.inner
                .overrides
                .get(name)
                .map(|p| (p.value.clone(), p.source.clone()))
        } else {
            None
        }
        .unwrap_or_else(|| (default_value.clone(), "default".into()));
        if !descriptor.dynamic_typing
            && default_value != ParameterValue::NotSet
            && value == ParameterValue::NotSet
        {
            return Err(ParameterError::WrongType(format!(
                "{name:?} cannot replace a concrete static default with a NotSet startup override"
            )));
        }
        if value != ParameterValue::NotSet || descriptor.dynamic_typing {
            descriptor.check_value(name, &value)?;
        }
        if descriptor.dynamic_typing {
            descriptor.parameter_type = Some(value.parameter_type());
        }
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        if state.entries.contains_key(name) {
            return Err(ParameterError::AlreadyDeclared(name.into()));
        }
        self.transaction(
            &state,
            vec![Parameter::new(name, value.clone())],
            vec![Mutation::Upsert(Box::new(ParameterRecord {
                name: name.into(),
                value,
                default_value,
                descriptor,
                version: 0,
                source,
            }))],
            None,
            timeout,
        )
    }
    pub fn set_atomically(&self, parameters: Vec<Parameter>) -> ParameterResult<ParameterCommit> {
        self.prepare_set(parameters)?.commit()
    }
    pub fn prepare_set(&self, parameters: Vec<Parameter>) -> ParameterResult<ParameterTransaction> {
        self.prepare_set_with_timeout(parameters, DEFAULT_PARAMETER_TRANSACTION_TIMEOUT)
    }
    pub fn prepare_set_with_timeout(
        &self,
        parameters: Vec<Parameter>,
        timeout: Duration,
    ) -> ParameterResult<ParameterTransaction> {
        self.prepare_update(parameters, timeout, false)
    }
    /// Restore values against the target's declarations. NotSet asserts that a
    /// declaration is still uninitialized; it never deletes it. Unchanged values
    /// remain in candidate() for validators and lifecycle-state checks.
    pub fn prepare_load_values(
        &self,
        parameters: Vec<Parameter>,
    ) -> ParameterResult<ParameterTransaction> {
        self.prepare_load_values_with_timeout(parameters, DEFAULT_PARAMETER_TRANSACTION_TIMEOUT)
    }
    pub fn prepare_load_values_with_timeout(
        &self,
        parameters: Vec<Parameter>,
        timeout: Duration,
    ) -> ParameterResult<ParameterTransaction> {
        self.prepare_update(parameters, timeout, true)
    }
    fn prepare_update(
        &self,
        parameters: Vec<Parameter>,
        timeout: Duration,
        is_load: bool,
    ) -> ParameterResult<ParameterTransaction> {
        if parameters.is_empty() && !is_load {
            return Err(ParameterError::InvalidArgument(
                "parameter update cannot be empty".into(),
            ));
        }
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        let mut names = BTreeSet::new();
        let mut mutations = Vec::with_capacity(parameters.len());
        for parameter in &parameters {
            validate_name(&parameter.name)?;
            if !names.insert(&parameter.name) {
                return Err(ParameterError::InvalidArgument(format!(
                    "duplicate parameter {:?}",
                    parameter.name
                )));
            }
            let mut record = state
                .entries
                .get(&parameter.name)
                .cloned()
                .ok_or_else(|| ParameterError::NotDeclared(parameter.name.clone()))?;
            if is_load && parameter.value == ParameterValue::NotSet {
                if record.value != ParameterValue::NotSet {
                    return Err(ParameterError::Conflict(format!(
                        "{:?} was initialized after the uninitialized value was saved",
                        parameter.name
                    )));
                }
                continue;
            }
            if record.descriptor.read_only && (!is_load || parameter.value != record.value) {
                return Err(ParameterError::ReadOnly(parameter.name.clone()));
            }
            record
                .descriptor
                .check_value(&parameter.name, &parameter.value)?;
            if is_load && record.value == parameter.value {
                continue;
            }
            if parameter.value == ParameterValue::NotSet {
                mutations.push(Mutation::Delete(parameter.name.clone()));
            } else {
                record.value = parameter.value.clone();
                record.source = "runtime".into();
                if record.descriptor.dynamic_typing {
                    record.descriptor.parameter_type = Some(parameter.value.parameter_type());
                }
                mutations.push(Mutation::Upsert(Box::new(record)));
            }
        }
        let lifecycle_epoch = if parameters.iter().any(|parameter| {
            !state.entries[&parameter.name]
                .descriptor
                .allowed_states
                .is_empty()
        }) {
            let lifecycle = self.inner.lifecycle.as_ref().ok_or_else(|| {
                ParameterError::Unavailable("allowed_states requires a LifecycleNode".into())
            })?;
            let current = lifecycle.parameter_state().map_err(lifecycle_error)?;
            check_allowed_states(&state, &parameters, current.current)?;
            Some(current.epoch)
        } else {
            None
        };
        self.transaction(&state, parameters, mutations, lifecycle_epoch, timeout)
    }
    pub fn prepare_load(
        &self,
        snapshot: &ParameterSnapshot,
    ) -> ParameterResult<ParameterTransaction> {
        if snapshot.format_version != 1 {
            return Err(ParameterError::InvalidArgument(
                "unsupported parameter snapshot version".into(),
            ));
        }
        self.prepare_load_values(
            snapshot
                .parameters
                .iter()
                .map(|p| Parameter::new(&p.name, p.value.clone()))
                .collect(),
        )
    }
    fn transaction(
        &self,
        state: &ParameterState,
        candidate: Vec<Parameter>,
        mutations: Vec<Mutation>,
        lifecycle_epoch: Option<u64>,
        timeout: Duration,
    ) -> ParameterResult<ParameterTransaction> {
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            ParameterError::InvalidArgument("transaction timeout is too large".into())
        })?;
        Ok(ParameterTransaction {
            owner: Arc::downgrade(&self.inner),
            id: Uuid::new_v4().simple().to_string(),
            base_version: state.version,
            lifecycle_epoch,
            deadline,
            candidate,
            mutations,
        })
    }
    pub fn close(&self) -> ParameterResult<()> {
        self.inner.close()
    }
    pub(crate) fn shutdown_resource(&self) -> Arc<dyn ShutdownResource> {
        self.inner.clone()
    }
    pub(crate) fn set_event_publisher(&self, publisher: Option<Arc<TypedPublisher>>) {
        *self.inner.publisher.lock().unwrap() = publisher;
    }
}
fn validate_name(name: &str) -> ParameterResult<()> {
    // rclpy deliberately does not apply Topic-name grammar to parameters.
    if name.is_empty() {
        Err(ParameterError::InvalidArgument(
            "parameter name cannot be empty".into(),
        ))
    } else {
        Ok(())
    }
}
fn ensure_open(state: &ParameterState) -> ParameterResult<()> {
    if state.closed {
        Err(ParameterError::Closed)
    } else {
        Ok(())
    }
}
impl ParameterStoreInner {
    fn commit(&self, transaction: ParameterTransaction) -> ParameterResult<ParameterCommit> {
        let _order = self.commit_order.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        check_transaction(&state, &transaction)?;
        let sim_time = self
            .owns_ros_clock
            .then(|| {
                transaction.mutations.iter().find_map(|mutation| {
                    if let Mutation::Upsert(record) = mutation {
                        if record.name == "use_sim_time" {
                            if let ParameterValue::Bool(enabled) = record.value {
                                return Some(enabled);
                            }
                        }
                    }
                    None
                })
            })
            .flatten();
        if sim_time == Some(true) {
            // Subscription creation may enter the adapter. It is not user code,
            // and must not run under the parameter state lock.
            drop(state);
            if let Some(source) = self.sim_time_source.lock().unwrap().clone() {
                source().map_err(|e| ParameterError::Unavailable(e.to_string()))?;
            }
            state = self.state.lock().unwrap();
            check_transaction(&state, &transaction)?;
        }
        // Keep the lifecycle state-change barrier through the native commit,
        // including use_sim_time. The business gate is not held here: timer
        // dispatch may hold Clock/Timer locks while reading that gate.
        let lifecycle_state = if let Some(epoch) = transaction.lifecycle_epoch {
            let lifecycle = self.lifecycle.as_ref().ok_or_else(|| {
                ParameterError::Unavailable("allowed_states requires a LifecycleNode".into())
            })?;
            let current = lifecycle.parameter_state().map_err(lifecycle_error)?;
            check_transaction(&state, &transaction)?;
            if current.epoch != epoch {
                return Err(ParameterError::Conflict(
                    "lifecycle state changed after parameter validation".into(),
                ));
            }
            check_allowed_states(&state, &transaction.candidate, current.current)?;
            Some(current)
        } else {
            None
        };
        let version = if transaction.mutations.is_empty() {
            state.version
        } else {
            state
                .version
                .checked_add(1)
                .ok_or_else(|| ParameterError::Unavailable("parameter version overflow".into()))?
        };
        let stamp = if let Some(enabled) = sim_time {
            // All remaining parameter operations are infallible. Node Clock
            // setters delegate here, so they cannot create a second authority.
            self.clock.configure_sim_time(enabled)
        } else {
            self.clock.now()
        }
        .map_err(|e| ParameterError::Unavailable(e.to_string()))?;
        let mut event = ParameterEvent {
            node: self.node.clone(),
            node_id: self.node_id.clone(),
            version,
            transaction_id: transaction.id.clone(),
            stamp,
            new_parameters: Vec::new(),
            changed_parameters: Vec::new(),
            deleted_parameters: Vec::new(),
        };
        if transaction.mutations.is_empty() {
            return Ok(ParameterCommit {
                version,
                transaction_id: transaction.id,
                event,
                event_published: false,
                event_publish_error: None,
            });
        }
        for mutation in transaction.mutations {
            match mutation {
                Mutation::Upsert(mut record) => {
                    record.version = version;
                    let parameter = Parameter::new(&record.name, record.value.clone());
                    if state
                        .entries
                        .get(&record.name)
                        .is_some_and(|old| old.value != ParameterValue::NotSet)
                    {
                        event.changed_parameters.push(parameter);
                    } else {
                        event.new_parameters.push(parameter);
                    }
                    state.entries.insert(record.name.clone(), *record);
                }
                Mutation::Delete(name) => {
                    if state.entries.remove(&name).is_some() {
                        event
                            .deleted_parameters
                            .push(Parameter::new(name, ParameterValue::NotSet));
                    }
                }
            }
        }
        state.version = version;
        drop(state);
        drop(lifecycle_state);
        // Network publication occurs after commit and without the store lock.
        // The order lock keeps event versions in commit order.
        let publisher = self.publisher.lock().unwrap().clone();
        let (event_published, event_publish_error) = match publisher {
            Some(publisher) => match publisher.publish(&json!(event)) {
                Ok(()) => (true, None),
                Err(error) => (false, Some(error.to_string())),
            },
            None => (false, None),
        };
        Ok(ParameterCommit {
            version,
            transaction_id: transaction.id,
            event,
            event_published,
            event_publish_error,
        })
    }
    fn close(&self) -> ParameterResult<()> {
        let mut state = self.state.lock().unwrap();
        ensure_open(&state)?;
        state.closed = true;
        Ok(())
    }
}
fn lifecycle_error(error: Error) -> ParameterError {
    match error {
        Error::Closed => ParameterError::Closed,
        other => ParameterError::Unavailable(other.to_string()),
    }
}
fn check_allowed_states(
    state: &ParameterState,
    parameters: &[Parameter],
    current: LifecycleState,
) -> ParameterResult<()> {
    for parameter in parameters {
        let record = state
            .entries
            .get(&parameter.name)
            .ok_or_else(|| ParameterError::NotDeclared(parameter.name.clone()))?;
        let allowed = &record.descriptor.allowed_states;
        if !allowed.is_empty() && !allowed.contains(&current) {
            return Err(ParameterError::StateNotAllowed(format!(
                "{:?} cannot be changed while lifecycle is {}",
                parameter.name,
                current.label(),
            )));
        }
    }
    Ok(())
}
fn check_transaction(
    state: &ParameterState,
    transaction: &ParameterTransaction,
) -> ParameterResult<()> {
    ensure_open(state)?;
    if Instant::now() >= transaction.deadline {
        return Err(ParameterError::Expired);
    }
    if transaction.base_version != state.version {
        return Err(ParameterError::Conflict(format!(
            "prepared at version {}, current version {}",
            transaction.base_version, state.version,
        )));
    }
    Ok(())
}
impl ShutdownResource for ParameterStoreInner {
    fn shutdown(&self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn managed_store() -> (ParameterStore, Lifecycle, Clock) {
        let lifecycle = Lifecycle::new("/managed".into());
        let clock = Clock::new(ClockType::Ros);
        let store = ParameterStore::for_node(
            "/managed".into(),
            "managed".into(),
            BTreeMap::new(),
            clock.clone(),
            Some(lifecycle.clone()),
        )
        .unwrap();
        (store, lifecycle, clock)
    }
    fn transition(lifecycle: &Lifecycle, id: crate::LifecycleTransitionId) {
        let progress = lifecycle
            .begin_transition(id)
            .unwrap()
            .complete(
                crate::LifecycleCallbackReturn::Success,
                Some(Duration::ZERO),
            )
            .unwrap();
        assert!(progress.report.unwrap().successful);
    }
    fn restricted(states: &[LifecycleState]) -> ParameterDescriptor {
        ParameterDescriptor {
            allowed_states: states.to_vec(),
            ..Default::default()
        }
    }
    fn declare(store: &ParameterStore, name: &str, value: ParameterValue) {
        store
            .declare(name, value, ParameterDescriptor::default(), false)
            .unwrap();
    }
    #[test]
    fn allowed_states_restrict_atomic_set_delete_and_load_but_not_startup_declare() {
        use crate::LifecycleTransitionId::{Activate, Configure, Deactivate};
        let (store, lifecycle, _) = managed_store();
        let mut descriptor = restricted(&[LifecycleState::Inactive]);
        descriptor.dynamic_typing = true;
        store
            .declare("config", ParameterValue::Integer(1), descriptor, false)
            .unwrap();
        declare(&store, "unrestricted", ParameterValue::Integer(1));
        let snapshot = store.snapshot().unwrap();
        let before = serde_json::to_value(&snapshot).unwrap();
        for candidate in [
            vec![Parameter::new("config", ParameterValue::Integer(2))],
            vec![Parameter::new("config", ParameterValue::NotSet)],
            vec![
                Parameter::new("unrestricted", ParameterValue::Integer(2)),
                Parameter::new("config", ParameterValue::Integer(2)),
            ],
        ] {
            assert!(matches!(
                store.set_atomically(candidate),
                Err(ParameterError::StateNotAllowed(_))
            ));
            assert_eq!(
                serde_json::to_value(store.snapshot().unwrap()).unwrap(),
                before
            );
        }
        assert!(matches!(
            store.prepare_load(&snapshot),
            Err(ParameterError::StateNotAllowed(_))
        ));
        assert_eq!(
            store.describe(&["config".into()]).unwrap()[0]
                .descriptor
                .allowed_states,
            [LifecycleState::Inactive]
        );
        transition(&lifecycle, Configure);
        store
            .set_atomically(vec![Parameter::new("config", ParameterValue::Integer(2))])
            .unwrap();
        store.prepare_load(&snapshot).unwrap().commit().unwrap();
        assert_eq!(store.get("config").unwrap(), ParameterValue::Integer(1));
        transition(&lifecycle, Activate);
        assert!(matches!(
            store.set_atomically(vec![Parameter::new("config", ParameterValue::Integer(3))]),
            Err(ParameterError::StateNotAllowed(_)),
        ));
        transition(&lifecycle, Deactivate);
        store
            .set_atomically(vec![Parameter::new("config", ParameterValue::NotSet)])
            .unwrap();
        assert!(matches!(
            store.get("config"),
            Err(ParameterError::NotDeclared(_))
        ));
    }
    #[test]
    fn allowed_states_commit_rejects_a_validator_crossing_state_epochs() {
        use crate::LifecycleTransitionId::{Activate, Configure, Deactivate};
        let (store, lifecycle, _) = managed_store();
        store
            .declare(
                "config",
                ParameterValue::Integer(1),
                restricted(&[LifecycleState::Inactive]),
                false,
            )
            .unwrap();
        transition(&lifecycle, Configure);
        let candidate = store
            .prepare_set(vec![Parameter::new("config", ParameterValue::Integer(2))])
            .unwrap();
        let version = store.version().unwrap();
        // A validator can call lifecycle controls outside all parameter locks.
        // Returning to the same label must not resurrect this stale candidate.
        transition(&lifecycle, Activate);
        transition(&lifecycle, Deactivate);
        assert!(matches!(
            candidate.commit(),
            Err(ParameterError::Conflict(_))
        ));
        assert_eq!(store.version().unwrap(), version);
        assert_eq!(store.get("config").unwrap(), ParameterValue::Integer(1));
        store
            .set_atomically(vec![Parameter::new("config", ParameterValue::Integer(2))])
            .unwrap();
    }

    #[test]
    fn no_op_load_checks_lifecycle_epoch_and_does_not_reconfigure_the_clock_source() {
        use crate::LifecycleTransitionId::{Activate, Configure, Deactivate};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (store, lifecycle, clock) = managed_store();
        store
            .declare(
                "constant",
                ParameterValue::Integer(1),
                ParameterDescriptor {
                    read_only: true,
                    allowed_states: vec![LifecycleState::Inactive],
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        store
            .set_atomically(vec![Parameter::new(
                "use_sim_time",
                ParameterValue::Bool(true),
            )])
            .unwrap();
        let source_calls = Arc::new(AtomicUsize::new(0));
        let observed = source_calls.clone();
        store.set_sim_time_source(Arc::new(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));
        let snapshot = store.snapshot().unwrap();
        assert!(matches!(
            store.prepare_load(&snapshot),
            Err(ParameterError::StateNotAllowed(_))
        ));
        transition(&lifecycle, Configure);
        let pending = store.prepare_load(&snapshot).unwrap();
        transition(&lifecycle, Activate);
        transition(&lifecycle, Deactivate);
        assert!(matches!(pending.commit(), Err(ParameterError::Conflict(_))));
        assert_eq!(source_calls.load(Ordering::Relaxed), 0);

        let before = clock.now().unwrap();
        let commit = store.prepare_load(&snapshot).unwrap().commit().unwrap();
        assert_eq!(commit.version, snapshot.version);
        assert_eq!(source_calls.load(Ordering::Relaxed), 0);
        assert_eq!(json!(clock.now().unwrap()), json!(before));
        assert!(commit.event.changed_parameters.is_empty());
        assert!(!commit.event_published);
    }
    #[test]
    fn restricted_transaction_expiry_is_rechecked_after_waiting_for_state_commit() {
        let (store, lifecycle, _) = managed_store();
        store
            .declare(
                "config",
                ParameterValue::Integer(1),
                restricted(&[LifecycleState::Unconfigured]),
                false,
            )
            .unwrap();
        let version = store.version().unwrap();
        let candidate = store
            .prepare_set_with_timeout(
                vec![Parameter::new("config", ParameterValue::Integer(2))],
                Duration::from_secs(1),
            )
            .unwrap();
        let expires = candidate.deadline;
        let state_commit = lifecycle.parameter_state().unwrap();
        let worker = std::thread::spawn(move || candidate.commit());
        // Wait until commit has acquired the parameter state and is blocked on
        // the lifecycle barrier; this exercises the second expiry check.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if matches!(
                store.inner.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ) {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        std::thread::sleep(
            expires.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        drop(state_commit);
        assert!(matches!(
            worker.join().unwrap(),
            Err(ParameterError::Expired)
        ));
        assert_eq!(store.version().unwrap(), version);
        assert_eq!(store.get("config").unwrap(), ParameterValue::Integer(1));
    }
    #[test]
    fn allowed_states_validate_owner_primary_states_and_leave_unrestricted_controls_available() {
        let ordinary = ParameterStore::new("/node", "node");
        assert!(matches!(
            ordinary.declare(
                "config",
                ParameterValue::Integer(1),
                restricted(&[LifecycleState::Inactive]),
                false,
            ),
            Err(ParameterError::Unavailable(_))
        ));
        assert!(ordinary.list(&[], 0).unwrap().names.is_empty());
        let (store, lifecycle, _) = managed_store();
        for states in [
            vec![LifecycleState::Unknown],
            vec![LifecycleState::Configuring],
            vec![LifecycleState::Inactive, LifecycleState::Inactive],
        ] {
            assert!(matches!(
                store.declare(
                    "invalid",
                    ParameterValue::Integer(1),
                    restricted(&states),
                    false,
                ),
                Err(ParameterError::InvalidArgument(_))
            ));
        }
        declare(&store, "control", ParameterValue::Integer(1));
        let mut configuring = lifecycle
            .begin_transition(crate::LifecycleTransitionId::Configure)
            .unwrap();
        store
            .set_atomically(vec![Parameter::new("control", ParameterValue::Integer(2))])
            .unwrap();
        configuring
            .complete(crate::LifecycleCallbackReturn::Success, None)
            .unwrap();
        assert_eq!(store.get("control").unwrap(), ParameterValue::Integer(2));
        let encoded = serde_json::to_value(restricted(&[
            LifecycleState::Inactive,
            LifecycleState::Active,
        ]))
        .unwrap();
        assert_eq!(encoded["allowed_states"], json!(["inactive", "active"]));
    }
    #[test]
    fn restricted_clock_batch_and_ready_timers_do_not_invert_lifecycle_locks() {
        use crate::LifecycleTransitionId::{Activate, Configure};
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            mpsc, Barrier,
        };
        let (store, lifecycle, clock) = managed_store();
        store
            .declare(
                "config",
                ParameterValue::Integer(0),
                restricted(&[LifecycleState::Active]),
                false,
            )
            .unwrap();
        transition(&lifecycle, Configure);
        transition(&lifecycle, Activate);
        let timer =
            crate::Timer::new_managed(clock.clone(), Duration::ZERO, true, lifecycle, "/managed")
                .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(Barrier::new(2));
        let (done, completed) = mpsc::channel();
        let timer_thread = {
            let stop = stop.clone();
            let ready = ready.clone();
            std::thread::spawn(move || {
                ready.wait();
                while !stop.load(Ordering::Acquire) {
                    drop(timer.take_ready().unwrap());
                    std::thread::yield_now();
                }
            })
        };
        let writer = std::thread::spawn(move || {
            ready.wait();
            for value in 0..500 {
                store
                    .set_atomically(vec![
                        Parameter::new("config", ParameterValue::Integer(value)),
                        Parameter::new("use_sim_time", ParameterValue::Bool(value % 2 == 0)),
                    ])
                    .unwrap();
                clock.update_ros_time(value).unwrap();
            }
            done.send(()).unwrap();
        });
        let result = completed.recv_timeout(Duration::from_secs(3));
        stop.store(true, Ordering::Release);
        assert!(
            result.is_ok(),
            "restricted clock commit stalled: {result:?}"
        );
        writer.join().unwrap();
        timer_thread.join().unwrap();
    }
    #[test]
    fn types_preserve_arrays_and_reject_ambiguous_or_invalid_json() {
        assert_eq!(
            ParameterValue::from_json(&json!([1, 2])).unwrap(),
            ParameterValue::IntegerArray(vec![1, 2])
        );
        assert_eq!(
            ParameterValue::from_json(&json!({"type":"byte_array","value":[]})).unwrap(),
            ParameterValue::ByteArray(vec![])
        );
        assert!(ParameterValue::from_json(&json!([])).is_err());
        assert!(ParameterValue::from_json(&json!([1, 1.5])).is_err());
        assert!(ParameterValue::from_json(&json!(u64::MAX)).is_err());
        assert!(ParameterValue::Double(f64::NAN).validate().is_err());
        assert!(ParameterValue::from_json(&json!({"type":"not_set","value":2})).is_err());
        assert!(ParameterValue::from_json(&json!({"type":"integer","value":2,"typo":1})).is_err());
    }
    #[test]
    fn atomic_failure_preserves_all_values_version_and_no_event() {
        let store = ParameterStore::new("/node", "id");
        declare(&store, "a", ParameterValue::Integer(1));
        declare(&store, "b", ParameterValue::Integer(2));
        let version = store.version().unwrap();
        assert!(matches!(
            store.set_atomically(vec![
                Parameter::new("a", ParameterValue::Integer(9)),
                Parameter::new("b", ParameterValue::Bool(true))
            ]),
            Err(ParameterError::WrongType(_))
        ));
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(1));
        assert_eq!(store.version().unwrap(), version);
        let commit = store
            .set_atomically(vec![
                Parameter::new("a", ParameterValue::Integer(9)),
                Parameter::new("b", ParameterValue::Integer(8)),
            ])
            .unwrap();
        assert_eq!(commit.version, version + 1);
        assert_eq!(commit.event.changed_parameters.len(), 2);
        assert_eq!(
            store
                .describe(&["a".into(), "b".into()])
                .unwrap()
                .iter()
                .map(|p| p.version)
                .collect::<Vec<_>>(),
            vec![commit.version; 2]
        );
    }
    #[test]
    fn validator_can_read_reenter_and_reject_without_lock_or_partial_state() {
        let store = ParameterStore::new("/node", "id");
        declare(&store, "a", ParameterValue::Integer(1));
        let prepared = store
            .prepare_set(vec![Parameter::new("a", ParameterValue::Integer(2))])
            .unwrap();
        assert_eq!(prepared.candidate()[0].value, ParameterValue::Integer(2));
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(1));
        prepared.abort();
        assert_eq!(store.version().unwrap(), 1);
        let stale = store
            .prepare_set(vec![Parameter::new("a", ParameterValue::Integer(3))])
            .unwrap();
        store
            .set_atomically(vec![Parameter::new("a", ParameterValue::Integer(4))])
            .unwrap();
        assert!(matches!(stale.commit(), Err(ParameterError::Conflict(_))));
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(4));
    }
    #[test]
    fn transaction_expiry_and_owner_close_are_terminal() {
        let store = ParameterStore::new("/node", "id");
        declare(&store, "a", ParameterValue::Bool(false));
        let tx = store
            .prepare_set_with_timeout(
                vec![Parameter::new("a", ParameterValue::Bool(true))],
                Duration::ZERO,
            )
            .unwrap();
        assert!(matches!(tx.commit(), Err(ParameterError::Expired)));
        let tx = store
            .prepare_set(vec![Parameter::new("a", ParameterValue::Bool(true))])
            .unwrap();
        store.close().unwrap();
        assert!(matches!(tx.commit(), Err(ParameterError::Closed)));
        assert!(matches!(store.get("a"), Err(ParameterError::Closed)));
    }
    #[test]
    fn readonly_uninitialized_dynamic_and_undeclared_contracts() {
        let store = ParameterStore::new("/node", "id");
        store
            .declare(
                "fixed",
                ParameterValue::Integer(1),
                ParameterDescriptor {
                    read_only: true,
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        assert!(matches!(
            store.set_atomically(vec![Parameter::new("fixed", ParameterValue::Integer(1))]),
            Err(ParameterError::ReadOnly(_))
        ));
        assert!(matches!(
            store.set_atomically(vec![Parameter::new("unknown", ParameterValue::Integer(1))]),
            Err(ParameterError::NotDeclared(_))
        ));
        store
            .declare(
                "pending",
                ParameterValue::NotSet,
                ParameterDescriptor {
                    parameter_type: Some(ParameterType::String),
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        assert!(matches!(
            store.get("pending"),
            Err(ParameterError::Uninitialized(_))
        ));
        assert_eq!(
            store.get_types(&["pending".into()]).unwrap(),
            vec![ParameterType::NotSet]
        );
        store
            .set_atomically(vec![Parameter::new(
                "pending",
                ParameterValue::String("ready".into()),
            )])
            .unwrap();
        assert!(store
            .set_atomically(vec![Parameter::new("pending", ParameterValue::NotSet)])
            .is_err());
        store
            .declare(
                "dynamic",
                ParameterValue::Bool(false),
                ParameterDescriptor {
                    dynamic_typing: true,
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        store
            .set_atomically(vec![Parameter::new("dynamic", ParameterValue::Integer(3))])
            .unwrap();
        let commit = store
            .set_atomically(vec![Parameter::new("dynamic", ParameterValue::NotSet)])
            .unwrap();
        assert_eq!(
            commit.event.deleted_parameters[0].value,
            ParameterValue::NotSet
        );
        assert!(matches!(
            store.get("dynamic"),
            Err(ParameterError::NotDeclared(_))
        ));
    }
    #[test]
    fn inclusive_ranges_steps_and_large_integer_arithmetic_follow_jazzy() {
        let store = ParameterStore::new("/node", "id");
        store
            .declare(
                "n",
                ParameterValue::Integer(i64::MIN),
                ParameterDescriptor {
                    integer_range: Some(IntegerRange {
                        from_value: i64::MAX,
                        to_value: i64::MIN,
                        step: 2,
                    }),
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        store
            .set_atomically(vec![Parameter::new("n", ParameterValue::Integer(i64::MAX))])
            .unwrap();
        assert!(matches!(
            store.set_atomically(vec![Parameter::new(
                "n",
                ParameterValue::Integer(i64::MIN + 1)
            )]),
            Err(ParameterError::OutOfRange(_))
        ));
        store
            .declare(
                "f",
                ParameterValue::Double(0.0),
                ParameterDescriptor {
                    floating_point_range: Some(FloatingPointRange {
                        from_value: 1.0,
                        to_value: 0.0,
                        step: -0.3,
                    }),
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        for value in [0.3, 0.6, 1.0] {
            store
                .set_atomically(vec![Parameter::new("f", ParameterValue::Double(value))])
                .unwrap();
        }
        assert!(matches!(
            store.set_atomically(vec![Parameter::new("f", ParameterValue::Double(0.4))]),
            Err(ParameterError::OutOfRange(_))
        ));
    }
    #[test]
    fn overrides_dump_load_and_hierarchical_listing() {
        let store = ParameterStore::with_overrides(
            "/node",
            "id",
            BTreeMap::from([(
                "a.x".into(),
                ParameterOverride {
                    value: ParameterValue::Integer(5),
                    source: "cli".into(),
                },
            )]),
        );
        declare(&store, "a.x", ParameterValue::Integer(1));
        declare(&store, "a.deep.y", ParameterValue::Integer(2));
        declare(&store, "b", ParameterValue::String("text".into()));
        assert_eq!(store.get("a.x").unwrap(), ParameterValue::Integer(5));
        assert_eq!(store.list(&["a".into()], 1).unwrap().names, vec!["a.x"]);
        assert_eq!(store.list(&[], 1).unwrap().names, vec!["b"]);
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.parameters[1].source, "cli");
        assert_eq!(
            snapshot.startup_config()["parameters"]["a.x"],
            json!({"type":"integer","value":5})
        );
        store
            .set_atomically(vec![Parameter::new("a.x", ParameterValue::Integer(9))])
            .unwrap();
        store.prepare_load(&snapshot).unwrap().commit().unwrap();
        assert_eq!(store.get("a.x").unwrap(), ParameterValue::Integer(5));
        assert!(
            ParameterSnapshot::from_json(r#"{"format_version":1,"format_version":2}"#).is_err()
        );
        let overridden = ParameterStore::with_overrides(
            "/n",
            "id",
            BTreeMap::from([(
                "bounded".into(),
                ParameterOverride {
                    value: ParameterValue::Integer(5),
                    source: "cli".into(),
                },
            )]),
        );
        let descriptor = ParameterDescriptor {
            integer_range: Some(IntegerRange {
                from_value: 0,
                to_value: 10,
                step: 0,
            }),
            ..Default::default()
        };
        overridden
            .declare(
                "bounded",
                ParameterValue::Integer(100),
                descriptor.clone(),
                false,
            )
            .unwrap();
        assert_eq!(
            overridden.get("bounded").unwrap(),
            ParameterValue::Integer(5)
        );
        assert!(matches!(
            ParameterStore::new("/n", "new").declare(
                "bounded",
                ParameterValue::Integer(100),
                descriptor,
                false
            ),
            Err(ParameterError::OutOfRange(_))
        ));
    }
}
