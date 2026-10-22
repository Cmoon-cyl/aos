// Copyright 2016 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// The 25-entry transition table is translated from ROS 2 Jazzy rcl
// 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0:
// rcl_lifecycle/src/default_state_machine.c. The two callback phases follow
// rclpy 4e607c09d624debc39a4ae5405680c06d87fac35 lifecycle/node.py:309-338.
// AOS adds a shared business gate, RAII work leases and bounded drain reports.
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Condvar, Mutex, OnceLock, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    clock::ShutdownResource, Error, Interface, LifecycleStateReply, Result, TypedPublisher,
};

pub const DEFAULT_LIFECYCLE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub fn lifecycle_event_interface() -> Interface {
    static INTERFACE: OnceLock<Interface> = OnceLock::new();
    INTERFACE.get_or_init(|| {
        let state = serde_json::json!({
            "type":"object","additionalProperties":false,
            "properties":{"id":{"type":"integer","minimum":0,"maximum":255},"label":{"type":"string"}},
            "required":["id","label"]
        });
        Interface::from_json(serde_json::json!({
            "name":"aos_interfaces/msg/TransitionEvent","kind":"msg",
            "message":{
                "type":"object","additionalProperties":false,
                "properties":{
                    "sequence":{"type":"integer","minimum":0},
                    "timestamp_ns":{"type":"integer","minimum":0},
                    "transition":{
                        "type":"object","additionalProperties":false,
                        "properties":{
                            "id":{"type":"integer","minimum":0,"maximum":255},
                            "label":{"type":"string"},"start_state":state,"goal_state":state
                        },
                        "required":["id","label","start_state","goal_state"]
                    },
                    "reason":{"type":"string"}
                },
                "required":["sequence","timestamp_ns","transition","reason"]
            }
        })).expect("built-in lifecycle event schema is valid")
    }).clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum LifecycleState {
    Unknown = 0,
    Unconfigured = 1,
    Inactive = 2,
    Active = 3,
    Finalized = 4,
    Configuring = 10,
    CleaningUp = 11,
    ShuttingDown = 12,
    Activating = 13,
    Deactivating = 14,
    ErrorProcessing = 15,
}
impl LifecycleState {
    pub const ALL: [Self; 11] = [
        Self::Unknown,
        Self::Unconfigured,
        Self::Inactive,
        Self::Active,
        Self::Finalized,
        Self::Configuring,
        Self::CleaningUp,
        Self::ShuttingDown,
        Self::Activating,
        Self::Deactivating,
        Self::ErrorProcessing,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Unconfigured => "unconfigured",
            Self::Inactive => "inactive",
            Self::Active => "active",
            Self::Finalized => "finalized",
            Self::Configuring => "configuring",
            Self::CleaningUp => "cleaningup",
            Self::ShuttingDown => "shuttingdown",
            Self::Activating => "activating",
            Self::Deactivating => "deactivating",
            Self::ErrorProcessing => "errorprocessing",
        }
    }
    pub fn description(self) -> LifecycleStateDescription {
        LifecycleStateDescription {
            id: self as u8,
            label: self.label().into(),
        }
    }
}

/// Public transition IDs. Callback-completion IDs cannot be requested remotely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum LifecycleTransitionId {
    Configure = 1,
    Cleanup = 2,
    Activate = 3,
    Deactivate = 4,
    UnconfiguredShutdown = 5,
    InactiveShutdown = 6,
    ActiveShutdown = 7,
}
impl LifecycleTransitionId {
    pub fn from_id(id: u8) -> Result<Self> {
        match id {
            1 => Ok(Self::Configure),
            2 => Ok(Self::Cleanup),
            3 => Ok(Self::Activate),
            4 => Ok(Self::Deactivate),
            5 => Ok(Self::UnconfiguredShutdown),
            6 => Ok(Self::InactiveShutdown),
            7 => Ok(Self::ActiveShutdown),
            _ => Err(Error::InvalidArgument(format!(
                "{id} is not a public lifecycle transition"
            ))),
        }
    }
    pub fn from_label(label: &str, state: LifecycleState) -> Result<Self> {
        match label {
            "configure" => Ok(Self::Configure),
            "cleanup" => Ok(Self::Cleanup),
            "activate" => Ok(Self::Activate),
            "deactivate" => Ok(Self::Deactivate),
            "shutdown" => match state {
                LifecycleState::Unconfigured => Ok(Self::UnconfiguredShutdown),
                LifecycleState::Inactive => Ok(Self::InactiveShutdown),
                LifecycleState::Active => Ok(Self::ActiveShutdown),
                _ => Err(Error::InvalidArgument(format!(
                    "shutdown is unavailable in {}",
                    state.label()
                ))),
            },
            _ => Err(Error::InvalidArgument(format!(
                "unknown lifecycle transition {label:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum LifecycleCallbackReturn {
    Success = 97,
    Failure = 98,
    Error = 99,
}
impl LifecycleCallbackReturn {
    pub fn from_id(id: u8) -> Result<Self> {
        match id {
            97 => Ok(Self::Success),
            98 => Ok(Self::Failure),
            99 => Ok(Self::Error),
            _ => Err(Error::InvalidArgument(format!(
                "{id} is not a lifecycle callback return"
            ))),
        }
    }
    pub fn from_label(label: &str) -> Result<Self> {
        match label {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            "error" => Ok(Self::Error),
            _ => Err(Error::InvalidArgument(format!(
                "unknown lifecycle callback return {label:?}"
            ))),
        }
    }
    fn offset(self) -> u8 {
        self as u8 - Self::Success as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleStateDescription {
    pub id: u8,
    pub label: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleTransitionDescription {
    pub id: u8,
    pub label: String,
    pub start_state: LifecycleStateDescription,
    pub goal_state: LifecycleStateDescription,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleWorkKind {
    Publisher,
    Timer,
    Subscription,
    Service,
    Action,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleInFlight {
    pub lease_id: u64,
    pub kind: LifecycleWorkKind,
    pub name: String,
    pub entity_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleSnapshot {
    pub current_state: LifecycleStateDescription,
    pub transitioning: bool,
    pub gate_open: bool,
    pub epoch: u64,
    pub in_flight: Vec<LifecycleInFlight>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleTransitionReport {
    pub transition_id: u8,
    pub initial_state: LifecycleStateDescription,
    pub final_state: LifecycleStateDescription,
    pub callback_return: LifecycleCallbackReturn,
    pub error_callback_return: Option<LifecycleCallbackReturn>,
    pub successful: bool,
    pub drain_timed_out: bool,
    pub remaining: Vec<LifecycleInFlight>,
    pub abandoned: bool,
    pub reason: String,
}
/// Error completion requests a second, on_error callback on the same token.
/// A completed report does not mean Node resources or its process were closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleProgress {
    pub callback_state: Option<LifecycleStateDescription>,
    pub report: Option<LifecycleTransitionReport>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleEvent {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub transition: LifecycleTransitionDescription,
    pub reason: String,
}

#[derive(Clone, Copy)]
struct TransitionEdge {
    id: u8,
    label: &'static str,
    start: LifecycleState,
    goal: LifecycleState,
}
impl TransitionEdge {
    fn description(self) -> LifecycleTransitionDescription {
        LifecycleTransitionDescription {
            id: self.id,
            label: self.label.into(),
            start_state: self.start.description(),
            goal_state: self.goal.description(),
        }
    }
}
use LifecycleState::*;
const TRANSITIONS: [TransitionEdge; 25] = [
    edge(1, "configure", Unconfigured, Configuring),
    edge(10, "transition_success", Configuring, Inactive),
    edge(11, "transition_failure", Configuring, Unconfigured),
    edge(12, "transition_error", Configuring, ErrorProcessing),
    edge(2, "cleanup", Inactive, CleaningUp),
    edge(20, "transition_success", CleaningUp, Unconfigured),
    edge(21, "transition_failure", CleaningUp, Inactive),
    edge(22, "transition_error", CleaningUp, ErrorProcessing),
    edge(3, "activate", Inactive, Activating),
    edge(30, "transition_success", Activating, Active),
    edge(31, "transition_failure", Activating, Inactive),
    edge(32, "transition_error", Activating, ErrorProcessing),
    edge(4, "deactivate", Active, Deactivating),
    edge(40, "transition_success", Deactivating, Inactive),
    edge(41, "transition_failure", Deactivating, Active),
    edge(42, "transition_error", Deactivating, ErrorProcessing),
    edge(5, "shutdown", Unconfigured, ShuttingDown),
    edge(6, "shutdown", Inactive, ShuttingDown),
    edge(7, "shutdown", Active, ShuttingDown),
    edge(50, "transition_success", ShuttingDown, Finalized),
    edge(51, "transition_failure", ShuttingDown, Finalized),
    edge(52, "transition_error", ShuttingDown, ErrorProcessing),
    edge(60, "transition_success", ErrorProcessing, Unconfigured),
    edge(61, "transition_failure", ErrorProcessing, Finalized),
    edge(62, "transition_error", ErrorProcessing, Finalized),
];
const fn edge(
    id: u8,
    label: &'static str,
    start: LifecycleState,
    goal: LifecycleState,
) -> TransitionEdge {
    TransitionEdge {
        id,
        label,
        start,
        goal,
    }
}

/// One state authority and business gate per managed Node.
///
/// Callbacks run in the caller's executor, with no lifecycle lock held. A
/// callback that never returns remains in progress; core cannot terminate it.
#[derive(Clone)]
pub struct Lifecycle {
    inner: Arc<LifecycleInner>,
}
type ActivationHook = Arc<dyn Fn() -> Result<()> + Send + Sync>;
struct LifecycleInner {
    node: String,
    // Serializes state changes with a restricted parameter commit. This lock
    // is separate from the business gate: parameter clock changes may acquire
    // a Clock/Timer lock, while ready timers acquire the business gate.
    transition_commit: Mutex<()>,
    state: Mutex<LifecycleData>,
    changed: Condvar,
    publisher: Mutex<Option<Arc<TypedPublisher>>>,
    activation_hook: Mutex<Option<ActivationHook>>,
}
struct LifecycleData {
    closed: bool,
    current: LifecycleState,
    transition: Option<u64>,
    serial: u64,
    epoch: u64,
    next_lease: u64,
    in_flight: BTreeMap<u64, LifecycleInFlight>,
    events: VecDeque<LifecycleEvent>,
    last_report: Option<LifecycleTransitionReport>,
}
impl Lifecycle {
    pub(crate) fn new(node: String) -> Self {
        Self {
            inner: Arc::new(LifecycleInner {
                node,
                transition_commit: Mutex::new(()),
                state: Mutex::new(LifecycleData {
                    closed: false,
                    current: Unconfigured,
                    transition: None,
                    serial: 0,
                    epoch: 0,
                    next_lease: 0,
                    in_flight: BTreeMap::new(),
                    events: VecDeque::new(),
                    last_report: None,
                }),
                changed: Condvar::new(),
                publisher: Mutex::new(None),
                activation_hook: Mutex::new(None),
            }),
        }
    }
    pub fn current_state(&self) -> Result<LifecycleState> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(state.current)
    }
    pub fn snapshot(&self) -> Result<LifecycleSnapshot> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(LifecycleSnapshot {
            current_state: state.current.description(),
            transitioning: state.transition.is_some(),
            gate_open: state.current == Active,
            epoch: state.epoch,
            in_flight: state.in_flight.values().cloned().collect(),
        })
    }
    /// The remote state query observes its snapshot and last report atomically.
    pub fn state_reply(&self) -> Result<LifecycleStateReply> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        let current_state = state.current.description();
        Ok(LifecycleStateReply {
            current_state: current_state.clone(),
            snapshot: LifecycleSnapshot {
                current_state,
                transitioning: state.transition.is_some(),
                gate_open: state.current == Active,
                epoch: state.epoch,
                in_flight: state.in_flight.values().cloned().collect(),
            },
            last_report: state.last_report.clone(),
        })
    }
    pub fn available_states(&self) -> Vec<LifecycleStateDescription> {
        LifecycleState::ALL
            .into_iter()
            .map(LifecycleState::description)
            .collect()
    }
    pub fn transition_graph(&self) -> Vec<LifecycleTransitionDescription> {
        TRANSITIONS
            .into_iter()
            .map(TransitionEdge::description)
            .collect()
    }
    /// Includes the callback-result edges when a transition is in progress,
    /// matching rcl's available transition query. begin_transition only accepts
    /// public IDs, and callbacks complete through their owned token.
    pub fn available_transitions(&self) -> Result<Vec<LifecycleTransitionDescription>> {
        let current = self.current_state()?;
        Ok(TRANSITIONS
            .into_iter()
            .filter(|edge| edge.start == current)
            .map(TransitionEdge::description)
            .collect())
    }
    pub fn begin_transition(
        &self,
        transition: LifecycleTransitionId,
    ) -> Result<LifecycleTransition> {
        let (initial, serial, event) = {
            let _commit = self.inner.transition_commit.lock().unwrap();
            let mut state = self.inner.state.lock().unwrap();
            ensure_open(&state)?;
            if state.transition.is_some() {
                return Err(Error::InvalidArgument(
                    "a lifecycle transition is already in progress".into(),
                ));
            }
            let edge = find_edge(state.current, transition as u8)?;
            let initial = state.current;
            state.serial = state.serial.wrapping_add(1);
            let serial = state.serial;
            state.transition = Some(serial);
            let event = move_state(&mut state, edge, "");
            (initial, serial, event)
        };
        self.inner.changed.notify_all();
        self.inner.publish_event(&event);
        Ok(LifecycleTransition {
            lifecycle: self.clone(),
            serial,
            transition,
            initial,
            phase: event.transition.goal_state.id,
            original_outcome: None,
            drain_timed_out: false,
            reason: String::new(),
            completed: false,
        })
    }
    pub fn last_transition_report(&self) -> Result<Option<LifecycleTransitionReport>> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(state.last_report.clone())
    }
    /// Bounded diagnostic history. Events are also sent on transition_event
    /// when the Node enables its lifecycle control endpoints.
    pub fn take_transition_events(&self) -> Vec<LifecycleEvent> {
        self.inner.state.lock().unwrap().events.drain(..).collect()
    }
    pub(crate) fn active_epoch(&self) -> Result<Option<u64>> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok((state.current == Active).then_some(state.epoch))
    }
    /// Hold only the state-change barrier, never the business gate, while a
    /// native parameter transaction commits. No callback or transport operation
    /// may execute before this guard is released.
    pub(crate) fn parameter_state(&self) -> Result<ParameterLifecycleState<'_>> {
        let guard = self.inner.transition_commit.lock().unwrap();
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        Ok(ParameterLifecycleState {
            _guard: guard,
            current: state.current,
            epoch: state.epoch,
        })
    }
    /// Only a nonblocking native queue read may run here. Replies and user
    /// callbacks run after this lock is released.
    pub(crate) fn receive_if_inactive<T>(
        &self,
        receive: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        if state.current == Active {
            Ok(None)
        } else {
            receive().map(Some)
        }
    }
    pub(crate) fn acquire(
        &self,
        kind: LifecycleWorkKind,
        name: &str,
        entity_id: &str,
    ) -> Result<LifecycleLease> {
        self.acquire_in_epoch(kind, name, entity_id, None)
    }
    /// Commit only native admission state while holding the gate. This is not
    /// a callback API: callers must not run user code or transport operations.
    pub(crate) fn with_active_epoch<T>(
        &self,
        epoch: u64,
        commit: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        if state.current != Active || epoch != state.epoch {
            return Err(Error::Inactive(
                "lifecycle changed before business admission completed".into(),
            ));
        }
        commit()
    }
    pub(crate) fn acquire_in_epoch(
        &self,
        kind: LifecycleWorkKind,
        name: &str,
        entity_id: &str,
        epoch: Option<u64>,
    ) -> Result<LifecycleLease> {
        let mut state = self.inner.state.lock().unwrap();
        ensure_open(&state)?;
        if state.current != Active || epoch.is_some_and(|epoch| epoch != state.epoch) {
            return Err(Error::Inactive(format!(
                "{} is {}; {name} cannot start new business",
                self.inner.node,
                state.current.label()
            )));
        }
        state.next_lease = state
            .next_lease
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("lifecycle lease counter overflow".into()))?;
        let id = state.next_lease;
        state.in_flight.insert(
            id,
            LifecycleInFlight {
                lease_id: id,
                kind,
                name: name.into(),
                entity_id: entity_id.into(),
            },
        );
        Ok(LifecycleLease {
            owner: Arc::downgrade(&self.inner),
            id,
            epoch: state.epoch,
        })
    }
    pub(crate) fn set_activation_hook(&self, hook: ActivationHook) {
        *self.inner.activation_hook.lock().unwrap() = Some(hook);
    }
    pub(crate) fn set_event_publisher(&self, publisher: Arc<TypedPublisher>) {
        *self.inner.publisher.lock().unwrap() = Some(publisher);
    }
    pub(crate) fn shutdown_resource(&self) -> Arc<dyn ShutdownResource> {
        self.inner.clone()
    }
    pub(crate) fn close(&self) {
        self.inner.shutdown();
    }
}
impl LifecycleInner {
    fn publish_event(&self, event: &LifecycleEvent) {
        let publisher = self.publisher.lock().unwrap().clone();
        if let Some(publisher) = publisher {
            // State transitions are authoritative even if transport delivery
            // fails. The queryable snapshot and report remain available.
            let _ = publisher.publish(&serde_json::json!(event));
        }
    }
}
impl ShutdownResource for LifecycleInner {
    fn shutdown(&self) {
        let _commit = self.transition_commit.lock().unwrap();
        self.state.lock().unwrap().closed = true;
        self.changed.notify_all();
    }
}

pub(crate) struct ParameterLifecycleState<'a> {
    _guard: std::sync::MutexGuard<'a, ()>,
    pub(crate) current: LifecycleState,
    pub(crate) epoch: u64,
}

pub(crate) struct LifecycleLease {
    owner: Weak<LifecycleInner>,
    id: u64,
    epoch: u64,
}
impl LifecycleLease {
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}
impl Drop for LifecycleLease {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.state.lock().unwrap().in_flight.remove(&self.id);
            owner.changed.notify_all();
        }
    }
}

/// Single transition owner. Invoke the indicated callback outside core, then
/// call complete. If callback_state is errorprocessing, invoke on_error and
/// complete the same token again.
///
/// Completion may block only for drain, measured by a steady clock. The caller
/// must keep the business executor running during this wait. The timeout does
/// not interrupt synchronous user callbacks or claim that their work ended.
pub struct LifecycleTransition {
    lifecycle: Lifecycle,
    serial: u64,
    transition: LifecycleTransitionId,
    initial: LifecycleState,
    phase: u8,
    original_outcome: Option<LifecycleCallbackReturn>,
    drain_timed_out: bool,
    reason: String,
    completed: bool,
}
impl LifecycleTransition {
    pub fn previous_state(&self) -> LifecycleStateDescription {
        self.initial.description()
    }
    pub fn callback_state(&self) -> Result<LifecycleStateDescription> {
        let state = self.lifecycle.inner.state.lock().unwrap();
        self.ensure_current(&state)?;
        Ok(state.current.description())
    }
    pub fn complete(
        &mut self,
        outcome: LifecycleCallbackReturn,
        drain_timeout: Option<Duration>,
    ) -> Result<LifecycleProgress> {
        self.complete_with_reason(outcome, drain_timeout, "")
    }
    pub fn complete_with_reason(
        &mut self,
        mut outcome: LifecycleCallbackReturn,
        drain_timeout: Option<Duration>,
        callback_reason: &str,
    ) -> Result<LifecycleProgress> {
        let callback_return = outcome;
        let owner = &self.lifecycle.inner;
        let timeout = drain_timeout.unwrap_or(DEFAULT_LIFECYCLE_DRAIN_TIMEOUT);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("lifecycle drain timeout overflow".into()))?;
        let mut reason = callback_reason.to_owned();
        let mut timed_out = false;
        let mut state = owner.state.lock().unwrap();
        self.ensure_current(&state)?;
        if outcome == LifecycleCallbackReturn::Success
            && matches!(state.current, Deactivating | ShuttingDown)
        {
            while !state.in_flight.is_empty() {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    timed_out = true;
                    outcome = LifecycleCallbackReturn::Failure;
                    reason = "business drain timed out; remaining work was not terminated".into();
                    break;
                }
                state = owner.changed.wait_timeout(state, left).unwrap().0;
                self.ensure_current(&state)?;
            }
        }
        let edge = result_edge(state.current, outcome)?;
        if edge.goal == Active {
            // The gate remains closed while queues and timer schedules reset.
            // This hook has no application callback and runs outside gate locks.
            drop(state);
            let hook = owner.activation_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                if let Err(error) = hook() {
                    outcome = LifecycleCallbackReturn::Error;
                    reason = format!("cannot prepare managed entities for activation: {error}");
                }
            }
            state = owner.state.lock().unwrap();
            self.ensure_current(&state)?;
        }
        // Drain and activation hooks run without the state-change barrier.
        // Recheck after acquiring it so parameter commit and this state change
        // have one linearization point, without a Clock/Timer lock inversion.
        drop(state);
        let commit = owner.transition_commit.lock().unwrap();
        let mut state = owner.state.lock().unwrap();
        self.ensure_current(&state)?;
        let edge = result_edge(state.current, outcome)?;
        let event = move_state(&mut state, edge, &reason);
        self.phase = edge.goal as u8;
        self.drain_timed_out |= timed_out;
        if !reason.is_empty() {
            if !self.reason.is_empty() {
                self.reason.push_str("; ");
            }
            self.reason.push_str(&reason);
        }
        let progress = if edge.goal == ErrorProcessing {
            self.original_outcome = Some(callback_return);
            LifecycleProgress {
                callback_state: Some(ErrorProcessing.description()),
                report: None,
            }
        } else {
            let original = self.original_outcome.unwrap_or(callback_return);
            let report = LifecycleTransitionReport {
                transition_id: self.transition as u8,
                initial_state: self.initial.description(),
                final_state: edge.goal.description(),
                callback_return: original,
                error_callback_return: self.original_outcome.map(|_| callback_return),
                successful: self.original_outcome.is_none()
                    && outcome == LifecycleCallbackReturn::Success,
                drain_timed_out: self.drain_timed_out,
                remaining: state.in_flight.values().cloned().collect(),
                abandoned: false,
                reason: self.reason.clone(),
            };
            state.last_report = Some(report.clone());
            state.transition = None;
            self.completed = true;
            LifecycleProgress {
                callback_state: None,
                report: Some(report),
            }
        };
        drop(state);
        drop(commit);
        owner.changed.notify_all();
        owner.publish_event(&event);
        Ok(progress)
    }
    fn ensure_current(&self, state: &LifecycleData) -> Result<()> {
        ensure_open(state)?;
        if self.completed
            || state.transition != Some(self.serial)
            || state.current as u8 != self.phase
        {
            return Err(Error::InvalidArgument(
                "lifecycle transition token is no longer current".into(),
            ));
        }
        Ok(())
    }
}
impl Drop for LifecycleTransition {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let owner = &self.lifecycle.inner;
        let commit = owner.transition_commit.lock().unwrap();
        let mut state = owner.state.lock().unwrap();
        if self.ensure_current(&state).is_err() {
            return;
        }
        let reason = "transition token abandoned; callbacks were not assumed to have completed";
        let mut events = Vec::new();
        if state.current != ErrorProcessing {
            let edge = result_edge(state.current, LifecycleCallbackReturn::Error)
                .expect("a live transition has a callback-result edge");
            events.push(move_state(&mut state, edge, reason));
        }
        let edge = result_edge(ErrorProcessing, LifecycleCallbackReturn::Error).unwrap();
        events.push(move_state(&mut state, edge, reason));
        state.transition = None;
        state.last_report = Some(LifecycleTransitionReport {
            transition_id: self.transition as u8,
            initial_state: self.initial.description(),
            final_state: Finalized.description(),
            callback_return: LifecycleCallbackReturn::Error,
            error_callback_return: None,
            successful: false,
            drain_timed_out: false,
            remaining: state.in_flight.values().cloned().collect(),
            abandoned: true,
            reason: reason.into(),
        });
        drop(state);
        drop(commit);
        owner.changed.notify_all();
        for event in events {
            owner.publish_event(&event);
        }
    }
}

fn ensure_open(state: &LifecycleData) -> Result<()> {
    if state.closed {
        Err(Error::Closed)
    } else {
        Ok(())
    }
}
fn find_edge(state: LifecycleState, id: u8) -> Result<TransitionEdge> {
    TRANSITIONS
        .into_iter()
        .find(|edge| edge.start == state && edge.id == id)
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "lifecycle transition {id} is unavailable in {}",
                state.label()
            ))
        })
}
fn result_edge(state: LifecycleState, outcome: LifecycleCallbackReturn) -> Result<TransitionEdge> {
    let base = match state {
        Configuring => 10,
        CleaningUp => 20,
        Activating => 30,
        Deactivating => 40,
        ShuttingDown => 50,
        ErrorProcessing => 60,
        _ => {
            return Err(Error::InvalidArgument(
                "current lifecycle state has no callback".into(),
            ))
        }
    };
    find_edge(state, base + outcome.offset())
}
fn move_state(state: &mut LifecycleData, edge: TransitionEdge, reason: &str) -> LifecycleEvent {
    state.current = edge.goal;
    state.epoch = state.epoch.wrapping_add(1);
    let event = LifecycleEvent {
        sequence: state.epoch,
        timestamp_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64,
        transition: edge.description(),
        reason: reason.into(),
    };
    if state.events.len() == 256 {
        state.events.pop_front();
    }
    state.events.push_back(event.clone());
    event
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use LifecycleCallbackReturn::{Error as CallbackError, Failure, Success};
    use LifecycleTransitionId::*;

    fn state_at(target: LifecycleState) -> Lifecycle {
        let lifecycle = Lifecycle::new("/managed".into());
        if target != Unconfigured {
            let report = lifecycle
                .begin_transition(Configure)
                .unwrap()
                .complete(Success, None)
                .unwrap()
                .report
                .unwrap();
            assert!(report.successful);
        }
        if target == Active {
            lifecycle
                .begin_transition(Activate)
                .unwrap()
                .complete(Success, None)
                .unwrap();
        }
        assert_eq!(lifecycle.current_state().unwrap(), target);
        lifecycle
    }

    #[test]
    fn fixed_jazzy_primary_transitions_and_callback_result_paths() {
        let cases = [
            (Unconfigured, Configure, Configuring, Inactive, Unconfigured),
            (Inactive, Cleanup, CleaningUp, Unconfigured, Inactive),
            (Inactive, Activate, Activating, Active, Inactive),
            (Active, Deactivate, Deactivating, Inactive, Active),
            (
                Unconfigured,
                UnconfiguredShutdown,
                ShuttingDown,
                Finalized,
                Finalized,
            ),
            (
                Inactive,
                InactiveShutdown,
                ShuttingDown,
                Finalized,
                Finalized,
            ),
            (Active, ActiveShutdown, ShuttingDown, Finalized, Finalized),
        ];
        for (initial, request, intermediate, success_state, failure_state) in cases {
            for (outcome, expected) in [(Success, success_state), (Failure, failure_state)] {
                let lifecycle = state_at(initial);
                let mut transition = lifecycle.begin_transition(request).unwrap();
                assert_eq!(lifecycle.current_state().unwrap(), intermediate);
                assert_eq!(transition.previous_state(), initial.description());
                let report = transition.complete(outcome, None).unwrap().report.unwrap();
                assert_eq!(report.final_state, expected.description());
                assert_eq!(report.successful, outcome == Success);
                assert_eq!(lifecycle.snapshot().unwrap().gate_open, expected == Active);
            }
            for (error_result, expected) in [
                (Success, Unconfigured),
                (Failure, Finalized),
                (CallbackError, Finalized),
            ] {
                let lifecycle = state_at(initial);
                let mut transition = lifecycle.begin_transition(request).unwrap();
                let progress = transition
                    .complete_with_reason(CallbackError, None, "callback raised")
                    .unwrap();
                assert!(progress.report.is_none());
                assert_eq!(progress.callback_state, Some(ErrorProcessing.description()));
                assert_eq!(lifecycle.current_state().unwrap(), ErrorProcessing);
                let report = transition
                    .complete(error_result, None)
                    .unwrap()
                    .report
                    .unwrap();
                assert_eq!(report.final_state, expected.description());
                assert_eq!(report.callback_return, CallbackError);
                assert_eq!(report.error_callback_return, Some(error_result));
                assert_eq!(report.reason, "callback raised");
                assert!(!report.successful);
            }
        }
        assert_eq!(state_at(Unconfigured).transition_graph().len(), 25);
        assert!(LifecycleTransitionId::from_id(41).is_err());
    }

    #[test]
    fn serialized_transition_keeps_queries_and_callback_reentry_available() {
        let lifecycle = state_at(Unconfigured);
        assert!(lifecycle.begin_transition(Activate).is_err());
        let mut token = lifecycle.begin_transition(Configure).unwrap();
        assert!(lifecycle.begin_transition(Configure).is_err());
        let other = lifecycle.clone();
        thread::spawn(move || {
            assert_eq!(other.current_state().unwrap(), Configuring);
            assert_eq!(other.available_transitions().unwrap().len(), 3);
            assert!(!other.snapshot().unwrap().gate_open);
        })
        .join()
        .unwrap();
        token.complete(Success, None).unwrap();
        assert!(token.complete(Success, None).is_err());
        assert_eq!(
            lifecycle
                .take_transition_events()
                .iter()
                .map(|event| event.transition.id)
                .collect::<Vec<_>>(),
            [1, 10]
        );
    }

    #[test]
    fn drain_waits_for_real_lease_and_timeout_reopens_active_gate_with_report() {
        let lifecycle = state_at(Active);
        let lease = lifecycle
            .acquire(LifecycleWorkKind::Service, "/service", "service-id")
            .unwrap();
        let mut transition = lifecycle.begin_transition(Deactivate).unwrap();
        assert!(matches!(
            lifecycle.acquire(LifecycleWorkKind::Publisher, "/topic", "publisher-id"),
            Err(Error::Inactive(_))
        ));
        let report = transition
            .complete(Success, Some(Duration::ZERO))
            .unwrap()
            .report
            .unwrap();
        assert!(!report.successful);
        assert!(report.drain_timed_out);
        assert_eq!(report.callback_return, Success);
        assert_eq!(report.final_state, Active.description());
        assert_eq!(report.remaining.len(), 1);
        assert_eq!(report.remaining[0].name, "/service");
        assert!(lifecycle.snapshot().unwrap().gate_open);
        let mut transition = lifecycle.begin_transition(Deactivate).unwrap();
        let observer = lifecycle.clone();
        let task = thread::spawn(move || {
            assert_eq!(observer.current_state().unwrap(), Deactivating);
            drop(lease);
        });
        let report = transition
            .complete(Success, Some(Duration::from_secs(1)))
            .unwrap()
            .report
            .unwrap();
        task.join().unwrap();
        assert!(report.successful);
        assert!(!report.drain_timed_out);
        assert!(report.remaining.is_empty());
        assert_eq!(lifecycle.current_state().unwrap(), Inactive);
    }

    #[test]
    fn shutdown_failure_and_abandonment_never_claim_work_was_closed() {
        let lifecycle = state_at(Active);
        let lease = lifecycle
            .acquire(LifecycleWorkKind::Action, "/action", "goal-id")
            .unwrap();
        let report = lifecycle
            .begin_transition(ActiveShutdown)
            .unwrap()
            .complete(Failure, None)
            .unwrap()
            .report
            .unwrap();
        assert_eq!(report.final_state, Finalized.description());
        assert!(!report.successful);
        assert_eq!(report.remaining[0].entity_id, "goal-id");
        assert_eq!(lifecycle.snapshot().unwrap().in_flight.len(), 1);
        drop(lease);
        assert!(lifecycle.snapshot().unwrap().in_flight.is_empty());
        assert!(lifecycle.last_transition_report().unwrap().is_some());
        let abandoned = state_at(Unconfigured);
        drop(abandoned.begin_transition(Configure).unwrap());
        assert_eq!(abandoned.current_state().unwrap(), Finalized);
        let report = abandoned.last_transition_report().unwrap().unwrap();
        assert!(report.abandoned);
        assert!(!report.successful);
        assert_eq!(report.error_callback_return, None);
    }
}
