// Copyright 2018 Open Source Robotics Foundation, Inc.
// Copyright 2019 Open Source Robotics Foundation, Inc.
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
// Cancel candidate selection is translated from ROS 2 Jazzy rcl at
// 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0, action_server.c:816-905.
// Goal acceptance, execution completion and deferred result ordering follow
// rclpy 4e607c09d624debc39a4ae5405680c06d87fac35 action/server.py.
// AOS adaptation: Rust owns state, JSON wrappers and result waiters; language
// executors take owned request objects and run user callbacks outside locks.
use super::{
    limits::{json_size, text_size, ByteBudget, BytePermit, CountBudget, CountPermit},
    messages::*,
    state::*,
    ActionResourceLimits,
};
#[cfg(test)]
#[path = "server_limits_tests.rs"]
mod limits_tests;
#[cfg(test)]
#[path = "server_resource_tests.rs"]
mod resource_tests;
use crate::{
    lifecycle::LifecycleLease,
    node::{Common, Resource},
    ActionQos, EndpointOptions, EntityKind, Error, Interface, LifecycleWorkKind, Node, Result,
    Service, ServiceRequest, TelemetryComponent, TelemetryEvent, TelemetryOutcome, TypedPublisher,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

pub struct ActionServer {
    inner: Arc<ServerInner>,
}
struct ServerInner {
    common: Common,
    interface: Interface,
    result_timeout: Duration,
    result_waiters: Arc<CountBudget>,
    resource_limits: ActionResourceLimits,
    goal_slots: Arc<CountBudget>,
    result_bytes: Arc<ByteBudget>,
    registry: Mutex<Registry>,
    status_publish: Mutex<()>,
    goal_receive: Mutex<()>,
    goal_depth: usize,
    send_goal: Arc<Service>,
    get_result: Arc<Service>,
    cancel_goal: Arc<Service>,
    feedback: TypedPublisher,
    status: TypedPublisher,
}
#[derive(Default)]
struct Registry {
    goals: BTreeMap<String, Arc<GoalRecord>>,
}
struct GoalRecord {
    info: GoalInfo,
    request: Value,
    request_id: Option<String>,
    runtime: Mutex<GoalRuntime>,
}
struct GoalRuntime {
    status: GoalStatus,
    execution_scheduled: bool,
    result: Option<Arc<RetainedResult>>,
    waiters: Vec<ResultWaiter>,
    terminal_at: Option<Instant>,
    expired: bool,
    lifecycle_lease: Option<Arc<LifecycleLease>>,
    goal_slot: Option<CountPermit>,
}
struct ResultWaiter {
    request: ServiceRequest,
    _permit: CountPermit,
}
struct RetainedResult {
    response: Result<Value>,
    _bytes: Option<BytePermit>,
}
struct PreparedResult {
    value: Result<Value>,
    bytes: Option<BytePermit>,
    provider_error: Option<Error>,
}
impl PreparedResult {
    fn resource_error(code: &'static str) -> Self {
        // Fixed control metadata is bounded by max_goals. Never retain the
        // rejected business value or an arbitrarily sized error message.
        let error = Error::Remote(code.into());
        Self {
            value: Err(error.clone()),
            bytes: None,
            provider_error: Some(error),
        }
    }
}

impl ActionServer {
    pub fn new(
        node: &Node,
        interface: &Interface,
        name: &str,
        options: ActionServerOptions,
        metadata: EndpointOptions,
    ) -> Result<Self> {
        Self::new_with_qos(
            node,
            interface,
            name,
            options,
            metadata,
            ActionQos::default(),
        )
    }
    pub fn new_with_qos(
        node: &Node,
        interface: &Interface,
        name: &str,
        options: ActionServerOptions,
        metadata: EndpointOptions,
        qos: ActionQos,
    ) -> Result<Self> {
        options.validate()?;
        qos.validate()?;
        let types = ActionInterfaces::new(interface)?;
        let endpoint = node.endpoint(interface, name)?;
        let base = endpoint.name.clone();
        let goal_depth = qos.goal_service.profile.depth;
        let send_goal = Arc::new(node.create_control_service(
            &types.send_goal,
            &format!("{base}/_action/send_goal"),
            qos.goal_service,
            EndpointOptions::default(),
        )?);
        let get_result = Arc::new(node.create_control_service(
            &types.get_result,
            &format!("{base}/_action/get_result"),
            qos.result_service,
            EndpointOptions::default(),
        )?);
        let cancel_goal = Arc::new(node.create_control_service(
            &types.cancel_goal,
            &format!("{base}/_action/cancel_goal"),
            qos.cancel_service,
            EndpointOptions::default(),
        )?);
        let feedback = node.create_control_publisher(
            &types.feedback,
            &format!("{base}/_action/feedback"),
            qos.feedback,
            EndpointOptions::default(),
        )?;
        let status = node.create_control_publisher(
            &types.status,
            &format!("{base}/_action/status"),
            qos.status,
            EndpointOptions::default(),
        )?;
        let common = node.common(
            interface,
            endpoint,
            EntityKind::ActionServer,
            None,
            name,
            metadata,
        )?;
        let inner = Arc::new(ServerInner {
            common,
            interface: interface.clone(),
            result_timeout: options.result_timeout,
            result_waiters: CountBudget::new(options.max_result_waiters),
            resource_limits: options.resource_limits,
            goal_slots: CountBudget::new(options.resource_limits.max_goals),
            result_bytes: ByteBudget::new(options.resource_limits.max_result_cache_bytes),
            registry: Mutex::new(Registry::default()),
            status_publish: Mutex::new(()),
            goal_receive: Mutex::new(()),
            goal_depth,
            send_goal,
            get_result,
            cancel_goal,
            feedback,
            status,
        });
        node.register(inner.clone())?;
        inner.publish_status()?;
        if let Some(lifecycle) = inner.common.lifecycle() {
            let weak = Arc::downgrade(&inner);
            std::thread::Builder::new()
                .name("aos-action-inactive".into())
                .spawn(move || loop {
                    let Some(server) = weak.upgrade() else { break };
                    if server.common.ensure_open().is_err() {
                        break;
                    }
                    let received = {
                        let _receiving = server.goal_receive.lock().unwrap();
                        server.send_goal.take_inactive_backend(&lifecycle)
                    };
                    // Invalid input can require an error reply. Decode only
                    // after the native take has released both receive locks
                    // and the lifecycle gate, just like normal Service calls.
                    let received = received.and_then(|request| {
                        request
                            .map(|request| server.send_goal.decode_request(request, None))
                            .transpose()
                    });
                    match received {
                        Ok(Some(request)) => {
                            server.reject_goal(request, TelemetryOutcome::Inactive)
                        }
                        Ok(None)
                        | Err(Error::Timeout | Error::Validation(_) | Error::ResourceLimit(_)) => {}
                        Err(_) => break,
                    }
                    drop(server);
                    std::thread::sleep(Duration::from_millis(5));
                })
                .map_err(|e| Error::Transport(format!("cannot start inactive goal worker: {e}")))?;
        }
        let weak = Arc::downgrade(&inner);
        let result_service = inner.get_result.clone();
        std::thread::Builder::new()
            .name("aos-action-results".into())
            .spawn(move || {
                loop {
                    // The result service owns its queue independently. Waiting here
                    // never holds the ActionServer alive or takes a business slot.
                    let received = result_service.recv_timeout(Duration::from_millis(20));
                    let Some(server) = weak.upgrade() else {
                        break;
                    };
                    if server.common.ensure_open().is_err() {
                        break;
                    }
                    server.expire_goals();
                    match received {
                        Ok(request) => server.process_result(request),
                        Err(Error::Timeout | Error::Validation(_) | Error::ResourceLimit(_)) => {}
                        Err(Error::Closed) => break,
                        Err(_) => break,
                    }
                }
            })
            .map_err(|e| Error::Transport(format!("cannot start action result worker: {e}")))?;
        Ok(Self { inner })
    }

    pub fn take_goal_request(&self, timeout: Duration) -> Result<GoalRequest> {
        self.inner.common.ensure_open()?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("timeout overflow".into()))?;
        loop {
            let (request, lease) = {
                let _receiving = self.inner.goal_receive.lock().unwrap();
                let epoch = self
                    .inner
                    .common
                    .lifecycle()
                    .map(|lifecycle| {
                        lifecycle.active_epoch()?.ok_or_else(|| {
                            Error::Inactive("managed ActionServer is not active".into())
                        })
                    })
                    .transpose()?;
                let left = deadline.saturating_duration_since(Instant::now());
                let wait = if epoch.is_some() {
                    left.min(Duration::from_millis(20))
                } else {
                    left
                };
                let request = match self.inner.send_goal.recv_timeout(wait) {
                    Ok(request) => request,
                    Err(Error::Timeout) if Instant::now() < deadline => continue,
                    Err(error) => return Err(error),
                };
                let lease = self.inner.common.business_lease_in_epoch(
                    LifecycleWorkKind::Action,
                    request.request()["goal_id"].as_str(),
                    epoch,
                );
                (request, lease)
            };
            let lease = match lease {
                Ok(lease) => lease,
                Err(error) => {
                    self.inner
                        .reject_goal(request, TelemetryOutcome::from(&error));
                    return Err(error);
                }
            };
            self.inner.common.ensure_open()?;
            let goal_id = request.request()["goal_id"].as_str().unwrap().to_owned();
            if json_size(
                &request.request()["goal"],
                self.inner.resource_limits.max_goal_bytes,
            )?
            .is_none()
            {
                self.inner.reject_goal(request, TelemetryOutcome::Rejected);
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                continue;
            }
            self.inner.expire_goals();
            let goal_slot = {
                let registry = self.inner.registry.lock().unwrap();
                self.inner.common.ensure_open()?;
                (!registry.goals.contains_key(&goal_id))
                    .then(|| self.inner.goal_slots.acquire())
                    .flatten()
            };
            let Some(goal_slot) = goal_slot else {
                self.inner.reject_goal(request, TelemetryOutcome::Rejected);
                // Neither duplicates nor resource rejection reach the user
                // callback. Rejected traffic must not defeat the take deadline.
                if Instant::now() >= deadline {
                    return Err(Error::Timeout);
                }
                continue;
            };
            let goal = request.request()["goal"].clone();
            return Ok(GoalRequest {
                server: Arc::downgrade(&self.inner),
                goal_id,
                goal,
                request: Mutex::new(Some(request)),
                lifecycle_lease: Mutex::new(lease),
                goal_slot: Mutex::new(Some(goal_slot)),
            });
        }
    }

    pub fn take_cancel_request(&self, timeout: Duration) -> Result<CancelRequest> {
        self.inner.common.ensure_open()?;
        let request = self.inner.cancel_goal.recv_timeout(timeout)?;
        self.inner.common.ensure_open()?;
        let info: GoalInfo = serde_json::from_value(request.request()["goal_info"].clone())
            .map_err(|e| Error::Validation(e.to_string()))?;
        self.inner.expire_goals();
        let (return_code, goals) = self.inner.select_cancel(&info);
        Ok(CancelRequest {
            server: Arc::downgrade(&self.inner),
            value: request.request().clone(),
            request: Mutex::new(Some(request)),
            return_code,
            candidates: goals
                .into_iter()
                .map(|goal| ServerGoalHandle {
                    server: Arc::downgrade(&self.inner),
                    goal,
                })
                .collect(),
        })
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
impl Drop for ActionServer {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Resource for ServerInner {
    fn common(&self) -> &Common {
        &self.common
    }
    fn prepare_activation(&self) -> Result<()> {
        if self.common.lifecycle().is_none() {
            return Ok(());
        }
        let requests = {
            let _receiving = self.goal_receive.lock().unwrap();
            let mut requests = Vec::new();
            for _ in 0..self.goal_depth {
                match self.send_goal.recv_timeout(Duration::ZERO) {
                    Ok(request) => requests.push(request),
                    Err(Error::Timeout) => break,
                    Err(Error::Validation(_) | Error::ResourceLimit(_)) => continue,
                    Err(error) => return Err(error),
                }
            }
            requests
        };
        for request in requests {
            self.reject_goal(request, TelemetryOutcome::Inactive);
        }
        Ok(())
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| {
            let goals = {
                let mut registry = self.registry.lock().unwrap();
                std::mem::take(&mut registry.goals)
            };
            for goal in goals.values() {
                let (waiters, lease, result, slot) = {
                    let mut state = goal.runtime.lock().unwrap();
                    state.expired = true;
                    (
                        std::mem::take(&mut state.waiters),
                        state.lifecycle_lease.take(),
                        state.result.take(),
                        state.goal_slot.take(),
                    )
                };
                drop((result, slot));
                for waiter in waiters {
                    let _ = waiter.request.reply_error("action server closed");
                }
                drop(lease);
            }
            close_all([
                self.send_goal.close(),
                self.get_result.close(),
                self.cancel_goal.close(),
                self.feedback.close(),
                self.status.close(),
            ])
        })
    }
}
impl Drop for ServerInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl ServerInner {
    fn reject_goal(&self, request: ServiceRequest, outcome: TelemetryOutcome) {
        self.record(
            "goal_rejected",
            request.request()["goal_id"].as_str(),
            request.request_id(),
            outcome,
        );
        let _ = request.reply(&json!({"accepted":false,"stamp":Timestamp::default()}));
    }
    fn record(
        &self,
        name: &str,
        goal_id: Option<&str>,
        request_id: Option<&str>,
        outcome: TelemetryOutcome,
    ) {
        let mut event = TelemetryEvent::new(TelemetryComponent::Action, name);
        event.goal_id = goal_id.map(str::to_owned);
        event.request_id = request_id.map(str::to_owned);
        event.outcome = outcome;
        self.common.record(event);
    }
    fn record_goal(&self, name: &str, goal: &GoalRecord, outcome: TelemetryOutcome) {
        self.record(
            name,
            Some(&goal.info.goal_id),
            goal.request_id.as_deref(),
            outcome,
        );
    }
    fn prepare_error(&self, message: std::fmt::Arguments<'_>) -> PreparedResult {
        let Some(size) = text_size(message, self.resource_limits.max_result_bytes) else {
            return PreparedResult::resource_error("ACTION_RESULT_TOO_LARGE");
        };
        let Some(bytes) = self.result_bytes.acquire(size) else {
            return PreparedResult::resource_error("ACTION_RESULT_CACHE_LIMIT");
        };
        PreparedResult {
            value: Err(Error::Remote(message.to_string())),
            bytes: Some(bytes),
            provider_error: None,
        }
    }
    fn unknown_result(&self) -> Result<Value> {
        let default = &self.interface.definition()["result"]["default"];
        if json_size(default, self.resource_limits.max_result_bytes)?.is_none() {
            return Err(Error::Remote("ACTION_RESULT_TOO_LARGE".into()));
        }
        // This immediate synthetic reply is not entered into the result cache.
        Ok(json!({"status":GoalStatus::Unknown as u8,"result":default}))
    }
    fn publish_status(&self) -> Result<()> {
        self.common.ensure_open()?;
        // Serialize snapshot+publish so retained snapshots cannot regress when
        // multiple execution and cancellation threads publish concurrently.
        let _publish = self.status_publish.lock().unwrap();
        let goals: Vec<_> = self
            .registry
            .lock()
            .unwrap()
            .goals
            .values()
            .cloned()
            .collect();
        let status_list: Vec<_> = goals
            .iter()
            .filter_map(|goal| {
                let state = goal.runtime.lock().unwrap();
                (!state.expired).then(|| json!({"goal_info":goal.info,"status":state.status as u8}))
            })
            .collect();
        self.status.publish(&json!({"status_list":status_list}))
    }
    fn process_result(&self, request: ServiceRequest) {
        let id = request.request()["goal_id"].as_str().unwrap();
        let goal = self.registry.lock().unwrap().goals.get(id).cloned();
        if let Some(goal) = goal {
            let result = {
                let mut state = goal.runtime.lock().unwrap();
                if state.expired {
                    Some(Arc::new(RetainedResult {
                        response: self.unknown_result(),
                        _bytes: None,
                    }))
                } else if let Some(result) = &state.result {
                    Some(result.clone())
                } else if let Some(permit) = self.result_waiters.acquire() {
                    state.waiters.push(ResultWaiter {
                        request,
                        _permit: permit,
                    });
                    return;
                } else {
                    None
                }
            };
            if let Some(result) = result {
                self.reply_result(request, &result.response);
            } else {
                let _ = request.reply_error("ACTION_RESULT_WAITER_LIMIT");
                self.record(
                    "result_waiter_rejected",
                    Some(id),
                    request.request_id(),
                    TelemetryOutcome::Rejected,
                );
            }
        } else {
            self.reply_result(request, &self.unknown_result());
        }
    }
    fn reply_result(&self, request: ServiceRequest, result: &Result<Value>) {
        let sent = match result {
            Ok(value) => request.reply(value),
            Err(error) => request.reply_error(&error.to_string()),
        };
        self.record(
            "result_sent",
            request.request()["goal_id"].as_str(),
            request.request_id(),
            sent.as_ref()
                .err()
                .or(result.as_ref().err())
                .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from),
        );
    }
    fn reply_waiter(&self, waiter: ResultWaiter, result: &Result<Value>) {
        let ResultWaiter { request, _permit } = waiter;
        self.reply_result(request, result);
        // Keep the permit while replying, including a failed or blocked reply.
        drop(_permit);
    }
    fn expire_goals(&self) {
        let now = Instant::now();
        let mut expired_waiters = Vec::new();
        let mut expired_results = Vec::new();
        let mut expired_slots = Vec::new();
        let mut expired_records = Vec::new();
        let expired = {
            let mut registry = self.registry.lock().unwrap();
            let before = registry.goals.len();
            registry.goals.retain(|_, goal| {
                let mut state = goal.runtime.lock().unwrap();
                let expired = !state.status.is_active()
                    && state.lifecycle_lease.is_none()
                    && state
                        .terminal_at
                        .is_some_and(|at| now.saturating_duration_since(at) >= self.result_timeout);
                if expired {
                    state.expired = true;
                    expired_waiters.append(&mut state.waiters);
                    expired_results.extend(state.result.take());
                    expired_slots.extend(state.goal_slot.take());
                    expired_records.push(goal.clone());
                }
                !expired
            });
            before != registry.goals.len()
        };
        // Drop potentially large values after releasing both registry and goal
        // locks. An in-flight reply's Arc still owns its byte reservation.
        drop((expired_results, expired_slots, expired_records));
        for request in expired_waiters {
            self.reply_waiter(request, &self.unknown_result());
        }
        if expired {
            let _ = self.publish_status();
        }
    }

    // Direct translation of Jazzy's UUID/time branches. In the exact-ID
    // branch, CANCELING is not cancelable and therefore yields TERMINATED,
    // despite still being active. The callback phase preserves return_code.
    fn select_cancel(&self, request: &GoalInfo) -> (u8, Vec<Arc<GoalRecord>>) {
        let registry = self.registry.lock().unwrap();
        if request.goal_id != ZERO_UUID && request.stamp.is_zero() {
            match registry.goals.get(&request.goal_id) {
                None => (ERROR_UNKNOWN_GOAL_ID, Vec::new()),
                Some(goal) if goal.runtime.lock().unwrap().status.is_cancelable() => {
                    (ERROR_NONE, vec![goal.clone()])
                }
                Some(_) => (ERROR_GOAL_TERMINATED, Vec::new()),
            }
        } else {
            let all = request.goal_id == ZERO_UUID && request.stamp.is_zero();
            let candidates = registry
                .goals
                .values()
                .filter(|goal| {
                    goal.runtime.lock().unwrap().status.is_cancelable()
                        && (all
                            || goal.info.stamp <= request.stamp
                            || goal.info.goal_id == request.goal_id)
                })
                .cloned()
                .collect();
            (ERROR_NONE, candidates)
        }
    }
}

/// A validated goal admission request. The language executor invokes its
/// goal callback, then calls decide; default SDK policy is accept.
pub struct GoalRequest {
    server: Weak<ServerInner>,
    goal_id: String,
    goal: Value,
    request: Mutex<Option<ServiceRequest>>,
    lifecycle_lease: Mutex<Option<LifecycleLease>>,
    goal_slot: Mutex<Option<CountPermit>>,
}
impl GoalRequest {
    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }
    pub fn request(&self) -> &Value {
        &self.goal
    }
    pub fn decide(&self, accept: bool) -> Result<Option<ServerGoalHandle>> {
        let request = self
            .request
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::InvalidArgument("goal admission already answered".into()))?;
        let mut lease = self.lifecycle_lease.lock().unwrap().take();
        let mut goal_slot = self.goal_slot.lock().unwrap().take();
        // Consuming a decision releases its admission resources on every
        // failure, including a server closed while the callback was running.
        let server = self.server.upgrade().ok_or(Error::Closed)?;
        server.common.ensure_open()?;
        let epoch = lease.as_ref().map(LifecycleLease::epoch);
        let mut decide = || {
            let mut registry = server.registry.lock().unwrap();
            server.common.ensure_open()?;
            // As in rcl_action_accept_new_goal, recheck after the language
            // callback: another request may have accepted this UUID meanwhile.
            let accepted = accept && !registry.goals.contains_key(&self.goal_id);
            let stamp = if accepted {
                Timestamp::now()?
            } else {
                Timestamp::default()
            };
            let goal = accepted.then(|| {
                Arc::new(GoalRecord {
                    info: GoalInfo {
                        goal_id: self.goal_id.clone(),
                        stamp,
                    },
                    request: self.goal.clone(),
                    request_id: request.request_id().map(str::to_owned),
                    runtime: Mutex::new(GoalRuntime {
                        status: GoalStatus::Accepted,
                        execution_scheduled: false,
                        result: None,
                        waiters: Vec::new(),
                        terminal_at: None,
                        expired: false,
                        lifecycle_lease: lease.take().map(Arc::new),
                        goal_slot: goal_slot.take(),
                    }),
                })
            });
            if let Some(goal) = &goal {
                registry.goals.insert(self.goal_id.clone(), goal.clone());
            }
            Ok((accepted, stamp, goal))
        };
        let admission = match (accept, server.common.lifecycle(), epoch) {
            (true, Some(lifecycle), Some(epoch)) => lifecycle.with_active_epoch(epoch, decide),
            (true, Some(_), None) => {
                Err(Error::Inactive("goal admission has no active lease".into()))
            }
            _ => decide(),
        };
        let (accepted, stamp, goal, rejected_outcome) = match admission {
            Ok((accepted, stamp, goal)) => (accepted, stamp, goal, TelemetryOutcome::Rejected),
            Err(Error::Inactive(_)) => (
                false,
                Timestamp::default(),
                None,
                TelemetryOutcome::Inactive,
            ),
            Err(error) => return Err(error),
        };
        server.record(
            if accepted {
                "goal_accepted"
            } else {
                "goal_rejected"
            },
            Some(&self.goal_id),
            request.request_id(),
            if accepted {
                TelemetryOutcome::Ok
            } else {
                rejected_outcome
            },
        );
        // rclpy order: create authoritative handle/future, reply accepted,
        // publish accepted status, then expose handle to accepted callback.
        request.reply(&json!({"accepted":accepted,"stamp":stamp}))?;
        if let Some(goal) = goal {
            server.publish_status()?;
            Ok(Some(ServerGoalHandle {
                server: Arc::downgrade(&server),
                goal,
            }))
        } else {
            Ok(None)
        }
    }
}
impl Drop for GoalRequest {
    fn drop(&mut self) {
        if let Some(request) = self.request.get_mut().unwrap().take() {
            if let Some(server) = self.server.upgrade() {
                server.record(
                    "goal_rejected",
                    Some(&self.goal_id),
                    request.request_id(),
                    TelemetryOutcome::Rejected,
                );
            }
            let _ = request.reply(&json!({"accepted":false,"stamp":Timestamp::default()}));
        }
    }
}

pub struct CancelRequest {
    server: Weak<ServerInner>,
    value: Value,
    request: Mutex<Option<ServiceRequest>>,
    return_code: u8,
    candidates: Vec<ServerGoalHandle>,
}
impl CancelRequest {
    pub fn request(&self) -> &Value {
        &self.value
    }
    pub fn candidates(&self) -> Vec<ServerGoalHandle> {
        self.candidates.clone()
    }
    pub fn return_code(&self) -> u8 {
        self.return_code
    }
    pub fn respond(&self, accepted_goal_ids: &[String]) -> Result<Value> {
        let server = self.server.upgrade().ok_or(Error::Closed)?;
        server.common.ensure_open()?;
        let request = self
            .request
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::InvalidArgument("cancel request already answered".into()))?;
        let mut canceling = Vec::new();
        for goal in &self.candidates {
            let accepted = accepted_goal_ids.iter().any(|id| id == goal.goal_id())
                && goal.update(GoalEvent::CancelGoal).is_ok();
            if accepted {
                canceling.push(goal.goal.info.clone());
            }
            server.record(
                if accepted {
                    "cancel_accepted"
                } else {
                    "cancel_rejected"
                },
                Some(goal.goal_id()),
                request.request_id(),
                if accepted {
                    TelemetryOutcome::Ok
                } else {
                    TelemetryOutcome::Rejected
                },
            );
        }
        if self.candidates.is_empty() {
            server.record(
                "cancel_rejected",
                self.value["goal_info"]["goal_id"].as_str(),
                request.request_id(),
                TelemetryOutcome::Rejected,
            );
        }
        let response = json!({"return_code":self.return_code,"goals_canceling":canceling});
        request.reply(&response)?;
        Ok(response)
    }
}
impl Drop for CancelRequest {
    fn drop(&mut self) {
        if let Some(request) = self.request.get_mut().unwrap().take() {
            // Default SDK cancellation policy rejects every candidate, while
            // retaining rcl's selection return_code just like Jazzy rclpy.
            if let Some(server) = self.server.upgrade() {
                if self.candidates.is_empty() {
                    server.record(
                        "cancel_rejected",
                        self.value["goal_info"]["goal_id"].as_str(),
                        request.request_id(),
                        TelemetryOutcome::Rejected,
                    );
                } else {
                    for goal in &self.candidates {
                        server.record(
                            "cancel_rejected",
                            Some(goal.goal_id()),
                            request.request_id(),
                            TelemetryOutcome::Rejected,
                        );
                    }
                }
            }
            let _ = request.reply(&json!({"return_code":self.return_code,"goals_canceling":[]}));
        }
    }
}

#[derive(Clone)]
pub struct ServerGoalHandle {
    server: Weak<ServerInner>,
    goal: Arc<GoalRecord>,
}
impl ServerGoalHandle {
    pub fn goal_id(&self) -> &str {
        &self.goal.info.goal_id
    }
    pub fn request(&self) -> &Value {
        &self.goal.request
    }
    pub fn stamp(&self) -> Timestamp {
        self.goal.info.stamp
    }
    pub fn status(&self) -> u8 {
        if self
            .server
            .upgrade()
            .is_none_or(|s| s.common.ensure_open().is_err())
        {
            return GoalStatus::Unknown as u8;
        }
        let state = self.goal.runtime.lock().unwrap();
        if state.expired {
            GoalStatus::Unknown as u8
        } else {
            state.status as u8
        }
    }
    pub fn is_active(&self) -> bool {
        matches!(self.status(), 1..=3)
    }
    pub fn is_cancel_requested(&self) -> bool {
        self.status() == GoalStatus::Canceling as u8
    }
    pub fn execute(&self) -> Result<()> {
        let server = self.server()?;
        {
            let mut state = self.goal.runtime.lock().unwrap();
            if state.expired {
                return Err(Error::Closed);
            }
            if state.execution_scheduled {
                return Err(Error::InvalidArgument(
                    "goal execution already scheduled".into(),
                ));
            }
            if state.status != GoalStatus::Canceling {
                state.status = transition(state.status, GoalEvent::Execute)?;
            }
            state.execution_scheduled = true;
        }
        server.record_goal("execution_started", &self.goal, TelemetryOutcome::Ok);
        server.publish_status()
    }
    pub fn succeed(&self) -> Result<()> {
        self.update(GoalEvent::Succeed)
    }
    pub fn abort(&self) -> Result<()> {
        self.update(GoalEvent::Abort)
    }
    pub fn canceled(&self) -> Result<()> {
        self.update(GoalEvent::Canceled)
    }
    fn update(&self, event: GoalEvent) -> Result<()> {
        let server = self.server()?;
        {
            let mut state = self.goal.runtime.lock().unwrap();
            if state.expired {
                return Err(Error::Closed);
            }
            state.status = transition(state.status, event)?;
            if !state.status.is_active() {
                state.terminal_at = Some(Instant::now());
            }
        }
        server.record_goal(
            match event {
                GoalEvent::Execute => "execution_started",
                GoalEvent::CancelGoal => "cancel_requested",
                GoalEvent::Succeed => "goal_succeeded",
                GoalEvent::Abort => "goal_aborted",
                GoalEvent::Canceled => "goal_canceled",
            },
            &self.goal,
            TelemetryOutcome::Ok,
        );
        server.publish_status()
    }
    pub fn publish_feedback(&self, value: &Value) -> Result<()> {
        let server = self.server()?;
        server.interface.validate("feedback", value)?;
        let _lease = {
            let state = self.goal.runtime.lock().unwrap();
            if state.expired {
                return Err(Error::Closed);
            }
            if server.common.lifecycle().is_some() && state.lifecycle_lease.is_none() {
                return Err(Error::Inactive(
                    "managed goal execution has already finished".into(),
                ));
            }
            state.lifecycle_lease.clone()
        };
        server
            .feedback
            .publish(&json!({"goal_id":self.goal_id(),"feedback":value}))
    }
    /// Called after execute callback returns. Active goals abort; an explicit
    /// terminal state remains unchanged, including on a later callback exception.
    pub fn finish(&self, result: Option<&Value>) -> Result<()> {
        let server = self.server()?;
        let result = result.unwrap_or(&server.interface.definition()["result"]["default"]);
        let Some(size) = json_size(result, server.resource_limits.max_result_bytes)? else {
            return self.finalize(PreparedResult::resource_error("ACTION_RESULT_TOO_LARGE"));
        };
        if let Err(error) = server.interface.validate("result", result) {
            let mut prepared = server.prepare_error(format_args!("invalid action result: {error}"));
            if prepared.provider_error.is_none() {
                prepared.provider_error = Some(error);
            }
            return self.finalize(prepared);
        }
        let Some(bytes) = server.result_bytes.acquire(size) else {
            return self.finalize(PreparedResult::resource_error("ACTION_RESULT_CACHE_LIMIT"));
        };
        self.finalize(PreparedResult {
            value: Ok(result.clone()),
            bytes: Some(bytes),
            provider_error: None,
        })
    }
    /// A language return value could not be encoded/validated as business JSON.
    /// Cache the error and wake result waiters, preserving legal state changes.
    pub fn finish_error(&self, message: &str) -> Result<()> {
        let server = self.server()?;
        self.finalize(server.prepare_error(format_args!("{message}")))
    }
    fn finalize(&self, prepared: PreparedResult) -> Result<()> {
        let server = self.server()?;
        let PreparedResult {
            value,
            bytes,
            provider_error,
        } = prepared;
        let (response, waiters, changed, lease) = {
            let mut state = self.goal.runtime.lock().unwrap();
            if state.expired {
                return Err(Error::Closed);
            }
            if state.result.is_some() {
                return Err(Error::InvalidArgument(
                    "goal result already completed".into(),
                ));
            }
            let active = state.status.is_active();
            if active {
                state.status = transition(state.status, GoalEvent::Abort)?;
                state.terminal_at = Some(Instant::now());
            }
            let response = Arc::new(RetainedResult {
                response: value.map(|value| {
                    let mut response = serde_json::Map::new();
                    response.insert("status".into(), Value::from(state.status as u8));
                    response.insert("result".into(), value);
                    Value::Object(response)
                }),
                _bytes: bytes,
            });
            state.result = Some(response.clone());
            (
                response,
                std::mem::take(&mut state.waiters),
                active,
                state.lifecycle_lease.take(),
            )
        };
        if changed {
            server.record_goal("goal_aborted", &self.goal, TelemetryOutcome::Ok);
        }
        server.record_goal(
            "result_completed",
            &self.goal,
            response
                .response
                .as_ref()
                .err()
                .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from),
        );
        // No user callback or transport reply runs while the goal lock is held.
        for waiter in waiters {
            server.reply_waiter(waiter, &response.response);
        }
        let published = if changed {
            server.publish_status()
        } else {
            Ok(())
        };
        drop(lease);
        provider_error.map_or(published, Err)
    }
    fn server(&self) -> Result<Arc<ServerInner>> {
        let server = self.server.upgrade().ok_or(Error::Closed)?;
        server.common.ensure_open()?;
        Ok(server)
    }
}

pub(super) fn close_all<const N: usize>(results: [Result<()>; N]) -> Result<()> {
    results
        .into_iter()
        .find_map(|result| match result {
            Err(e) if e != Error::Closed => Some(Err(e)),
            _ => None,
        })
        .unwrap_or(Ok(()))
}
