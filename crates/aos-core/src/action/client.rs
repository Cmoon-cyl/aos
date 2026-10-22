//! Client-side Action observations and single-request operations. Goal state
//! is received from the provider; clients never advance its authoritative FSM.
use super::{messages::*, server::close_all, state::GoalStatus};
use crate::{
    node::{Common, Resource},
    ActionQos, Client, EndpointOptions, EntityKind, Error, Interface, LifecycleWorkKind, Node,
    Result, SubscriptionCall, TypedSubscription,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use uuid::Uuid;

#[cfg(test)]
#[path = "client_resource_tests.rs"]
mod resource_tests;

pub struct ActionClient {
    inner: Arc<ClientInner>,
}
struct ClientInner {
    common: Common,
    send_goal: Client,
    get_result: Client,
    cancel_goal: Client,
    feedback: TypedSubscription,
    status: TypedSubscription,
    observed: Mutex<BTreeMap<String, Weak<GoalObservation>>>,
}
/// Only live handles and an in-flight SendGoal own an observation. The weak
/// index routes provider evidence without retaining every historical UUID.
struct GoalObservation {
    client: Weak<ClientInner>,
    goal_id: String,
    status: AtomicU8,
}
impl Drop for GoalObservation {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            let mut observed = client.observed.lock().unwrap();
            // A concurrent send can already have replaced this dead weak
            // entry for the same UUID. Never remove that new observation.
            if observed
                .get(&self.goal_id)
                .is_some_and(|current| std::ptr::eq(current.as_ptr(), self))
            {
                observed.remove(&self.goal_id);
            }
        }
    }
}
impl ActionClient {
    pub fn new(
        node: &Node,
        interface: &Interface,
        name: &str,
        metadata: EndpointOptions,
    ) -> Result<Self> {
        Self::new_with_qos(node, interface, name, metadata, ActionQos::default())
    }
    pub fn new_with_qos(
        node: &Node,
        interface: &Interface,
        name: &str,
        metadata: EndpointOptions,
        qos: ActionQos,
    ) -> Result<Self> {
        qos.validate()?;
        let types = ActionInterfaces::new(interface)?;
        let endpoint = node.endpoint(interface, name)?;
        let base = endpoint.name.clone();
        let send_goal = node.create_client_with_qos(
            &types.send_goal,
            &format!("{base}/_action/send_goal"),
            qos.goal_service,
            EndpointOptions::default(),
        )?;
        let get_result = node.create_client_with_qos(
            &types.get_result,
            &format!("{base}/_action/get_result"),
            qos.result_service,
            EndpointOptions::default(),
        )?;
        let cancel_goal = node.create_client_with_qos(
            &types.cancel_goal,
            &format!("{base}/_action/cancel_goal"),
            qos.cancel_service,
            EndpointOptions::default(),
        )?;
        let feedback = node.create_subscription(
            &types.feedback,
            &format!("{base}/_action/feedback"),
            qos.feedback,
            EndpointOptions::default(),
        )?;
        let status = node.create_control_subscription(
            &types.status,
            &format!("{base}/_action/status"),
            qos.status,
            EndpointOptions::default(),
        )?;
        let common = node.common(
            interface,
            endpoint,
            EntityKind::ActionClient,
            None,
            name,
            metadata,
        )?;
        let inner = Arc::new(ClientInner {
            common,
            send_goal,
            get_result,
            cancel_goal,
            feedback,
            status,
            observed: Mutex::new(BTreeMap::new()),
        });
        node.register(inner.clone())?;
        Ok(Self { inner })
    }
    /// Wait for the typed ActionServer graph declaration. The provider creates
    /// its five protocol entities before declaring that ActionServer identity.
    /// Timeout returns false; no goal or other business request is sent.
    pub fn wait_for_server(&self, timeout: Option<Duration>) -> Result<bool> {
        self.inner
            .common
            .wait_for_ready(EntityKind::ActionServer, timeout)
    }
    /// A caller-thread check may interrupt this local wait without closing the
    /// client or submitting a goal. The discovery interest is not recreated.
    pub fn wait_for_server_with_check(
        &self,
        timeout: Option<Duration>,
        mut check: impl FnMut() -> Result<()>,
    ) -> Result<bool> {
        self.inner
            .common
            .wait_for_ready_with_check(EntityKind::ActionServer, timeout, &mut check)
    }
    pub fn send_goal(
        &self,
        goal: &Value,
        goal_id: Option<&str>,
        timeout: Duration,
    ) -> Result<ClientGoalHandle> {
        self.inner.common.ensure_open()?;
        let goal_id = goal_id
            .map(str::to_owned)
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        validate_goal_id(&goal_id)?;
        let _lease = self.inner.common.business_lease_in_epoch(
            LifecycleWorkKind::Action,
            Some(&goal_id),
            None,
        )?;
        // The provider may publish status before the SendGoal reply arrives.
        let observation = self.inner.observe_goal(&goal_id);
        let response = self
            .inner
            .send_goal
            .call(&json!({"goal_id":goal_id,"goal":goal}), timeout)?;
        let accepted = response["accepted"].as_bool().unwrap();
        let stamp = serde_json::from_value(response["stamp"].clone())
            .map_err(|e| Error::Validation(e.to_string()))?;
        Ok(ClientGoalHandle {
            client: Arc::downgrade(&self.inner),
            goal_id,
            accepted,
            stamp,
            observation,
        })
    }
    pub fn get_result(&self, goal_id: &str, timeout: Duration) -> Result<Value> {
        self.get_result_wait(goal_id, Some(timeout))
    }
    pub fn get_result_wait(&self, goal_id: &str, timeout: Option<Duration>) -> Result<Value> {
        self.inner.get_result(goal_id, timeout)
    }
    pub fn cancel_goal(&self, goal_id: &str, timeout: Duration) -> Result<Value> {
        self.inner.cancel_goal(goal_id, timeout)
    }
    pub fn cancel(&self, request: &Value, timeout: Duration) -> Result<Value> {
        self.inner.common.ensure_open()?;
        self.inner.cancel_goal.call(request, timeout)
    }
    /// Receive a feedback value on an ordinary Node. Managed Nodes return
    /// Unsupported: use take_feedback and retain its SubscriptionCall until
    /// the feedback handler finishes, then call finish() or drop the call.
    /// Returning a bare Value cannot retain that business drain lease.
    pub fn recv_feedback(&self, timeout: Duration) -> Result<Value> {
        self.inner.common.ensure_open()?;
        self.inner.feedback.recv_timeout(timeout)
    }
    /// Managed clients must retain this lease through the feedback callback.
    /// Provider-side feedback for an accepted goal remains available; this
    /// receiving Node independently decides when to dispatch business work.
    pub fn take_feedback(&self, timeout: Duration) -> Result<SubscriptionCall> {
        self.inner.common.ensure_open()?;
        self.inner.feedback.take_message(timeout)
    }
    pub fn recv_status(&self, timeout: Duration) -> Result<Value> {
        self.inner.common.ensure_open()?;
        let message = self.inner.status.recv_timeout(timeout)?;
        for item in message["status_list"].as_array().unwrap() {
            self.inner.update_observation(
                item["goal_info"]["goal_id"].as_str().unwrap(),
                item["status"].as_u64().unwrap() as u8,
            );
        }
        Ok(message)
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
impl Drop for ActionClient {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl Resource for ClientInner {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| {
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
impl Drop for ClientInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl ClientInner {
    fn observe_goal(self: &Arc<Self>, goal_id: &str) -> Arc<GoalObservation> {
        let mut observed = self.observed.lock().unwrap();
        if let Some(observation) = observed.get(goal_id).and_then(Weak::upgrade) {
            return observation;
        }
        let observation = Arc::new(GoalObservation {
            client: Arc::downgrade(self),
            goal_id: goal_id.into(),
            status: AtomicU8::new(GoalStatus::Unknown as u8),
        });
        observed.insert(goal_id.into(), Arc::downgrade(&observation));
        observation
    }
    fn update_observation(&self, goal_id: &str, status: u8) {
        let observation = self
            .observed
            .lock()
            .unwrap()
            .get(goal_id)
            .and_then(Weak::upgrade);
        // Drop the upgraded Arc outside the map lock: its last owner may
        // remove the corresponding weak entry in GoalObservation::drop.
        if let Some(observation) = observation {
            observation.status.store(status, Ordering::Release);
        }
    }
    fn get_result(&self, goal_id: &str, timeout: Option<Duration>) -> Result<Value> {
        self.common.ensure_open()?;
        validate_goal_id(goal_id)?;
        let result = self
            .get_result
            .call_wait(&json!({"goal_id":goal_id}), timeout)?;
        self.update_observation(goal_id, result["status"].as_u64().unwrap() as u8);
        Ok(result)
    }
    fn cancel_goal(&self, goal_id: &str, timeout: Duration) -> Result<Value> {
        self.common.ensure_open()?;
        validate_goal_id(goal_id)?;
        self.cancel_goal.call(
            &json!({"goal_info":{"goal_id":goal_id,"stamp":Timestamp::default()}}),
            timeout,
        )
    }
}
#[derive(Clone)]
pub struct ClientGoalHandle {
    client: Weak<ClientInner>,
    goal_id: String,
    accepted: bool,
    stamp: Timestamp,
    observation: Arc<GoalObservation>,
}
impl ClientGoalHandle {
    pub fn goal_id(&self) -> &str {
        &self.goal_id
    }
    pub fn accepted(&self) -> bool {
        self.accepted
    }
    pub fn stamp(&self) -> Timestamp {
        self.stamp
    }
    pub fn status(&self) -> u8 {
        self.client
            .upgrade()
            .map(|_| self.observation.status.load(Ordering::Acquire))
            .unwrap_or(GoalStatus::Unknown as u8)
    }
    pub fn get_result(&self, timeout: Duration) -> Result<Value> {
        self.get_result_wait(Some(timeout))
    }
    pub fn get_result_wait(&self, timeout: Option<Duration>) -> Result<Value> {
        self.client
            .upgrade()
            .ok_or(Error::Closed)?
            .get_result(&self.goal_id, timeout)
    }
    pub fn cancel_goal(&self, timeout: Duration) -> Result<Value> {
        self.client
            .upgrade()
            .ok_or(Error::Closed)?
            .cancel_goal(&self.goal_id, timeout)
    }
}
