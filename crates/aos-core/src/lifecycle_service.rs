//! Lifecycle services follow the five ROS 2 Jazzy lifecycle service names.
//! Queries run in a native control worker; change_state is taken by the
//! application Executor, which invokes every lifecycle callback itself.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock, Weak,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    resolve_qos, Client, EndpointOptions, Error, Interface, Lifecycle, LifecycleSnapshot,
    LifecycleStateDescription, LifecycleTransition, LifecycleTransitionId,
    LifecycleTransitionReport, Node, Result, Service, ServiceRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleServiceKind {
    ChangeState,
    GetState,
    GetAvailableStates,
    GetAvailableTransitions,
    GetTransitionGraph,
}
impl LifecycleServiceKind {
    pub const ALL: [Self; 5] = [
        Self::ChangeState,
        Self::GetState,
        Self::GetAvailableStates,
        Self::GetAvailableTransitions,
        Self::GetTransitionGraph,
    ];
    pub fn suffix(self) -> &'static str {
        match self {
            Self::ChangeState => "change_state",
            Self::GetState => "get_state",
            Self::GetAvailableStates => "get_available_states",
            Self::GetAvailableTransitions => "get_available_transitions",
            Self::GetTransitionGraph => "get_transition_graph",
        }
    }
    pub fn interface(self) -> Interface {
        static INTERFACES: OnceLock<BTreeMap<LifecycleServiceKind, Interface>> = OnceLock::new();
        INTERFACES.get_or_init(|| {
            Self::ALL
                .into_iter()
                .map(|kind| (kind, build_interface(kind)))
                .collect()
        })[&self]
            .clone()
    }
}

/// id=0 selects by label; a nonzero public ID takes precedence, as in rclpy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleTransitionSelector {
    pub id: u8,
    pub label: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleChangeStateRequest {
    pub transition: LifecycleTransitionSelector,
    /// Missing means the five-second default; zero requests a nonblocking drain.
    #[serde(default)]
    pub drain_timeout_ms: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleServiceError {
    pub code: String,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleChangeStateReply {
    pub success: bool,
    pub report: Option<LifecycleTransitionReport>,
    pub error: Option<LifecycleServiceError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleStateReply {
    pub current_state: LifecycleStateDescription,
    pub snapshot: LifecycleSnapshot,
    pub last_report: Option<LifecycleTransitionReport>,
}

#[derive(Clone)]
pub struct LifecycleServer {
    inner: Arc<LifecycleServerInner>,
}
struct LifecycleServerInner {
    lifecycle: Lifecycle,
    services: BTreeMap<LifecycleServiceKind, Arc<Service>>,
    closed: AtomicBool,
}
impl LifecycleServer {
    pub(crate) fn new(node: &Node) -> Result<Self> {
        let lifecycle = node.lifecycle().ok_or_else(|| {
            Error::InvalidArgument("ordinary Node has no lifecycle state machine".into())
        })?;
        let qos = resolve_qos(&Value::Null, "lifecycle_services", true)?;
        let mut services = BTreeMap::new();
        for kind in LifecycleServiceKind::ALL {
            let service = node.create_control_service(
                &kind.interface(),
                &format!("~/{}", kind.suffix()),
                qos.clone(),
                EndpointOptions {
                    summary: format!("Node lifecycle operation {}", kind.suffix()),
                    side_effects: Some(
                        if kind == LifecycleServiceKind::ChangeState {
                            "write"
                        } else {
                            "read_only"
                        }
                        .into(),
                    ),
                    ..Default::default()
                },
            )?;
            services.insert(kind, Arc::new(service));
        }
        let inner = Arc::new(LifecycleServerInner {
            lifecycle,
            services,
            closed: AtomicBool::new(false),
        });
        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("aos-lifecycle-queries".into())
            .spawn(move || loop {
                let Some(owner) = weak.upgrade() else { break };
                if owner.closed.load(Ordering::Acquire) {
                    break;
                }
                let mut received = false;
                for (&kind, service) in &owner.services {
                    if kind == LifecycleServiceKind::ChangeState {
                        continue;
                    }
                    match service.recv_timeout(Duration::ZERO) {
                        Ok(request) => {
                            received = true;
                            match owner.query(kind) {
                                Ok(response) => {
                                    let _ = request.reply(&response);
                                }
                                Err(error) => {
                                    let _ = request.reply_error(&error.to_string());
                                }
                            }
                        }
                        Err(Error::Timeout | Error::Validation(_) | Error::ResourceLimit(_)) => {}
                        Err(_) => return,
                    }
                }
                drop(owner);
                if !received {
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
            .map_err(|e| Error::Transport(format!("cannot start lifecycle query worker: {e}")))?;
        Ok(Self { inner })
    }
    /// Only change_state reaches the language Executor. Queries remain
    /// independent while a synchronous callback or business drain is pending.
    pub fn take_change_request(&self, timeout: Duration) -> Result<LifecycleChangeRequest> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let request =
            self.inner.services[&LifecycleServiceKind::ChangeState].recv_timeout(timeout)?;
        let parsed = match serde_json::from_value(request.request().clone()) {
            Ok(parsed) => parsed,
            Err(error) => {
                let error = Error::Validation(format!("invalid lifecycle request: {error}"));
                // Keep the decode failure as the executor-visible result even
                // if the best-effort error reply also fails in the transport.
                let _ = request.reply(&change_state_error(&error));
                return Err(error);
            }
        };
        Ok(LifecycleChangeRequest {
            request,
            parsed,
            lifecycle: self.inner.lifecycle.clone(),
            owner: Arc::downgrade(&self.inner),
            begun: AtomicBool::new(false),
        })
    }
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
impl LifecycleServerInner {
    fn query(&self, kind: LifecycleServiceKind) -> Result<Value> {
        self.lifecycle.current_state()?;
        Ok(match kind {
            LifecycleServiceKind::GetState => json!(self.lifecycle.state_reply()?),
            LifecycleServiceKind::GetAvailableStates => {
                json!({"available_states":self.lifecycle.available_states()})
            }
            LifecycleServiceKind::GetAvailableTransitions => {
                json!({"available_transitions":self.lifecycle.available_transitions()?})
            }
            LifecycleServiceKind::GetTransitionGraph => {
                json!({"available_transitions":self.lifecycle.transition_graph()})
            }
            LifecycleServiceKind::ChangeState => {
                return Err(Error::InvalidArgument(
                    "change_state requires the application Executor".into(),
                ))
            }
        })
    }
    fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(Error::Closed);
        }
        let mut error = None;
        for service in self.services.values() {
            if let Err(e) = service.close() {
                if e != Error::Closed && error.is_none() {
                    error = Some(e);
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}
impl Drop for LifecycleServerInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Keep this owned request until the Executor has completed all callbacks and
/// replied. Taking a request never changes lifecycle state or invokes code.
pub struct LifecycleChangeRequest {
    request: ServiceRequest,
    parsed: LifecycleChangeStateRequest,
    lifecycle: Lifecycle,
    owner: Weak<LifecycleServerInner>,
    begun: AtomicBool,
}
impl LifecycleChangeRequest {
    pub fn request(&self) -> &Value {
        self.request.request()
    }
    pub fn transition(&self) -> Result<LifecycleTransitionId> {
        if self.parsed.transition.id == 0 {
            LifecycleTransitionId::from_label(
                &self.parsed.transition.label,
                self.lifecycle.current_state()?,
            )
        } else {
            LifecycleTransitionId::from_id(self.parsed.transition.id)
        }
    }
    pub fn drain_timeout(&self) -> Option<Duration> {
        self.parsed.drain_timeout_ms.map(Duration::from_millis)
    }
    pub fn begin_transition(&self) -> Result<LifecycleTransition> {
        self.ensure_open()?;
        if self.begun.swap(true, Ordering::AcqRel) {
            return Err(Error::InvalidArgument(
                "lifecycle change request was already begun".into(),
            ));
        }
        self.lifecycle.begin_transition(self.transition()?)
    }
    pub fn reply(&self, report: &LifecycleTransitionReport) -> Result<()> {
        self.ensure_open()?;
        self.request.reply(&json!(LifecycleChangeStateReply {
            success: report.successful,
            report: Some(report.clone()),
            error: None,
        }))
    }
    pub fn reply_error(&self, error: &Error) -> Result<()> {
        self.ensure_open()?;
        self.request.reply(&change_state_error(error))
    }
    fn ensure_open(&self) -> Result<()> {
        if self
            .owner
            .upgrade()
            .is_none_or(|owner| owner.closed.load(Ordering::Acquire))
        {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}

fn change_state_error(error: &Error) -> Value {
    json!(LifecycleChangeStateReply {
        success: false,
        report: None,
        error: Some(LifecycleServiceError {
            code: error_code(error).into(),
            message: error.to_string(),
        }),
    })
}

/// The CLI and SDK use the same ordinary service interfaces.
pub struct LifecycleClient {
    clients: BTreeMap<LifecycleServiceKind, Client>,
}
impl LifecycleClient {
    pub fn new(node: &Node, remote_node: &str) -> Result<Self> {
        crate::names::validate_absolute_name(remote_node)?;
        let qos = resolve_qos(&Value::Null, "lifecycle_services", true)?;
        let clients = LifecycleServiceKind::ALL
            .into_iter()
            .map(|kind| {
                node.create_client_with_qos(
                    &kind.interface(),
                    &format!("{remote_node}/{}", kind.suffix()),
                    qos.clone(),
                    EndpointOptions::default(),
                )
                .map(|client| (kind, client))
            })
            .collect::<Result<_>>()?;
        Ok(Self { clients })
    }
    pub fn call(
        &self,
        kind: LifecycleServiceKind,
        request: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        self.clients[&kind].call(request, timeout)
    }
    pub fn state(&self, timeout: Duration) -> Result<LifecycleStateReply> {
        serde_json::from_value(self.call(LifecycleServiceKind::GetState, &json!({}), timeout)?)
            .map_err(|e| Error::Validation(e.to_string()))
    }
    pub fn change_state(
        &self,
        transition: LifecycleTransitionSelector,
        drain_timeout: Option<Duration>,
        timeout: Duration,
    ) -> Result<LifecycleChangeStateReply> {
        let drain_timeout_ms = drain_timeout
            .map(|value| {
                u64::try_from(value.as_millis()).map_err(|_| {
                    Error::InvalidArgument("lifecycle drain timeout overflows milliseconds".into())
                })
            })
            .transpose()?;
        serde_json::from_value(self.call(
            LifecycleServiceKind::ChangeState,
            &json!(LifecycleChangeStateRequest {
                transition,
                drain_timeout_ms
            }),
            timeout,
        )?)
        .map_err(|e| Error::Validation(e.to_string()))
    }
    pub fn close(&self) -> Result<()> {
        let mut error = None;
        for client in self.clients.values() {
            if let Err(e) = client.close() {
                if e != Error::Closed && error.is_none() {
                    error = Some(e);
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}

fn error_code(error: &Error) -> &'static str {
    match error {
        Error::Closed => "CLOSED",
        Error::Timeout => "TIMEOUT",
        Error::InvalidArgument(_) => "INVALID_ARGUMENT",
        Error::Validation(_) => "VALIDATION",
        Error::ResourceLimit(_) => "RESOURCE_LIMIT",
        Error::Transport(_) => "TRANSPORT",
        Error::NoResponse(_) => "NO_RESPONSE",
        Error::Unsupported(_) => "UNSUPPORTED",
        Error::Remote(_) => "REMOTE",
        Error::QueueOverflow(_) => "QUEUE_OVERFLOW",
        Error::Inactive(_) => "INACTIVE",
    }
}
fn build_interface(kind: LifecycleServiceKind) -> Interface {
    let empty = object(json!({}), &[]);
    let (name, request, response) = match kind {
        LifecycleServiceKind::ChangeState => (
            "ChangeState",
            object(
                json!({
                        "transition":state_schema(),
                "drain_timeout_ms":nullable(json!({"type":"integer","minimum":0}))
                    }),
                &["transition"],
            ),
            object(
                json!({
                    "success":{"type":"boolean"}, "report":nullable(report_schema()),
                    "error":nullable(object(json!({
                        "code":{"type":"string"},"message":{"type":"string"}
                    }), &["code","message"]))
                }),
                &["success", "report", "error"],
            ),
        ),
        LifecycleServiceKind::GetState => (
            "GetState",
            empty,
            object(
                json!({
                    "current_state":state_schema(), "snapshot":snapshot_schema(), "last_report":nullable(report_schema())
                }),
                &["current_state", "snapshot", "last_report"],
            ),
        ),
        LifecycleServiceKind::GetAvailableStates => (
            "GetAvailableStates",
            empty,
            object(
                json!({
                    "available_states":{"type":"array","items":state_schema()}
                }),
                &["available_states"],
            ),
        ),
        LifecycleServiceKind::GetAvailableTransitions => (
            "GetAvailableTransitions",
            empty,
            object(
                json!({
                    "available_transitions":{"type":"array","items":transition_schema()}
                }),
                &["available_transitions"],
            ),
        ),
        LifecycleServiceKind::GetTransitionGraph => (
            "GetTransitionGraph",
            empty,
            object(
                json!({
                    "available_transitions":{"type":"array","items":transition_schema()}
                }),
                &["available_transitions"],
            ),
        ),
    };
    Interface::from_json(json!({
        "name":format!("aos_interfaces/srv/{name}"),"kind":"srv","request":request,"response":response
    })).expect("built-in lifecycle service schema is valid")
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}
fn state_schema() -> Value {
    object(
        json!({
            "id":{"type":"integer","minimum":0,"maximum":255},"label":{"type":"string"}
        }),
        &["id", "label"],
    )
}
fn transition_schema() -> Value {
    object(
        json!({
            "id":{"type":"integer","minimum":0,"maximum":255},"label":{"type":"string"},
            "start_state":state_schema(),"goal_state":state_schema()
        }),
        &["id", "label", "start_state", "goal_state"],
    )
}
fn in_flight_schema() -> Value {
    json!({"type":"array","items":object(json!({
        "lease_id":{"type":"integer","minimum":0},
        "kind":{"enum":["publisher","timer","subscription","service","action"]},
        "name":{"type":"string"},"entity_id":{"type":"string"}
    }), &["lease_id","kind","name","entity_id"])})
}
fn snapshot_schema() -> Value {
    object(
        json!({
            "current_state":state_schema(),"transitioning":{"type":"boolean"},
            "gate_open":{"type":"boolean"},"epoch":{"type":"integer","minimum":0},
            "in_flight":in_flight_schema()
        }),
        &[
            "current_state",
            "transitioning",
            "gate_open",
            "epoch",
            "in_flight",
        ],
    )
}
fn report_schema() -> Value {
    let callback = json!({"enum":["success","failure","error"]});
    object(
        json!({
            "transition_id":{"type":"integer","minimum":1,"maximum":7},
            "initial_state":state_schema(),"final_state":state_schema(),"callback_return":callback,
            "error_callback_return":nullable(callback),
            "successful":{"type":"boolean"},"drain_timed_out":{"type":"boolean"},
            "remaining":in_flight_schema(),"abandoned":{"type":"boolean"},"reason":{"type":"string"}
        }),
        &[
            "transition_id",
            "initial_state",
            "final_state",
            "callback_return",
            "error_callback_return",
            "successful",
            "drain_timed_out",
            "remaining",
            "abandoned",
            "reason",
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LifecycleCallbackReturn;

    #[test]
    fn five_service_schemas_validate_shared_core_state_and_reports() {
        let lifecycle = Lifecycle::new("/schema/node".into());
        let mut token = lifecycle
            .begin_transition(LifecycleTransitionId::Configure)
            .unwrap();
        let progress = token
            .complete(LifecycleCallbackReturn::Success, None)
            .unwrap();
        let report = progress.report.unwrap();
        for kind in LifecycleServiceKind::ALL {
            let (request, response) = match kind {
                LifecycleServiceKind::ChangeState => (
                    json!({
                        "transition":{"id":0,"label":"configure"}
                    }),
                    json!(LifecycleChangeStateReply {
                        success: true,
                        report: Some(report.clone()),
                        error: None
                    }),
                ),
                LifecycleServiceKind::GetState => {
                    (json!({}), json!(lifecycle.state_reply().unwrap()))
                }
                LifecycleServiceKind::GetAvailableStates => (
                    json!({}),
                    json!({"available_states":lifecycle.available_states()}),
                ),
                LifecycleServiceKind::GetAvailableTransitions => (
                    json!({}),
                    json!({"available_transitions":lifecycle.available_transitions().unwrap()}),
                ),
                LifecycleServiceKind::GetTransitionGraph => (
                    json!({}),
                    json!({"available_transitions":lifecycle.transition_graph()}),
                ),
            };
            kind.interface().validate("request", &request).unwrap();
            kind.interface().validate("response", &response).unwrap();
        }
        assert!(LifecycleServiceKind::ChangeState
            .interface()
            .validate(
                "request",
                &json!({"transition":{"id":0,"label":"configure"},"drain_timeout_ms":-1})
            )
            .is_err());
        assert!(LifecycleServiceKind::GetState
            .interface()
            .validate("request", &json!({"private":"value"}))
            .is_err());
    }
}
