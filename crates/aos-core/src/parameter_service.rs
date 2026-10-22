//! Ordinary parameter services. These names and operations follow fixed Jazzy
//! rclpy parameter_service.py; the typed JSON envelopes are AOS wire types.
//! recv_timeout only queues work. execute/handle_request run validators in the
//! calling Executor thread, never inside a transport callback.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, OnceLock, Weak,
    },
    time::{Duration, Instant},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    resolve_qos, Client, EndpointOptions, Error, Interface, ListParametersResult, Node, Parameter,
    ParameterCommit, ParameterError, ParameterRecord, ParameterResult, ParameterSnapshot,
    ParameterStore, ParameterType, ParameterValue, Result, Service, ServiceRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterServiceKind {
    DescribeParameters,
    GetParameterTypes,
    GetParameters,
    ListParameters,
    SetParameters,
    SetParametersAtomically,
}
impl ParameterServiceKind {
    pub const ALL: [Self; 6] = [
        Self::DescribeParameters,
        Self::GetParameterTypes,
        Self::GetParameters,
        Self::ListParameters,
        Self::SetParameters,
        Self::SetParametersAtomically,
    ];
    pub fn suffix(self) -> &'static str {
        match self {
            Self::DescribeParameters => "describe_parameters",
            Self::GetParameterTypes => "get_parameter_types",
            Self::GetParameters => "get_parameters",
            Self::ListParameters => "list_parameters",
            Self::SetParameters => "set_parameters",
            Self::SetParametersAtomically => "set_parameters_atomically",
        }
    }
    pub fn interface(self) -> Interface {
        static INTERFACES: OnceLock<BTreeMap<ParameterServiceKind, Interface>> = OnceLock::new();
        INTERFACES.get_or_init(|| {
            Self::ALL
                .into_iter()
                .map(|kind| (kind, build_interface(kind)))
                .collect()
        })[&self]
            .clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterNamesRequest {
    pub names: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParametersRequest {
    pub prefixes: Vec<String>,
    pub depth: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetParametersRequest {
    pub parameters: Vec<Parameter>,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterUpdateMode {
    #[default]
    Set,
    Load,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetParametersAtomicallyRequest {
    pub parameters: Vec<Parameter>,
    /// Absent retains set semantics, including explicit NotSet deletion.
    #[serde(default)]
    pub mode: ParameterUpdateMode,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterQueryReply<T> {
    /// Version and values come from a single snapshot. A closed owner has none.
    pub version: Option<u64>,
    pub value: Option<T>,
    pub error: Option<ParameterError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterSetResult {
    pub successful: bool,
    pub code: Option<String>,
    pub reason: String,
    pub version: Option<u64>,
    pub transaction_id: Option<String>,
    pub event_published: bool,
    pub event_publish_error: Option<String>,
}
impl ParameterSetResult {
    pub fn from_result(result: ParameterResult<ParameterCommit>) -> Self {
        match result {
            Ok(commit) => Self {
                successful: true,
                code: None,
                reason: String::new(),
                version: Some(commit.version),
                transaction_id: Some(commit.transaction_id),
                event_published: commit.event_published,
                event_publish_error: commit.event_publish_error,
            },
            Err(error) => Self {
                successful: false,
                code: Some(error.code().into()),
                reason: error.to_string(),
                version: None,
                transaction_id: None,
                event_published: false,
                event_publish_error: None,
            },
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetParametersReply {
    pub results: Vec<ParameterSetResult>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetParametersAtomicallyReply {
    pub result: ParameterSetResult,
}

impl ParameterStore {
    /// Called by an SDK Executor or by a Rust application. No callback is kept
    /// by core; callers choose the callback group and scheduling policy.
    pub fn handle_request<F>(
        &self,
        kind: ParameterServiceKind,
        request: &Value,
        mut validator: F,
    ) -> ParameterResult<Value>
    where
        F: FnMut(&[Parameter]) -> ParameterResult<()>,
    {
        kind.interface()
            .validate("request", request)
            .map_err(|e| ParameterError::InvalidArgument(e.to_string()))?;
        match kind {
            ParameterServiceKind::SetParametersAtomically => {
                let request: SetParametersAtomicallyRequest = parse(request)?;
                let result = (|| {
                    let transaction = match request.mode {
                        ParameterUpdateMode::Set => self.prepare_set(request.parameters)?,
                        ParameterUpdateMode::Load => {
                            self.prepare_load_values(request.parameters)?
                        }
                    };
                    validator(transaction.candidate())?;
                    transaction.commit()
                })();
                Ok(json!(SetParametersAtomicallyReply {
                    result: ParameterSetResult::from_result(result),
                }))
            }
            ParameterServiceKind::SetParameters => {
                let request: SetParametersRequest = parse(request)?;
                let mut set = |parameter| {
                    let transaction = self.prepare_set(vec![parameter])?;
                    validator(transaction.candidate())?;
                    transaction.commit()
                };
                Ok(json!(SetParametersReply {
                    results: request
                        .parameters
                        .into_iter()
                        .map(|parameter| ParameterSetResult::from_result(set(parameter)))
                        .collect(),
                }))
            }
            ParameterServiceKind::ListParameters => {
                let request: ListParametersRequest = parse(request)?;
                // Derive both the list and its version from the same snapshot.
                Ok(query(self.snapshot(), |snapshot| {
                    Ok(list_snapshot(snapshot, &request.prefixes, request.depth))
                }))
            }
            kind => {
                let request: ParameterNamesRequest = parse(request)?;
                Ok(query(self.snapshot(), |snapshot| {
                    let records = request
                        .names
                        .iter()
                        .map(|name| {
                            snapshot
                                .parameters
                                .iter()
                                .find(|record| &record.name == name)
                                .cloned()
                                .ok_or_else(|| ParameterError::NotDeclared(name.clone()))
                        })
                        .collect::<ParameterResult<Vec<_>>>()?;
                    match kind {
                        ParameterServiceKind::DescribeParameters => Ok(json!(records)),
                        ParameterServiceKind::GetParameterTypes => Ok(json!(records
                            .iter()
                            .map(|p| p.value.parameter_type())
                            .collect::<Vec<_>>())),
                        ParameterServiceKind::GetParameters => {
                            let values = records
                                .into_iter()
                                .map(|p| {
                                    if p.value == ParameterValue::NotSet
                                        && !p.descriptor.dynamic_typing
                                    {
                                        Err(ParameterError::Uninitialized(p.name))
                                    } else {
                                        Ok(p.value)
                                    }
                                })
                                .collect::<ParameterResult<Vec<_>>>()?;
                            Ok(json!(values))
                        }
                        _ => unreachable!(),
                    }
                }))
            }
        }
    }
}
fn parse<T: DeserializeOwned>(value: &Value) -> ParameterResult<T> {
    serde_json::from_value(value.clone())
        .map_err(|e| ParameterError::InvalidArgument(e.to_string()))
}
fn query<T: Serialize>(
    snapshot: ParameterResult<ParameterSnapshot>,
    extract: impl FnOnce(&ParameterSnapshot) -> ParameterResult<T>,
) -> Value {
    match snapshot {
        Ok(snapshot) => {
            let version = Some(snapshot.version);
            match extract(&snapshot) {
                Ok(value) => json!(ParameterQueryReply {
                    version,
                    value: Some(value),
                    error: None
                }),
                Err(error) => json!(ParameterQueryReply::<T> {
                    version,
                    value: None,
                    error: Some(error)
                }),
            }
        }
        Err(error) => json!(ParameterQueryReply::<T> {
            version: None,
            value: None,
            error: Some(error)
        }),
    }
}
fn list_snapshot(
    snapshot: &ParameterSnapshot,
    prefixes: &[String],
    depth: u64,
) -> ListParametersResult {
    let names: Vec<_> = snapshot
        .parameters
        .iter()
        .map(|p| &p.name)
        .filter(|name| {
            if prefixes.is_empty() {
                depth == 0 || (name.matches('.').count() as u64) < depth
            } else {
                prefixes.iter().any(|prefix| {
                    name.as_str() == prefix
                        || name
                            .strip_prefix(&format!("{prefix}."))
                            .is_some_and(|tail| {
                                depth == 0 || (tail.matches('.').count() as u64) < depth
                            })
                })
            }
        })
        .cloned()
        .collect();
    let prefixes = names
        .iter()
        .filter_map(|name| name.rsplit_once('.').map(|(prefix, _)| prefix.to_owned()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    ListParametersResult { names, prefixes }
}

/// Node-owned service set, polled by an application Executor. Clones share
/// ownership; closing the Node invalidates services and the parameter store.
#[derive(Clone)]
pub struct ParameterServer {
    inner: Arc<ParameterServerInner>,
}
struct ParameterServerInner {
    store: ParameterStore,
    services: Vec<(ParameterServiceKind, Arc<Service>)>,
    closed: AtomicBool,
    next_service: AtomicUsize,
}
impl ParameterServer {
    pub(crate) fn new(node: &Node) -> Result<Self> {
        let qos = resolve_qos(&Value::Null, "parameters", true)?;
        let mut services = Vec::new();
        for kind in ParameterServiceKind::ALL {
            let service = node.create_control_service(
                &kind.interface(),
                &format!("~/{}", kind.suffix()),
                qos.clone(),
                EndpointOptions {
                    summary: format!("Node parameter operation {}", kind.suffix()),
                    side_effects: Some(
                        if matches!(
                            kind,
                            ParameterServiceKind::SetParameters
                                | ParameterServiceKind::SetParametersAtomically
                        ) {
                            "write"
                        } else {
                            "read_only"
                        }
                        .into(),
                    ),
                    ..Default::default()
                },
            )?;
            services.push((kind, Arc::new(service)));
        }
        let store = node.parameters();
        Ok(Self {
            inner: Arc::new(ParameterServerInner {
                store,
                services,
                closed: AtomicBool::new(false),
                next_service: AtomicUsize::new(0),
            }),
        })
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<ParameterServiceRequest> {
        let start = Instant::now();
        loop {
            if self.is_closed() {
                return Err(Error::Closed);
            }
            self.inner.store.version()?;
            let first =
                self.inner.next_service.fetch_add(1, Ordering::Relaxed) % self.inner.services.len();
            for offset in 0..self.inner.services.len() {
                let (kind, service) =
                    &self.inner.services[(first + offset) % self.inner.services.len()];
                match service.recv_timeout(Duration::ZERO) {
                    Ok(request) => {
                        return Ok(ParameterServiceRequest {
                            kind: *kind,
                            request,
                            store: self.inner.store.clone(),
                            owner: Arc::downgrade(&self.inner),
                        })
                    }
                    Err(Error::Timeout) => {}
                    Err(error) => return Err(error),
                }
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            std::thread::sleep(remaining.min(Duration::from_millis(1)));
        }
    }
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
impl ParameterServerInner {
    fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(Error::Closed);
        }
        let mut error = None;
        for (_, service) in &self.services {
            if let Err(e) = service.close() {
                if e != Error::Closed && error.is_none() {
                    error = Some(e);
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}
impl Drop for ParameterServerInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub struct ParameterServiceRequest {
    kind: ParameterServiceKind,
    request: ServiceRequest,
    store: ParameterStore,
    owner: Weak<ParameterServerInner>,
}
impl ParameterServiceRequest {
    pub fn kind(&self) -> ParameterServiceKind {
        self.kind
    }
    pub fn request(&self) -> &Value {
        self.request.request()
    }
    pub fn execute<F>(self, mut validator: F) -> Result<()>
    where
        F: FnMut(&[Parameter]) -> ParameterResult<()>,
    {
        let owner = self.owner.upgrade().ok_or(Error::Closed)?;
        if owner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        match self
            .store
            .handle_request(self.kind, self.request.request(), |candidate| {
                validator(candidate)?;
                if owner.closed.load(Ordering::Acquire) {
                    return Err(ParameterError::Closed);
                }
                Ok(())
            }) {
            Ok(response) => self.request.reply(&response),
            Err(error) => {
                let _ = self.request.reply_error(&error.to_string());
                Err(error.into())
            }
        }
    }
}

/// Rust/CLI facade over the same ordinary services used by SDKs.
pub struct ParameterClient {
    clients: BTreeMap<ParameterServiceKind, Client>,
}
impl ParameterClient {
    pub fn new(node: &Node, remote_node: &str) -> Result<Self> {
        crate::names::validate_absolute_name(remote_node)?;
        let qos = resolve_qos(&Value::Null, "parameters", true)?;
        let clients = ParameterServiceKind::ALL
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
        kind: ParameterServiceKind,
        request: &Value,
        timeout: Duration,
    ) -> Result<Value> {
        self.clients[&kind].call(request, timeout)
    }
    pub fn get(
        &self,
        names: &[String],
        timeout: Duration,
    ) -> Result<ParameterQueryReply<Vec<ParameterValue>>> {
        self.typed_call(
            ParameterServiceKind::GetParameters,
            &json!({"names":names}),
            timeout,
        )
    }
    pub fn describe(
        &self,
        names: &[String],
        timeout: Duration,
    ) -> Result<ParameterQueryReply<Vec<ParameterRecord>>> {
        self.typed_call(
            ParameterServiceKind::DescribeParameters,
            &json!({"names":names}),
            timeout,
        )
    }
    pub fn get_types(
        &self,
        names: &[String],
        timeout: Duration,
    ) -> Result<ParameterQueryReply<Vec<ParameterType>>> {
        self.typed_call(
            ParameterServiceKind::GetParameterTypes,
            &json!({"names":names}),
            timeout,
        )
    }
    pub fn list(
        &self,
        prefixes: &[String],
        depth: u64,
        timeout: Duration,
    ) -> Result<ParameterQueryReply<ListParametersResult>> {
        self.typed_call(
            ParameterServiceKind::ListParameters,
            &json!({"prefixes":prefixes,"depth":depth}),
            timeout,
        )
    }
    pub fn set(&self, parameters: &[Parameter], timeout: Duration) -> Result<SetParametersReply> {
        self.typed_call(
            ParameterServiceKind::SetParameters,
            &json!({"parameters":parameters}),
            timeout,
        )
    }
    pub fn set_atomically(
        &self,
        parameters: &[Parameter],
        timeout: Duration,
    ) -> Result<ParameterSetResult> {
        let response: SetParametersAtomicallyReply = self.typed_call(
            ParameterServiceKind::SetParametersAtomically,
            &json!({"parameters":parameters}),
            timeout,
        )?;
        Ok(response.result)
    }
    /// Load the entire candidate in one server-side transaction. The server
    /// checks unchanged/read-only/uninitialized entries without client filtering.
    pub fn load_atomically(
        &self,
        parameters: &[Parameter],
        timeout: Duration,
    ) -> Result<ParameterSetResult> {
        let response: SetParametersAtomicallyReply = self.typed_call(
            ParameterServiceKind::SetParametersAtomically,
            &json!({"parameters":parameters,"mode":"load"}),
            timeout,
        )?;
        Ok(response.result)
    }
    fn typed_call<T: DeserializeOwned>(
        &self,
        kind: ParameterServiceKind,
        request: &Value,
        timeout: Duration,
    ) -> Result<T> {
        serde_json::from_value(self.call(kind, request, timeout)?)
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

pub fn parameter_event_interface() -> Interface {
    static INTERFACE: OnceLock<Interface> = OnceLock::new();
    INTERFACE.get_or_init(|| Interface::from_json(json!({
        "name":"aos_interfaces/msg/ParameterEvent","kind":"msg",
        "message":object(
            json!({
                "node":{"type":"string"},"node_id":{"type":"string"},
                "version":{"type":"integer","minimum":0},"transaction_id":{"type":"string"},
                "stamp":object(json!({
                    "nanoseconds":{"type":"integer"},"clock_type":{"enum":["steady","system","ros"]},
                    "initialized":{"type":"boolean"},"use_sim_time":{"type":"boolean"}
                }), &["nanoseconds","clock_type","initialized","use_sim_time"]),
                "new_parameters":{"type":"array","items":parameter_schema()},
                "changed_parameters":{"type":"array","items":parameter_schema()},
                "deleted_parameters":{"type":"array","items":parameter_schema()}
            }),
            &["node","node_id","version","transaction_id","stamp","new_parameters","changed_parameters","deleted_parameters"]
        )
    })).expect("built-in parameter event schema is valid")).clone()
}
fn build_interface(kind: ParameterServiceKind) -> Interface {
    let (name, request, response) = match kind {
        ParameterServiceKind::DescribeParameters => (
            "DescribeParameters",
            names_schema(),
            query_schema(json!({"type":"array","items":record_schema()})),
        ),
        ParameterServiceKind::GetParameterTypes => (
            "GetParameterTypes",
            names_schema(),
            query_schema(json!({"type":"array","items":type_schema()})),
        ),
        ParameterServiceKind::GetParameters => (
            "GetParameters",
            names_schema(),
            query_schema(json!({"type":"array","items":value_schema()})),
        ),
        ParameterServiceKind::ListParameters => (
            "ListParameters",
            object(
                json!({"prefixes":{"type":"array","items":{"type":"string"}},"depth":{"type":"integer","minimum":0}}),
                &["prefixes", "depth"],
            ),
            query_schema(object(
                json!({"names":{"type":"array","items":{"type":"string"}},"prefixes":{"type":"array","items":{"type":"string"}}}),
                &["names", "prefixes"],
            )),
        ),
        ParameterServiceKind::SetParameters => (
            "SetParameters",
            set_schema(),
            object(
                json!({"results":{"type":"array","items":set_result_schema()}}),
                &["results"],
            ),
        ),
        ParameterServiceKind::SetParametersAtomically => (
            "SetParametersAtomically",
            object(
                json!({
                    "parameters":{"type":"array","items":parameter_schema()},
                    "mode":{"enum":["set","load"]}
                }),
                &["parameters"],
            ),
            object(json!({"result":set_result_schema()}), &["result"]),
        ),
    };
    Interface::from_json(json!({"name":format!("aos_interfaces/srv/{name}"),"kind":"srv","request":request,"response":response}))
        .expect("built-in parameter service schema is valid")
}
fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}
fn type_schema() -> Value {
    json!({"enum":["not_set","bool","integer","double","string","byte_array","bool_array","integer_array","double_array","string_array"]})
}
fn value_schema() -> Value {
    let mut variants = vec![object(json!({"type":{"const":"not_set"}}), &["type"])];
    for (name, schema) in [
        ("bool", json!({"type":"boolean"})),
        ("integer", json!({"type":"integer"})),
        ("double", json!({"type":"number"})),
        ("string", json!({"type":"string"})),
        (
            "byte_array",
            json!({"type":"array","items":{"type":"integer","minimum":0,"maximum":255}}),
        ),
        (
            "bool_array",
            json!({"type":"array","items":{"type":"boolean"}}),
        ),
        (
            "integer_array",
            json!({"type":"array","items":{"type":"integer"}}),
        ),
        (
            "double_array",
            json!({"type":"array","items":{"type":"number"}}),
        ),
        (
            "string_array",
            json!({"type":"array","items":{"type":"string"}}),
        ),
    ] {
        variants.push(object(
            json!({"type":{"const":name},"value":schema}),
            &["type", "value"],
        ));
    }
    json!({"oneOf":variants})
}
fn parameter_schema() -> Value {
    object(
        json!({"name":{"type":"string","minLength":1},"value":value_schema()}),
        &["name", "value"],
    )
}
fn record_schema() -> Value {
    object(
        json!({
            "name":{"type":"string","minLength":1},"value":value_schema(),"default_value":value_schema(),
            "descriptor":object(json!({
                "parameter_type":nullable(type_schema()),"description":{"type":"string"},
                "additional_constraints":{"type":"string"},"read_only":{"type":"boolean"},
                "dynamic_typing":{"type":"boolean"},
                "integer_range":nullable(object(json!({"from_value":{"type":"integer"},"to_value":{"type":"integer"},"step":{"type":"integer","minimum":0}}), &["from_value","to_value","step"])),
                "floating_point_range":nullable(object(json!({"from_value":{"type":"number"},"to_value":{"type":"number"},"step":{"type":"number"}}), &["from_value","to_value","step"])),
                "allowed_states":{"type":"array","uniqueItems":true,"items":{"enum":["unconfigured","inactive","active","finalized"]}}
            }), &["parameter_type","description","additional_constraints","read_only","dynamic_typing","integer_range","floating_point_range","allowed_states"]),
            "version":{"type":"integer","minimum":0},"source":{"type":"string"}
        }),
        &[
            "name",
            "value",
            "default_value",
            "descriptor",
            "version",
            "source",
        ],
    )
}
fn names_schema() -> Value {
    object(
        json!({"names":{"type":"array","items":{"type":"string","minLength":1}}}),
        &["names"],
    )
}
fn set_schema() -> Value {
    object(
        json!({"parameters":{"type":"array","items":parameter_schema()}}),
        &["parameters"],
    )
}
fn query_schema(value: Value) -> Value {
    object(
        json!({
            "version":{"type":["integer","null"],"minimum":0},"value":nullable(value),
            "error":nullable(object(json!({"code":{"type":"string"},"reason":{"type":"string"}}), &["code"]))
        }),
        &["version", "value", "error"],
    )
}
fn set_result_schema() -> Value {
    object(
        json!({
            "successful":{"type":"boolean"},"code":{"type":["string","null"]},"reason":{"type":"string"},
            "version":{"type":["integer","null"],"minimum":0},"transaction_id":{"type":["string","null"]},
            "event_published":{"type":"boolean"},"event_publish_error":{"type":["string","null"]}
        }),
        &[
            "successful",
            "code",
            "reason",
            "version",
            "transaction_id",
            "event_published",
            "event_publish_error",
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ParameterDescriptor;
    #[test]
    fn service_helpers_keep_atomic_and_sequential_ros_semantics_and_structured_errors() {
        let store = ParameterStore::new("/n", "id");
        for name in ["a", "b"] {
            store
                .declare(
                    name,
                    ParameterValue::Integer(1),
                    ParameterDescriptor::default(),
                    false,
                )
                .unwrap();
        }
        let request = json!({"parameters":[{"name":"a","value":{"type":"integer","value":2}},{"name":"b","value":{"type":"bool","value":true}}]});
        let atomic = store
            .handle_request(
                ParameterServiceKind::SetParametersAtomically,
                &request,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(atomic["result"]["code"], "wrong_type");
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(1));
        let sequential = store
            .handle_request(ParameterServiceKind::SetParameters, &request, |_| Ok(()))
            .unwrap();
        assert_eq!(sequential["results"][0]["successful"], true);
        assert_eq!(sequential["results"][1]["code"], "wrong_type");
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(2));
        let rejected = store
            .handle_request(
                ParameterServiceKind::SetParametersAtomically,
                &json!({"parameters":[{"name":"a","value":{"type":"integer","value":4}}]}),
                |_| Err(ParameterError::Validation("application says no".into())),
            )
            .unwrap();
        assert_eq!(rejected["result"]["code"], "validation");
        assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(2));
    }
    #[test]
    fn all_six_wire_schemas_validate_shared_core_results() {
        let store = ParameterStore::new("/n", "id");
        store
            .declare(
                "a.x",
                ParameterValue::Integer(1),
                ParameterDescriptor::default(),
                false,
            )
            .unwrap();
        for kind in ParameterServiceKind::ALL {
            let request = match kind {
                ParameterServiceKind::ListParameters => json!({"prefixes":[],"depth":0}),
                ParameterServiceKind::SetParameters
                | ParameterServiceKind::SetParametersAtomically => {
                    json!({"parameters":[{"name":"a.x","value":{"type":"integer","value":2}}]})
                }
                _ => json!({"names":["a.x"]}),
            };
            let response = store.handle_request(kind, &request, |_| Ok(())).unwrap();
            kind.interface().validate("response", &response).unwrap();
        }
        let missing = store
            .handle_request(
                ParameterServiceKind::GetParameters,
                &json!({"names":["missing"]}),
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(missing["error"]["code"], "not_declared");
        ParameterServiceKind::GetParameters
            .interface()
            .validate("response", &missing)
            .unwrap();
        let commit = store
            .set_atomically(vec![Parameter::new("a.x", ParameterValue::Integer(3))])
            .unwrap();
        parameter_event_interface()
            .validate("message", &json!(commit.event))
            .unwrap();
    }
}
