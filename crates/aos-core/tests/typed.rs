use aos_core::{transport::*, *};
use serde_json::json;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Condvar, Mutex, Weak,
    },
    time::{Duration, Instant},
};

struct Queue<T> {
    data: Mutex<(bool, VecDeque<T>)>,
    ready: Condvar,
    depth: usize,
    received: AtomicU64,
    dropped: AtomicU64,
}
impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            data: Mutex::new((false, VecDeque::new())),
            ready: Condvar::new(),
            depth: usize::MAX,
            received: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }
}
impl<T> Queue<T> {
    fn push(&self, value: T) -> Result<()> {
        let mut q = self.data.lock().unwrap();
        if q.0 {
            return Err(Error::Closed);
        }
        self.received.fetch_add(1, Ordering::Relaxed);
        if q.1.len() == self.depth {
            q.1.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        q.1.push_back(value);
        self.ready.notify_one();
        Ok(())
    }
    fn take(&self, timeout: Duration) -> Result<T> {
        let deadline = Instant::now() + timeout;
        let mut q = self.data.lock().unwrap();
        loop {
            if q.0 {
                return Err(Error::Closed);
            }
            if let Some(v) = q.1.pop_front() {
                return Ok(v);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(Error::Timeout);
            }
            q = self.ready.wait_timeout(q, left).unwrap().0;
        }
    }
    fn close(&self) {
        self.data.lock().unwrap().0 = true;
        self.ready.notify_all();
    }
    fn stats(&self) -> QueueStats {
        let state = self.data.lock().unwrap();
        QueueStats {
            depth: self.depth,
            queued: state.1.len(),
            received_total: self.received.load(Ordering::Relaxed),
            dropped_total: self.dropped.load(Ordering::Relaxed),
            closed: state.0,
            bytes: None,
        }
    }
}
type SubscriptionEntry = (String, Weak<Queue<Sample<Vec<u8>>>>);

#[derive(Default)]
struct Bus {
    graph: Mutex<BTreeMap<String, GraphEntity>>,
    services: Mutex<BTreeMap<String, Arc<Queue<Request>>>>,
    subscriptions: Mutex<Vec<SubscriptionEntry>>,
    clock_subscription_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
#[derive(Clone, Default)]
struct Memory(Arc<Bus>);
fn key(e: &Endpoint) -> String {
    format!("{}:{}:{}", e.name, e.type_name, e.type_hash)
}
struct Guard {
    bus: Arc<Bus>,
    id: String,
}
impl GraphGuard for Guard {
    fn close(&self) -> Result<()> {
        self.bus.graph.lock().unwrap().remove(&self.id);
        Ok(())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
struct Request {
    bytes: Vec<u8>,
    request_id: String,
    response: Mutex<Option<mpsc::Sender<Result<Vec<u8>>>>>,
    reply_block: Option<ReplyBlock>,
    reply_failure: Option<Error>,
}
struct ReplyBlock {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}
impl RequestBackend for Request {
    fn payload(&self) -> &[u8] {
        &self.bytes
    }
    fn request_id(&self) -> Option<&str> {
        Some(&self.request_id)
    }
    fn reply(&self, response: Result<Vec<u8>>) -> Result<()> {
        if let Some(error) = &self.reply_failure {
            return Err(error.clone());
        }
        if let Some(block) = &self.reply_block {
            block.started.send(()).map_err(|_| Error::Closed)?;
            block
                .release
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| Error::Closed)?;
        }
        self.response
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::InvalidArgument("already replied".into()))?
            .send(response)
            .map_err(|_| Error::Closed)
    }
}
struct Provider {
    queue: Arc<Queue<Request>>,
    bus: Arc<Bus>,
    key: String,
}
impl ServiceBackend for Provider {
    fn recv_timeout(&self, d: Duration) -> Result<Box<dyn RequestBackend>> {
        Ok(Box::new(self.queue.take(d)?))
    }
    fn close(&self) -> Result<()> {
        self.queue.close();
        self.bus.services.lock().unwrap().remove(&self.key);
        Ok(())
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
struct Pub {
    bus: Arc<Bus>,
    key: String,
}
impl PublisherBackend for Pub {
    fn publish(&self, p: &[u8]) -> Result<()> {
        for (k, q) in self.bus.subscriptions.lock().unwrap().iter() {
            if *k == self.key {
                if let Some(q) = q.upgrade() {
                    q.push(Sample::without_receive_metadata(p.to_vec()))?;
                }
            }
        }
        Ok(())
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
struct Sub(Arc<Queue<Sample<Vec<u8>>>>);
impl SubscriptionBackend for Sub {
    fn recv_timeout(&self, d: Duration) -> Result<Vec<u8>> {
        self.recv_sample_timeout(d).map(|sample| sample.payload)
    }
    fn recv_sample_timeout(&self, d: Duration) -> Result<Sample<Vec<u8>>> {
        self.0.take(d)
    }
    fn close(&self) -> Result<()> {
        self.0.close();
        Ok(())
    }
    fn queue_stats(&self) -> Option<QueueStats> {
        Some(self.0.stats())
    }
}
impl Drop for Sub {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl Transport for Memory {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("raw".into()))
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("raw".into()))
    }
    fn create_typed_publisher(&self, e: &Endpoint, _: &QoS) -> Result<Box<dyn PublisherBackend>> {
        Ok(Box::new(Pub {
            bus: self.0.clone(),
            key: key(e),
        }))
    }
    fn create_typed_subscription(
        &self,
        e: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        let q = Arc::new(Queue {
            depth: qos.depth,
            ..Default::default()
        });
        self.0
            .subscriptions
            .lock()
            .unwrap()
            .push((key(e), Arc::downgrade(&q)));
        if e.name == "/clock" {
            let hook = self.0.clock_subscription_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        Ok(Box::new(Sub(q)))
    }
    fn declare_graph_entity(&self, e: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        self.0.graph.lock().unwrap().insert(e.id.clone(), e.clone());
        Ok(Box::new(Guard {
            bus: self.0.clone(),
            id: e.id.clone(),
        }))
    }
    fn graph(&self, _: Duration) -> Result<Vec<GraphEntity>> {
        Ok(self.0.graph.lock().unwrap().values().cloned().collect())
    }
    fn create_service(&self, e: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        let q = Arc::new(Queue::default());
        self.0.services.lock().unwrap().insert(key(e), q.clone());
        Ok(Box::new(Provider {
            queue: q,
            bus: self.0.clone(),
            key: key(e),
        }))
    }
    fn call(&self, e: &Endpoint, p: &[u8], d: Duration) -> Result<Vec<u8>> {
        let q = self
            .0
            .services
            .lock()
            .unwrap()
            .get(&key(e))
            .cloned()
            .ok_or(Error::Timeout)?;
        let (tx, rx) = mpsc::channel();
        q.push(Request {
            bytes: p.to_vec(),
            request_id: Telemetry::new_request_id(),
            response: Mutex::new(Some(tx)),
            reply_block: None,
            reply_failure: None,
        })?;
        rx.recv_timeout(d).map_err(|_| Error::Timeout)?
    }
    fn call_cancelable(
        &self,
        e: &Endpoint,
        p: &[u8],
        d: Duration,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        let q = self
            .0
            .services
            .lock()
            .unwrap()
            .get(&key(e))
            .cloned()
            .ok_or(Error::Timeout)?;
        let (tx, rx) = mpsc::channel();
        q.push(Request {
            bytes: p.to_vec(),
            request_id: Telemetry::new_request_id(),
            response: Mutex::new(Some(tx)),
            reply_block: None,
            reply_failure: None,
        })?;
        let deadline = Instant::now() + d;
        loop {
            if cancel.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(remaining.min(Duration::from_millis(20))) {
                Ok(response) => return response,
                Err(mpsc::RecvTimeoutError::Timeout) if !remaining.is_zero() => {}
                Err(_) => return Err(Error::Timeout),
            }
        }
    }
    fn close(&self) -> Result<()> {
        for q in self.0.services.lock().unwrap().values() {
            q.close();
        }
        self.0.graph.lock().unwrap().clear();
        Ok(())
    }
}
const WAIT: Duration = Duration::from_secs(2);

// This adapter exercises parameter dispatch and ownership. Real queue depth,
// durability and overflow behavior are covered by the Zenoh adapter tests.
#[derive(Default)]
struct ParameterMemory(Memory);
impl Transport for ParameterMemory {
    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
        self.0.create_publisher(topic)
    }
    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
        self.0.create_subscription(topic)
    }
    fn create_typed_publisher(&self, e: &Endpoint, qos: &QoS) -> Result<Box<dyn PublisherBackend>> {
        self.0.create_typed_publisher(e, qos)
    }
    fn create_typed_subscription(
        &self,
        e: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        self.0.create_typed_subscription(e, qos)
    }
    fn declare_graph_entity(&self, entity: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        self.0.declare_graph_entity(entity)
    }
    fn graph(&self, timeout: Duration) -> Result<Vec<GraphEntity>> {
        self.0.graph(timeout)
    }
    fn create_service(&self, e: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        self.0.create_service(e)
    }
    fn create_service_with_qos(&self, e: &Endpoint, qos: &QoS) -> Result<Box<dyn ServiceBackend>> {
        qos.validate_service()?;
        self.0.create_service(e)
    }
    fn create_client_with_qos(
        &self,
        _: &Endpoint,
        qos: &QoS,
    ) -> Result<Option<Box<dyn ClientBackend>>> {
        qos.validate_service()?;
        Ok(None)
    }
    fn call(&self, e: &Endpoint, payload: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        self.0.call(e, payload, timeout)
    }
    fn call_cancelable(
        &self,
        e: &Endpoint,
        payload: &[u8],
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        self.0.call_cancelable(e, payload, timeout, cancel)
    }
    fn close(&self) -> Result<()> {
        self.0.close()
    }
}

#[test]
fn node_parameters_clock_startup_override_and_shutdown_have_one_owner() {
    let context = Context::new(Memory::default());
    let node = Node::new_with_config(
        context.clone(),
        "params",
        "/",
        NodeOptions::default(),
        NodeConfigRequest {
            program: json!({"parameters":{"limit":5}}),
            ..Default::default()
        },
    )
    .unwrap();
    let store = node.parameters();
    assert_eq!(
        store.get("use_sim_time").unwrap(),
        ParameterValue::Bool(false)
    );
    store
        .declare(
            "limit",
            ParameterValue::Integer(1),
            ParameterDescriptor::default(),
            false,
        )
        .unwrap();
    assert_eq!(store.get("limit").unwrap(), ParameterValue::Integer(5));
    let rejected = store
        .prepare_set(vec![Parameter::new(
            "use_sim_time",
            ParameterValue::Bool(true),
        )])
        .unwrap();
    rejected.abort();
    assert!(!node.clock().now().unwrap().use_sim_time);
    store
        .set_atomically(vec![Parameter::new(
            "use_sim_time",
            ParameterValue::Bool(true),
        )])
        .unwrap();
    assert!(node.clock().now().unwrap().use_sim_time);
    assert!(node.node_card()["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|endpoint| endpoint["name"] == "/clock"));
    node.clock().set_use_sim_time(false).unwrap();
    assert_eq!(
        store.get("use_sim_time").unwrap(),
        ParameterValue::Bool(false)
    );
    assert_eq!(node.effective_config()["use_sim_time"]["value"], false);
    assert_eq!(
        node.effective_config()["parameters"]["use_sim_time"]["value"],
        json!({"type":"bool","value":false})
    );
    node.set_use_sim_time(true).unwrap();
    assert_eq!(
        store.get("use_sim_time").unwrap(),
        ParameterValue::Bool(true)
    );
    let tx = store
        .prepare_set(vec![Parameter::new("limit", ParameterValue::Integer(8))])
        .unwrap();
    node.destroy_node().unwrap();
    assert!(matches!(tx.commit(), Err(ParameterError::Closed)));
    assert!(matches!(store.get("limit"), Err(ParameterError::Closed)));

    let second = Node::new(context.clone(), "second", "/", NodeOptions::default()).unwrap();
    let survivor = second.parameters();
    context.shutdown().unwrap();
    assert!(matches!(survivor.version(), Err(ParameterError::Closed)));
}

#[test]
fn parameter_services_require_executor_and_emit_only_committed_versions() {
    let context = Context::new(ParameterMemory::default());
    let node = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let store = node.parameters();
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
    node.enable_parameter_events().unwrap();
    let events = caller
        .create_subscription(
            &parameter_event_interface(),
            "/parameter_events",
            resolve_qos(&json!(null), "parameter_events", false).unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert!(!node.node_card()["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|endpoint| endpoint["name"] == "/provider/set_parameters"));
    let local = store
        .set_atomically(vec![Parameter::new("a", ParameterValue::Integer(1))])
        .unwrap();
    assert!(local.event_published);
    events.recv_timeout(WAIT).unwrap();
    let server = node.enable_parameter_services().unwrap();
    let client = Arc::new(ParameterClient::new(&caller, "/provider").unwrap());
    let remote = client.clone();
    let first = std::thread::spawn(move || {
        remote
            .set_atomically(
                &[
                    Parameter::new("a", ParameterValue::Integer(2)),
                    Parameter::new("b", ParameterValue::Integer(3)),
                ],
                WAIT,
            )
            .unwrap()
    });
    let request = server.recv_timeout(WAIT).unwrap();
    assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(1));
    assert!(matches!(
        events.recv_timeout(Duration::ZERO),
        Err(Error::Timeout)
    ));
    let executor_thread = std::thread::current().id();
    request
        .execute(|candidate| {
            assert_eq!(std::thread::current().id(), executor_thread);
            assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(1));
            assert_eq!(candidate.len(), 2);
            Ok(())
        })
        .unwrap();
    let result = first.join().unwrap();
    assert!(result.successful && result.event_published);
    let event: ParameterEvent = serde_json::from_value(events.recv_timeout(WAIT).unwrap()).unwrap();
    assert_eq!(Some(event.version), result.version);
    assert_eq!(Some(event.transaction_id), result.transaction_id);
    assert_eq!(event.changed_parameters.len(), 2);

    let remote = client.clone();
    let invalid = std::thread::spawn(move || {
        remote
            .set_atomically(
                &[
                    Parameter::new("a", ParameterValue::Integer(8)),
                    Parameter::new("b", ParameterValue::Bool(true)),
                ],
                WAIT,
            )
            .unwrap()
    });
    server
        .recv_timeout(WAIT)
        .unwrap()
        .execute(|_| panic!("invalid built-in type must fail before application validator"))
        .unwrap();
    assert_eq!(invalid.join().unwrap().code.as_deref(), Some("wrong_type"));
    assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(2));
    assert!(matches!(
        events.recv_timeout(Duration::ZERO),
        Err(Error::Timeout)
    ));

    let remote = client.clone();
    let rejected = std::thread::spawn(move || {
        remote
            .set_atomically(&[Parameter::new("a", ParameterValue::Integer(9))], WAIT)
            .unwrap()
    });
    server
        .recv_timeout(WAIT)
        .unwrap()
        .execute(|_| {
            Err(ParameterError::Validation(
                "application rejected candidate".into(),
            ))
        })
        .unwrap();
    assert_eq!(rejected.join().unwrap().code.as_deref(), Some("validation"));
    assert!(matches!(
        events.recv_timeout(Duration::ZERO),
        Err(Error::Timeout)
    ));

    let remote = client.clone();
    let described =
        std::thread::spawn(move || remote.describe(&["a".into(), "b".into()], WAIT).unwrap());
    server
        .recv_timeout(WAIT)
        .unwrap()
        .execute(|_| panic!("query must not run validators"))
        .unwrap();
    let described = described.join().unwrap();
    assert_eq!(described.version, result.version);
    assert_eq!(
        described
            .value
            .unwrap()
            .iter()
            .map(|r| r.version)
            .collect::<Vec<_>>(),
        vec![event.version; 2]
    );

    // Inject publication failure through the test bus after a successful commit.
    events.close().unwrap();
    let committed = store
        .set_atomically(vec![Parameter::new("a", ParameterValue::Integer(7))])
        .unwrap();
    assert!(!committed.event_published);
    assert!(committed.event_publish_error.is_some());
    assert_eq!(store.get("a").unwrap(), ParameterValue::Integer(7));
    server.close().unwrap();
    assert!(matches!(
        server.recv_timeout(Duration::ZERO),
        Err(Error::Closed)
    ));
    drop(events);
    let remaining_events = caller
        .create_subscription(
            &parameter_event_interface(),
            "/parameter_events",
            resolve_qos(&json!(null), "parameter_events", false).unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert!(
        store
            .set_atomically(vec![Parameter::new("a", ParameterValue::Integer(6))])
            .unwrap()
            .event_published
    );
    remaining_events.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        node.enable_parameter_services(),
        Err(Error::Closed)
    ));
    node.destroy_node().unwrap();
    assert!(matches!(
        server.recv_timeout(Duration::ZERO),
        Err(Error::Closed)
    ));
}
fn message() -> Interface {
    Interface::from_json(json!({"name":"test/msg/Value","kind":"msg","message":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})).unwrap()
}
fn service() -> Interface {
    Interface::from_json(json!({"name":"test/srv/Add","kind":"srv","request":{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false},"response":{"type":"object","properties":{"sum":{"type":"integer"}},"required":["sum"],"additionalProperties":false}})).unwrap()
}

#[test]
fn names_expand_like_ros_and_keep_hidden_entities() {
    use aos_core::names::*;
    assert_eq!(
        expand_name("chat", "worker", "/team").unwrap(),
        "/team/chat"
    );
    assert_eq!(
        expand_name("~/chat", "worker", "/").unwrap(),
        "/worker/chat"
    );
    assert_eq!(
        expand_name("{ns}/{node}/chat", "worker", "/team").unwrap(),
        "/team/worker/chat"
    );
    assert!(is_hidden("/task/_action/status"));
    for name in ["", "foo//bar", "~x", "/9bad", "a/*", "a/{unknown}"] {
        assert!(expand_name(name, "worker", "/").is_err());
    }
}

#[test]
fn shared_schema_validation_hash_and_action_default() {
    let a = message();
    let b=Interface::from_json(serde_json::from_str(r#"{"message":{"additionalProperties":false,"required":["value"],"properties":{"value":{"type":"integer"}},"type":"object"},"kind":"msg","name":"test/msg/Value"}"#).unwrap()).unwrap();
    assert_eq!(a.type_hash(), b.type_hash());
    assert_eq!(a.type_hash().len(), 64);
    assert!(
        matches!(a.validate("message",&json!({"value":"bad"})),Err(Error::Validation(e)) if e.contains("value"))
    );
    assert!(Interface::from_json(json!({"name":"test/msg/Remote","kind":"msg","message":{"$ref":"https://example.com/schema"}})).is_err());
    let mut lossy = message().definition().clone();
    lossy["message"]["properties"]["value"]["maximum"] = json!(9007199254740993u64);
    assert!(Interface::from_json(lossy).is_err());
    assert!(Interface::from_json(json!({"name":"test/msg/RefField","kind":"msg","message":{"type":"object","properties":{"$ref":{"type":"string"}}}})).is_ok());
    let mut action = json!({"name":"test/action/Run","kind":"action","goal":{"type":"object"},"result":{"type":"object","properties":{"done":{"type":"boolean"}},"required":["done"],"default":{"done":false}},"feedback":{"type":"object"}});
    assert!(Interface::from_json(action.clone()).is_ok());
    action["result"]["default"] = json!({"done":"wrong"});
    assert!(matches!(
        Interface::from_json(action),
        Err(Error::Validation(_))
    ));
}

#[test]
fn interface_patterns_use_bounded_non_backtracking_validation() {
    for (pattern, over_budget) in [
        (r"(?=(a+)+b)", false),
        (r"^(a+)\1$", false),
        (r"[a-z]{1000000}", true),
    ] {
        for schema in [
            json!({"type":"array","items":{"type":"string","pattern":pattern}}),
            json!({"type":"object","patternProperties":{(pattern):{"type":"string"}}}),
        ] {
            let error = Interface::from_json(json!({
                "name":"test/msg/Pattern","kind":"msg","message":schema
            }))
            .unwrap_err();
            if over_budget {
                assert!(
                    matches!(error, Error::ResourceLimit(_)),
                    "{pattern}: {error:?}"
                );
            } else {
                assert!(
                    matches!(error, Error::InvalidArgument(_)),
                    "{pattern}: {error:?}"
                );
            }
        }
    }
    let supported = Interface::from_json(json!({
        "name":"test/msg/Pattern","kind":"msg",
        "message":{"type":"array","items":{"type":"string","pattern":"^(a+)+b$"}}
    }))
    .unwrap();
    supported
        .validate("message", &json!(vec!["aaaab"; 10_000]))
        .unwrap();
    assert!(supported
        .validate("message", &json!(["aaaaaaaaaaaaaaaaaaaa!"]))
        .is_err());
}

#[test]
fn node_owns_typed_topics_graph_and_metadata() {
    let ctx = Context::new(Memory::default());
    let node = Node::new(
        ctx.clone(),
        "worker",
        "/team",
        NodeOptions {
            summary: "test worker".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let topic = message();
    let publisher = node
        .create_publisher(&topic, "values", QoS::default(), EndpointOptions::default())
        .unwrap();
    let sub = node
        .create_subscription(&topic, "values", QoS::default(), EndpointOptions::default())
        .unwrap();
    publisher.publish(&json!({"value":7})).unwrap();
    assert_eq!(sub.recv_timeout(WAIT).unwrap(), json!({"value":7}));
    assert!(matches!(
        publisher.publish(&json!({"value":"bad"})),
        Err(Error::Validation(_))
    ));
    let card = ctx.get_node_card("/team/worker", WAIT).unwrap();
    assert_eq!(card["summary"], "test worker");
    let endpoint = card["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "publisher")
        .unwrap();
    assert!(endpoint["side_effects"].is_null());
    assert_eq!(endpoint["type_hash"], topic.type_hash());
    assert_eq!(
        ctx.get_type_description(
            "/team/worker",
            topic.type_name(),
            Some(topic.type_hash()),
            WAIT
        )
        .unwrap()
        .type_hash(),
        topic.type_hash()
    );
    assert_eq!(ctx.graph(WAIT).unwrap().len(), 5);
    node.destroy_node().unwrap();
    assert!(ctx.graph(WAIT).unwrap().is_empty());
    assert_eq!(publisher.publish(&json!({"value":1})), Err(Error::Closed));
    assert_eq!(sub.recv_timeout(Duration::ZERO), Err(Error::Closed));
}

#[test]
fn service_is_executor_owned_and_validates_both_directions() {
    let bus = Memory::default();
    let ctx = Context::new(bus.clone());
    let node = Node::new(ctx.clone(), "worker", "/", NodeOptions::default()).unwrap();
    let interface = service();
    let provider = node
        .create_service(&interface, "add", EndpointOptions::default())
        .unwrap();
    let client = node
        .create_client(&interface, "add", EndpointOptions::default())
        .unwrap();
    std::thread::scope(|scope| {
        let call = scope.spawn(|| client.call(&json!({"a":2,"b":3}), WAIT));
        let request = provider.recv_timeout(WAIT).unwrap();
        assert_eq!(request.request(), &json!({"a":2,"b":3}));
        assert!(matches!(
            request.reply(&json!({"sum":"bad"})),
            Err(Error::Validation(_))
        ));
        request.reply(&json!({"sum":5})).unwrap();
        assert!(request.reply(&json!({"sum":6})).is_err());
        assert_eq!(call.join().unwrap().unwrap(), json!({"sum":5}));
    });
    assert!(matches!(
        client.call(&json!({"a":"bad","b":3}), WAIT),
        Err(Error::Validation(_))
    ));
    // Bypass the typed client to prove provider-side validation replies before
    // executor dispatch; malformed requests do not reach user callback code.
    std::thread::scope(|scope| {
        let call = scope.spawn(|| bus.call(provider.endpoint(), br#"{"a":"bad","b":3}"#, WAIT));
        assert!(matches!(
            provider.recv_timeout(WAIT),
            Err(Error::Validation(_))
        ));
        assert!(matches!(call.join().unwrap(), Err(Error::Validation(_))));
    });
    std::thread::scope(|scope| {
        let call = scope.spawn(|| client.call(&json!({"a":1,"b":2}), WAIT));
        provider
            .recv_timeout(WAIT)
            .unwrap()
            .reply_error("handler failed")
            .unwrap();
        assert_eq!(
            call.join().unwrap(),
            Err(Error::Remote("handler failed".into()))
        );
    });
}

#[test]
fn dropping_node_invalidates_surviving_entity_handles() {
    let ctx = Context::new(Memory::default());
    let node = Node::new(ctx.clone(), "worker", "/", NodeOptions::default()).unwrap();
    let publisher = node
        .create_publisher(
            &message(),
            "values",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    drop(node);
    assert_eq!(publisher.publish(&json!({"value":1})), Err(Error::Closed));
    assert!(ctx.graph(WAIT).unwrap().is_empty());
}

#[test]
fn client_close_interrupts_a_pending_wait_without_retry() {
    let ctx = Context::new(Memory::default());
    let node = Node::new(ctx, "worker", "/", NodeOptions::default()).unwrap();
    let interface = service();
    let provider = node
        .create_service(&interface, "add", EndpointOptions::default())
        .unwrap();
    let client = node
        .create_client(&interface, "add", EndpointOptions::default())
        .unwrap();
    std::thread::scope(|scope| {
        let call = scope.spawn(|| client.call(&json!({"a":1,"b":2}), Duration::from_secs(30)));
        let _unanswered = provider.recv_timeout(WAIT).unwrap();
        let started = Instant::now();
        client.close().unwrap();
        assert_eq!(call.join().unwrap(), Err(Error::Closed));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            provider.recv_timeout(Duration::ZERO),
            Err(Error::Timeout)
        ));
    });
}

fn action_type() -> Interface {
    Interface::from_json(json!({
        "name":"test/action/Count","kind":"action",
        "goal":{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false},
        "result":{"type":"object","properties":{"total":{"type":"integer"}},"required":["total"],"additionalProperties":false,"default":{"total":0}},
        "feedback":{"type":"object","properties":{"current":{"type":"integer"}},"required":["current"],"additionalProperties":false}
    })).unwrap()
}

fn complete_lifecycle(
    lifecycle: &Lifecycle,
    id: LifecycleTransitionId,
) -> LifecycleTransitionReport {
    lifecycle
        .begin_transition(id)
        .unwrap()
        .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
        .unwrap()
        .report
        .unwrap()
}
fn activate_lifecycle(lifecycle: &Lifecycle) {
    complete_lifecycle(lifecycle, LifecycleTransitionId::Configure);
    complete_lifecycle(lifecycle, LifecycleTransitionId::Activate);
}

#[test]
fn managed_topics_gate_callbacks_and_do_not_replay_inactive_backlog_across_cycles() {
    let context = Context::new(Memory::default());
    let node =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let plain = Node::new(context, "ordinary", "/", NodeOptions::default()).unwrap();
    assert!(plain.lifecycle().is_none());
    let lifecycle = node.lifecycle().unwrap();
    let source = plain
        .create_publisher(
            &message(),
            "/input",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let subscription = node
        .create_subscription(
            &message(),
            "/input",
            QoS {
                depth: 2,
                ..QoS::default()
            },
            EndpointOptions::default(),
        )
        .unwrap();
    let publisher = node
        .create_publisher(
            &message(),
            "/output",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let receiver = plain
        .create_subscription(
            &message(),
            "/output",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert!(matches!(
        publisher.publish(&json!({"value":0})),
        Err(Error::Inactive(_))
    ));
    assert!(matches!(
        subscription.take_message(Duration::ZERO),
        Err(Error::Inactive(_))
    ));
    assert!(matches!(
        subscription.recv_timeout(Duration::ZERO),
        Err(Error::Unsupported(_))
    ));
    for value in [-2, -1] {
        source.publish(&json!({"value":value})).unwrap();
    }
    activate_lifecycle(&lifecycle);
    assert!(matches!(
        subscription.take_message(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(subscription.lifecycle_skipped_total(), 2);
    publisher.publish(&json!({"value":7})).unwrap();
    assert_eq!(receiver.recv_timeout(WAIT).unwrap(), json!({"value":7}));
    source.publish(&json!({"value":100})).unwrap();
    let callback = subscription.take_message(WAIT).unwrap();
    let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
    assert!(report.drain_timed_out);
    assert_eq!(report.final_state.id, LifecycleState::Active as u8);
    assert_eq!(report.remaining.len(), 1);
    assert_eq!(report.remaining[0].kind, LifecycleWorkKind::Subscription);
    assert!(callback.is_valid());
    assert_eq!(callback.message(), &json!({"value":100}));
    callback.finish();
    assert!(complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate).successful);
    for value in [200, 201] {
        source.publish(&json!({"value":value})).unwrap();
    }
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Activate);
    assert!(matches!(
        subscription.take_message(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(subscription.lifecycle_skipped_total(), 4);

    // Transport overflow remains distinct from lifecycle discards. There is
    // exactly one bounded transport queue and no hidden SDK/core staging queue.
    for value in 0..6 {
        source.publish(&json!({"value":value})).unwrap();
    }
    let before = subscription.queue_stats().unwrap();
    assert_eq!(
        (before.depth, before.queued, before.dropped_total),
        (2, 2, 4)
    );
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Activate);
    let after = subscription.queue_stats().unwrap();
    assert_eq!(
        (after.queued, after.received_total, after.dropped_total),
        (0, before.received_total, 4)
    );
    assert_eq!(subscription.lifecycle_skipped_total(), 6);
    source.publish(&json!({"value":999})).unwrap();
    let fresh = subscription.take_message(WAIT).unwrap();
    assert_eq!(fresh.message(), &json!({"value":999}));
    fresh.finish();
}

#[test]
fn managed_subscription_metadata_remains_on_the_original_drain_lease() {
    let bus = Memory::default();
    let context = Context::new(bus.clone());
    let node = Node::new_lifecycle(context, "managed", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let subscription = node
        .create_subscription(
            &message(),
            "values",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    activate_lifecycle(&lifecycle);
    let endpoint_key = key(subscription.endpoint());
    let queue = bus
        .0
        .subscriptions
        .lock()
        .unwrap()
        .iter()
        .find_map(|(key, queue)| (key == &endpoint_key).then(|| queue.upgrade()).flatten())
        .unwrap();
    let payload = b" { \"value\" : 42 } \n".to_vec();
    let payload_size = payload.len();
    let received = ReceiveMetadata {
        steady: Instant::now() - Duration::from_secs(1),
        system: std::time::UNIX_EPOCH + Duration::from_secs(42),
        scope: ReceiveScope::AdapterDelivery,
    };
    let source = SourceMetadata {
        timestamp: Some(SourceTimestamp {
            kind: "fixture_clock".into(),
            value: "9007199254740993".into(),
            clock_id: "fixture_clock_id".into(),
            comparable: false,
            origin: "fixture".into(),
        }),
        publisher_id: Some("fixture_publisher".into()),
        sequence: Some(7),
    };
    queue
        .push(Sample {
            payload,
            payload_size_bytes: payload_size,
            received: Some(received),
            source: Some(source.clone()),
        })
        .unwrap();
    // The convenience API must not consume a managed sample without a lease.
    assert!(matches!(
        subscription.recv_sample_timeout(Duration::ZERO),
        Err(Error::Unsupported(_)),
    ));
    let call = subscription.take_message(WAIT).unwrap();
    assert_eq!(call.message(), &json!({"value":42}));
    assert_eq!(call.sample().payload_size_bytes, payload_size);
    let actual = call.sample().received.as_ref().unwrap();
    assert_eq!(actual.steady, received.steady);
    assert_eq!(actual.system, received.system);
    assert_eq!(actual.scope, received.scope);
    assert_eq!(call.sample().source.as_ref(), Some(&source));
    let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
    assert!(report.drain_timed_out);
    assert_eq!(report.remaining.len(), 1);
    assert!(call.is_valid());
    assert_eq!(
        call.sample().received.as_ref().unwrap().steady,
        received.steady
    );
    call.finish();
    assert!(lifecycle.snapshot().unwrap().in_flight.is_empty());
    assert!(complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate).successful);
}

#[test]
fn managed_timer_keeps_a_real_drain_lease_and_resets_paused_periods() {
    let context = Context::new(Memory::default());
    let node = Node::new_lifecycle(context, "timer", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let clock = Clock::new(ClockType::Ros);
    clock.set_use_sim_time(true).unwrap();
    clock.update_ros_time(0).unwrap();
    let timer = node
        .create_timer(Duration::from_nanos(10), Some(clock.clone()), true)
        .unwrap();
    let rate = node.create_rate(1.0, Some(clock.clone())).unwrap();
    clock.update_ros_time(100).unwrap();
    assert!(!timer.is_ready().unwrap());
    assert!(timer.take_ready().unwrap().is_none());
    assert!(matches!(rate.sleep(), Err(Error::Inactive(_))));
    activate_lifecycle(&lifecycle);
    clock.update_ros_time(109).unwrap();
    assert!(!timer.is_ready().unwrap());
    clock.update_ros_time(110).unwrap();
    let call = timer.take_ready().unwrap().unwrap();
    let mut transition = lifecycle
        .begin_transition(LifecycleTransitionId::Deactivate)
        .unwrap();
    assert_eq!(
        lifecycle.snapshot().unwrap().in_flight[0].kind,
        LifecycleWorkKind::Timer
    );
    let (sent, received) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let report = transition
            .complete(LifecycleCallbackReturn::Success, Some(WAIT))
            .unwrap()
            .report
            .unwrap();
        sent.send(report).unwrap();
    });
    clock.update_ros_time(1000).unwrap();
    assert!(call.is_valid());
    assert!(!timer.is_ready().unwrap());
    assert!(received.try_recv().is_err());
    drop(call);
    let report = received.recv_timeout(WAIT).unwrap();
    worker.join().unwrap();
    assert!(report.successful);
    assert!(report.remaining.is_empty());
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Activate);
    clock.update_ros_time(1009).unwrap();
    assert!(timer.take_ready().unwrap().is_none());
    clock.update_ros_time(1010).unwrap();
    assert!(timer.take_ready().unwrap().is_some());
}

#[test]
fn managed_service_rejects_without_executor_and_drains_only_admitted_requests() {
    let context = Context::new(Memory::default());
    let node =
        Node::new_lifecycle(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let provider = node
        .create_service(&service(), "/add", EndpointOptions::default())
        .unwrap();
    let client = caller
        .create_client(&service(), "/add", EndpointOptions::default())
        .unwrap();
    assert!(matches!(
        client.call(&json!({"a":1,"b":2}), WAIT),
        Err(Error::Inactive(_))
    ));
    activate_lifecycle(&lifecycle);
    std::thread::scope(|scope| {
        let called = scope.spawn(|| client.call(&json!({"a":2,"b":3}), WAIT));
        let request = provider.recv_timeout(WAIT).unwrap();
        let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
        assert!(!report.successful);
        assert!(report.drain_timed_out);
        assert_eq!(report.remaining[0].kind, LifecycleWorkKind::Service);
        assert!(lifecycle.snapshot().unwrap().gate_open);
        request.reply(&json!({"sum":5})).unwrap();
        assert_eq!(called.join().unwrap().unwrap(), json!({"sum":5}));
    });
    assert!(complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate).successful);
    assert!(matches!(
        client.call(&json!({"a":1,"b":2}), WAIT),
        Err(Error::Inactive(_))
    ));
    assert!(lifecycle.snapshot().unwrap().in_flight.is_empty());
}

#[test]
fn parameter_state_restrictions_are_described_and_enforced_by_remote_services() {
    let context = Context::new(ParameterMemory::default());
    let node =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let store = node.parameters();
    store
        .declare(
            "config",
            ParameterValue::Integer(1),
            ParameterDescriptor {
                allowed_states: vec![LifecycleState::Inactive],
                ..Default::default()
            },
            false,
        )
        .unwrap();
    let server = node.enable_parameter_services().unwrap();
    let client = ParameterClient::new(&caller, "/managed").unwrap();
    let events = caller
        .create_subscription(
            &parameter_event_interface(),
            "/parameter_events",
            resolve_qos(&json!(null), "parameter_events", false).unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let version = store.version().unwrap();
    std::thread::scope(|scope| {
        let describe = scope.spawn(|| client.describe(&["config".into()], WAIT));
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| Ok(()))
            .unwrap();
        assert_eq!(
            describe.join().unwrap().unwrap().value.unwrap()[0]
                .descriptor
                .allowed_states,
            [LifecycleState::Inactive],
        );
        let denied = scope.spawn(|| {
            client.set_atomically(
                &[Parameter::new("config", ParameterValue::Integer(2))],
                WAIT,
            )
        });
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| panic!("denied before validator"))
            .unwrap();
        let response = denied.join().unwrap().unwrap();
        assert!(!response.successful);
        assert_eq!(response.code.as_deref(), Some("state_not_allowed"));
        assert_eq!(store.version().unwrap(), version);
        assert_eq!(events.recv_timeout(Duration::ZERO), Err(Error::Timeout));
        complete_lifecycle(&lifecycle, LifecycleTransitionId::Configure);
        let stale = scope.spawn(|| {
            client.set_atomically(
                &[Parameter::new("config", ParameterValue::Integer(2))],
                WAIT,
            )
        });
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| {
                complete_lifecycle(&lifecycle, LifecycleTransitionId::Activate);
                complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
                Ok(())
            })
            .unwrap();
        let response = stale.join().unwrap().unwrap();
        assert!(!response.successful);
        assert_eq!(response.code.as_deref(), Some("conflict"));
        assert_eq!(store.version().unwrap(), version);
        assert_eq!(events.recv_timeout(Duration::ZERO), Err(Error::Timeout));
        let accepted = scope.spawn(|| {
            client.set_atomically(
                &[Parameter::new("config", ParameterValue::Integer(2))],
                WAIT,
            )
        });
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| Ok(()))
            .unwrap();
        assert!(accepted.join().unwrap().unwrap().successful);
        assert_eq!(events.recv_timeout(WAIT).unwrap()["version"], version + 1);
        assert_eq!(events.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    });
}

#[test]
fn lifecycle_decode_error_replies_without_starting_a_transition() {
    let context = Context::new(ParameterMemory::default());
    let node =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let server = node.enable_lifecycle_services().unwrap();
    let client = LifecycleClient::new(&caller, "/managed").unwrap();
    for payload in [
        json!({"transition":{"id":3.0,"label":"activate"}}),
        json!({"transition":{"id":1,"label":"configure"},"drain_timeout_ms":5.0}),
    ] {
        LifecycleServiceKind::ChangeState
            .interface()
            .validate("request", &payload)
            .unwrap();
        std::thread::scope(|scope| {
            let change =
                scope.spawn(|| client.call(LifecycleServiceKind::ChangeState, &payload, WAIT));
            assert!(matches!(
                server.take_change_request(WAIT),
                Err(Error::Validation(_))
            ));
            let response = change
                .join()
                .unwrap()
                .expect("a decoded service request must receive a structured error reply");
            assert_eq!(response["success"], false);
            assert_eq!(response["error"]["code"], "VALIDATION");
            assert!(response["report"].is_null());
        });
        assert_eq!(
            lifecycle.current_state().unwrap(),
            LifecycleState::Unconfigured
        );
    }
    std::thread::scope(|scope| {
        let change = scope.spawn(|| {
            client.change_state(
                LifecycleTransitionSelector {
                    id: 1,
                    label: "configure".into(),
                },
                Some(Duration::ZERO),
                WAIT,
            )
        });
        let request = server.take_change_request(WAIT).unwrap();
        let report = request
            .begin_transition()
            .unwrap()
            .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
            .unwrap()
            .report
            .unwrap();
        request.reply(&report).unwrap();
        assert!(change.join().unwrap().unwrap().success);
    });
}

#[test]
fn stage3_r2_lifecycle_decode_keeps_validation_when_the_error_reply_fails() {
    let bus = Memory::default();
    let context = Context::new(ParameterMemory(bus.clone()));
    let node = Node::new_lifecycle(context, "managed", "/", NodeOptions::default()).unwrap();
    let server = node.enable_lifecycle_services().unwrap();
    let interface = LifecycleServiceKind::ChangeState.interface();
    let endpoint = Endpoint {
        name: "/managed/change_state".into(),
        type_name: interface.type_name().into(),
        type_hash: interface.type_hash().into(),
    };
    let queue = bus
        .0
        .services
        .lock()
        .unwrap()
        .get(&key(&endpoint))
        .cloned()
        .unwrap();
    for failure in [
        Error::Transport("injected reply transport failure".into()),
        Error::QueueOverflow("injected reply queue overflow".into()),
        Error::Closed,
    ] {
        let (sent, _received) = mpsc::channel();
        queue
            .push(Request {
                bytes: br#"{"transition":{"id":3.0,"label":"activate"}}"#.to_vec(),
                request_id: Telemetry::new_request_id(),
                response: Mutex::new(Some(sent)),
                reply_block: None,
                reply_failure: Some(failure),
            })
            .unwrap();
        assert!(matches!(
            server.take_change_request(WAIT),
            Err(Error::Validation(message)) if message.contains("invalid lifecycle request")
        ));
        assert_eq!(
            node.lifecycle().unwrap().current_state().unwrap(),
            LifecycleState::Unconfigured
        );
    }
    // The same server still accepts the next valid request.
    let (sent, received) = mpsc::channel();
    queue
        .push(Request {
            bytes: br#"{"transition":{"id":1,"label":"configure"}}"#.to_vec(),
            request_id: Telemetry::new_request_id(),
            response: Mutex::new(Some(sent)),
            reply_block: None,
            reply_failure: None,
        })
        .unwrap();
    let request = server.take_change_request(WAIT).unwrap();
    let report = request
        .begin_transition()
        .unwrap()
        .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
        .unwrap()
        .report
        .unwrap();
    request.reply(&report).unwrap();
    let reply: serde_json::Value =
        serde_json::from_slice(&received.recv_timeout(WAIT).unwrap().unwrap()).unwrap();
    assert_eq!(reply["success"], true);
    assert_eq!(
        node.lifecycle().unwrap().current_state().unwrap(),
        LifecycleState::Inactive
    );
}

#[test]
fn rejected_clock_enable_caches_one_node_owned_source_and_destroy_releases_it() {
    let bus = Memory::default();
    let context = Context::new(ParameterMemory(bus.clone()));
    let node =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let store = node.parameters();
    store
        .declare(
            "limited",
            ParameterValue::Integer(1),
            ParameterDescriptor {
                allowed_states: vec![LifecycleState::Inactive],
                ..Default::default()
            },
            false,
        )
        .unwrap();
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Configure);
    let callback_lifecycle = lifecycle.clone();
    *bus.0.clock_subscription_hook.lock().unwrap() = Some(Arc::new(move || {
        complete_lifecycle(&callback_lifecycle, LifecycleTransitionId::Activate);
        complete_lifecycle(&callback_lifecycle, LifecycleTransitionId::Deactivate);
    }));
    let before = json!(store.snapshot().unwrap());
    let result = store.set_atomically(vec![
        Parameter::new("use_sim_time", ParameterValue::Bool(true)),
        Parameter::new("limited", ParameterValue::Integer(2)),
    ]);
    assert!(matches!(result, Err(ParameterError::Conflict(_))));
    assert_eq!(json!(store.snapshot().unwrap()), before);
    assert!(!node.clock().now().unwrap().use_sim_time);
    let clock_subscriptions = || {
        context
            .graph(Duration::ZERO)
            .unwrap()
            .into_iter()
            .filter(|entity| {
                entity.kind == EntityKind::Subscription
                    && entity.endpoint.as_ref().is_some_and(|e| e.name == "/clock")
            })
            .count()
    };
    assert_eq!(clock_subscriptions(), 1);
    *bus.0.clock_subscription_hook.lock().unwrap() = None;
    for enabled in [true, false, true, false] {
        store
            .set_atomically(vec![Parameter::new(
                "use_sim_time",
                ParameterValue::Bool(enabled),
            )])
            .unwrap();
        assert_eq!(clock_subscriptions(), 1);
    }
    let queue = bus
        .0
        .subscriptions
        .lock()
        .unwrap()
        .iter()
        .find(|(key, _)| key.starts_with("/clock:"))
        .unwrap()
        .1
        .clone();
    node.destroy_node().unwrap();
    assert_eq!(clock_subscriptions(), 0);
    let deadline = Instant::now() + WAIT;
    while queue.upgrade().is_some() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(queue.upgrade().is_none());
}

#[test]
fn atomic_load_service_keeps_no_ops_in_validation_without_publishing_events() {
    let context = Context::new(ParameterMemory::default());
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let observer = Node::new(context, "observer", "/", NodeOptions::default()).unwrap();
    let store = provider.parameters();
    store
        .declare(
            "pending",
            ParameterValue::NotSet,
            ParameterDescriptor {
                parameter_type: Some(ParameterType::Integer),
                ..Default::default()
            },
            false,
        )
        .unwrap();
    store
        .declare(
            "constant",
            ParameterValue::Integer(3),
            ParameterDescriptor {
                read_only: true,
                ..Default::default()
            },
            false,
        )
        .unwrap();
    store
        .declare(
            "changed",
            ParameterValue::Integer(1),
            ParameterDescriptor::default(),
            false,
        )
        .unwrap();
    let server = provider.enable_parameter_services().unwrap();
    let client = ParameterClient::new(&observer, "/provider").unwrap();
    let events = observer
        .create_subscription(
            &parameter_event_interface(),
            "/parameter_events",
            resolve_qos(&serde_json::Value::Null, "parameter_events", false).unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let mut candidate: Vec<_> = store
        .snapshot()
        .unwrap()
        .parameters
        .into_iter()
        .map(|p| Parameter::new(p.name, p.value))
        .collect();
    let version = store.version().unwrap();
    std::thread::scope(|scope| {
        let loaded = scope.spawn(|| client.load_atomically(&candidate, WAIT));
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|values| {
                assert_eq!(values, candidate);
                Ok(())
            })
            .unwrap();
        let response = loaded.join().unwrap().unwrap();
        assert!(response.successful);
        assert_eq!(response.version, Some(version));
        assert!(!response.event_published);
        assert_eq!(events.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    });
    candidate
        .iter_mut()
        .find(|p| p.name == "changed")
        .unwrap()
        .value = ParameterValue::Integer(2);
    std::thread::scope(|scope| {
        let loaded = scope.spawn(|| client.load_atomically(&candidate, WAIT));
        server
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|values| {
                assert_eq!(values, candidate);
                Ok(())
            })
            .unwrap();
        let response = loaded.join().unwrap().unwrap();
        assert!(response.successful);
        assert_eq!(response.version, Some(version + 1));
        assert!(response.event_published);
        let event = events.recv_timeout(WAIT).unwrap();
        assert_eq!(
            event["changed_parameters"],
            json!([Parameter::new("changed", ParameterValue::Integer(2))])
        );
        assert_eq!(event["new_parameters"], json!([]));
        assert_eq!(event["deleted_parameters"], json!([]));
        assert_eq!(events.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    });
}

#[test]
fn lifecycle_control_queries_and_parameters_survive_callbacks_and_finalized_drain_failure() {
    let context = Context::new(ParameterMemory::default());
    let node =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let server = node.enable_lifecycle_services().unwrap();
    let parameters = node.enable_parameter_services().unwrap();
    let client = LifecycleClient::new(&caller, "/managed").unwrap();
    let parameter_client = ParameterClient::new(&caller, "/managed").unwrap();
    assert_eq!(
        client.state(WAIT).unwrap().current_state.id,
        LifecycleState::Unconfigured as u8
    );
    assert_eq!(
        client
            .call(LifecycleServiceKind::GetAvailableStates, &json!({}), WAIT)
            .unwrap()["available_states"]
            .as_array()
            .unwrap()
            .len(),
        11
    );
    assert_eq!(
        client
            .call(LifecycleServiceKind::GetTransitionGraph, &json!({}), WAIT)
            .unwrap()["available_transitions"]
            .as_array()
            .unwrap()
            .len(),
        25
    );
    std::thread::scope(|scope| {
        let change = scope.spawn(|| {
            client.change_state(
                LifecycleTransitionSelector {
                    id: 0,
                    label: "configure".into(),
                },
                None,
                WAIT,
            )
        });
        let request = server.take_change_request(WAIT).unwrap();
        assert_eq!(
            lifecycle.current_state().unwrap(),
            LifecycleState::Unconfigured
        );
        let mut transition = request.begin_transition().unwrap();
        // Holding the token simulates a callback that has not returned. Query
        // workers and a parameter Executor slot must remain independently usable.
        assert_eq!(
            client.state(WAIT).unwrap().current_state.id,
            LifecycleState::Configuring as u8
        );
        let parameter_get = scope.spawn(|| parameter_client.get(&["use_sim_time".into()], WAIT));
        parameters
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| Ok(()))
            .unwrap();
        assert_eq!(
            parameter_get.join().unwrap().unwrap().value.unwrap(),
            [ParameterValue::Bool(false)]
        );
        assert!(lifecycle
            .begin_transition(LifecycleTransitionId::Configure)
            .is_err());
        assert_eq!(
            transition.previous_state().id,
            LifecycleState::Unconfigured as u8
        );
        let progress = transition
            .complete(LifecycleCallbackReturn::Error, None)
            .unwrap();
        assert_eq!(
            progress.callback_state.unwrap().id,
            LifecycleState::ErrorProcessing as u8
        );
        assert_eq!(
            client.state(WAIT).unwrap().current_state.id,
            LifecycleState::ErrorProcessing as u8
        );
        let report = transition
            .complete(LifecycleCallbackReturn::Success, None)
            .unwrap()
            .report
            .unwrap();
        assert!(!report.successful);
        assert_eq!(
            report.error_callback_return,
            Some(LifecycleCallbackReturn::Success)
        );
        request.reply(&report).unwrap();
        let response = change.join().unwrap().unwrap();
        assert!(!response.success);
        assert!(response.error.is_none());
        assert_eq!(
            response.report.unwrap().callback_return,
            LifecycleCallbackReturn::Error
        );
    });
    activate_lifecycle(&lifecycle);
    let timer = node.create_timer(Duration::ZERO, None, true).unwrap();
    let call = timer.take_ready().unwrap().unwrap();
    let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::ActiveShutdown);
    assert!(report.drain_timed_out);
    assert_eq!(report.final_state.id, LifecycleState::Finalized as u8);
    assert!(call.is_valid());
    let state = client.state(WAIT).unwrap();
    assert_eq!(state.current_state.id, LifecycleState::Finalized as u8);
    assert!(!state.snapshot.gate_open);
    assert_eq!(state.last_report.unwrap().remaining.len(), 1);
    std::thread::scope(|scope| {
        let parameter_get = scope.spawn(|| parameter_client.get(&["use_sim_time".into()], WAIT));
        parameters
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| Ok(()))
            .unwrap();
        assert!(parameter_get.join().unwrap().unwrap().error.is_none());
    });
    drop(call);
    let state = client.state(WAIT).unwrap();
    assert!(state.snapshot.in_flight.is_empty());
    assert_eq!(state.last_report.unwrap().remaining.len(), 1);
}

fn managed_action_pair() -> (Node, Node, ActionServer, ActionClient, Lifecycle) {
    let context = Context::new(Memory::default());
    let node =
        Node::new_lifecycle(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context, "caller", "/", NodeOptions::default()).unwrap();
    let server = ActionServer::new(
        &node,
        &action_type(),
        "/count",
        ActionServerOptions {
            result_timeout: WAIT,
            ..Default::default()
        },
        EndpointOptions::default(),
    )
    .unwrap();
    let client = ActionClient::new(
        &caller,
        &action_type(),
        "/count",
        EndpointOptions::default(),
    )
    .unwrap();
    let lifecycle = node.lifecycle().unwrap();
    (node, caller, server, client, lifecycle)
}

#[test]
fn invalid_inactive_goal_reply_does_not_hold_the_lifecycle_query_lock() {
    let bus = Memory::default();
    let context = Context::new(bus.clone());
    let node = Node::new_lifecycle(context, "provider", "/", NodeOptions::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    let _server = ActionServer::new(
        &node,
        &action_type(),
        "count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let interfaces = ActionInterfaces::new(&action_type()).unwrap();
    let endpoint = Endpoint {
        name: "/count/_action/send_goal".into(),
        type_name: interfaces.send_goal.type_name().into(),
        type_hash: interfaces.send_goal.type_hash().into(),
    };
    let queue = bus
        .0
        .services
        .lock()
        .unwrap()
        .get(&key(&endpoint))
        .cloned()
        .unwrap();
    let (replied, reply) = mpsc::channel();
    let (started, replying) = mpsc::channel();
    let (release, released) = mpsc::channel();
    queue
        .push(Request {
            bytes: br#"{"goal_id":"00000000000000000000000000000001","goal":{"count":"invalid"}}"#
                .to_vec(),
            request_id: Telemetry::new_request_id(),
            response: Mutex::new(Some(replied)),
            reply_block: Some(ReplyBlock {
                started,
                release: Mutex::new(released),
            }),
            reply_failure: None,
        })
        .unwrap();
    replying.recv_timeout(WAIT).unwrap();
    let (queried, query) = mpsc::channel();
    let query_thread = std::thread::spawn(move || {
        queried.send(lifecycle.snapshot()).unwrap();
    });
    let snapshot = query.recv_timeout(WAIT);
    // Always release the artificial slow transport before checking the result
    // so a regression fails without leaving a blocked worker in the test.
    release.send(()).unwrap();
    assert!(matches!(
        reply.recv_timeout(WAIT).unwrap(),
        Err(Error::Validation(_))
    ));
    query_thread.join().unwrap();
    assert_eq!(
        snapshot
            .expect("lifecycle query blocked behind an invalid-goal reply")
            .unwrap()
            .current_state
            .id,
        LifecycleState::Unconfigured as u8,
    );
}

#[test]
fn managed_action_rejection_telemetry_retains_wire_request_and_inactive_outcome() {
    let context = Context::new(Memory::default());
    context.enable_log_capture(64).unwrap();
    let provider =
        Node::new_lifecycle(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let caller = Node::new(context.clone(), "caller", "/", NodeOptions::default()).unwrap();
    let _server = ActionServer::new(
        &provider,
        &action_type(),
        "count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client =
        ActionClient::new(&caller, &action_type(), "count", EndpointOptions::default()).unwrap();
    let goal = client.send_goal(&json!({"count":3}), None, WAIT).unwrap();
    assert!(!goal.accepted());
    let records = context.drain_telemetry();
    let rejected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.event.component == TelemetryComponent::Action
                && record.event.event == "goal_rejected"
        })
        .collect();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].event.outcome, TelemetryOutcome::Inactive);
    assert_eq!(rejected[0].event.goal_id.as_deref(), Some(goal.goal_id()));
    assert!(rejected[0].event.request_id.is_some());
    assert_eq!(context.telemetry_snapshot().rejected_events_total, 0);
}

#[test]
fn managed_action_rejects_new_goals_and_drains_through_feedback_cancel_and_result() {
    let (_node, _caller, server, client, lifecycle) = managed_action_pair();
    assert!(!client
        .send_goal(&json!({"count":3}), None, WAIT)
        .unwrap()
        .accepted());
    activate_lifecycle(&lifecycle);
    let (goal, client_goal) = accepted_goal(&server, &client, None);
    goal.execute().unwrap();
    let mut transition = lifecycle
        .begin_transition(LifecycleTransitionId::Deactivate)
        .unwrap();
    let pending = lifecycle.snapshot().unwrap().in_flight;
    assert_eq!(
        (pending[0].kind, pending[0].entity_id.as_str()),
        (LifecycleWorkKind::Action, goal.goal_id())
    );
    assert!(!client
        .send_goal(&json!({"count":3}), None, WAIT)
        .unwrap()
        .accepted());
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Inactive(_))
    ));
    let (done, finished) = mpsc::channel();
    let drain = std::thread::spawn(move || {
        done.send(
            transition
                .complete(LifecycleCallbackReturn::Success, Some(WAIT))
                .unwrap()
                .report
                .unwrap(),
        )
        .unwrap();
    });
    goal.publish_feedback(&json!({"current":1})).unwrap();
    assert_eq!(
        client.recv_feedback(WAIT).unwrap()["feedback"]["current"],
        1
    );
    std::thread::scope(|scope| {
        let result = scope.spawn(|| client_goal.get_result(WAIT));
        let cancel = scope.spawn(|| client_goal.cancel_goal(WAIT));
        server
            .take_cancel_request(WAIT)
            .unwrap()
            .respond(&[goal.goal_id().into()])
            .unwrap();
        assert_eq!(
            cancel.join().unwrap().unwrap()["goals_canceling"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(goal.is_cancel_requested());
        goal.canceled().unwrap();
        assert_eq!(goal.status(), GoalStatus::Canceled as u8);
        assert!(finished.try_recv().is_err());
        // Terminal Action status alone does not release its execution lease.
        // Result expiry cannot steal it while the callback has not returned.
        assert_eq!(lifecycle.snapshot().unwrap().in_flight.len(), 1);
        goal.finish(Some(&json!({"total":1}))).unwrap();
        assert_eq!(
            result.join().unwrap().unwrap(),
            json!({"status":5,"result":{"total":1}})
        );
    });
    let report = finished.recv_timeout(WAIT).unwrap();
    drain.join().unwrap();
    assert!(report.successful);
    assert_eq!(report.final_state.id, LifecycleState::Inactive as u8);
    assert!(report.remaining.is_empty());
    assert!(matches!(
        goal.publish_feedback(&json!({"current":2})),
        Err(Error::Inactive(_) | Error::Closed)
    ));
}

#[test]
fn managed_action_admission_callback_cannot_accept_after_gate_closes() {
    let (_node, _caller, server, client, lifecycle) = managed_action_pair();
    activate_lifecycle(&lifecycle);
    std::thread::scope(|scope| {
        let send = scope.spawn(|| client.send_goal(&json!({"count":3}), None, WAIT));
        let request = server.take_goal_request(WAIT).unwrap();
        let mut transition = lifecycle
            .begin_transition(LifecycleTransitionId::Deactivate)
            .unwrap();
        assert_eq!(lifecycle.snapshot().unwrap().in_flight.len(), 1);
        assert!(request.decide(true).unwrap().is_none());
        assert!(!send.join().unwrap().unwrap().accepted());
        let report = transition
            .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
            .unwrap()
            .report
            .unwrap();
        assert!(report.successful);
        assert!(report.remaining.is_empty());
    });
}

#[test]
fn managed_action_shutdown_timeout_does_not_synthesize_cancellation_or_close_controls() {
    let (_node, _caller, server, client, lifecycle) = managed_action_pair();
    activate_lifecycle(&lifecycle);
    let (goal, client_goal) = accepted_goal(&server, &client, None);
    goal.execute().unwrap();
    let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::ActiveShutdown);
    assert!(report.drain_timed_out);
    assert_eq!(report.final_state.id, LifecycleState::Finalized as u8);
    assert_eq!(goal.status(), GoalStatus::Executing as u8);
    assert!(!goal.is_cancel_requested());
    assert!(!client
        .send_goal(&json!({"count":3}), None, WAIT)
        .unwrap()
        .accepted());
    goal.publish_feedback(&json!({"current":2})).unwrap();
    assert_eq!(
        client.recv_feedback(WAIT).unwrap()["feedback"]["current"],
        2
    );
    std::thread::scope(|scope| {
        let result = scope.spawn(|| client_goal.get_result(WAIT));
        goal.succeed().unwrap();
        assert_eq!(lifecycle.snapshot().unwrap().in_flight.len(), 1);
        goal.finish(Some(&json!({"total":3}))).unwrap();
        assert_eq!(
            result.join().unwrap().unwrap(),
            json!({"status":4,"result":{"total":3}})
        );
    });
    assert!(lifecycle.snapshot().unwrap().in_flight.is_empty());
}

#[test]
fn managed_action_client_gates_feedback_callbacks_but_preserves_remote_goal_controls() {
    let context = Context::new(Memory::default());
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let caller = Node::new_lifecycle(context, "caller", "/", NodeOptions::default()).unwrap();
    let lifecycle = caller.lifecycle().unwrap();
    let server = ActionServer::new(
        &provider,
        &action_type(),
        "/count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client = ActionClient::new(
        &caller,
        &action_type(),
        "/count",
        EndpointOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        client.send_goal(&json!({"count":3}), None, WAIT),
        Err(Error::Inactive(_))
    ));
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Timeout)
    ));
    activate_lifecycle(&lifecycle);
    let (goal, client_goal) = accepted_goal(&server, &client, None);
    goal.execute().unwrap();
    goal.publish_feedback(&json!({"current":1})).unwrap();
    assert!(matches!(
        client.recv_feedback(Duration::ZERO),
        Err(Error::Unsupported(_))
    ));
    let callback = client.take_feedback(WAIT).unwrap();
    assert_eq!(callback.message()["feedback"]["current"], 1);
    let report = complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
    assert!(report.drain_timed_out);
    assert_eq!(report.remaining[0].kind, LifecycleWorkKind::Subscription);
    callback.finish();
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Deactivate);
    goal.publish_feedback(&json!({"current":2})).unwrap();
    assert!(matches!(
        client.take_feedback(Duration::ZERO),
        Err(Error::Inactive(_))
    ));
    assert!(client.recv_status(WAIT).is_ok());
    std::thread::scope(|scope| {
        let cancel = scope.spawn(|| client_goal.cancel_goal(WAIT));
        server
            .take_cancel_request(WAIT)
            .unwrap()
            .respond(&[])
            .unwrap();
        assert!(cancel.join().unwrap().unwrap()["goals_canceling"]
            .as_array()
            .unwrap()
            .is_empty());
        let result = scope.spawn(|| client_goal.get_result(WAIT));
        goal.succeed().unwrap();
        goal.finish(Some(&json!({"total":3}))).unwrap();
        assert_eq!(
            result.join().unwrap().unwrap()["status"],
            GoalStatus::Succeeded as u8
        );
    });
    complete_lifecycle(&lifecycle, LifecycleTransitionId::Activate);
    assert!(matches!(
        client.take_feedback(Duration::ZERO),
        Err(Error::Timeout)
    ));
}

#[test]
fn node_config_reaches_graph_card_and_whole_action_channels() {
    let context = Context::new(Memory::default());
    let node = Node::new_with_config(
        context.clone(),
        "worker",
        "/old",
        NodeOptions::default(),
        NodeConfigRequest {
            cli: json!({"name":"deployed","namespace":"/new","remaps":[
                {"kind":"topic","from":"~/input","to":"/shared/input"},
                {"kind":"action","from":"job","to":"/shared/job"},
                {"kind":"action","from":"/shared/job","to":"/must_not_chain"}
            ]}),
            ..Default::default()
        },
    )
    .unwrap();
    let publisher = node
        .create_publisher(
            &message(),
            "~/input",
            resolve_qos(&json!({"preset":"sensor_data","depth":7}), "default", false).unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(publisher.endpoint().name, "/shared/input");
    let server = ActionServer::new(
        &node,
        &action_type(),
        "job",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let graph = context.graph(Duration::ZERO).unwrap();
    let action_entities: Vec<_> = graph
        .iter()
        .filter(|e| {
            e.endpoint
                .as_ref()
                .is_some_and(|e| e.name.starts_with("/shared/job"))
        })
        .collect();
    assert_eq!(action_entities.len(), 6);
    assert!(graph
        .iter()
        .all(|e| e.node_name == "deployed" && e.namespace == "/new"));
    for suffix in [
        "send_goal",
        "get_result",
        "cancel_goal",
        "feedback",
        "status",
    ] {
        assert!(action_entities
            .iter()
            .any(|e| e.endpoint.as_ref().unwrap().name == format!("/shared/job/_action/{suffix}")));
    }
    let card = node.node_card();
    let topic = card["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "/shared/input")
        .unwrap();
    assert_eq!(topic["original_name"], "~/input");
    assert_eq!(topic["qos"]["preset"], "sensor_data");
    assert_eq!(topic["qos"]["sources"]["depth"], "override");
    let status = card["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "/shared/job/_action/status")
        .unwrap();
    assert_eq!(status["qos"]["preset"], "action_status");
    assert_eq!(card["effective_config"]["name"]["source"], "cli");
    server.close().unwrap();
}

#[test]
fn legacy_transport_refuses_nondefault_service_and_client_qos_before_graph() {
    let context = Context::new(Memory::default());
    let node = Node::new(context.clone(), "node", "/", NodeOptions::default()).unwrap();
    let before = context.graph(Duration::ZERO).unwrap().len();
    let qos = QoS {
        depth: 3,
        ..Default::default()
    };
    assert!(matches!(
        node.create_client_with_qos(&service(), "svc", qos.clone(), EndpointOptions::default()),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        node.create_service_with_qos(&service(), "svc", qos, EndpointOptions::default()),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(context.graph(Duration::ZERO).unwrap().len(), before);
}

#[test]
fn node_clock_subscription_advances_timers_without_business_executor() {
    let context = Context::new(Memory::default());
    let node=Node::new_with_config(context.clone(),"sim","/",NodeOptions::default(),NodeConfigRequest {
        cli:json!({"use_sim_time":true,"remaps":[{"kind":"topic","from":"/clock","to":"/test_clock"}]}),
        ..Default::default()
    }).unwrap();
    let source = Node::new(context, "source", "/", NodeOptions::default()).unwrap();
    let clock_publisher = source
        .create_publisher(
            &clock_interface(),
            "/test_clock",
            qos_preset("clock").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let timer = node
        .create_timer(Duration::from_millis(10), None, true)
        .unwrap();
    assert!(!node.clock().now().unwrap().initialized);
    // Ordinary service creation still works with uninitialized ROS time.
    let service = node
        .create_service(&service(), "work", EndpointOptions::default())
        .unwrap();
    clock_publisher
        .publish(&json!({"clock":{"sec":0,"nanosec":0}}))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while !node.clock().now().unwrap().initialized {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(timer.take_ready().unwrap().is_none());
    clock_publisher
        .publish(&json!({"clock":{"sec":0,"nanosec":10_000_000}}))
        .unwrap();
    let call = loop {
        if let Some(call) = timer.take_ready().unwrap() {
            break call;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    };
    assert!(call.is_valid());
    assert!(timer.take_ready().unwrap().is_none());
    node.destroy_node().unwrap();
    assert!(!call.is_valid());
    assert_eq!(timer.is_ready(), Err(Error::Closed));
    assert_eq!(
        service.recv_timeout(Duration::ZERO).err(),
        Some(Error::Closed)
    );
}

#[test]
fn context_and_node_shutdown_wake_rates_and_leave_external_clock_owned_by_caller() {
    let context = Context::new(Memory::default());
    let node = Node::new(context.clone(), "timer_owner", "/", NodeOptions::default()).unwrap();
    let external = Clock::new(ClockType::Ros);
    external.set_use_sim_time(true).unwrap();
    let rate = node.create_rate(1.0, Some(external.clone())).unwrap();
    let worker = rate.clone();
    let thread = std::thread::spawn(move || worker.sleep());
    node.destroy_node().unwrap();
    assert_eq!(thread.join().unwrap(), Err(Error::Closed));
    assert!(external.now().is_ok());
    let clock = context.create_clock(ClockType::Ros).unwrap();
    clock.set_use_sim_time(true).unwrap();
    let rate = Rate::new(clock, 1.0).unwrap();
    let thread = std::thread::spawn(move || rate.sleep());
    context.shutdown().unwrap();
    assert_eq!(thread.join().unwrap(), Err(Error::Closed));
}
fn action_pair(timeout: Duration) -> (Node, ActionServer, ActionClient) {
    let ctx = Context::new(Memory::default());
    let node = Node::new(ctx, "worker", "/", NodeOptions::default()).unwrap();
    let interface = action_type();
    let server = ActionServer::new(
        &node,
        &interface,
        "count",
        ActionServerOptions {
            result_timeout: timeout,
            ..Default::default()
        },
        EndpointOptions {
            summary: "Count numbers".into(),
            side_effects: Some("read_only".into()),
            contract_version: None,
        },
    )
    .unwrap();
    let client = ActionClient::new(&node, &interface, "count", EndpointOptions::default()).unwrap();
    (node, server, client)
}

#[test]
fn telemetry_counts_logical_resources_once_and_closes_on_context_shutdown() {
    let ctx = Context::new(Memory::default());
    ctx.enable_log_capture(64).unwrap();
    let node = Node::new(ctx.clone(), "worker", "/", NodeOptions::default()).unwrap();
    let copy = node.clone();
    assert_eq!(
        ctx.telemetry_snapshot().active_resources[&TelemetryComponent::Node],
        1
    );
    let server = ActionServer::new(
        &node,
        &action_type(),
        "count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client =
        ActionClient::new(&node, &action_type(), "count", EndpointOptions::default()).unwrap();
    assert_eq!(
        ctx.telemetry_snapshot().active_resources[&TelemetryComponent::Action],
        2
    );
    client.close().unwrap();
    assert_eq!(client.close(), Err(Error::Closed));
    drop(client);
    assert_eq!(
        ctx.telemetry_snapshot().active_resources[&TelemetryComponent::Action],
        1
    );
    drop(copy);
    assert_eq!(
        ctx.telemetry_snapshot().active_resources[&TelemetryComponent::Node],
        1
    );
    ctx.shutdown().unwrap();
    assert!(ctx.telemetry_snapshot().active_resources.is_empty());
    drop(server);
    drop(node);
    assert!(ctx.telemetry_snapshot().active_resources.is_empty());
    let records = ctx.drain_telemetry();
    for name in ["created", "destroyed"] {
        let events: Vec<_> = records
            .iter()
            .filter(|record| {
                record.event.component == TelemetryComponent::Node && record.event.event == name
            })
            .collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.endpoint.as_deref(), Some("/worker"));
        assert!(events[0].event.node_id.is_some());
    }
}

#[test]
fn telemetry_service_completion_preserves_request_identity_without_error_text() {
    let ctx = Context::new(Memory::default());
    ctx.enable_log_capture(64).unwrap();
    let node = Node::new(ctx.clone(), "worker", "/", NodeOptions::default()).unwrap();
    let provider = node
        .create_service(&service(), "add", EndpointOptions::default())
        .unwrap();
    let client = node
        .create_client(&service(), "add", EndpointOptions::default())
        .unwrap();
    let mut ids = Vec::new();
    std::thread::scope(|scope| {
        for success in [true, false] {
            let call = scope.spawn(|| client.call(&json!({"a":2,"b":3}), WAIT));
            let request = provider.recv_timeout(WAIT).unwrap();
            ids.push(request.request_id().unwrap().to_owned());
            if success {
                request.reply(&json!({"sum":5})).unwrap();
                assert!(call.join().unwrap().is_ok());
            } else {
                request.reply_error("private_business_exception").unwrap();
                assert!(matches!(call.join().unwrap(), Err(Error::Remote(_))));
            }
            assert!(request.reply(&json!({"sum":5})).is_err());
        }
    });
    let records = ctx.drain_telemetry();
    let completed: Vec<_> = records
        .iter()
        .filter(|record| {
            record.event.component == TelemetryComponent::Service
                && record.event.event == "callback_completed"
        })
        .collect();
    assert_eq!(completed.len(), 2);
    for (record, id) in completed.iter().zip(&ids) {
        assert_eq!(record.event.request_id.as_ref(), Some(id));
        assert_eq!(record.event.node_id.as_deref(), Some(node.id()));
        assert_eq!(record.event.endpoint.as_deref(), Some("/add"));
        assert!(record.event.duration_ns.is_some());
    }
    assert_eq!(completed[0].event.outcome, TelemetryOutcome::Ok);
    assert_eq!(completed[1].event.outcome, TelemetryOutcome::Remote);
    assert!(!serde_json::to_string(&records)
        .unwrap()
        .contains("private_business_exception"));
}

#[test]
fn telemetry_action_events_link_goal_admission_and_separate_cancel_result_requests() {
    let ctx = Context::new(Memory::default());
    ctx.enable_log_capture(128).unwrap();
    let node = Node::new(ctx.clone(), "worker", "/", NodeOptions::default()).unwrap();
    let server = ActionServer::new(
        &node,
        &action_type(),
        "count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client =
        ActionClient::new(&node, &action_type(), "count", EndpointOptions::default()).unwrap();
    std::thread::scope(|scope| {
        let rejected = scope.spawn(|| client.send_goal(&json!({"count":9}), None, WAIT));
        drop(server.take_goal_request(WAIT).unwrap());
        assert!(!rejected.join().unwrap().unwrap().accepted());
    });
    let (goal, remote) = accepted_goal(&server, &client, None);
    goal.execute().unwrap();
    assert!(goal.execute().is_err());
    std::thread::scope(|scope| {
        let rejected = scope.spawn(|| remote.cancel_goal(WAIT));
        server
            .take_cancel_request(WAIT)
            .unwrap()
            .respond(&[])
            .unwrap();
        assert!(rejected.join().unwrap().unwrap()["goals_canceling"]
            .as_array()
            .unwrap()
            .is_empty());
        let accepted = scope.spawn(|| remote.cancel_goal(WAIT));
        server
            .take_cancel_request(WAIT)
            .unwrap()
            .respond(&[goal.goal_id().into()])
            .unwrap();
        assert_eq!(
            accepted.join().unwrap().unwrap()["goals_canceling"][0]["goal_id"],
            goal.goal_id()
        );
    });
    goal.canceled().unwrap();
    goal.finish_error("private_action_exception").unwrap();
    assert!(goal.finish_error("private_duplicate").is_err());
    assert!(matches!(remote.get_result(WAIT), Err(Error::Remote(_))));
    // A received reply can race the provider's post-send diagnostic record.
    let deadline = Instant::now() + WAIT;
    while !ctx.telemetry_snapshot().metrics.iter().any(|metric| {
        metric.key.component == TelemetryComponent::Action && metric.key.event == "result_sent"
    }) {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let records = ctx.drain_telemetry();
    let events: Vec<_> = records
        .iter()
        .filter(|record| record.event.component == TelemetryComponent::Action)
        .map(|record| &record.event)
        .collect();
    let event = |name: &str| {
        let matching: Vec<_> = events.iter().filter(|event| event.event == name).collect();
        assert_eq!(matching.len(), 1, "{name}");
        *matching[0]
    };
    assert_eq!(event("goal_rejected").outcome, TelemetryOutcome::Rejected);
    let admission = event("goal_accepted").request_id.as_ref().unwrap();
    for name in [
        "goal_accepted",
        "execution_started",
        "cancel_requested",
        "goal_canceled",
        "result_completed",
    ] {
        assert_eq!(event(name).request_id.as_ref(), Some(admission), "{name}");
        assert_eq!(event(name).goal_id.as_deref(), Some(goal.goal_id()));
        assert_eq!(event(name).node_id.as_deref(), Some(node.id()));
        assert_eq!(event(name).endpoint.as_deref(), Some("/count"));
    }
    let canceled = event("cancel_accepted").request_id.as_ref().unwrap();
    let rejected = event("cancel_rejected").request_id.as_ref().unwrap();
    let result = event("result_sent").request_id.as_ref().unwrap();
    assert_ne!(canceled, admission);
    assert_ne!(rejected, canceled);
    assert_ne!(result, admission);
    assert_ne!(result, canceled);
    assert_eq!(event("result_completed").outcome, TelemetryOutcome::Remote);
    assert_eq!(event("result_sent").outcome, TelemetryOutcome::Remote);
    for name in ["cancel_accepted", "cancel_rejected", "result_sent"] {
        let identity = event(name).request_id.as_ref().unwrap();
        assert!(records.iter().any(|record| {
            record.event.component == TelemetryComponent::Service
                && record.event.event == "callback_completed"
                && record.event.request_id.as_ref() == Some(identity)
        }));
    }
    let serialized = serde_json::to_string(&records).unwrap();
    assert!(!serialized.contains("private_action_exception"));
    assert!(!serialized.contains("private_duplicate"));
    assert_eq!(ctx.telemetry_snapshot().rejected_events_total, 0);
}

fn accepted_goal(
    server: &ActionServer,
    client: &ActionClient,
    id: Option<&str>,
) -> (ServerGoalHandle, ClientGoalHandle) {
    std::thread::scope(|scope| {
        let send = scope.spawn(|| client.send_goal(&json!({"count":3}), id, WAIT));
        let request = server.take_goal_request(WAIT).unwrap();
        assert_eq!(request.request(), &json!({"count":3}));
        let handle = request.decide(true).unwrap().unwrap();
        let client_handle = send.join().unwrap().unwrap();
        assert!(client_handle.accepted());
        assert_eq!(handle.goal_id(), client_handle.goal_id());
        assert_eq!(handle.stamp(), client_handle.stamp());
        (handle, client_handle)
    })
}

#[test]
fn action_jazzy_transition_table_has_exactly_eight_legal_entries() {
    use aos_core::action::{transition, GoalEvent as E, GoalStatus as S};
    let rows = [
        (S::Accepted, E::Execute, S::Executing),
        (S::Accepted, E::CancelGoal, S::Canceling),
        (S::Executing, E::CancelGoal, S::Canceling),
        (S::Executing, E::Succeed, S::Succeeded),
        (S::Executing, E::Abort, S::Aborted),
        (S::Canceling, E::Succeed, S::Succeeded),
        (S::Canceling, E::Abort, S::Aborted),
        (S::Canceling, E::Canceled, S::Canceled),
    ];
    let states = [
        S::Unknown,
        S::Accepted,
        S::Executing,
        S::Canceling,
        S::Succeeded,
        S::Canceled,
        S::Aborted,
    ];
    let events = [E::Execute, E::CancelGoal, E::Succeed, E::Abort, E::Canceled];
    for state in states {
        for event in events {
            let result = transition(state, event);
            match rows.iter().find(|(s, e, _)| *s == state && *e == event) {
                Some((_, _, next)) => assert_eq!(result, Ok(*next)),
                None => assert!(result.is_err()),
            }
        }
    }
}

#[test]
fn action_wrappers_preserve_local_schema_references_and_original_identity() {
    let mut definition = action_type().definition().clone();
    definition["goal"] = json!({"$defs":{"Count":{"type":"integer","minimum":1}},"type":"object","properties":{"count":{"$ref":"#/$defs/Count"}},"required":["count"]});
    let interface = Interface::from_json(definition).unwrap();
    let wrappers = ActionInterfaces::new(&interface).unwrap();
    wrappers
        .send_goal
        .validate(
            "request",
            &json!({"goal_id":"11111111111111111111111111111111","goal":{"count":2}}),
        )
        .unwrap();
    assert!(wrappers
        .send_goal
        .validate(
            "request",
            &json!({"goal_id":"11111111111111111111111111111111","goal":{"count":0}})
        )
        .is_err());
    assert_eq!(
        wrappers.status.definition()["action_hash"],
        interface.type_hash()
    );
    assert_eq!(aos_core::action::status_qos().depth, 1);
    assert_eq!(
        aos_core::action::status_qos().durability,
        Durability::TransientLocal
    );
}

#[test]
fn action_success_feedback_repeat_result_and_expiry() {
    let (node, server, client) = action_pair(Duration::from_millis(200));
    let (goal, remote) = accepted_goal(&server, &client, None);
    assert_eq!(goal.status(), 1);
    goal.execute().unwrap();
    goal.publish_feedback(&json!({"current":1})).unwrap();
    assert_eq!(
        client.recv_feedback(WAIT).unwrap(),
        json!({"goal_id":goal.goal_id(),"feedback":{"current":1}})
    );
    std::thread::scope(|scope| {
        let result = scope.spawn(|| remote.get_result(WAIT));
        goal.succeed().unwrap();
        goal.finish(Some(&json!({"total":3}))).unwrap();
        assert_eq!(
            result.join().unwrap().unwrap(),
            json!({"status":4,"result":{"total":3}})
        );
    });
    assert_eq!(
        remote.get_result(WAIT).unwrap(),
        json!({"status":4,"result":{"total":3}})
    );
    assert_eq!(remote.status(), 4);
    let card = node.node_card();
    let endpoint = card["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "action_server")
        .unwrap();
    assert_eq!(endpoint["type_name"], "test/action/Count");
    assert_eq!(endpoint["summary"], "Count numbers");
    std::thread::sleep(Duration::from_millis(230));
    assert_eq!(
        remote.get_result(WAIT).unwrap(),
        json!({"status":0,"result":{"total":0}})
    );
    assert_eq!(goal.status(), 0);
}

#[test]
fn action_cancel_progresses_while_result_is_pending_and_rejects_by_default() {
    let (_node, server, client) = action_pair(Duration::from_secs(10));
    let (goal, remote) = accepted_goal(&server, &client, None);
    goal.execute().unwrap();
    std::thread::scope(|scope| {
        let result = scope.spawn(|| remote.get_result(WAIT));
        let rejected = scope.spawn(|| remote.cancel_goal(WAIT));
        let request = server.take_cancel_request(WAIT).unwrap();
        assert_eq!(request.candidates().len(), 1);
        request.respond(&[]).unwrap();
        assert_eq!(
            rejected.join().unwrap().unwrap(),
            json!({"return_code":0,"goals_canceling":[]})
        );
        assert!(!goal.is_cancel_requested());
        let accepted = scope.spawn(|| remote.cancel_goal(WAIT));
        let request = server.take_cancel_request(WAIT).unwrap();
        request.respond(&[goal.goal_id().into()]).unwrap();
        assert_eq!(
            accepted.join().unwrap().unwrap()["goals_canceling"][0]["goal_id"],
            goal.goal_id()
        );
        assert!(goal.is_cancel_requested());
        goal.canceled().unwrap();
        goal.finish(Some(&json!({"total":1}))).unwrap();
        assert_eq!(
            result.join().unwrap().unwrap(),
            json!({"status":5,"result":{"total":1}})
        );
    });
}

#[test]
fn action_cancel_uses_four_jazzy_predicates_and_runs_prestart_cancel_cleanup() {
    let (_node, server, client) = action_pair(Duration::from_secs(10));
    let (first, _) = accepted_goal(&server, &client, Some("11111111111111111111111111111111"));
    let (second, remote) =
        accepted_goal(&server, &client, Some("22222222222222222222222222222222"));
    let cases = [
        (
            second.goal_id(),
            Timestamp::default(),
            vec![second.goal_id()],
        ),
        (
            aos_core::action::ZERO_UUID,
            first.stamp(),
            vec![first.goal_id()],
        ),
        (
            second.goal_id(),
            first.stamp(),
            vec![first.goal_id(), second.goal_id()],
        ),
        (
            aos_core::action::ZERO_UUID,
            Timestamp::default(),
            vec![first.goal_id(), second.goal_id()],
        ),
    ];
    for (id, stamp, expected) in cases {
        let request = json!({"goal_info":{"goal_id":id,"stamp":stamp}});
        std::thread::scope(|scope| {
            let call = scope.spawn(|| client.cancel(&request, WAIT));
            let cancel = server.take_cancel_request(WAIT).unwrap();
            let actual: Vec<_> = cancel
                .candidates()
                .iter()
                .map(|g| g.goal_id().to_owned())
                .collect();
            assert_eq!(actual, expected);
            cancel.respond(&[]).unwrap();
            assert_eq!(call.join().unwrap().unwrap()["return_code"], 0);
        });
    }
    std::thread::scope(|scope| {
        let call = scope.spawn(|| remote.cancel_goal(WAIT));
        server
            .take_cancel_request(WAIT)
            .unwrap()
            .respond(&[second.goal_id().into()])
            .unwrap();
        assert_eq!(
            call.join().unwrap().unwrap()["goals_canceling"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    });
    assert_eq!(second.status(), 3);
    second.execute().unwrap(); // No illegal CANCELING -> EXECUTING transition.
    assert_eq!(second.status(), 3);
    second.canceled().unwrap();
    second.finish(None).unwrap();
    assert_eq!(remote.get_result(WAIT).unwrap()["status"], 5);
}

#[test]
fn action_duplicate_uuid_rejects_before_callback_and_failures_settle_results() {
    let (_node, server, client) = action_pair(Duration::from_secs(10));
    let (goal, remote) = accepted_goal(&server, &client, Some("11111111111111111111111111111111"));
    std::thread::scope(|scope| {
        let duplicate =
            scope.spawn(|| client.send_goal(&json!({"count":3}), Some(goal.goal_id()), WAIT));
        assert!(matches!(
            server.take_goal_request(Duration::from_millis(100)),
            Err(Error::Timeout)
        ));
        assert!(!duplicate.join().unwrap().unwrap().accepted());
    });
    goal.execute().unwrap();
    goal.finish(None).unwrap(); // execute callback raised without a terminal state.
    assert_eq!(
        remote.get_result(WAIT).unwrap(),
        json!({"status":6,"result":{"total":0}})
    );
    let (bad, remote_bad) = accepted_goal(&server, &client, None);
    bad.execute().unwrap();
    bad.succeed().unwrap();
    assert!(matches!(
        bad.finish(Some(&json!({"total":"bad"}))),
        Err(Error::Validation(_))
    ));
    assert_eq!(bad.status(), 4); // Never invent an illegal terminal -> aborted edge.
    assert!(matches!(remote_bad.get_result(WAIT), Err(Error::Remote(_))));
    assert!(matches!(remote_bad.get_result(WAIT), Err(Error::Remote(_))));
}

#[test]
fn bag_serialized_endpoints_preserve_bytes_and_adapter_evidence() {
    let bus = Memory::default();
    let context = Context::new(bus.clone());
    let node = Node::new(context, "raw", "/", NodeOptions::default()).unwrap();
    let sub = node
        .create_subscription(
            &message(),
            "/raw",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let publisher = node
        .create_publisher(
            &message(),
            "/raw",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let bytes = b" { \"value\" : 42 } \n";
    publisher.publish_serialized(bytes).unwrap();
    let first = sub.recv_serialized_timeout(WAIT).unwrap();
    assert_eq!(first.payload, bytes);
    assert!(first.received.is_none() && first.source.is_none());
    assert!(matches!(
        publisher.publish_serialized(b"{\"value\":true}"),
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        sub.recv_serialized_timeout(Duration::ZERO),
        Err(Error::Timeout)
    ));
    let queue = bus
        .0
        .subscriptions
        .lock()
        .unwrap()
        .iter()
        .find_map(|(endpoint, q)| {
            (endpoint == &key(sub.endpoint()))
                .then(|| q.upgrade())
                .flatten()
        })
        .unwrap();
    let received = ReceiveMetadata {
        steady: Instant::now(),
        system: std::time::SystemTime::now(),
        scope: ReceiveScope::AdapterDelivery,
    };
    let source = SourceMetadata {
        publisher_id: Some("actual_sender".into()),
        sequence: Some(u64::MAX),
        timestamp: None,
    };
    queue
        .push(Sample {
            payload: bytes.to_vec(),
            payload_size_bytes: bytes.len(),
            received: Some(received),
            source: Some(source.clone()),
        })
        .unwrap();
    let sample = sub.recv_serialized_timeout(WAIT).unwrap();
    assert_eq!(sample.payload, bytes);
    assert_eq!(sample.payload_size_bytes, bytes.len());
    assert_eq!(sample.received.unwrap().steady, received.steady);
    assert_eq!(sample.received.unwrap().system, received.system);
    assert_eq!(sample.source, Some(source));
    queue
        .push(Sample::without_receive_metadata(vec![b'x'; 1024]))
        .unwrap();
    assert!(matches!(
        sub.recv_serialized_bounded_timeout(WAIT, 64).unwrap(),
        aos_core::BoundedSerializedSample::Oversized {
            payload_size_bytes: 1024
        }
    ));
    queue
        .push(Sample::without_receive_metadata(b"x".to_vec()))
        .unwrap();
    assert!(matches!(
        sub.recv_serialized_bounded_timeout(WAIT, 64),
        Err(Error::Validation(_))
    ));
    queue
        .push(Sample::without_receive_metadata(bytes.to_vec()))
        .unwrap();
    match sub
        .recv_serialized_bounded_timeout(WAIT, bytes.len())
        .unwrap()
    {
        aos_core::BoundedSerializedSample::Sample(sample) => assert_eq!(sample.payload, bytes),
        aos_core::BoundedSerializedSample::Oversized { .. } => panic!("boundary payload rejected"),
    }
    queue
        .push(Sample::without_receive_metadata(
            b"{\"value\":false}".to_vec(),
        ))
        .unwrap();
    assert!(matches!(
        sub.recv_serialized_timeout(WAIT),
        Err(Error::Validation(_))
    ));
    node.destroy_node().unwrap();
    assert!(matches!(
        publisher.publish_serialized(bytes),
        Err(Error::Closed)
    ));
    assert!(matches!(
        sub.recv_serialized_timeout(Duration::ZERO),
        Err(Error::Closed)
    ));
}

#[test]
fn bag_serialized_endpoints_keep_lifecycle_gates() {
    let context = Context::new(Memory::default());
    let node = Node::new_lifecycle(context, "raw_managed", "/", NodeOptions::default()).unwrap();
    let sub = node
        .create_subscription(
            &message(),
            "/raw_managed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let publisher = node
        .create_publisher(
            &message(),
            "/raw_managed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert!(matches!(
        publisher.publish_serialized(b"{\"value\":1}"),
        Err(Error::Inactive(_))
    ));
    assert!(matches!(
        sub.recv_serialized_timeout(Duration::ZERO),
        Err(Error::Unsupported(_))
    ));
    activate_lifecycle(&node.lifecycle().unwrap());
    publisher.publish_serialized(b"{\"value\":1}").unwrap();
    assert!(matches!(
        sub.recv_serialized_timeout(Duration::ZERO),
        Err(Error::Unsupported(_))
    ));
    let call = sub.take_message(WAIT).unwrap();
    assert_eq!(call.message(), &json!({"value":1}));
    call.finish();
}

#[test]
fn bag_action_observation_resolves_only_the_base_and_has_no_goal_control() {
    let context = Context::new(Memory::default());
    let node = Node::new_with_config(
        context.clone(),
        "observer",
        "/",
        NodeOptions::default(),
        NodeConfigRequest {
            program: json!({"remaps":[{"kind":"action","from":"/count","to":"/replay/count"}]}),
            ..Default::default()
        },
    )
    .unwrap();
    let action = action_type();
    for kind in [
        ActionObservationKind::Feedback,
        ActionObservationKind::Status,
    ] {
        let publisher = node
            .create_action_observation_publisher(
                &action,
                "/count",
                kind,
                QoS::default(),
                EndpointOptions::default(),
            )
            .unwrap();
        let sub = node
            .create_action_observation_subscription(
                &action,
                "/count",
                kind,
                QoS::default(),
                EndpointOptions::default(),
            )
            .unwrap();
        assert_eq!(
            publisher.endpoint().name,
            format!("/replay/count{}", kind.suffix())
        );
        assert_eq!(sub.endpoint(), publisher.endpoint());
        let types = ActionInterfaces::new(&action).unwrap();
        let expected = if kind == ActionObservationKind::Feedback {
            types.feedback
        } else {
            types.status
        };
        assert_eq!(sub.endpoint().type_hash, expected.type_hash());
        assert_eq!(
            context
                .get_type_description(
                    "/observer",
                    action.type_name(),
                    Some(action.type_hash()),
                    WAIT,
                )
                .unwrap()
                .type_hash(),
            action.type_hash()
        );
        assert!(context
            .graph(Duration::ZERO)
            .unwrap()
            .iter()
            .all(|e| !matches!(
                e.kind,
                EntityKind::ActionClient | EntityKind::ActionServer | EntityKind::Client
            )));
    }
    assert!(matches!(
        node.create_action_observation_publisher(
            &message(),
            "/count",
            ActionObservationKind::Status,
            QoS::default(),
            EndpointOptions::default()
        ),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        node.create_action_observation_subscription(
            &action,
            "/count/_action/get_result",
            ActionObservationKind::Status,
            QoS::default(),
            EndpointOptions::default()
        ),
        Err(Error::InvalidArgument(_))
    ));
    assert!(serde_json::from_str::<ActionObservationKind>("\"send_goal\"").is_err());
}

#[test]
fn bag_action_observation_rejects_an_internal_base_after_remapping() {
    let context = Context::new(Memory::default());
    let node = Node::new_with_config(
        context.clone(),
        "observer",
        "/",
        NodeOptions::default(),
        NodeConfigRequest {
            program: json!({"remaps":[{
                "kind":"action","from":"/count","to":"/nested/_action/count"
            }]}),
            ..Default::default()
        },
    )
    .unwrap();
    let action = action_type();
    for kind in [
        ActionObservationKind::Feedback,
        ActionObservationKind::Status,
    ] {
        assert!(matches!(
            node.create_action_observation_publisher(
                &action,
                "/count",
                kind,
                QoS::default(),
                EndpointOptions::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            node.create_action_observation_subscription(
                &action,
                "/count",
                kind,
                QoS::default(),
                EndpointOptions::default()
            ),
            Err(Error::InvalidArgument(_))
        ));
    }
    assert!(context.graph(Duration::ZERO).unwrap().iter().all(|entity| {
        !matches!(
            entity.kind,
            EntityKind::Publisher | EntityKind::Subscription
        )
    }));
}

#[test]
fn image_payload_survives_typed_publish_and_subscribe_resource_admission() {
    let interface = Interface::from_json(json!({"name":"test/msg/Image","kind":"msg","message":{
        "type":"object","properties":{
            "header":{"type":"object","properties":{
                "stamp":{"type":"object","properties":{"sec":{"type":"integer"},"nanosec":{"type":"integer"}}},
                "frame_id":{"type":"string"}}},
            "height":{"type":"integer"},"width":{"type":"integer"},"encoding":{"type":"string"},
            "step":{"type":"integer"},"data":{"type":"string"}
        }
    }})).unwrap();
    let payload = json!({"header":{"stamp":{"sec":1,"nanosec":0},"frame_id":"camera"},
        "height":480,"width":640,"encoding":"mono16","step":1280,"data":"A".repeat(819_200)});
    let context = Context::new(Memory::default());
    let node = Node::new(context, "image_resource", "/", NodeOptions::default()).unwrap();
    let subscription = node
        .create_subscription(
            &interface,
            "image",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let publisher = node
        .create_publisher(
            &interface,
            "image",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    publisher.publish(&payload).unwrap();
    assert_eq!(subscription.recv_timeout(WAIT).unwrap(), payload);
    node.destroy_node().unwrap();
}
