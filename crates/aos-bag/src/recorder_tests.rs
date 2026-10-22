use super::*;
use crate::{
    runtime_tests::{record_options, until, Network},
    storage_tests::{interface, Temp},
    InspectOptions,
};
use aos_core::{
    transport::{
        GraphGuard, GraphWatchBackend, PublisherBackend, ServiceBackend, SubscriptionBackend,
        Transport,
    },
    ActionServer, ActionServerOptions, Endpoint, Error, QoS, QueueLimits,
};
use aos_zenoh::ZenohTransport;
use serde_json::json;
use std::sync::atomic::AtomicUsize;

const WAIT: Duration = Duration::from_secs(6);

#[derive(Default)]
struct Faults {
    graph: Mutex<Vec<GraphEntity>>,
    raw_node_cards: Mutex<BTreeMap<String, Vec<u8>>>,
    interfaces: Mutex<BTreeMap<(String, String), Interface>>,
    raw_interfaces: Mutex<BTreeMap<(String, String), serde_json::Value>>,
    type_fail_nodes: Mutex<BTreeSet<String>>,
    type_delay_ms: Mutex<BTreeMap<String, u64>>,
    slow_calls: AtomicUsize,
    slow_gate: Mutex<Option<Arc<SlowGate>>>,
    cleanup_failed: AtomicBool,
    declared_limits: Mutex<BTreeMap<String, QueueLimits>>,
}
#[derive(Default)]
struct SlowGate {
    released: Mutex<bool>,
    changed: Condvar,
}
struct ReleaseSlowGate(Arc<SlowGate>);
impl Drop for ReleaseSlowGate {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
struct RecorderTransport {
    inner: ZenohTransport,
    faults: Arc<Faults>,
}
struct ExtraGraph {
    inner: Box<dyn GraphWatchBackend>,
    faults: Arc<Faults>,
}
impl GraphWatchBackend for ExtraGraph {
    fn snapshot(&self) -> aos_core::Result<Vec<GraphEntity>> {
        let mut graph = self.inner.snapshot()?;
        graph.extend(self.faults.graph.lock().unwrap().iter().cloned());
        Ok(graph)
    }
    fn wait_for_change(&self, timeout: Duration) -> aos_core::Result<()> {
        self.inner.wait_for_change(timeout)
    }
}
struct FailingNodeGuard(Box<dyn GraphGuard>);
impl GraphGuard for FailingNodeGuard {
    fn close(&self) -> aos_core::Result<()> {
        let _ = self.0.close();
        Err(Error::Transport(
            "injected recorder Node close failure".into(),
        ))
    }
}
impl Transport for RecorderTransport {
    fn create_publisher(&self, topic: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
        self.inner.create_publisher(topic)
    }
    fn create_subscription(&self, topic: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.inner.create_subscription(topic)
    }
    fn create_typed_publisher(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> aos_core::Result<Box<dyn PublisherBackend>> {
        self.inner.create_typed_publisher(endpoint, qos)
    }
    fn create_typed_subscription(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.inner.create_typed_subscription(endpoint, qos)
    }
    fn create_typed_subscription_with_limits(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
        limits: QueueLimits,
    ) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.faults
            .declared_limits
            .lock()
            .unwrap()
            .insert(endpoint.name.clone(), limits);
        self.inner
            .create_typed_subscription_with_limits(endpoint, qos, limits)
    }
    fn create_service(&self, endpoint: &Endpoint) -> aos_core::Result<Box<dyn ServiceBackend>> {
        self.inner.create_service(endpoint)
    }
    fn declare_graph_entity(&self, entity: &GraphEntity) -> aos_core::Result<Box<dyn GraphGuard>> {
        let guard = self.inner.declare_graph_entity(entity)?;
        if entity.kind == EntityKind::Node && self.faults.cleanup_failed.load(Ordering::Acquire) {
            Ok(Box::new(FailingNodeGuard(guard)))
        } else {
            Ok(guard)
        }
    }
    fn graph(&self, timeout: Duration) -> aos_core::Result<Vec<GraphEntity>> {
        let mut graph = self.inner.graph(timeout)?;
        graph.extend(self.faults.graph.lock().unwrap().iter().cloned());
        Ok(graph)
    }
    fn watch_graph(&self) -> aos_core::Result<Option<Box<dyn GraphWatchBackend>>> {
        Ok(self.inner.watch_graph()?.map(|inner| {
            Box::new(ExtraGraph {
                inner,
                faults: self.faults.clone(),
            }) as Box<dyn GraphWatchBackend>
        }))
    }
    fn call(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Duration,
    ) -> aos_core::Result<Vec<u8>> {
        if let Some(reply) = self
            .faults
            .raw_node_cards
            .lock()
            .unwrap()
            .get(&endpoint.name)
        {
            return Ok(reply.clone());
        }
        if endpoint.name.starts_with("/unresponsive_") {
            self.faults.slow_calls.fetch_add(1, Ordering::AcqRel);
            let gate = self.faults.slow_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                drop(
                    gate.changed
                        .wait_while(gate.released.lock().unwrap(), |released| !*released)
                        .unwrap(),
                );
            } else {
                thread::sleep(timeout);
            }
            return Err(Error::Timeout);
        }
        if endpoint.name.ends_with("/get_type_description") {
            if self
                .faults
                .type_fail_nodes
                .lock()
                .unwrap()
                .contains(&endpoint.name)
            {
                self.faults.slow_calls.fetch_add(1, Ordering::AcqRel);
                thread::sleep(timeout);
                return Err(Error::Timeout);
            }
            let request: serde_json::Value = serde_json::from_slice(payload).unwrap();
            let key = (
                request["type_name"].as_str().unwrap().to_owned(),
                request["type_hash"].as_str().unwrap().to_owned(),
            );
            let delay = self
                .faults
                .type_delay_ms
                .lock()
                .unwrap()
                .get(&key.0)
                .copied();
            if let Some(delay) = delay {
                let delay = Duration::from_millis(delay);
                thread::sleep(delay.min(timeout));
                if delay > timeout {
                    return Err(Error::Timeout);
                }
            }
            if let Some(definition) = self.faults.raw_interfaces.lock().unwrap().get(&key) {
                return Ok(serde_json::to_vec(definition).unwrap());
            }
            if let Some(interface) = self.faults.interfaces.lock().unwrap().get(&key) {
                return Ok(serde_json::to_vec(interface.definition()).unwrap());
            }
        }
        self.inner.call(endpoint, payload, timeout)
    }
    fn close(&self) -> aos_core::Result<()> {
        self.inner.close()
    }
}
fn fault_context(network: &Network, faults: &Arc<Faults>) -> Context {
    Context::new(RecorderTransport {
        inner: network.transport(),
        faults: faults.clone(),
    })
}
fn advertise(faults: &Faults, provider: &Node, name: &str, interface: &Interface, qos: QoS) {
    faults.interfaces.lock().unwrap().insert(
        (interface.type_name().into(), interface.type_hash().into()),
        interface.clone(),
    );
    faults.graph.lock().unwrap().push(GraphEntity {
        id: uuid::Uuid::new_v4().to_string(),
        node_id: provider.id().into(),
        node_name: provider.name().into(),
        namespace: provider.namespace().into(),
        kind: EntityKind::Publisher,
        endpoint: Some(Endpoint {
            name: name.into(),
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
        }),
        qos: Some(qos),
    });
}
fn channel() -> Arc<ChannelInfo> {
    Arc::new(
        ChannelInfo::new(
            "/healthy".into(),
            &interface(),
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    )
}

#[test]
fn oversized_node_card_is_rejected_before_decode_while_healthy_topic_records() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "healthy_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let publisher = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let attacker = Node::new(
        provider_context.clone(),
        "oversized_card_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let faults = Arc::new(Faults::default());
    faults.raw_node_cards.lock().unwrap().insert(
        "/oversized_card_provider/get_node_card".into(),
        vec![b'!'; 2 * 1024 * 1024],
    );
    advertise(
        &faults,
        &attacker,
        "/oversized_card",
        &interface(),
        QoS::default(),
    );
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let directory = temp.0.join("node-card-bound");
    let recorder = Recorder::start(context.clone(), record_options(directory.clone())).unwrap();
    until(|| {
        let progress = recorder.progress();
        progress.active_channel_count == 1 && progress.counters.description_failed_total > 0
    });
    let status = recorder.status();
    assert_eq!(status.state, RecordState::Recording);
    assert!(status.error.is_none());
    let unresolved = status
        .unresolved
        .iter()
        .find(|endpoint| endpoint.topic == "/oversized_card")
        .unwrap();
    assert!(
        unresolved.reason.contains("1048576 bytes"),
        "{unresolved:?}"
    );
    assert!(!unresolved.reason.contains("JSON"), "{unresolved:?}");
    publisher.wait_for_subscribers(WAIT).unwrap();
    for sequence in 0..4 {
        publisher
            .publish(&json!({"value":sequence,"text":"healthy"}))
            .unwrap();
        until(|| recorder.progress().counters.written_total == sequence + 1);
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Finished);
    assert!(report.manifest.clean_close && report.status.resources_closed);
    assert_eq!(report.manifest.counters.written_total, 4);
    assert_eq!(
        crate::inspect(&directory, InspectOptions::default())
            .unwrap()
            .messages,
        4
    );
    eprintln!(
        "oversized NodeCard: {:?}; healthy messages={}; closed={}",
        unresolved.reason, report.manifest.counters.written_total, report.status.resources_closed
    );
    // RecorderTransport uses the legacy Vec fallback. The core bounded-card
    // tests separately prove propagation of the pre-copy adapter limit.
    context.shutdown().unwrap();
    attacker.destroy_node().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn rejected_endpoint_descriptors_leave_healthy_recording_running() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let publisher = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let _second = provider
        .create_publisher(
            &interface(),
            "/healthy_two",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let faults = Arc::new(Faults::default());
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("rejected"));
    options.storage.limits.channels = 4;
    options.storage.limits.schema_bytes = 4096;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| recorder.progress().active_channel_count == 2);
    publisher.wait_for_subscribers(WAIT).unwrap();

    let mut large = interface().definition().clone();
    large["name"] = json!("test/msg/Large");
    large["message"]["description"] = json!("x".repeat(8192));
    advertise(
        &faults,
        &provider,
        "/large_schema",
        &Interface::from_json(large).unwrap(),
        QoS::default(),
    );
    advertise(
        &faults,
        &provider,
        "/invalid_qos",
        &interface(),
        QoS {
            depth: 0,
            ..QoS::default()
        },
    );
    advertise(
        &faults,
        &provider,
        "/capacity",
        &interface(),
        QoS::default(),
    );
    let action = action_interface();
    advertise(
        &faults,
        &provider,
        "/_action/feedback",
        &ActionInterfaces::new(&action).unwrap().feedback,
        QoS::default(),
    );
    until(|| recorder.progress().unresolved_count == 4);
    let status = recorder.status();
    assert_eq!(status.state, RecordState::Recording);
    assert!(status.error.is_none());
    assert_eq!(status.active_channels.len(), 2);
    for topic in [
        "/large_schema",
        "/invalid_qos",
        "/capacity",
        "/_action/feedback",
    ] {
        assert!(
            status
                .unresolved
                .iter()
                .any(|endpoint| endpoint.topic == topic),
            "{status:?}"
        );
    }
    for sequence in 0..20 {
        publisher
            .publish(&json!({"value": sequence, "text":"healthy"}))
            .unwrap();
        until(|| recorder.progress().counters.written_total == sequence + 1);
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Finished);
    assert_eq!(report.manifest.counters.written_total, 20);
    assert!(report.manifest.counters.endpoint_rejected_total >= 4);
    assert!(report.manifest.clean_close && report.manifest.error.is_none());
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn ten_unresponsive_providers_do_not_block_existing_topic_ingress() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "healthy",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let publisher = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let faults = Arc::new(Faults::default());
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("unresponsive"));
    options.qos.depth = 8;
    options.description_timeout_ms = 120;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| recorder.progress().active_channel_count == 1);
    publisher.wait_for_subscribers(WAIT).unwrap();
    let gate = Arc::new(SlowGate::default());
    *faults.slow_gate.lock().unwrap() = Some(gate.clone());
    // Always release the test-only gate on panic, before Recorder is dropped.
    let release = ReleaseSlowGate(gate.clone());
    *faults.graph.lock().unwrap() = unresponsive_providers();
    until(|| faults.slow_calls.load(Ordering::Acquire) > 0);
    for sequence in 0..160 {
        publisher
            .publish(&json!({"value":sequence,"text":"continuous"}))
            .unwrap();
        until(|| recorder.progress().counters.written_total == sequence + 1);
    }
    let progress = recorder.progress();
    // Progress happened while the discovery call was still blocked. This
    // causal assertion does not assume a machine-specific throughput budget.
    assert!(!*gate.released.lock().unwrap());
    assert_eq!(faults.slow_calls.load(Ordering::Acquire), 1);
    assert_eq!(progress.counters.adapter_dropped_total, 0);
    assert_eq!(progress.counters.writer_queue_dropped_total, 0);
    assert_eq!(progress.state, RecordState::Recording);
    recorder.stop(RecordStopReason::Requested).unwrap();
    drop(release);
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.written_total, 160);
    assert!(report.status.resources_closed);
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

fn unresponsive_providers() -> Vec<GraphEntity> {
    (0..10)
        .map(|index| GraphEntity {
            id: format!("unresponsive-publisher-{index}"),
            node_id: format!("unresponsive-{index}"),
            node_name: format!("unresponsive_{index}"),
            namespace: "/".into(),
            kind: EntityKind::Publisher,
            endpoint: Some(Endpoint {
                name: format!("/slow_{index}"),
                type_name: interface().type_name().into(),
                type_hash: interface().type_hash().into(),
            }),
            qos: Some(QoS::default()),
        })
        .collect()
}

struct DescriptionReplies {
    interface: Interface,
    failed_node: Option<String>,
    calls: Arc<Mutex<Vec<String>>>,
}
impl Transport for DescriptionReplies {
    fn create_publisher(&self, _: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("description test only".into()))
    }
    fn create_subscription(&self, _: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("description test only".into()))
    }
    fn call(&self, endpoint: &Endpoint, _: &[u8], _: Duration) -> aos_core::Result<Vec<u8>> {
        self.calls.lock().unwrap().push(endpoint.name.clone());
        if self
            .failed_node
            .as_ref()
            .is_some_and(|node| endpoint.name == format!("/{node}/get_type_description"))
        {
            return Err(Error::Timeout);
        }
        Ok(serde_json::to_vec(self.interface.definition()).unwrap())
    }
    fn close(&self) -> aos_core::Result<()> {
        Ok(())
    }
}

#[test]
fn deferred_unstarted_provider_preserves_next_cursor_after_real_failure() {
    let interface = interface();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let context = Context::new(DescriptionReplies {
        interface: interface.clone(),
        failed_node: Some("unresponsive_0".into()),
        calls: calls.clone(),
    });
    let mut providers = unresponsive_providers();
    providers.truncate(3);
    for provider in &mut providers {
        provider.endpoint.as_mut().unwrap().name = "/shared".into();
    }
    let providers: Vec<_> = providers.iter().collect();
    let key = (
        "/shared".into(),
        interface.type_name().into(),
        interface.type_hash().into(),
    );
    let mut discovery = DiscoveryState::default();
    let options = record_options("unused-description-test".into());
    let mut budgets = [
        Some(Duration::from_millis(1)), // Start provider 0: real timeout.
        Some(Duration::from_millis(1)), // Budget remains after that failure.
        None,                           // Expires before provider 1 starts.
        None,
    ]
    .into_iter();
    let error = describe_with_budget(&context, &providers, &key, &mut discovery, &options, || {
        budgets.next().expect("unexpected budget read")
    })
    .unwrap_err();
    assert_eq!(error.kind, BagErrorKind::Timeout);
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(discovery.next_provider.get(&key), Some(&1));
    eprintln!(
        "deferred provider: calls={:?}; next_provider={:?}; error={:?}",
        calls.lock().unwrap(),
        discovery.next_provider.get(&key),
        error.kind
    );
    let resolved =
        describe_with_budget(&context, &providers, &key, &mut discovery, &options, || {
            Some(Duration::from_millis(1))
        })
        .unwrap()
        .unwrap();
    assert_eq!(resolved.0.type_hash(), interface.type_hash());
    assert_eq!(
        calls.lock().unwrap().last().unwrap(),
        "/unresponsive_1/get_type_description"
    );
    assert!(!discovery.next_provider.contains_key(&key));
    context.shutdown().unwrap();
}

#[test]
fn occupied_action_continuation_defers_without_failure_or_cursor_change() {
    let feedback = ActionInterfaces::new(&action_interface()).unwrap().feedback;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let context = Context::new(DescriptionReplies {
        interface: feedback.clone(),
        failed_node: None,
        calls: calls.clone(),
    });
    let mut providers = unresponsive_providers();
    providers.truncate(1);
    providers[0].endpoint = Some(Endpoint {
        name: "/new/_action/feedback".into(),
        type_name: feedback.type_name().into(),
        type_hash: feedback.type_hash().into(),
    });
    let key = (
        "/new/_action/feedback".into(),
        feedback.type_name().into(),
        feedback.type_hash().into(),
    );
    let existing_key = (
        "/existing/_action/feedback".into(),
        feedback.type_name().into(),
        feedback.type_hash().into(),
    );
    let mut discovery = DiscoveryState {
        pending_action: Some(PendingActionDescription {
            key: existing_key.clone(),
            provider_id: "existing-provider".into(),
            interface: feedback,
        }),
        ..Default::default()
    };
    let options = record_options("unused-description-test".into());
    let mut budgets = [Some(Duration::from_millis(1)), None, None].into_iter();
    let result = describe_with_budget(
        &context,
        &[&providers[0]],
        &key,
        &mut discovery,
        &options,
        || budgets.next().expect("unexpected budget read"),
    );
    assert!(matches!(result, Ok(None)), "{result:?}");
    assert_eq!(calls.lock().unwrap().len(), 1);
    let pending = discovery.pending_action.as_ref().unwrap();
    assert_eq!(pending.key, existing_key);
    assert_eq!(pending.provider_id, "existing-provider");
    assert!(!discovery.next_provider.contains_key(&key));
    assert!(!discovery.failed_endpoints.contains_key(&key));
    eprintln!(
        "occupied continuation: result=deferred; pending={:?}; provider={}; cursor_unchanged=true",
        pending.key, pending.provider_id
    );
    context.shutdown().unwrap();
}

#[test]
fn unresponsive_provider_descriptions_share_one_round_budget() {
    let network = Network::new();
    let faults = Arc::new(Faults::default());
    let context = fault_context(&network, &faults);
    let node = Node::new(context.clone(), "recorder", "/", NodeOptions::default()).unwrap();
    let mut shared = super::tests::shared();
    shared.options.description_timeout_ms = 120;
    discover(
        &shared,
        &context,
        &node,
        unresponsive_providers(),
        &mut BTreeMap::new(),
        &mut DiscoveryState::default(),
        Instant::now() + Duration::from_millis(shared.options.description_timeout_ms),
    );
    // The first timeout consumes the whole round. Scheduler delay can also
    // consume the budget before the first call; it cannot permit extra RPCs.
    assert!(faults.slow_calls.load(Ordering::Acquire) <= 1);
    assert_eq!(shared.progress().unresolved_count, 10);
    node.destroy_node().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn remote_backtracking_pattern_is_rejected_while_healthy_topic_keeps_recording() {
    use sha2::{Digest, Sha256};
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let healthy = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let faults = Arc::new(Faults::default());
    let definition = json!({
        "name":"test/msg/Unsafe","kind":"msg",
        "message":{"type":"array","items":{"type":"string","pattern":"(?=(a+)+b)"}}
    });
    // This ASCII-only document has no numbers or escaped characters. The
    // sorted serde_json map serialization is its RFC 8785 representation.
    let hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&definition).unwrap())
    );
    let endpoint = Endpoint {
        name: "/unsafe".into(),
        type_name: "test/msg/Unsafe".into(),
        type_hash: hash.clone(),
    };
    faults
        .raw_interfaces
        .lock()
        .unwrap()
        .insert((endpoint.type_name.clone(), hash), definition);
    faults.graph.lock().unwrap().push(GraphEntity {
        id: uuid::Uuid::new_v4().to_string(),
        node_id: provider.id().into(),
        node_name: provider.name().into(),
        namespace: provider.namespace().into(),
        kind: EntityKind::Publisher,
        endpoint: Some(endpoint.clone()),
        qos: Some(QoS::default()),
    });
    let raw_transport = network.transport();
    let raw = raw_transport
        .create_typed_publisher(&endpoint, &QoS::default())
        .unwrap();
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let recorder = Recorder::start(
        context.clone(),
        record_options(temp.0.join("unsafe-schema")),
    )
    .unwrap();
    until(|| {
        let status = recorder.status();
        status.active_channels.len() == 1 && status.unresolved.iter().any(|e| e.topic == "/unsafe")
    });
    let status = recorder.status();
    let rejected = status
        .unresolved
        .iter()
        .find(|e| e.topic == "/unsafe")
        .unwrap();
    assert!(
        rejected.reason.contains("invalid message schema"),
        "{rejected:?}"
    );
    healthy.wait_for_subscribers(WAIT).unwrap();
    let hostile = serde_json::to_vec(&json!(vec!["aaaaaaaaaaaaaaaaaaaaaaaa!"; 10_000])).unwrap();
    assert!(hostile.len() < 1024 * 1024);
    for sequence in 0..20 {
        raw.publish(&hostile).unwrap();
        healthy
            .publish(&json!({"value":sequence,"text":"healthy"}))
            .unwrap();
        until(|| recorder.progress().counters.written_total == sequence + 1);
    }
    assert!(!faults
        .declared_limits
        .lock()
        .unwrap()
        .contains_key("/unsafe"));
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Finished);
    assert_eq!(report.manifest.counters.written_total, 20);
    assert!(report.status.resources_closed);
    raw.close().unwrap();
    raw_transport.close().unwrap();
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn remote_reply_and_schema_limits_precede_compilation_and_keep_healthy_recording() {
    use sha2::{Digest, Sha256};
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let healthy = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let faults = Arc::new(Faults::default());
    // Both documents deliberately contain an invalid pattern. A compiler
    // error would show that resource admission happened too late.
    for (topic, name, bytes) in [
        ("/large_reply", "LargeReply", 4 * 1024 * 1024),
        ("/large_schema", "LargeSchema", 8192),
    ] {
        let definition = json!({
            "name":format!("test/msg/{name}"),"kind":"msg",
            "message":{"type":"string","pattern":"(?=a)","description":"x".repeat(bytes)}
        });
        let type_name = definition["name"].as_str().unwrap().to_owned();
        let type_hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&definition).unwrap())
        );
        faults
            .raw_interfaces
            .lock()
            .unwrap()
            .insert((type_name.clone(), type_hash.clone()), definition);
        faults.graph.lock().unwrap().push(GraphEntity {
            id: uuid::Uuid::new_v4().to_string(),
            node_id: provider.id().into(),
            node_name: provider.name().into(),
            namespace: provider.namespace().into(),
            kind: EntityKind::Publisher,
            endpoint: Some(Endpoint {
                name: topic.into(),
                type_name,
                type_hash,
            }),
            qos: Some(QoS::default()),
        });
    }
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("remote-resource-limits"));
    options.storage.limits.schema_bytes = 4096;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| {
        let status = recorder.status();
        status.active_channels.len() == 1 && status.unresolved.len() == 2
    });
    let status = recorder.status();
    for (topic, expected) in [
        ("/large_reply", "reply exceeds 4194304 bytes"),
        ("/large_schema", "message schema_bytes exceeds 4096 bytes"),
    ] {
        let rejected = status
            .unresolved
            .iter()
            .find(|item| item.topic == topic)
            .unwrap();
        assert!(rejected.reason.contains(expected), "{rejected:?}");
        assert!(
            !rejected.reason.contains("invalid message schema"),
            "{rejected:?}"
        );
        assert!(!faults.declared_limits.lock().unwrap().contains_key(topic));
    }
    healthy.wait_for_subscribers(WAIT).unwrap();
    for sequence in 0..8 {
        healthy
            .publish(&json!({"value":sequence,"text":"healthy"}))
            .unwrap();
        until(|| recorder.progress().counters.written_total == sequence + 1);
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.written_total, 8);
    assert_eq!(report.manifest.counters.validation_failed_total, 0);
    assert_eq!(report.status.state, RecordState::Finished);
    assert!(report.status.resources_closed);
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn accepted_work_limit_payload_and_rejected_overload_allow_healthy_topic_progress() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let heavy = Interface::from_json(json!({
        "name":"test/msg/WorkLimit","kind":"msg",
        "message":{"type":"array","items":{"allOf":(0..1022)
            .map(|index| json!({"type":"integer","minimum":-index}))
            .collect::<Vec<_>>()}}
    }))
    .unwrap();
    let publisher = provider
        .create_publisher(
            &heavy,
            "/work_limit",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let healthy = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let raw_transport = network.transport();
    let raw = raw_transport
        .create_typed_publisher(publisher.endpoint(), &QoS::default())
        .unwrap();
    let context = network.context();
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("validation-work"));
    options.storage = crate::StorageOptions::default();
    options.message_bytes = 1024 * 1024;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| recorder.progress().active_channel_count == 2);
    raw.wait_for_subscribers(WAIT).unwrap();
    healthy.wait_for_subscribers(WAIT).unwrap();
    // Cost 1024 * (8191 array items + one array node) is exactly the
    // common 8 Mi-unit admission boundary. The larger valid JSON Schema
    // payload is below the byte limit but must be rejected before validation.
    let at_limit = serde_json::to_vec(&json!(vec![0; 8191])).unwrap();
    let over_limit = serde_json::to_vec(&json!(vec![0; 8192])).unwrap();
    let schema_mismatch = serde_json::to_vec(&json!(["not an integer"])).unwrap();
    assert!(over_limit.len() < 1024 * 1024);
    for sequence in 0..8 {
        raw.publish(&over_limit).unwrap();
        raw.publish(&schema_mismatch).unwrap();
        raw.publish(&at_limit).unwrap();
        healthy
            .publish(&json!({"value":sequence,"text":"healthy"}))
            .unwrap();
        until(|| {
            let counters = recorder.progress().counters;
            counters.resource_rejected_total == sequence + 1
                && counters.validation_failed_total == sequence + 1
                && counters.written_total == 2 * (sequence + 1)
        });
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.validation_failed_total, 8);
    assert_eq!(report.manifest.counters.resource_rejected_total, 8);
    assert_eq!(report.manifest.counters.received_total, 32);
    eprintln!(
        "validation admission: {}",
        serde_json::to_string(&report.manifest.counters).unwrap()
    );
    assert_eq!(report.manifest.counters.written_total, 16);
    assert_eq!(report.manifest.counters.adapter_oversized_total, 0);
    assert_eq!(report.manifest.counters.adapter_dropped_total, 0);
    assert_eq!(report.manifest.counters.writer_queue_dropped_total, 0);
    assert_eq!(report.status.state, RecordState::Finished);
    assert!(report.status.resources_closed);
    raw.close().unwrap();
    raw_transport.close().unwrap();
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn many_large_channel_status_polls_do_not_add_ingress_drops() {
    // Compare polling against the same bounded ingress workload without
    // overlapping the multi-MiB/256-shard disk fixtures in other tests.
    // Queue bounds, workload, deadline and zero-loss assertions stay intact.
    let _fixture = crate::reader::r2_tests::acceptance_fixture_guard();
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let mut definition = interface().definition().clone();
    definition["message"]["description"] = json!("schema context ".repeat(1200));
    let large = Interface::from_json(definition).unwrap();
    let mut publishers = Vec::new();
    for index in 0..24 {
        publishers.push(
            provider
                .create_publisher(
                    &large,
                    &format!("/channel_{index:02}"),
                    QoS::default(),
                    EndpointOptions::default(),
                )
                .unwrap(),
        );
    }
    let context = network.context();
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("many_channels"));
    options.storage = crate::StorageOptions::default();
    options.qos.depth = 32;
    let recorder = Arc::new(Recorder::start(context.clone(), options).unwrap());
    let discovery_deadline = Instant::now() + WAIT;
    loop {
        let progress = recorder.progress();
        if progress.active_channel_count == 24 {
            break;
        }
        assert!(
            Instant::now() < discovery_deadline,
            "24-channel discovery timed out: {progress:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
    for publisher in &publishers {
        publisher.wait_for_subscribers(WAIT).unwrap();
    }
    let mut expected = 0;
    for phase in [false, true] {
        let done = Arc::new(AtomicBool::new(false));
        let reader = recorder.clone();
        let reader_done = done.clone();
        let poller = phase.then(|| {
            thread::spawn(move || {
                let mut full_snapshots = 0;
                let mut next_full = Instant::now();
                while !reader_done.load(Ordering::Acquire) {
                    let progress = reader.progress();
                    assert_eq!(progress.active_channel_count, 24);
                    assert!(serde_json::to_vec(&progress).unwrap().len() < 8192);
                    if Instant::now() >= next_full {
                        let status = reader.status();
                        assert_eq!(status.active_channels.len(), 24);
                        assert!(
                            status.active_channels[0].interface["message"]["description"]
                                .as_str()
                                .unwrap()
                                .len()
                                >= 16_000
                        );
                        full_snapshots += 1;
                        next_full = Instant::now() + Duration::from_millis(20);
                    }
                    thread::sleep(Duration::from_millis(2));
                }
                full_snapshots
            })
        });
        for round in 0..48 {
            for publisher in &publishers {
                publisher
                    .publish(&json!({"value":round,"text":"steady ingress"}))
                    .unwrap();
                expected += 1;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let deadline = Instant::now() + WAIT;
        let settled = loop {
            let progress = recorder.progress();
            if progress.counters.written_total == expected {
                break Ok(());
            }
            if Instant::now() >= deadline
                || progress.counters.adapter_dropped_total != 0
                || progress.counters.writer_queue_dropped_total != 0
            {
                break Err(progress);
            }
            thread::sleep(Duration::from_millis(5));
        };
        done.store(true, Ordering::Release);
        if let Some(poller) = poller {
            assert!(poller.join().unwrap() >= 5);
        }
        assert!(
            settled.is_ok(),
            "polling={phase}, expected={expected}, final progress={settled:?}"
        );
        let progress = recorder.progress();
        assert_eq!(
            progress.counters.adapter_dropped_total, 0,
            "polling={phase}"
        );
        assert_eq!(
            progress.counters.writer_queue_dropped_total, 0,
            "polling={phase}"
        );
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.written_total, 2304);
    assert!(report.manifest.clean_close && report.status.resources_closed);
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn oversized_malformed_payload_is_classified_before_decode_and_next_sample_survives() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let publisher = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let raw_transport = network.transport();
    let raw = raw_transport
        .create_typed_publisher(publisher.endpoint(), &QoS::default())
        .unwrap();
    let context = network.context();
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("bounded"));
    options.message_bytes = 128;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| recorder.progress().active_channel_count == 1);
    raw.wait_for_subscribers(WAIT).unwrap();
    raw.publish(&vec![b'!'; 1024]).unwrap();
    raw.publish(b"invalid json").unwrap();
    raw.publish(br#"{"value":42,"text":"valid"}"#).unwrap();
    until(|| {
        let counters = recorder.progress().counters;
        counters.received_total == 2 && counters.adapter_oversized_total == 1
    });
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.received_total, 2);
    assert_eq!(report.manifest.counters.adapter_oversized_total, 1);
    assert_eq!(report.manifest.counters.adapter_oversized_bytes, 1024);
    assert_eq!(report.manifest.counters.adapter_capacity_rejected_total, 0);
    assert_eq!(report.manifest.counters.oversized_total, 0);
    assert_eq!(report.manifest.counters.validation_failed_total, 1);
    assert_eq!(report.manifest.counters.written_total, 1);
    assert!(report.manifest.clean_close);
    raw.close().unwrap();
    raw_transport.close().unwrap();
    context.shutdown().unwrap();
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
}

#[test]
fn node_cleanup_failure_keeps_clean_file_separate_from_resources_closed() {
    let network = Network::new();
    let faults = Arc::new(Faults::default());
    faults.cleanup_failed.store(true, Ordering::Release);
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let path = temp.0.join("cleanup");
    let recorder = Recorder::start(context.clone(), record_options(path.clone())).unwrap();
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Failed);
    assert!(!report.status.resources_closed);
    assert!(report
        .status
        .cleanup_error
        .as_ref()
        .unwrap()
        .message
        .contains("Node close failure"));
    assert!(report.manifest.clean_close && report.manifest.error.is_none());
    assert!(
        crate::inspect(&path, InspectOptions::default())
            .unwrap()
            .manifest
            .clean_close
    );
    assert!(!context.is_shutdown());
    context.shutdown().unwrap();
}

#[test]
fn final_manifest_write_failure_is_reported_as_unconfirmed_cleanup() {
    let network = Network::new();
    let context = network.context();
    let temp = Temp::new();
    let path = temp.0.join("manifest_failure");
    let recorder = Recorder::start(context.clone(), record_options(path.clone())).unwrap();
    let manifest_path = path.join("manifest.json");
    std::fs::remove_file(&manifest_path).unwrap();
    std::fs::create_dir(&manifest_path).unwrap();
    recorder.stop(RecordStopReason::Requested).unwrap();
    assert_eq!(recorder.wait(WAIT).unwrap_err().kind, BagErrorKind::Io);
    let status = recorder.status();
    assert_eq!(status.state, RecordState::Failed);
    assert!(!status.resources_closed);
    assert_eq!(status.cleanup_error.unwrap().kind, BagErrorKind::Io);
    context.shutdown().unwrap();
}

#[test]
fn registration_timeout_cancels_pending_admission_without_stopping_ingress() {
    let mut shared = super::tests::shared();
    shared.options.control_timeout_ms = 25;
    let mut channel = channel();
    assert_eq!(
        shared.register(channel.clone()).unwrap_err().kind,
        BagErrorKind::Timeout
    );
    assert!(!shared.stopping());
    assert!(shared.state.lock().unwrap().registration.is_none());
    shared
        .enqueue(
            &mut channel,
            aos_core::Sample::without_receive_metadata(br#"{"value":1,"text":"healthy"}"#.to_vec()),
        )
        .unwrap();
    assert_eq!(shared.status().counters.received_total, 1);
    let shared = Arc::new(shared);
    let writer = shared.clone();
    let worker = thread::spawn(move || {
        let mut state = writer.state.lock().unwrap();
        while state.registration.is_none() {
            state = writer.changed.wait(state).unwrap();
        }
        let registration = state.registration.take().unwrap();
        assert!(!registration.completion.lock().unwrap().cancelled);
        registration.reply.send(Ok(())).unwrap();
    });
    shared.register(channel).unwrap();
    worker.join().unwrap();
}

#[test]
fn short_round_remainder_defers_node_and_type_descriptions_without_backoff() {
    let network = Network::new();
    let faults = Arc::new(Faults::default());
    let context = fault_context(&network, &faults);
    let node = Node::new(context.clone(), "recorder", "/", NodeOptions::default()).unwrap();
    let mut shared = super::tests::shared();
    shared.options.description_timeout_ms = 120;
    for cached_nodes in [false, true] {
        let graph = unresponsive_providers();
        let mut discovery = DiscoveryState::default();
        if cached_nodes {
            for entity in &graph {
                discovery
                    .excluded_nodes
                    .insert(entity.node_id.clone(), false);
            }
        }
        discover(
            &shared,
            &context,
            &node,
            graph,
            &mut BTreeMap::new(),
            &mut discovery,
            Instant::now() + Duration::from_millis(5),
        );
        assert_eq!(faults.slow_calls.load(Ordering::Acquire), 0);
        assert!(discovery.failed_nodes.is_empty());
        assert!(discovery.failed_endpoints.is_empty());
        assert_eq!(shared.progress().counters.description_failed_total, 0);
        assert_eq!(shared.progress().unresolved_count, 10);
        assert!(shared
            .status()
            .unresolved
            .iter()
            .all(|e| e.reason.contains("deferred")));
    }
    node.destroy_node().unwrap();
    context.shutdown().unwrap();
}

fn wait_active(recorder: &Recorder, count: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if recorder.progress().active_channel_count == count {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

#[test]
fn timed_out_first_provider_does_not_pin_discovery_or_hide_healthy_alternative() {
    let network = Network::new();
    let provider_context = network.context();
    let stalled = Node::new(
        provider_context.clone(),
        "stalled_type",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let healthy = Node::new(
        provider_context.clone(),
        "healthy_type",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let faults = Arc::new(Faults::default());
    faults
        .type_fail_nodes
        .lock()
        .unwrap()
        .insert("/stalled_type/get_type_description".into());
    // Only these synthetic Publisher declarations enter the discovery group;
    // their real Nodes still serve NodeCards through the native transport.
    advertise(&faults, &stalled, "/a_shared", &interface(), QoS::default());
    advertise(&faults, &healthy, "/a_shared", &interface(), QoS::default());
    advertise(
        &faults,
        &healthy,
        "/z_healthy",
        &interface(),
        QoS::default(),
    );
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("provider-fairness"));
    options.description_timeout_ms = 200;
    options.discovery_interval_ms = 30;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    let active = wait_active(&recorder, 2, Duration::from_secs(3));
    let status = recorder.status();
    recorder.stop(RecordStopReason::Requested).unwrap();
    recorder.wait(WAIT).unwrap();
    stalled.destroy_node().unwrap();
    healthy.destroy_node().unwrap();
    context.shutdown().unwrap();
    provider_context.shutdown().unwrap();
    assert!(
        active,
        "a timed-out first provider pinned discovery: {status:?}"
    );
    assert!(faults.slow_calls.load(Ordering::Acquire) > 0);
    assert!(status.counters.description_failed_total > 0);
    let names: BTreeSet<_> = status
        .active_channels
        .iter()
        .map(|c| c.topic.as_str())
        .collect();
    assert_eq!(names, BTreeSet::from(["/a_shared", "/z_healthy"]));
}

#[test]
fn action_description_spans_rounds_without_starving_the_next_topic() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "slow_action_types",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let parent = Interface::from_json(json!({
        "name":"bag_test/action/SlowDescription","kind":"action",
        "goal":{"type":"object"},
        "result":{"type":"object","default":{}},
        "feedback":{"type":"object"}
    }))
    .unwrap();
    let feedback = ActionInterfaces::new(&parent).unwrap().feedback;
    let faults = Arc::new(Faults::default());
    faults.interfaces.lock().unwrap().insert(
        (parent.type_name().into(), parent.type_hash().into()),
        parent.clone(),
    );
    faults
        .type_delay_ms
        .lock()
        .unwrap()
        .insert(feedback.type_name().into(), 160);
    faults
        .type_delay_ms
        .lock()
        .unwrap()
        .insert(parent.type_name().into(), 160);
    advertise(
        &faults,
        &provider,
        "/a_task/_action/feedback",
        &feedback,
        QoS::default(),
    );
    advertise(
        &faults,
        &provider,
        "/z_healthy",
        &interface(),
        QoS::default(),
    );
    let context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("action-description-fairness"));
    options.description_timeout_ms = 200;
    options.discovery_interval_ms = 30;
    let recorder = Recorder::start(context.clone(), options).unwrap();
    let active = wait_active(&recorder, 2, Duration::from_secs(3));
    let status = recorder.status();
    recorder.stop(RecordStopReason::Requested).unwrap();
    recorder.wait(WAIT).unwrap();
    provider.destroy_node().unwrap();
    context.shutdown().unwrap();
    provider_context.shutdown().unwrap();
    assert!(
        active,
        "two individually in-budget Action descriptions made no progress: {status:?}"
    );
    let names: BTreeSet<_> = status
        .active_channels
        .iter()
        .map(|c| c.topic.as_str())
        .collect();
    assert_eq!(
        names,
        BTreeSet::from(["/a_task/_action/feedback", "/z_healthy"])
    );
}

#[test]
fn idle_adapter_statistics_do_not_acquire_the_global_state_lock() {
    let network = Network::new();
    let context = network.context();
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let _publisher = provider
        .create_publisher(
            &interface(),
            "/healthy",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let temp = Temp::new();
    let recorder =
        Recorder::start(context.clone(), record_options(temp.0.join("idle-stats"))).unwrap();
    until(|| recorder.progress().active_channel_count == 1);
    let shared = recorder.shared.clone();
    let state = shared.state.lock().unwrap();
    let subscription = state.subscriptions[0].clone();
    let worker_shared = shared.clone();
    let (done, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        observe_queue(&worker_shared, &subscription, false);
        done.send(()).unwrap();
    });
    let observed = receive.recv_timeout(Duration::from_millis(250));
    // Release before assertions so the old implementation cannot deadlock
    // cleanup when this regression is run against it.
    drop(state);
    worker.join().unwrap();
    assert!(
        observed.is_ok(),
        "idle stats waited for the global recorder lock"
    );
    recorder.stop(RecordStopReason::Requested).unwrap();
    recorder.wait(WAIT).unwrap();
    provider.destroy_node().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn every_idle_subscription_stop_check_is_independent_of_the_state_lock() {
    let shared = Arc::new(super::tests::shared());
    let state = shared.state.lock().unwrap();
    let worker_shared = shared.clone();
    let (done, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        for _ in 0..4_096 {
            assert!(!worker_shared.stopping());
        }
        done.send(()).unwrap();
    });
    let observed = receive.recv_timeout(Duration::from_millis(250));
    drop(state);
    worker.join().unwrap();
    assert!(
        observed.is_ok(),
        "idle per-subscription stop checks waited for the global recorder lock"
    );
    shared.request_stop(RecordStopReason::Requested, None);
    assert!(shared.stopping());
    assert_eq!(
        shared.status().stop_reason,
        Some(RecordStopReason::Requested)
    );
}

#[test]
fn registration_timeout_reports_late_commit_and_retry_is_idempotent() {
    let mut shared = super::tests::shared();
    shared.options.control_timeout_ms = 50;
    let shared = Arc::new(shared);
    let temp = Temp::new();
    let directory = temp.0.join("late-registration");
    let mut options = crate::storage_tests::storage_options();
    // Exactly one business channel plus its companion; duplicate registration
    // must succeed without consuming a second channel admission.
    options.limits.channels = 2;
    let store = StorageWriter::create(
        &directory,
        options,
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    let writer_shared = shared.clone();
    let gate = Arc::new(SlowGate::default());
    let writer_gate = gate.clone();
    let release = ReleaseSlowGate(gate);
    let (entered, receive_entered) = mpsc::channel();
    let writer = thread::spawn(move || {
        let mut store = store;
        let mut state = writer_shared.state.lock().unwrap();
        while state.registration.is_none() {
            state = writer_shared.changed.wait(state).unwrap();
        }
        let registration = state.registration.take().unwrap();
        drop(state);
        registration
            .execute(&writer_shared, |channel| {
                entered.send(()).unwrap();
                drop(
                    writer_gate
                        .changed
                        .wait_while(writer_gate.released.lock().unwrap(), |released| !*released)
                        .unwrap(),
                );
                store.register_admitted(channel)
            })
            .unwrap();
        store
    });
    let caller_shared = shared.clone();
    let caller = thread::spawn(move || caller_shared.register(channel()));
    receive_entered.recv_timeout(WAIT).unwrap();
    let result = caller.join().unwrap();
    assert_eq!(result.unwrap_err().kind, BagErrorKind::Timeout);
    assert_eq!(
        shared
            .progress()
            .counters
            .descriptor_registration_timeouts_total,
        1
    );
    assert_eq!(
        shared
            .progress()
            .counters
            .descriptor_registration_late_commits_total,
        0
    );
    drop(release);
    let mut store = writer.join().unwrap();
    assert_eq!(
        shared
            .progress()
            .counters
            .descriptor_registration_late_commits_total,
        1
    );
    for _ in 0..3 {
        store.register_admitted(channel()).unwrap().unwrap();
    }
    let manifest = store
        .finish(
            RecordStopReason::Requested,
            shared.progress().counters,
            None,
        )
        .unwrap();
    assert!(manifest.clean_close);
    assert_eq!(
        crate::inspect(&directory, InspectOptions::default())
            .unwrap()
            .channels
            .len(),
        1
    );
}

#[test]
fn channel_schema_is_shared_until_sequence_epoch_rollover() {
    let shared = super::tests::shared();
    let mut channel = channel();
    let initial = channel.clone();
    shared
        .enqueue(
            &mut channel,
            aos_core::Sample::without_receive_metadata(b"{}".to_vec()),
        )
        .unwrap();
    shared
        .enqueue(
            &mut channel,
            aos_core::Sample::without_receive_metadata(b"{}".to_vec()),
        )
        .unwrap();
    {
        let mut state = shared.state.lock().unwrap();
        assert!(Arc::ptr_eq(&state.queue[0].0.channel, &initial));
        assert!(Arc::ptr_eq(&state.queue[1].0.channel, &initial));
        state.next_sequence = 1u64 << 32;
    }
    shared
        .enqueue(
            &mut channel,
            aos_core::Sample::without_receive_metadata(b"{}".to_vec()),
        )
        .unwrap();
    let second_epoch = channel.clone();
    shared
        .enqueue(
            &mut channel,
            aos_core::Sample::without_receive_metadata(b"{}".to_vec()),
        )
        .unwrap();
    let state = shared.state.lock().unwrap();
    assert!(!Arc::ptr_eq(&second_epoch, &initial));
    assert_eq!(second_epoch.sequence_epoch, 1);
    assert!(Arc::ptr_eq(&state.queue[2].0.channel, &second_epoch));
    assert!(Arc::ptr_eq(&state.queue[3].0.channel, &second_epoch));
}

#[test]
fn wait_checks_are_outside_state_lock_and_keep_one_deadline() {
    let shared = Arc::new(super::tests::shared());
    let recorder = Recorder {
        shared,
        workers: Mutex::new(Vec::new()),
    };
    let mut checks = 0;
    let started = Instant::now();
    let error = recorder
        .wait_with_check(Duration::from_millis(115), || {
            checks += 1;
            assert_eq!(recorder.progress().state, RecordState::Recording);
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, BagErrorKind::Timeout);
    assert!(checks >= 3);
    assert!(started.elapsed() < Duration::from_millis(250));
    let error = recorder
        .wait_with_check(WAIT, || {
            recorder.stop(RecordStopReason::Requested)?;
            Err(BagError::invalid("interrupted by caller"))
        })
        .unwrap_err();
    assert!(error.message.contains("interrupted by caller"));
    assert_eq!(
        recorder.progress().stop_reason,
        Some(RecordStopReason::Requested)
    );
}

fn action_interface() -> Interface {
    Interface::from_json(json!({
        "name":"test/action/Probe", "kind":"action",
        "goal":{"type":"object","properties":{},"additionalProperties":false},
        "result":{"type":"object","properties":{},"additionalProperties":false,"default":{}},
        "feedback":{"type":"object","properties":{},"additionalProperties":false}
    }))
    .unwrap()
}

#[test]
fn ordinary_action_server_publishes_resolvable_parent_descriptions_for_recording() {
    let network = Network::new();
    let context = network.context();
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let action = action_interface();
    let server = ActionServer::new(
        &provider,
        &action,
        "/probe",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let bag_context = network.context();
    let temp = Temp::new();
    let recorder =
        Recorder::start(bag_context.clone(), record_options(temp.0.join("action"))).unwrap();
    until(|| recorder.progress().active_channel_count == 2);
    let status = recorder.status();
    assert!(status.unresolved.is_empty(), "{status:?}");
    for channel in status.active_channels {
        assert_eq!(channel.action.as_ref().unwrap().base, "/probe");
        assert_eq!(
            channel.action.as_ref().unwrap().interface,
            *action.definition()
        );
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    assert!(recorder.wait(WAIT).unwrap().manifest.clean_close);
    server.close().unwrap();
    provider.destroy_node().unwrap();
    context.shutdown().unwrap();
    bag_context.shutdown().unwrap();
}

#[test]
fn recorder_applies_payload_limits_to_topic_and_both_action_observations() {
    let network = Network::new();
    let context = network.context();
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let topic = provider
        .create_publisher(
            &interface(),
            "/bounded_topic",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let server = ActionServer::new(
        &provider,
        &action_interface(),
        "/bounded_action",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let faults = Arc::new(Faults::default());
    let bag_context = fault_context(&network, &faults);
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("adapter_limits"));
    options.message_bytes = 1024;
    options.adapter_queue_bytes = 4096;
    options.qos.depth = 4;
    let expected = QueueLimits {
        message_bytes: 1024,
        queue_bytes: 4096,
    };
    let recorder = Recorder::start(bag_context.clone(), options).unwrap();
    until(|| recorder.progress().active_channel_count == 3);
    // The retained empty status snapshot is the sole initial business sample.
    until(|| recorder.progress().counters.written_total == 1);
    let limits = faults.declared_limits.lock().unwrap().clone();
    assert_eq!(limits.len(), 3);
    assert!(limits.values().all(|limits| *limits == expected));
    let subscriptions = recorder.shared.state.lock().unwrap().subscriptions.clone();
    for subscription in subscriptions.iter() {
        let bytes = subscription
            .subscription
            .queue_stats()
            .unwrap()
            .bytes
            .unwrap();
        assert_eq!(bytes.message_limit_bytes, expected.message_bytes);
        assert_eq!(bytes.queue_limit_bytes, expected.queue_bytes);
    }
    let raw_transport = network.transport();
    let mut raw_publishers = Vec::new();
    for subscription in subscriptions.iter() {
        let raw = raw_transport
            .create_typed_publisher(
                subscription.subscription.endpoint(),
                &subscription.channel.record_qos,
            )
            .unwrap();
        raw.wait_for_subscribers(WAIT).unwrap();
        // Invalid JSON must be rejected at adapter admission, before decoding.
        raw.publish(&vec![b'!'; 1025]).unwrap();
        raw_publishers.push(raw);
    }
    until(|| recorder.progress().counters.adapter_oversized_total == 3);
    topic
        .publish(&json!({"value":7,"text":"accepted"}))
        .unwrap();
    until(|| recorder.progress().counters.written_total == 2);
    for _ in 0..20 {
        let counters = recorder.progress().counters;
        assert_eq!(counters.received_total, 2);
        assert_eq!(counters.adapter_oversized_total, 3);
        assert_eq!(counters.adapter_oversized_bytes, 3075);
        assert_eq!(counters.adapter_dropped_total, 0);
        assert_eq!(counters.adapter_capacity_rejected_total, 0);
        assert_eq!(counters.oversized_total, 0);
        assert_eq!(counters.validation_failed_total, 0);
        assert_eq!(counters.end_to_end_lost_total, None);
    }
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.manifest.counters.adapter_oversized_total, 3);
    assert_eq!(report.manifest.counters.written_total, 2);
    assert!(report.manifest.clean_close && report.status.resources_closed);
    for raw in raw_publishers {
        raw.close().unwrap();
    }
    raw_transport.close().unwrap();
    server.close().unwrap();
    provider.destroy_node().unwrap();
    context.shutdown().unwrap();
    bag_context.shutdown().unwrap();
}

#[test]
fn old_record_counter_json_defaults_adapter_byte_rejections_to_zero() {
    let mut legacy = serde_json::to_value(crate::RecordCounters {
        received_total: 7,
        written_total: 6,
        ..Default::default()
    })
    .unwrap();
    for name in [
        "adapter_oversized_total",
        "adapter_oversized_bytes",
        "adapter_capacity_rejected_total",
        "descriptor_registration_timeouts_total",
        "descriptor_registration_late_commits_total",
    ] {
        legacy.as_object_mut().unwrap().remove(name);
    }
    let counters: crate::RecordCounters = serde_json::from_value(legacy).unwrap();
    assert_eq!(counters.received_total, 7);
    assert_eq!(counters.written_total, 6);
    assert_eq!(counters.adapter_oversized_total, 0);
    assert_eq!(counters.adapter_oversized_bytes, 0);
    assert_eq!(counters.adapter_capacity_rejected_total, 0);
    assert_eq!(counters.descriptor_registration_timeouts_total, 0);
    assert_eq!(counters.descriptor_registration_late_commits_total, 0);
}
