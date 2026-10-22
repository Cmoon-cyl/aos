use super::*;
use crate::{
    storage::StorageWriter,
    storage_tests::{interface, message, storage_options, Temp},
};
use aos_core::{
    transport::{
        GraphGuard, GraphWatchBackend, PublisherBackend, ServiceBackend, SubscriptionBackend,
        Transport,
    },
    ActionObservationKind, Context, Endpoint, EndpointOptions, EntityKind, GraphEntity, Interface,
    LifecycleCallbackReturn, LifecycleState, LifecycleTransitionId, Node, NodeConfigRequest,
    NodeOptions, Parameter, ParameterDescriptor, ParameterValue, ProcessBootstrap, QoS,
};
use aos_zenoh::ZenohTransport;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use zenoh::Wait;

const WAIT: Duration = Duration::from_secs(6);
pub(crate) struct Network {
    _router: zenoh::Session,
    endpoint: String,
}
impl Network {
    pub(crate) fn new() -> Self {
        let config = zenoh::Config::from_json5(
            &json!({
                "mode":"router", "listen":{"endpoints":["tcp/127.0.0.1:0"]},
                "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}},
                "timestamping":{"enabled":true}
            })
            .to_string(),
        )
        .unwrap();
        let router = zenoh::open(config).wait().unwrap();
        let endpoint = router.info().locators().wait()[0].to_string();
        Self {
            _router: router,
            endpoint,
        }
    }
    pub(crate) fn transport(&self) -> ZenohTransport {
        let config = zenoh::Config::from_json5(&json!({
            "mode":"client", "connect":{"endpoints":[self.endpoint],"timeout_ms":3000,"exit_on_failure":true},
            "listen":{"endpoints":[]}, "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}},
            "timestamping":{"enabled":true}
        }).to_string()).unwrap();
        ZenohTransport::open(config, 0).unwrap()
    }
    pub(crate) fn context(&self) -> Context {
        Context::new(self.transport())
    }
}
pub(crate) fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "bounded runtime acceptance timed out"
        );
        thread::sleep(Duration::from_millis(5));
    }
}
pub(crate) fn record_options(directory: std::path::PathBuf) -> RecordOptions {
    RecordOptions {
        directory,
        all_topics: true,
        storage: storage_options(),
        message_bytes: 16 * 1024,
        metadata_bytes: 4 * 1024,
        discovery_interval_ms: 20,
        description_timeout_ms: 500,
        ..Default::default()
    }
}

fn bootstrap_context(transport: impl Transport, node: serde_json::Value) -> Context {
    let bootstrap = ProcessBootstrap::from_json(
        &json!({
            "schema_version":1,
            "launch_id":"606c355e-00a9-405b-9f41-c5855426dfe2",
            "process_id":"bag-worker",
            "generation":1,
            "context":{},
            "node":node
        })
        .to_string(),
    )
    .unwrap();
    Context::from_configured_transport(
        transport,
        aos_core::resolve_context_config(&Default::default()).unwrap(),
        Some(bootstrap),
    )
    .unwrap()
}

#[test]
fn process_bootstrap_applies_to_bag_node_identity_and_playback_remaps() {
    let network = Network::new();
    let context = bootstrap_context(
        network.transport(),
        json!({"launch":{"nodes":{
            "aos_bag_recorder":{"name":"launched_recorder","namespace":"/deployed"},
            "aos_bag_player":{
                "name":"launched_player","namespace":"/deployed",
                "remaps":[{"kind":"topic","from":"/replay/data","to":"/replay/launched_data"}]
            }
        }}}),
    );
    let observer_context = network.context();
    let temp = Temp::new();
    let mut options = record_options(temp.0.join("recording"));
    options.node = NodeConfigRequest {
        program: json!({"name":"program_recorder","namespace":"/program"}),
        launch: json!({"summary":"local disjoint launch field"}),
        ..Default::default()
    };
    let recorder = Recorder::start(context.clone(), options).unwrap();
    until(|| {
        observer_context
            .graph(Duration::from_millis(20))
            .unwrap()
            .iter()
            .any(|entity| {
                entity.kind == EntityKind::Node
                    && entity.node_name == "launched_recorder"
                    && entity.namespace == "/deployed"
            })
    });
    let card = observer_context
        .get_node_card("/deployed/launched_recorder", WAIT)
        .unwrap();
    assert_eq!(card["summary"], "local disjoint launch field");
    recorder.stop(RecordStopReason::Requested).unwrap();
    let recorded = recorder.wait(WAIT).unwrap();
    assert_eq!(
        recorded.manifest.configuration["node_fqn"],
        "/deployed/launched_recorder"
    );
    assert!(!context.is_shutdown());

    let source = temp.0.join("playback-source");
    playback_fixture(&source);
    let consumer = Node::new(
        observer_context.clone(),
        "consumer",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let subscription = consumer
        .create_subscription(
            &interface(),
            "/replay/launched_data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let player = Player::open(
        context.clone(),
        &source,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            rate: 1000.0,
            subscriber_wait_ms: 1000,
            node: NodeConfigRequest {
                program: json!({"name":"program_player","namespace":"/program"}),
                launch: json!({"summary":"local playback summary"}),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.status().endpoints[0].target, "/replay/launched_data");
    until(|| {
        observer_context
            .graph(Duration::from_millis(20))
            .unwrap()
            .iter()
            .any(|entity| {
                entity.kind == EntityKind::Publisher
                    && entity.node_name == "launched_player"
                    && entity.namespace == "/deployed"
                    && entity
                        .endpoint
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.name == "/replay/launched_data")
            })
    });
    player.start().unwrap();
    for value in 0..4 {
        assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], value);
    }
    assert_eq!(player.wait(WAIT).unwrap().status.published_total, 4);
    assert!(!context.is_shutdown());
    consumer.destroy_node().unwrap();
    observer_context.shutdown().unwrap();
    context.shutdown().unwrap();
}

struct RejectDeclarations {
    attempts: Arc<AtomicUsize>,
}
impl RejectDeclarations {
    fn reject<T>(&self) -> aos_core::Result<T> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Err(aos_core::Error::Unsupported(
            "unexpected network declaration during preflight".into(),
        ))
    }
}
impl Transport for RejectDeclarations {
    fn create_publisher(&self, _topic: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
        self.reject()
    }
    fn create_subscription(&self, _topic: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.reject()
    }
    fn declare_graph_entity(&self, _entity: &GraphEntity) -> aos_core::Result<Box<dyn GraphGuard>> {
        self.reject()
    }
    fn close(&self) -> aos_core::Result<()> {
        Ok(())
    }
}

#[test]
fn process_bootstrap_preflight_rejects_escape_and_overlap_before_declarations() {
    let temp = Temp::new();
    let source = temp.0.join("playback-source");
    playback_fixture(&source);
    for (name, node, local, clock) in [
        (
            "data-escape",
            json!({"launch":{"remaps":[{"kind":"topic","from":"/replay/data","to":"/live/data"}]}}),
            NodeConfigRequest::default(),
            None,
        ),
        (
            "clock-escape",
            json!({"launch":{"remaps":[{"kind":"topic","from":"/replay/clock","to":"/clock"}]}}),
            NodeConfigRequest::default(),
            Some(PlaybackClockOptions::default()),
        ),
        (
            "same-source-overlap",
            json!({"launch":{"name":"launched"}}),
            NodeConfigRequest {
                launch: json!({"name":"local"}),
                ..Default::default()
            },
            None,
        ),
    ] {
        let attempts = Arc::new(AtomicUsize::new(0));
        let context = bootstrap_context(
            RejectDeclarations {
                attempts: attempts.clone(),
            },
            node,
        );
        let result = Player::open(
            context.clone(),
            &source,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                node: local,
                clock,
                ..Default::default()
            },
        );
        let error = result.err().expect("unsafe configuration was accepted");
        assert_eq!(error.kind, BagErrorKind::InvalidArgument, "{name}: {error}");
        assert_eq!(attempts.load(Ordering::Acquire), 0, "{name}");
        assert!(!context.is_shutdown());
        context.shutdown().unwrap();
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let context = bootstrap_context(
        RejectDeclarations {
            attempts: attempts.clone(),
        },
        json!({"launch":{"name":"launched"}}),
    );
    let directory = temp.0.join("overlapping-recorder");
    let mut options = record_options(directory.clone());
    options.node.launch = json!({"name":"local"});
    let error = Recorder::start(context.clone(), options)
        .err()
        .expect("overlapping recorder configuration was accepted");
    assert_eq!(error.kind, BagErrorKind::InvalidArgument);
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    assert!(!directory.exists());
    context.shutdown().unwrap();
}

#[test]
fn process_bootstrap_recording_remap_cannot_relabel_selected_source() {
    let network = Network::new();
    let provider_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let _publisher = provider
        .create_publisher(
            &interface(),
            "/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let context = bootstrap_context(
        network.transport(),
        json!({"launch":{"remaps":[{"kind":"topic","from":"/data","to":"/other"}]}}),
    );
    let temp = Temp::new();
    let recorder =
        Recorder::start(context.clone(), record_options(temp.0.join("recording"))).unwrap();
    until(|| {
        recorder
            .progress()
            .unresolved_sample
            .iter()
            .any(|endpoint| endpoint.reason.contains("remap changes a selected source"))
    });
    assert_eq!(recorder.progress().state, RecordState::Recording);
    recorder.stop(RecordStopReason::Requested).unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Finished);
    assert!(report.status.error.is_none());
    assert!(report.status.counters.endpoint_rejected_total > 0);
    assert_eq!(report.status.counters.written_total, 0);
    assert!(report.status.active_channels.is_empty());
    assert!(!context.is_shutdown());
    provider.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn real_loopback_dynamic_hashes_offline_inspection_and_original_consumer_replay() {
    let network = Network::new();
    let provider_context = network.context();
    let bag_context = network.context();
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let first_type = interface();
    let first = provider
        .create_publisher(
            &first_type,
            "/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let options = record_options(directory.clone());
    let consumer_qos = options.qos.clone();
    let recorder = Recorder::start(bag_context.clone(), options).unwrap();
    until(|| recorder.status().active_channels.len() == 1);
    first.wait_for_subscribers(WAIT).unwrap();
    let first_bytes = b" { \"value\" : 7, \"text\":\"first\" } \n";
    first.publish_serialized(first_bytes).unwrap();
    until(|| recorder.status().counters.written_total == 1);
    let mut second_definition = first_type.definition().clone();
    second_definition["message"]["properties"]["value"]["maximum"] = json!(100);
    let second_type = Interface::from_json(second_definition).unwrap();
    assert_ne!(first_type.type_hash(), second_type.type_hash());
    let second = provider
        .create_publisher(
            &second_type,
            "/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    until(|| recorder.status().active_channels.len() == 2);
    second.wait_for_subscribers(WAIT).unwrap();
    let second_bytes = b"\n{\"text\":\"second\", \"value\":8}\t";
    second.publish_serialized(second_bytes).unwrap();
    until(|| recorder.status().counters.written_total == 2);
    recorder.stop(RecordStopReason::Requested).unwrap();
    let recorded = recorder.wait(WAIT).unwrap();
    assert_eq!(recorded.status.state, RecordState::Finished, "{recorded:?}");
    assert_eq!(recorded.manifest.counters.written_total, 2);
    assert_eq!(recorded.manifest.counters.writer_queue_dropped_total, 0);
    assert!(recorded.manifest.counters.end_to_end_lost_total.is_none());
    assert!(!bag_context.is_shutdown());
    provider.destroy_node().unwrap();
    let info = inspect(&directory, InspectOptions::default()).unwrap();
    assert_eq!(info.messages, 2);
    assert_eq!(info.channels.len(), 2);
    for shard in &info.manifest.shards {
        let mut reader = crate::reader::BagReader::open(
            &directory.join(&shard.file),
            ReaderLimits::default(),
            false,
        )
        .unwrap();
        while let Some(item) = reader.next_message().unwrap() {
            assert_eq!(
                item.metadata.received.as_ref().unwrap().scope,
                "adapter_delivery"
            );
            let timestamp = item
                .metadata
                .source
                .as_ref()
                .unwrap()
                .timestamp
                .as_ref()
                .unwrap();
            assert_eq!(timestamp.kind, "zenoh_hlc");
            assert!(!timestamp.comparable);
            assert_eq!(item.publish_time, item.log_time);
        }
    }
    let consumer = Node::new(
        provider_context.clone(),
        "consumer",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let first_sub = consumer
        .create_subscription(
            &first_type,
            "/replay/data",
            consumer_qos.clone(),
            EndpointOptions::default(),
        )
        .unwrap();
    let second_sub = consumer
        .create_subscription(
            &second_type,
            "/replay/data",
            consumer_qos,
            EndpointOptions::default(),
        )
        .unwrap();
    let player = Player::open(
        bag_context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            rate: 1000.0,
            subscriber_wait_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.status().endpoints.len(), 2);
    player.start().unwrap();
    assert_eq!(
        first_sub.recv_serialized_timeout(WAIT).unwrap().payload,
        first_bytes
    );
    assert_eq!(
        second_sub.recv_serialized_timeout(WAIT).unwrap().payload,
        second_bytes
    );
    let played = player.wait(WAIT).unwrap();
    assert_eq!(played.status.state, PlaybackState::Finished, "{played:?}");
    assert_eq!(played.status.published_total, 2);
    assert!(!bag_context.is_shutdown());
    consumer.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
    bag_context.shutdown().unwrap();
}

pub(crate) fn playback_fixture(directory: &std::path::Path) {
    let options = storage_options();
    let channel = Arc::new(
        ChannelInfo::new(
            "/data".into(),
            &interface(),
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    );
    let anchor = 1_000_000_000;
    let mut store = StorageWriter::create(
        directory,
        options,
        uuid::Uuid::new_v4().to_string(),
        anchor,
        json!({}),
    )
    .unwrap();
    for (sequence, offset) in [0, 100_000_000, 100_000_000, 1_000_000_000]
        .into_iter()
        .enumerate()
    {
        let mut item = message(&channel, sequence as u64, anchor + offset);
        item.payload = format!(" {{ \"text\":\"sample\", \"value\":{sequence} }}\n").into_bytes();
        item.metadata.recorded_payload_size_bytes = item.payload.len() as u64;
        item.metadata.adapter_payload_size_bytes = item.payload.len() as u64;
        store.append(&item).unwrap();
    }
    store
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap();
}

#[test]
fn real_consumer_clock_pause_rate_and_inclusive_seek_use_the_same_runtime() {
    let network = Network::new();
    let consumer_context = network.context();
    let bag_context = network.context();
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    playback_fixture(&directory);
    let consumer = Node::new_with_config(consumer_context.clone(), "clock_consumer", "/", NodeOptions::default(),
        NodeConfigRequest { program: json!({"use_sim_time":true,"remaps":[{"kind":"topic","from":"/clock","to":"/replay/clock"}]}), ..Default::default() }
    ).unwrap();
    assert!(!consumer.clock().now().unwrap().initialized);
    let subscription = consumer
        .create_subscription(
            &interface(),
            "/replay/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let timer = consumer
        .create_timer(Duration::from_millis(50), None, true)
        .unwrap();
    let player = Player::open(
        bag_context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            start_paused: true,
            clock: Some(PlaybackClockOptions {
                period_ms: 5,
                ..Default::default()
            }),
            subscriber_wait_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.start().unwrap().state, PlaybackState::Paused);
    until(|| consumer.clock().now().unwrap().initialized);
    assert_eq!(consumer.clock().now().unwrap().nanoseconds, 1_000_000_000);
    assert!(!timer.is_ready().unwrap());
    assert!(matches!(
        subscription.recv_timeout(Duration::from_millis(20)),
        Err(aos_core::Error::Timeout)
    ));
    let sought = player.seek(100_000_000).unwrap();
    assert_eq!(sought.state, PlaybackState::Paused);
    assert_eq!(sought.epoch, 1);
    until(|| consumer.clock().now().unwrap().nanoseconds == 1_100_000_000);
    assert!(timer.is_ready().unwrap());
    timer.take_ready().unwrap().unwrap().finish();
    player.resume().unwrap();
    assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], 1);
    assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], 2);
    assert_eq!(player.pause().unwrap().state, PlaybackState::Paused);
    let paused = player.status().position_ns;
    thread::sleep(Duration::from_millis(20));
    assert_eq!(player.status().position_ns, paused);
    assert!(matches!(
        subscription.recv_timeout(Duration::from_millis(20)),
        Err(aos_core::Error::Timeout)
    ));
    player.seek(0).unwrap();
    until(|| consumer.clock().now().unwrap().nanoseconds == 1_000_000_000);
    assert!(consumer
        .clock()
        .take_jump_events()
        .iter()
        .any(|jump| jump.reason == "backward"));
    assert!(!timer.is_ready().unwrap());
    assert_eq!(player.set_rate(100.0).unwrap().rate, 100.0);
    player.resume().unwrap();
    for expected in 0..4 {
        assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], expected);
    }
    let result = player.wait(WAIT).unwrap();
    assert_eq!(result.status.state, PlaybackState::Finished, "{result:?}");
    assert_eq!(result.status.published_total, 6);
    assert!(!consumer_context.is_shutdown() && !bag_context.is_shutdown());
    let beyond = Player::open(
        bag_context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/past_end".into()),
            start_paused: true,
            ..Default::default()
        },
    )
    .unwrap();
    beyond.start().unwrap();
    assert_eq!(
        beyond.seek(1_000_000_001).unwrap().state,
        PlaybackState::Finished
    );
    assert_eq!(beyond.wait(WAIT).unwrap().status.published_total, 0);
    consumer.destroy_node().unwrap();
    consumer_context.shutdown().unwrap();
    bag_context.shutdown().unwrap();
}

#[test]
fn replay_preflight_rejects_unsafe_names_conflicting_qos_and_duplicate_clock() {
    let network = Network::new();
    let context = network.context();
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    playback_fixture(&directory);
    assert!(Player::open(context.clone(), &directory, PlaybackOptions::default()).is_err());
    let escaped = PlaybackOptions {
        namespace: Some("/replay".into()),
        node: NodeConfigRequest {
            program: json!({"remaps":[{"kind":"topic","from":"/replay/data","to":"/original/data"}]}),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(Player::open(context.clone(), &directory, escaped).is_err());
    let clock_collision = PlaybackOptions {
        namespace: Some("/replay".into()),
        clock: Some(PlaybackClockOptions {
            topic: "/data".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(Player::open(context.clone(), &directory, clock_collision).is_err());
    let explicit = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            original_names: true,
            topic_qos: BTreeMap::from([(
                "/data".into(),
                aos_core::qos_preset("sensor_data").unwrap(),
            )]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(explicit.status().endpoints[0].target, "/data");
    assert_eq!(explicit.status().endpoints[0].qos_source, "topic_override");
    assert_eq!(explicit.status().endpoints[0].qos.depth, 5);
    explicit.stop().unwrap();
    assert_eq!(
        explicit.wait(WAIT).unwrap().status.state,
        PlaybackState::Stopped
    );
    assert!(context
        .graph(Duration::from_millis(10))
        .unwrap()
        .iter()
        .all(|entity| entity.node_name != "aos_bag_player"));
    context.shutdown().unwrap();
}

struct PublishGate {
    block_next: AtomicBool,
    entered: Mutex<bool>,
    released: Mutex<bool>,
    published: Mutex<Vec<Vec<u8>>>,
    changed: Condvar,
}
impl PublishGate {
    fn new(block_next: bool) -> Arc<Self> {
        Arc::new(Self {
            block_next: AtomicBool::new(block_next),
            entered: Mutex::new(false),
            released: Mutex::new(false),
            published: Mutex::new(Vec::new()),
            changed: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct ReleaseGate(Arc<PublishGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct GatedTransport {
    inner: ZenohTransport,
    gate: Arc<PublishGate>,
}
struct GatedPublisher {
    inner: Box<dyn PublisherBackend>,
    gate: Arc<PublishGate>,
}
impl PublisherBackend for GatedPublisher {
    fn publish(&self, bytes: &[u8]) -> aos_core::Result<()> {
        if self.gate.block_next.swap(false, Ordering::AcqRel) {
            *self.gate.entered.lock().unwrap() = true;
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
        }
        self.inner.publish(bytes)?;
        self.gate.published.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }
    fn wait_for_subscribers(&self, timeout: Duration) -> aos_core::Result<()> {
        self.inner.wait_for_subscribers(timeout)
    }
    fn close(&self) -> aos_core::Result<()> {
        self.inner.close()
    }
}
impl Transport for GatedTransport {
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
        let publisher = self.inner.create_typed_publisher(endpoint, qos)?;
        if endpoint.name == "/replay/data" {
            Ok(Box::new(GatedPublisher {
                inner: publisher,
                gate: self.gate.clone(),
            }))
        } else {
            Ok(publisher)
        }
    }
    fn create_typed_subscription(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.inner.create_typed_subscription(endpoint, qos)
    }
    fn create_service(&self, endpoint: &Endpoint) -> aos_core::Result<Box<dyn ServiceBackend>> {
        self.inner.create_service(endpoint)
    }
    fn declare_graph_entity(&self, entity: &GraphEntity) -> aos_core::Result<Box<dyn GraphGuard>> {
        self.inner.declare_graph_entity(entity)
    }
    fn graph(&self, timeout: Duration) -> aos_core::Result<Vec<GraphEntity>> {
        self.inner.graph(timeout)
    }
    fn watch_graph(&self) -> aos_core::Result<Option<Box<dyn GraphWatchBackend>>> {
        self.inner.watch_graph()
    }
    fn close(&self) -> aos_core::Result<()> {
        self.gate.release();
        self.inner.close()
    }
}

#[test]
fn blocked_publish_cannot_ack_pause_or_seek_and_cancelled_commands_never_apply_later() {
    let network = Network::new();
    let consumer_context = network.context();
    let gate = PublishGate::new(true);
    let _release = ReleaseGate(gate.clone());
    let context = Context::new(GatedTransport {
        inner: network.transport(),
        gate: gate.clone(),
    });
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    playback_fixture(&directory);
    let consumer = Node::new(
        consumer_context.clone(),
        "consumer",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let subscription = consumer
        .create_subscription(
            &interface(),
            "/replay/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            rate: 0.01,
            control_timeout_ms: 100,
            ..Default::default()
        },
    )
    .unwrap();
    // Avoid using the short command deadline for network discovery.
    until(|| {
        consumer_context
            .graph(Duration::from_millis(20))
            .unwrap()
            .iter()
            .any(|entity| {
                entity.kind == EntityKind::Publisher
                    && entity
                        .endpoint
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.name == "/replay/data")
            })
    });
    player.start().unwrap();
    until(|| *gate.entered.lock().unwrap());
    let started = Instant::now();
    assert_eq!(player.pause().unwrap_err().kind, BagErrorKind::Timeout);
    assert_eq!(
        player.seek(100_000_000).unwrap_err().kind,
        BagErrorKind::Timeout
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(player.status().state, PlaybackState::Playing);
    assert_eq!(player.status().epoch, 0);
    gate.release();
    assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], 0);
    assert_eq!(player.pause().unwrap().state, PlaybackState::Paused);
    assert_eq!(player.status().epoch, 0);
    assert!(matches!(
        subscription.recv_timeout(Duration::from_millis(30)),
        Err(aos_core::Error::Timeout)
    ));
    assert_eq!(player.seek(100_000_000).unwrap().epoch, 1);
    player.set_rate(100.0).unwrap();
    player.resume().unwrap();
    for expected in 1..4 {
        assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], expected);
    }
    assert_eq!(player.wait(WAIT).unwrap().status.published_total, 4);
    consumer.destroy_node().unwrap();
    consumer_context.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn stop_requests_closure_but_wait_cannot_succeed_while_publish_is_blocked() {
    let network = Network::new();
    let consumer_context = network.context();
    let gate = PublishGate::new(true);
    let _release = ReleaseGate(gate.clone());
    let context = Context::new(GatedTransport {
        inner: network.transport(),
        gate: gate.clone(),
    });
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    playback_fixture(&directory);
    let consumer = Node::new(
        consumer_context.clone(),
        "consumer",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let subscription = consumer
        .create_subscription(
            &interface(),
            "/replay/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            rate: 1000.0,
            subscriber_wait_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    player.start().unwrap();
    until(|| *gate.entered.lock().unwrap());
    let started = Instant::now();
    player.stop().unwrap();
    assert_eq!(
        player.wait(Duration::from_millis(20)).unwrap_err().kind,
        BagErrorKind::Timeout
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(player.status().state, PlaybackState::Playing);
    assert!(gate.published.lock().unwrap().is_empty());
    assert!(!context.is_shutdown());
    gate.release();
    let report = player.wait(WAIT).unwrap();
    assert_eq!(report.status.state, PlaybackState::Stopped);
    assert_eq!(report.status.published_total, 1);
    assert_eq!(gate.published.lock().unwrap().len(), 1);
    assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], 0);
    assert!(matches!(
        subscription.recv_timeout(Duration::from_millis(30)),
        Err(aos_core::Error::Timeout)
    ));
    let survivor = Node::new(context.clone(), "after_bag", "/", NodeOptions::default()).unwrap();
    survivor.destroy_node().unwrap();
    consumer.destroy_node().unwrap();
    consumer_context.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn caller_context_shutdown_closes_ready_paused_pending_player_and_drains_recorder() {
    let network = Network::new();
    let observer = network.context();
    let temp = Temp::new();
    let source = temp.0.join("playback-source");
    playback_fixture(&source);
    let consumer = Node::new(observer.clone(), "consumer", "/", NodeOptions::default()).unwrap();
    for state in [
        PlaybackState::Ready,
        PlaybackState::Paused,
        PlaybackState::Playing,
    ] {
        let context = network.context();
        let subscription = consumer
            .create_subscription(
                &interface(),
                "/replay/data",
                QoS::default(),
                EndpointOptions::default(),
            )
            .unwrap();
        let player = Player::open(
            context.clone(),
            &source,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                rate: 0.01,
                start_paused: state == PlaybackState::Paused,
                subscriber_wait_ms: 1000,
                ..Default::default()
            },
        )
        .unwrap();
        if state != PlaybackState::Ready {
            assert_eq!(player.start().unwrap().state, state);
        }
        if state == PlaybackState::Playing {
            assert_eq!(subscription.recv_timeout(WAIT).unwrap()["value"], 0);
        }
        context.shutdown().unwrap();
        let report = player.wait(WAIT).unwrap();
        assert_eq!(report.status.state, PlaybackState::Failed, "{state:?}");
        assert_eq!(
            report.status.error.unwrap().kind,
            BagErrorKind::Closed,
            "{state:?}"
        );
        assert_eq!(
            report.status.published_total,
            u64::from(state == PlaybackState::Playing)
        );
        assert_eq!(player.pause().unwrap_err().kind, BagErrorKind::Closed);
        until(|| {
            observer
                .graph(Duration::from_millis(20))
                .unwrap()
                .iter()
                .all(|entity| entity.node_name != "aos_bag_player")
        });
        subscription.close().unwrap();
    }

    let context = network.context();
    let publisher = consumer
        .create_publisher(
            &interface(),
            "/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let recording = temp.0.join("context-closed-recording");
    let recorder = Recorder::start(context.clone(), record_options(recording.clone())).unwrap();
    until(|| recorder.status().active_channels.len() == 1);
    publisher.wait_for_subscribers(WAIT).unwrap();
    publisher
        .publish_serialized(b" { \"value\":42, \"text\":\"accepted before shutdown\" }\n")
        .unwrap();
    until(|| recorder.status().counters.received_total == 1);
    context.shutdown().unwrap();
    let report = recorder.wait(WAIT).unwrap();
    assert_eq!(report.status.state, RecordState::Finished);
    assert_eq!(
        report.status.stop_reason,
        Some(RecordStopReason::ContextClosed)
    );
    assert!(report.status.error.is_none());
    assert_eq!(report.status.counters.received_total, 1);
    assert_eq!(report.status.counters.written_total, 1);
    assert!(report.manifest.clean_close && !report.manifest.incomplete);
    assert_eq!(
        inspect(&recording, InspectOptions::default())
            .unwrap()
            .messages,
        1
    );
    until(|| {
        observer
            .graph(Duration::from_millis(20))
            .unwrap()
            .iter()
            .all(|entity| entity.node_name != "aos_bag_recorder")
    });
    consumer.destroy_node().unwrap();
    observer.shutdown().unwrap();
}

#[test]
fn multi_shard_type_qos_and_sequence_evolution_seek_replays_in_exact_order() {
    let temp = Temp::new();
    let source = temp.0.join("multi-shard");
    let mut options = storage_options();
    options.messages_per_shard = 8;
    options.total_bytes = 8 * 1024 * 1024;
    let first_type = interface();
    let mut second_definition = first_type.definition().clone();
    second_definition["message"]["properties"]["value"]["maximum"] = json!(1000);
    let second_type = Interface::from_json(second_definition).unwrap();
    let types = [first_type, second_type];
    let mut channels = Vec::new();
    for message_type in &types {
        for depth in [10, 20] {
            channels.push(Arc::new(
                ChannelInfo::new(
                    "/data".into(),
                    message_type,
                    vec![QoS {
                        depth,
                        ..Default::default()
                    }],
                    QoS::default(),
                    None,
                )
                .unwrap(),
            ));
        }
    }
    let anchor = 1_000_000_000;
    let mut writer = StorageWriter::create(
        &source,
        options,
        uuid::Uuid::new_v4().to_string(),
        anchor,
        json!({}),
    )
    .unwrap();
    let payloads: Vec<_> = (0..256)
        .map(|value| {
            format!(" {{ \"text\":\"sample {value}\", \"value\":{value} }}\n").into_bytes()
        })
        .collect();
    for (index, payload) in payloads.iter().enumerate() {
        let channel_index = (index % 2) * 2 + (index / 16) % 2;
        let mut item = message(
            &channels[channel_index],
            (1u64 << 32) - 128 + index as u64,
            anchor + index as u64 * 1_000_000,
        );
        item.payload = payload.clone();
        item.metadata.recorded_payload_size_bytes = payload.len() as u64;
        item.metadata.adapter_payload_size_bytes = payload.len() as u64;
        writer.append(&item).unwrap();
    }
    let manifest = writer
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap();
    assert_eq!(manifest.shards.len(), 32);
    let info = inspect(&source, InspectOptions::default()).unwrap();
    assert_eq!(info.messages, 256);
    assert_eq!(info.channels.len(), 8);
    assert_eq!(
        info.channels
            .iter()
            .map(|channel| channel.sequence_epoch)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 1].into_iter().collect()
    );
    let attempts = Arc::new(AtomicUsize::new(0));
    let preflight_context = Context::new(RejectDeclarations {
        attempts: attempts.clone(),
    });
    assert!(Player::open(
        preflight_context.clone(),
        &source,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            limits: ReaderLimits {
                shards: 31,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .is_err());
    assert_eq!(attempts.load(Ordering::Acquire), 0);
    preflight_context.shutdown().unwrap();

    let network = Network::new();
    let observer = network.context();
    let gate = PublishGate::new(false);
    let context = Context::new(GatedTransport {
        inner: network.transport(),
        gate: gate.clone(),
    });
    let consumer = Node::new(observer.clone(), "consumer", "/", NodeOptions::default()).unwrap();
    let replay_qos = QoS {
        depth: 512,
        ..Default::default()
    };
    let subscriptions: Vec<_> = types
        .iter()
        .map(|message_type| {
            consumer
                .create_subscription(
                    message_type,
                    "/replay/data",
                    replay_qos.clone(),
                    EndpointOptions::default(),
                )
                .unwrap()
        })
        .collect();
    let player = Player::open(
        context.clone(),
        &source,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            start_paused: true,
            rate: 1000.0,
            subscriber_wait_ms: 1000,
            topic_qos: BTreeMap::from([("/data".into(), replay_qos)]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.status().endpoints.len(), 8);
    player.start().unwrap();
    let sought = player.seek(64_000_000).unwrap();
    assert_eq!(sought.state, PlaybackState::Paused);
    assert_eq!(sought.epoch, 1);
    assert!(gate.published.lock().unwrap().is_empty());
    player.resume().unwrap();
    for (index, expected) in payloads.iter().enumerate().skip(64) {
        assert_eq!(
            subscriptions[index % 2]
                .recv_serialized_timeout(WAIT)
                .unwrap()
                .payload,
            *expected
        );
    }
    let report = player.wait(WAIT).unwrap();
    assert_eq!(report.status.state, PlaybackState::Finished);
    assert_eq!(report.status.published_total, 192);
    assert_eq!(*gate.published.lock().unwrap(), payloads[64..]);
    consumer.destroy_node().unwrap();
    observer.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn action_parameter_and_lifecycle_replay_publishes_observations_without_mutating_provider() {
    let network = Network::new();
    let context = network.context();
    let bag_context = network.context();
    let provider = Node::new(context.clone(), "provider", "/", NodeOptions::default()).unwrap();
    let action_provider = Node::new(
        context.clone(),
        "action_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let managed =
        Node::new_lifecycle(context.clone(), "managed", "/", NodeOptions::default()).unwrap();
    let parameters = provider.parameters();
    parameters
        .declare(
            "value",
            ParameterValue::Integer(1),
            ParameterDescriptor::default(),
            false,
        )
        .unwrap();
    provider.enable_parameter_events().unwrap();
    let action = Interface::from_json(json!({
        "name":"test/action/Count","kind":"action",
        "goal":{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false},
        "result":{"type":"object","properties":{"total":{"type":"integer"}},"required":["total"],"additionalProperties":false,"default":{"total":0}},
        "feedback":{"type":"object","properties":{"current":{"type":"integer"}},"required":["current"],"additionalProperties":false}
    })).unwrap();
    let feedback = action_provider
        .create_action_observation_publisher(
            &action,
            "/count",
            ActionObservationKind::Feedback,
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let status = action_provider
        .create_action_observation_publisher(
            &action,
            "/count",
            ActionObservationKind::Status,
            aos_core::action::status_qos(),
            EndpointOptions::default(),
        )
        .unwrap();
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let recorder = Recorder::start(bag_context.clone(), record_options(directory.clone())).unwrap();
    until(|| recorder.status().active_channels.len() == 4);
    feedback.wait_for_subscribers(WAIT).unwrap();
    status.wait_for_subscribers(WAIT).unwrap();
    let feedback_bytes =
        br#" { "goal_id":"00000000000000000000000000000001", "feedback":{"current":7} } "#;
    feedback.publish_serialized(feedback_bytes).unwrap();
    status.publish(&json!({"status_list":[]})).unwrap();
    parameters
        .set_atomically(vec![Parameter::new("value", ParameterValue::Integer(2))])
        .unwrap();
    managed
        .lifecycle()
        .unwrap()
        .begin_transition(LifecycleTransitionId::Configure)
        .unwrap()
        .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
        .unwrap();
    until(|| recorder.status().counters.written_total == 5);
    recorder.stop(RecordStopReason::Requested).unwrap();
    assert_eq!(
        recorder.wait(WAIT).unwrap().status.state,
        RecordState::Finished
    );
    action_provider.destroy_node().unwrap();
    let info = inspect(&directory, InspectOptions::default()).unwrap();
    assert_eq!(info.channels.len(), 4);
    assert_eq!(
        info.channels
            .iter()
            .filter(|channel| channel.action.is_some())
            .count(),
        2
    );
    parameters
        .set_atomically(vec![Parameter::new("value", ParameterValue::Integer(99))])
        .unwrap();
    assert_eq!(
        managed.lifecycle().unwrap().current_state().unwrap(),
        LifecycleState::Inactive
    );
    let observer = Node::new(context.clone(), "observer", "/", NodeOptions::default()).unwrap();
    let feedback_sub = observer
        .create_action_observation_subscription(
            &action,
            "/replay/mapped",
            ActionObservationKind::Feedback,
            aos_core::qos_preset("sensor_data").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let status_sub = observer
        .create_action_observation_subscription(
            &action,
            "/replay/mapped",
            ActionObservationKind::Status,
            aos_core::action::status_qos(),
            EndpointOptions::default(),
        )
        .unwrap();
    let parameter_sub = observer
        .create_subscription(
            &aos_core::parameter_event_interface(),
            "/replay/parameter_events",
            aos_core::qos_preset("sensor_data").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let lifecycle_sub = observer
        .create_subscription(
            &aos_core::lifecycle_event_interface(),
            "/replay/managed/transition_event",
            aos_core::qos_preset("sensor_data").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let player = Player::open(bag_context.clone(), &directory, PlaybackOptions {
        namespace: Some("/replay".into()),
        node: NodeConfigRequest {
            program: json!({"remaps":[{"kind":"action","from":"/replay/count","to":"/replay/mapped"}]}),
            ..Default::default()
        },
        start_paused: true,
        rate: 1000.0,
        subscriber_wait_ms: 1000,
        ..Default::default()
    }).unwrap();
    player.start().unwrap();
    let graph = bag_context.graph(Duration::from_millis(100)).unwrap();
    let replay: Vec<_> = graph
        .iter()
        .filter(|entity| entity.node_name == "aos_bag_player")
        .collect();
    assert_eq!(
        replay
            .iter()
            .filter(|entity| entity.kind == EntityKind::Publisher)
            .count(),
        4
    );
    assert!(replay
        .iter()
        .all(|entity| entity.endpoint.as_ref().is_none_or(|endpoint| {
            !endpoint.name.ends_with("/send_goal")
                && !endpoint.name.ends_with("/get_result")
                && !endpoint.name.ends_with("/cancel_goal")
                && !endpoint.name.ends_with("/set_parameters")
                && !endpoint.name.contains("_aos_bag")
        })));
    player.resume().unwrap();
    assert_eq!(
        feedback_sub.recv_serialized_timeout(WAIT).unwrap().payload,
        feedback_bytes
    );
    assert_eq!(
        status_sub.recv_timeout(WAIT).unwrap(),
        json!({"status_list":[]})
    );
    assert!(parameter_sub.recv_timeout(WAIT).unwrap().is_object());
    lifecycle_sub.recv_timeout(WAIT).unwrap();
    lifecycle_sub.recv_timeout(WAIT).unwrap();
    assert_eq!(player.wait(WAIT).unwrap().status.published_total, 5);
    assert_eq!(
        parameters.get("value").unwrap(),
        ParameterValue::Integer(99)
    );
    assert_eq!(
        managed.lifecycle().unwrap().current_state().unwrap(),
        LifecycleState::Inactive
    );
    observer.destroy_node().unwrap();
    managed.destroy_node().unwrap();
    provider.destroy_node().unwrap();
    context.shutdown().unwrap();
    bag_context.shutdown().unwrap();
}
