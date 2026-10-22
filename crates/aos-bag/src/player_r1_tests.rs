use crate::{
    runtime_tests::playback_fixture,
    storage_tests::{interface, Temp},
    BagErrorKind, PlaybackClockOptions, PlaybackOptions, PlaybackState, Player,
};
use aos_core::{
    transport::{
        GraphGuard, GraphWatchBackend, PublisherBackend, ServiceBackend, SubscriptionBackend,
        Transport,
    },
    Context, Endpoint, EndpointOptions, EntityKind, GraphEntity, Node, NodeOptions, QoS,
};
use aos_zenoh::ZenohTransport;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use zenoh::Wait;

const WAIT: Duration = Duration::from_secs(5);
struct Network {
    _router: zenoh::Session,
    endpoint: String,
}
impl Network {
    fn new() -> Self {
        let config = zenoh::Config::from_json5(
            &json!({
                "mode":"router", "listen":{"endpoints":["tcp/127.0.0.1:0"]},
                "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}
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
    fn transport(&self) -> ZenohTransport {
        let config = zenoh::Config::from_json5(&json!({
            "mode":"client", "connect":{"endpoints":[self.endpoint],"timeout_ms":3000,"exit_on_failure":true},
            "listen":{"endpoints":[]}, "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}
        }).to_string()).unwrap();
        ZenohTransport::open(config, 0).unwrap()
    }
    fn context(&self) -> Context {
        Context::new(self.transport())
    }
}
fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "player regression deadline elapsed"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn ready_seek_does_not_publish_data_or_clock_until_start() {
    let network = Network::new();
    let context = network.context();
    let observer = network.context();
    let node = Node::new(observer.clone(), "consumer", "/", NodeOptions::default()).unwrap();
    let data = node
        .create_subscription(
            &interface(),
            "/replay/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let clock = node
        .create_subscription(
            &aos_core::clock_interface(),
            "/replay/clock",
            aos_core::qos_preset("clock").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            start_paused: true,
            rate: 100.0,
            subscriber_wait_ms: 1000,
            clock: Some(PlaybackClockOptions {
                period_ms: 10,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let status = player.seek(100_000_000).unwrap();
    assert_eq!(status.state, PlaybackState::Ready);
    assert_eq!(status.position_ns, 100_000_000);
    assert_eq!(status.epoch, 1);
    assert!(matches!(
        clock.recv_timeout(Duration::from_millis(60)),
        Err(aos_core::Error::Timeout)
    ));
    assert!(matches!(
        data.recv_timeout(Duration::from_millis(30)),
        Err(aos_core::Error::Timeout)
    ));
    assert_eq!(player.start().unwrap().state, PlaybackState::Paused);
    let time = clock.recv_timeout(WAIT).unwrap();
    assert_eq!(time["clock"]["sec"], 1);
    assert_eq!(time["clock"]["nanosec"], 100_000_000);
    player.resume().unwrap();
    for value in 1..4 {
        assert_eq!(data.recv_timeout(WAIT).unwrap()["value"], value);
    }
    let report = player.wait(WAIT).unwrap();
    assert_eq!(report.status.published_total, 3);
    assert!(report.status.resources_closed && report.status.cleanup_error.is_none());
    node.destroy_node().unwrap();
    observer.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn readiness_wait_uses_its_full_budget_and_identifies_the_unmatched_clock() {
    let network = Network::new();
    let context = network.context();
    let observer = network.context();
    let node = Node::new(observer.clone(), "consumer", "/", NodeOptions::default()).unwrap();
    let data = node
        .create_subscription(
            &interface(),
            "/replay/data",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            start_paused: true,
            subscriber_wait_ms: 300,
            control_timeout_ms: 50,
            clock: Some(PlaybackClockOptions::default()),
            ..Default::default()
        },
    )
    .unwrap();
    let started = Instant::now();
    let error = player.start().unwrap_err();
    assert_eq!(error.kind, BagErrorKind::Timeout);
    assert!(error.message.contains("/replay/clock"), "{error}");
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert_eq!(player.status().state, PlaybackState::Ready);
    assert_eq!(player.status().published_total, 0);
    assert!(matches!(
        data.recv_timeout(Duration::from_millis(30)),
        Err(aos_core::Error::Timeout)
    ));
    let clock = node
        .create_subscription(
            &aos_core::clock_interface(),
            "/replay/clock",
            aos_core::qos_preset("clock").unwrap(),
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(player.start().unwrap().state, PlaybackState::Paused);
    clock.recv_timeout(WAIT).unwrap();
    player.stop().unwrap();
    assert!(player.wait(WAIT).unwrap().status.resources_closed);
    node.destroy_node().unwrap();
    observer.shutdown().unwrap();
    context.shutdown().unwrap();
}

#[test]
fn readiness_without_any_subscriber_keeps_ready_without_side_effects() {
    let network = Network::new();
    let context = network.context();
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/unmatched".into()),
            subscriber_wait_ms: 100,
            control_timeout_ms: 50,
            ..Default::default()
        },
    )
    .unwrap();
    let error = player.start().unwrap_err();
    assert_eq!(error.kind, BagErrorKind::Timeout);
    assert!(error.message.contains("/unmatched/data"), "{error}");
    assert_eq!(player.status().state, PlaybackState::Ready);
    assert_eq!(player.status().published_total, 0);
    player.stop().unwrap();
    assert!(player.wait(WAIT).unwrap().status.resources_closed);
    context.shutdown().unwrap();
}

struct ClockGate {
    block: AtomicBool,
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
    fail_publish: bool,
}
impl ClockGate {
    fn new(fail_publish: bool) -> Arc<Self> {
        Arc::new(Self {
            block: AtomicBool::new(true),
            entered: AtomicBool::new(false),
            released: Mutex::new(false),
            changed: Condvar::new(),
            fail_publish,
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<ClockGate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct HookTransport {
    inner: ZenohTransport,
    gate: Option<Arc<ClockGate>>,
    fail_node_close: bool,
}
struct ClockPublisher {
    inner: Box<dyn PublisherBackend>,
    gate: Arc<ClockGate>,
}
impl PublisherBackend for ClockPublisher {
    fn publish(&self, bytes: &[u8]) -> aos_core::Result<()> {
        if self.gate.block.swap(false, Ordering::AcqRel) {
            self.gate.entered.store(true, Ordering::Release);
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
        }
        if self.gate.fail_publish {
            return Err(aos_core::Error::Transport(
                "injected clock publication failure".into(),
            ));
        }
        self.inner.publish(bytes)
    }
    fn wait_for_subscribers(&self, timeout: Duration) -> aos_core::Result<()> {
        self.inner.wait_for_subscribers(timeout)
    }
    fn close(&self) -> aos_core::Result<()> {
        self.inner.close()
    }
}
struct FailingNodeGuard(Box<dyn GraphGuard>);
impl GraphGuard for FailingNodeGuard {
    fn close(&self) -> aos_core::Result<()> {
        self.0.close()?;
        Err(aos_core::Error::Transport(
            "injected player Node cleanup failure".into(),
        ))
    }
}
impl Transport for HookTransport {
    fn create_publisher(&self, name: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
        self.inner.create_publisher(name)
    }
    fn create_subscription(&self, name: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
        self.inner.create_subscription(name)
    }
    fn create_typed_publisher(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> aos_core::Result<Box<dyn PublisherBackend>> {
        let inner = self.inner.create_typed_publisher(endpoint, qos)?;
        if endpoint.name == "/replay/clock" {
            if let Some(gate) = &self.gate {
                return Ok(Box::new(ClockPublisher {
                    inner,
                    gate: gate.clone(),
                }));
            }
        }
        Ok(inner)
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
        let inner = self.inner.declare_graph_entity(entity)?;
        if self.fail_node_close && entity.kind == EntityKind::Node {
            Ok(Box::new(FailingNodeGuard(inner)))
        } else {
            Ok(inner)
        }
    }
    fn graph(&self, timeout: Duration) -> aos_core::Result<Vec<GraphEntity>> {
        self.inner.graph(timeout)
    }
    fn watch_graph(&self) -> aos_core::Result<Option<Box<dyn GraphWatchBackend>>> {
        self.inner.watch_graph()
    }
    fn close(&self) -> aos_core::Result<()> {
        if let Some(gate) = &self.gate {
            gate.release();
        }
        self.inner.close()
    }
}

#[test]
fn blocked_clock_happens_after_ack_and_its_failure_is_reported_as_playback_failure() {
    let network = Network::new();
    let gate = ClockGate::new(true);
    let _release = Release(gate.clone());
    let context = Context::new(HookTransport {
        inner: network.transport(),
        gate: Some(gate.clone()),
        fail_node_close: false,
    });
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            start_paused: true,
            control_timeout_ms: 100,
            clock: Some(PlaybackClockOptions::default()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.start().unwrap().state, PlaybackState::Paused);
    until(|| gate.entered.load(Ordering::Acquire));
    assert_eq!(player.status().state, PlaybackState::Paused);
    assert_eq!(
        player.wait(Duration::from_millis(30)).unwrap_err().kind,
        BagErrorKind::Timeout
    );
    gate.release();
    let report = player.wait(WAIT).unwrap();
    assert_eq!(report.status.state, PlaybackState::Failed);
    assert_eq!(report.status.error.unwrap().kind, BagErrorKind::Transport);
    assert_eq!(report.status.published_total, 0);
    assert!(report.status.resources_closed);
    context.shutdown().unwrap();
}

#[test]
fn node_cleanup_error_is_visible_without_relabeling_valid_playback_data() {
    let network = Network::new();
    let context = Context::new(HookTransport {
        inner: network.transport(),
        gate: None,
        fail_node_close: true,
    });
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            ..Default::default()
        },
    )
    .unwrap();
    player.stop().unwrap();
    let report = player.wait(WAIT).unwrap();
    assert_eq!(report.status.state, PlaybackState::Failed);
    assert!(report
        .status
        .error
        .as_ref()
        .unwrap()
        .message
        .contains("Node cleanup failure"));
    assert!(!report.status.resources_closed);
    assert!(report
        .status
        .cleanup_error
        .unwrap()
        .message
        .contains("Node cleanup failure"));
    assert_eq!(
        crate::inspect(&directory, Default::default())
            .unwrap()
            .messages,
        4
    );
    context.shutdown().unwrap();
}

#[test]
fn caller_context_shutdown_reports_released_player_and_clock_resources() {
    let network = Network::new();
    let temp = Temp::new();
    let directory = temp.0.join("source");
    playback_fixture(&directory);
    for start in [false, true] {
        let context = network.context();
        let player = Player::open(
            context.clone(),
            &directory,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                start_paused: true,
                clock: Some(PlaybackClockOptions::default()),
                ..Default::default()
            },
        )
        .unwrap();
        if start {
            player.start().unwrap();
        }
        context.shutdown().unwrap();
        let status = player.wait(WAIT).unwrap().status;
        assert_eq!(status.state, PlaybackState::Failed);
        assert_eq!(status.error.unwrap().kind, BagErrorKind::Closed);
        assert!(status.resources_closed);
        assert!(status.cleanup_error.is_none());
    }
}

#[test]
fn player_opens_seventeen_one_mib_schema_shards_with_default_limits() {
    let _fixture = crate::reader::r2_tests::acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("large-source");
    crate::reader::r2_tests::large_schema_fixture(&directory);
    let network = Network::new();
    let context = network.context();
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(player.status().endpoints.len(), 1);
    player.stop().unwrap();
    assert!(player.wait(WAIT).unwrap().status.resources_closed);
    context.shutdown().unwrap();
}

#[test]
fn sparse_multishard_playback_keeps_pause_and_seek_within_control_budget() {
    let _fixture = crate::reader::r2_tests::acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("sparse-source");
    let limits = crate::reader::r2_tests::sparse_fixture(&directory, true);
    let network = Network::new();
    let context = network.context();
    let player = Player::open(
        context.clone(),
        &directory,
        PlaybackOptions {
            namespace: Some("/replay".into()),
            topics: vec!["/rare".into()],
            control_timeout_ms: 300,
            limits,
            ..Default::default()
        },
    )
    .unwrap();
    player.start().unwrap();
    until(|| player.progress().published_total == 1);
    let pause_started = Instant::now();
    assert_eq!(player.pause().unwrap().state, PlaybackState::Paused);
    let pause = pause_started.elapsed();
    assert!(pause < Duration::from_millis(300));
    let seek_started = Instant::now();
    let seek = player.seek(1).unwrap();
    let seek_time = seek_started.elapsed();
    assert_eq!(seek.position_ns, 1);
    assert!(seek_time < Duration::from_millis(300));
    assert_eq!(seek.published_total, 1);
    player.stop().unwrap();
    assert!(player.wait(WAIT).unwrap().status.resources_closed);
    context.shutdown().unwrap();
    println!(
        "sparse controls over 64 MiB irrelevant payload: pause_us={} seek_us={}",
        pause.as_micros(),
        seek_time.as_micros()
    );
}

#[test]
fn many_endpoint_progress_and_full_status_polls_preserve_scheduled_publication_rate() {
    let _fixture = crate::reader::r2_tests::acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("many-endpoints");
    let mut storage = crate::StorageOptions::default();
    storage.limits.shards = 1;
    let mut writer = crate::storage::StorageWriter::create(
        &directory,
        storage,
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    let channels: Vec<_> = (0..128)
        .map(|index| {
            Arc::new(
                crate::ChannelInfo::new(
                    format!("/endpoint_{index}_{}", "x".repeat(128)),
                    &interface(),
                    vec![QoS::default()],
                    QoS::default(),
                    None,
                )
                .unwrap(),
            )
        })
        .collect();
    for sequence in 0..512 {
        writer
            .append(&crate::storage_tests::message(
                &channels[sequence as usize % channels.len()],
                sequence,
                10 + sequence * 1_000_000,
            ))
            .unwrap();
    }
    writer
        .finish(
            crate::RecordStopReason::Requested,
            crate::RecordCounters::default(),
            None,
        )
        .unwrap();
    let network = Network::new();
    let context = network.context();
    let mut elapsed = Vec::new();
    for polling in [false, true] {
        let player = Arc::new(
            Player::open(
                context.clone(),
                &directory,
                PlaybackOptions {
                    namespace: Some("/replay".into()),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let reader = player.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let reader_stopped = stopped.clone();
        let poller = polling.then(|| {
            thread::spawn(move || {
                let mut polls = 0;
                while !reader_stopped.load(Ordering::Acquire) {
                    let progress = reader.progress();
                    assert_eq!(progress.endpoint_count, 128);
                    assert!(serde_json::to_vec(&progress).unwrap().len() < 1024);
                    let full = reader.status();
                    assert_eq!(full.endpoints.len(), 128);
                    assert!(full.endpoints[0].source.len() > 128);
                    polls += 1;
                    thread::yield_now();
                }
                polls
            })
        });
        let started = Instant::now();
        player.start().unwrap();
        let report = player.wait(WAIT).unwrap();
        elapsed.push(started.elapsed());
        stopped.store(true, Ordering::Release);
        if let Some(poller) = poller {
            assert!(poller.join().unwrap() > 100);
        }
        assert_eq!(report.status.published_total, 512);
        assert!(report.status.resources_closed && report.status.error.is_none());
        assert_eq!(report.status.endpoints.len(), 128);
    }
    assert!(
        elapsed[1] <= elapsed[0] + Duration::from_millis(250),
        "baseline={:?} polling={:?}",
        elapsed[0],
        elapsed[1]
    );
    println!("128 endpoints / 512 scheduled publications: baseline_ms={} high_frequency_full_and_progress_polling_ms={}", elapsed[0].as_millis(), elapsed[1].as_millis());
    context.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn playback_and_recovery_of_256_shards_fit_a_256_descriptor_soft_limit() {
    let _fixture = crate::reader::r2_tests::acceptance_fixture_guard();
    const CHILD: &str = "AOS_BAG_R2_FD_LIMIT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", "ulimit -n 256; exec \"$@\"", "aos-bag-fd-test"])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", "player_r1_tests::playback_and_recovery_of_256_shards_fit_a_256_descriptor_soft_limit", "--nocapture"])
            .env(CHILD, "1")
            .output().unwrap();
        println!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let temp = Temp::new();
    let source = temp.0.join("source");
    let target = temp.0.join("target");
    let storage = crate::StorageOptions {
        messages_per_shard: 1,
        ..Default::default()
    };
    let mut writer = crate::storage::StorageWriter::create(
        &source,
        storage.clone(),
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    let channel = Arc::new(
        crate::ChannelInfo::new(
            "/data".into(),
            &interface(),
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    );
    for sequence in 0..256 {
        writer
            .append(&crate::storage_tests::message(
                &channel,
                sequence,
                10 + sequence,
            ))
            .unwrap();
    }
    let manifest = writer
        .finish(
            crate::RecordStopReason::Requested,
            crate::RecordCounters::default(),
            None,
        )
        .unwrap();
    assert_eq!(manifest.shards.len(), 256);
    let network = Network::new();
    let context = network.context();
    let play = (|| -> crate::BagResult<_> {
        let player = Player::open(
            context.clone(),
            &source,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                ..Default::default()
            },
        )?;
        player.start()?;
        player.wait(WAIT)
    })();
    let recovery = crate::recover(&source, &target, crate::RecoveryOptions { storage });
    println!(
        "fd_limit=256 play_result={:?} recover_result={:?}",
        play.as_ref().map(|r| r.status.published_total),
        recovery.as_ref().map(|r| r.info.messages)
    );
    assert_eq!(play.unwrap().status.published_total, 256);
    let recovery = recovery.unwrap();
    assert_eq!(recovery.info.messages, 256);
    assert_eq!(recovery.sources.len(), 256);
    assert!(recovery.sources.iter().all(|s| s.verified_unchanged));
    context.shutdown().unwrap();
}
