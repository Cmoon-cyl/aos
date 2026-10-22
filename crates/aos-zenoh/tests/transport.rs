//! aos-core Context over ZenohTransport, against an in-process Zenoh router.
//! `real_zenohd.rs` repeats the exchange across processes with upstream
//! zenohd.

mod support;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aos_core::{Context, Error};
use aos_zenoh::ZenohTransport;
use support::{free_port, peer_config, publish_until_received};
use zenoh::Wait;

struct Router {
    session: zenoh::Session,
    endpoint: String,
}

impl Router {
    fn start() -> Self {
        let mut config = support::config("zenoh-router.json5");
        config
            .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
            .unwrap();
        let session = zenoh::open(config).wait().expect("router must open");
        let endpoint = session.info().locators().wait()[0].to_string();
        Self { session, endpoint }
    }

    fn transport(&self) -> ZenohTransport {
        let transport = ZenohTransport::open(peer_config(&self.endpoint), 0).expect("peer opens");
        let expected = vec![self.session.info().zid().wait().to_string()];
        // The default peer opens before its background Router connection is
        // ready. Observe that connection before these tests exercise transport.
        assert!(
            wait_until(Duration::from_secs(5), || transport.router_zids()
                == expected),
            "peer did not connect to {expected:?}; observed {:?}",
            transport.router_zids()
        );
        transport
    }

    fn context(&self) -> Context {
        Context::new(self.transport())
    }

    fn peer_count(&self) -> usize {
        self.session.info().peers_zid().wait().count()
    }
}

fn wait_until(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}

#[test]
fn telemetry_is_quiet_by_default_and_shutdown_releases_retained_entity_gauges() {
    use aos_core::transport::{QoS, Transport};
    let router = Router::start();
    let transport = router.transport();
    let telemetry = transport.telemetry();
    let publisher = transport.create_publisher("/metrics").unwrap();
    let subscription = transport.create_subscription("/metrics").unwrap();
    let ep = endpoint("/metrics_service", "demo/srv/Metrics", 'a');
    let service = transport.create_service(&ep).unwrap();
    let client = transport
        .create_client_with_qos(&ep, &QoS::default())
        .unwrap()
        .unwrap();
    let graph = transport
        .declare_graph_entity(&graph_node("metrics_provider"))
        .unwrap();
    let active = || telemetry.snapshot().active_resources.values().sum::<u64>();
    assert_eq!(active(), 6);
    let dropped = transport.create_publisher("/metrics_drop").unwrap();
    assert_eq!(active(), 7);
    drop(dropped);
    assert_eq!(active(), 6);
    let watch = transport.watch_graph().unwrap().unwrap();
    assert_eq!(active(), 7);
    let temporary_watch = transport.watch_graph().unwrap().unwrap();
    assert_eq!(active(), 8);
    drop(temporary_watch);
    assert_eq!(active(), 7, "dropping a watch releases its resource count");
    publisher.publish(b"payload-must-not-be-logged").unwrap();
    assert!(telemetry.snapshot().events_total > 0);
    assert!(!telemetry.snapshot().log_capture_enabled);
    assert!(telemetry.drain_records().is_empty());
    transport.close().unwrap();
    assert_eq!(
        active(),
        0,
        "retained wrappers must not look live after session close"
    );
    assert_eq!(publisher.publish(b"closed"), Err(Error::Closed));
    assert!(matches!(watch.snapshot(), Err(Error::Closed)));
    drop((publisher, subscription, service, client, graph, watch));
    assert_eq!(active(), 0, "Drop after close must not decrement twice");
    assert_eq!(transport.close(), Err(Error::Closed));
    assert_eq!(active(), 0);
}

#[test]
fn two_contexts_exchange_bytes_through_router() {
    let router = Router::start();
    let talker = router.context();
    let listener = router.context();

    let subscription = listener.create_subscription("/chatter").unwrap();
    let publisher = talker.create_publisher("/chatter").unwrap();
    for payload in [&br#"{"data":"hi"}"#[..], b"", &[0xff, 0x00, 0xfe]] {
        let received =
            publish_until_received(&publisher, &subscription, payload, Duration::from_secs(5));
        assert_eq!(received.as_deref(), Some(payload));
        // Drain repeats so the next payload is checked on its own.
        while subscription
            .recv_timeout(Duration::from_millis(100))
            .is_ok()
        {}
    }

    talker.shutdown().unwrap();
    listener.shutdown().unwrap();
}

#[test]
fn different_topics_and_domains_do_not_cross() {
    let router = Router::start();
    let context = router.context();
    let other_domain =
        Context::new(ZenohTransport::open(peer_config(&router.endpoint), 1).unwrap());
    let a = context.create_subscription("/a").unwrap();
    let b = context.create_subscription("/b").unwrap();
    let a_domain1 = other_domain.create_subscription("/a").unwrap();
    let publisher = context.create_publisher("/a").unwrap();
    assert!(publish_until_received(&publisher, &a, b"1", Duration::from_secs(5)).is_some());
    assert_eq!(
        b.recv_timeout(Duration::from_millis(200)),
        Err(Error::Timeout)
    );
    assert_eq!(
        a_domain1.recv_timeout(Duration::from_millis(200)),
        Err(Error::Timeout)
    );
}

#[test]
fn idle_wait_times_out_and_zero_polls() {
    let router = Router::start();
    let context = router.context();
    let subscription = context.create_subscription("/idle").unwrap();
    let started = Instant::now();
    assert_eq!(
        subscription.recv_timeout(Duration::from_millis(300)),
        Err(Error::Timeout)
    );
    assert!(started.elapsed() >= Duration::from_millis(300));
    let started = Instant::now();
    assert_eq!(
        subscription.recv_timeout(Duration::ZERO),
        Err(Error::Timeout)
    );
    assert!(started.elapsed() < Duration::from_millis(50));
}

#[test]
fn receive_queue_keeps_last_ten() {
    let router = Router::start();
    let context = router.context();
    let subscription = context.create_subscription("/depth").unwrap();
    let publisher = context.create_publisher("/depth").unwrap();
    assert!(
        publish_until_received(&publisher, &subscription, b"warmup", Duration::from_secs(5))
            .is_some()
    );
    while subscription
        .recv_timeout(Duration::from_millis(100))
        .is_ok()
    {}

    for i in 0u8..15 {
        publisher.publish(&[i]).unwrap();
    }
    thread::sleep(Duration::from_millis(200));
    let mut received = Vec::new();
    while let Ok(payload) = subscription.recv_timeout(Duration::from_millis(100)) {
        received.extend(payload);
    }
    assert_eq!(received, (5u8..15).collect::<Vec<_>>());
}

#[test]
fn concurrent_receivers_take_each_sample_once() {
    let router = Router::start();
    let context = router.context();
    let subscription = Arc::new(context.create_subscription("/compete").unwrap());
    let publisher = context.create_publisher("/compete").unwrap();
    assert!(
        publish_until_received(&publisher, &subscription, b"warmup", Duration::from_secs(5))
            .is_some()
    );
    while subscription
        .recv_timeout(Duration::from_millis(100))
        .is_ok()
    {}

    let seen = Arc::new(Mutex::new(Vec::new()));
    let receivers: Vec<_> = (0..3)
        .map(|_| {
            let subscription = subscription.clone();
            let seen = seen.clone();
            thread::spawn(move || {
                while let Ok(payload) = subscription.recv_timeout(Duration::from_millis(300)) {
                    seen.lock().unwrap().push(payload[0]);
                }
            })
        })
        .collect();
    // Publish slower than the receivers drain, so the depth-10 queue never
    // drops samples here.
    for i in 0u8..30 {
        publisher.publish(&[i]).unwrap();
        thread::sleep(Duration::from_millis(5));
    }
    for receiver in receivers {
        receiver.join().unwrap();
    }
    let mut seen = seen.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, (0u8..30).collect::<Vec<_>>());
}

#[test]
fn explicit_fail_fast_profile_without_router_obeys_its_connect_window() {
    let endpoint = format!("tcp/127.0.0.1:{}", free_port());
    let started = Instant::now();
    let mut config = peer_config(&endpoint);
    config.insert_json5("connect/timeout_ms", "3000").unwrap();
    config
        .insert_json5("connect/exit_on_failure", "true")
        .unwrap();
    let error = match ZenohTransport::open(config, 0) {
        Ok(_) => panic!("open must fail without a router"),
        Err(error) => error,
    };
    let elapsed = started.elapsed();
    assert!(matches!(error, Error::Transport(_)), "{error}");
    // Explicit fail-fast remains available; the default reconnects in background.
    assert!(elapsed < Duration::from_secs(10), "took {elapsed:?}");
    eprintln!("open without router after {elapsed:?}: {error}");
}

#[test]
fn shutdown_wakes_all_receivers_and_invalidates_entities() {
    let router = Router::start();
    let context = router.context();
    let first = Arc::new(context.create_subscription("/wake/a").unwrap());
    let second = Arc::new(context.create_subscription("/wake/b").unwrap());
    let publisher = context.create_publisher("/wake/a").unwrap();

    let waiters: Vec<_> = [first.clone(), first.clone(), second.clone()]
        .into_iter()
        .map(|subscription| {
            thread::spawn(move || {
                let started = Instant::now();
                (
                    subscription.recv_timeout(Duration::from_secs(30)),
                    started.elapsed(),
                )
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(200));
    context.shutdown().unwrap();
    for waiter in waiters {
        let (result, elapsed) = waiter.join().unwrap();
        assert_eq!(result, Err(Error::Closed));
        assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
    }

    assert_eq!(publisher.publish(b"x"), Err(Error::Closed));
    assert_eq!(first.recv_timeout(Duration::ZERO), Err(Error::Closed));
    assert!(matches!(
        context.create_publisher("/wake/a"),
        Err(Error::Closed)
    ));
    assert!(matches!(
        context.create_subscription("/wake/a"),
        Err(Error::Closed)
    ));
    // Entity close after session close is safe.
    assert!(matches!(publisher.close(), Ok(()) | Err(Error::Closed)));
    assert!(matches!(second.close(), Ok(()) | Err(Error::Closed)));
}

#[test]
fn transport_close_rejects_new_declarations() {
    use aos_core::transport::Transport;

    let router = Router::start();
    let transport = router.transport();
    transport.close().unwrap();
    assert_eq!(transport.close().err(), Some(Error::Closed));
    assert_eq!(transport.create_publisher("/t").err(), Some(Error::Closed));
    assert_eq!(
        transport.create_subscription("/t").err(),
        Some(Error::Closed)
    );
}

#[test]
fn entity_close_wakes_only_its_receivers() {
    let router = Router::start();
    let context = router.context();
    let closing = Arc::new(context.create_subscription("/entity").unwrap());
    let staying = context.create_subscription("/entity").unwrap();
    let publisher = context.create_publisher("/entity").unwrap();

    let waiter = {
        let closing = closing.clone();
        thread::spawn(move || closing.recv_timeout(Duration::from_secs(30)))
    };
    thread::sleep(Duration::from_millis(200));
    closing.close().unwrap();
    assert_eq!(waiter.join().unwrap(), Err(Error::Closed));

    assert!(publish_until_received(&publisher, &staying, b"ok", Duration::from_secs(5)).is_some());
    publisher.close().unwrap();
    assert_eq!(publisher.publish(b"x"), Err(Error::Closed));
    let other = context.create_publisher("/entity").unwrap();
    assert!(publish_until_received(&other, &staying, b"ok", Duration::from_secs(5)).is_some());
}

#[test]
fn declarations_racing_shutdown_end_closed() {
    let router = Router::start();
    let context = router.context();
    let declarers: Vec<_> = (0..4)
        .map(|i| {
            let context = context.clone();
            thread::spawn(move || {
                let mut survivors = Vec::new();
                loop {
                    match context.create_subscription(&format!("/race/t{i}")) {
                        Ok(subscription) => survivors.push(subscription),
                        Err(Error::Closed) => return survivors,
                        Err(error) => panic!("unexpected {error}"),
                    }
                }
            })
        })
        .collect();
    thread::sleep(Duration::from_millis(100));
    context.shutdown().unwrap();
    for declarer in declarers {
        for subscription in declarer.join().unwrap() {
            assert_eq!(
                subscription.recv_timeout(Duration::ZERO),
                Err(Error::Closed)
            );
        }
    }
}

#[test]
fn dropping_last_owner_releases_the_session() {
    let router = Router::start();
    let context = router.context();
    let subscription = context.create_subscription("/release").unwrap();
    assert!(wait_until(Duration::from_secs(5), || router.peer_count() == 1));
    drop(context);
    // The subscription still owns the Context; the session stays open.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(router.peer_count(), 1);
    drop(subscription);
    assert!(
        wait_until(Duration::from_secs(5), || router.peer_count() == 0),
        "router still sees the peer"
    );
}

#[test]
fn aos_keys_are_prefixed_and_isolated_from_unprefixed_keys() {
    let router = Router::start();
    let context = router.context();
    let raw = zenoh::open(peer_config(&router.endpoint)).wait().unwrap();
    let aos_wire = raw
        .declare_subscriber("aos/0/iso/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH")
        .wait()
        .unwrap();
    // The unprefixed rmw_zenoh-style space of domain 0, including the old key.
    let unprefixed = raw.declare_subscriber("0/**").wait().unwrap();
    let old_key = raw
        .declare_publisher("0/iso/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH")
        .wait()
        .unwrap();

    let subscription = context.create_subscription("/iso").unwrap();
    let publisher = context.create_publisher("/iso").unwrap();
    assert!(
        publish_until_received(&publisher, &subscription, b"aos", Duration::from_secs(5)).is_some()
    );
    // The AOS publication arrives on the documented prefixed wire key.
    let deadline = Instant::now() + Duration::from_secs(5);
    let sample = loop {
        if let Some(sample) = aos_wire.try_recv().unwrap() {
            break sample;
        }
        assert!(Instant::now() < deadline, "no sample on the aos/ wire key");
        publisher.publish(b"aos").unwrap();
        thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(sample.payload().to_bytes().as_ref(), b"aos");
    assert_eq!(
        sample.key_expr().as_str(),
        "aos/0/iso/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH"
    );
    while subscription
        .recv_timeout(Duration::from_millis(100))
        .is_ok()
    {}

    for _ in 0..10 {
        publisher.publish(b"aos").unwrap();
        old_key.put(b"old".to_vec()).wait().unwrap();
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(200));
    let mut received = Vec::new();
    while let Ok(payload) = subscription.recv_timeout(Duration::from_millis(100)) {
        received.push(payload);
    }
    assert!(!received.is_empty());
    assert!(
        received.iter().all(|payload| payload == b"aos"),
        "{received:?}"
    );
    // Only the old-key publisher reached the unprefixed space.
    while let Some(sample) = unprefixed.try_recv().unwrap() {
        assert_eq!(sample.payload().to_bytes().as_ref(), b"old");
        assert!(!sample.key_expr().as_str().starts_with("aos/"));
    }
    raw.close().wait().unwrap();
}

fn endpoint(name: &str, type_name: &str, hash: char) -> aos_core::transport::Endpoint {
    aos_core::transport::Endpoint {
        name: name.into(),
        type_name: type_name.into(),
        type_hash: hash.to_string().repeat(64),
    }
}

#[test]
fn typed_keys_separate_type_hash_and_keep_last_depth() {
    use aos_core::transport::{QoS, Reliability, Transport};
    let router = Router::start();
    let talker = router.transport();
    let listener = router.transport();
    let ep = endpoint("/typed", "demo/msg/State", 'a');
    let qos = QoS {
        depth: 2,
        reliability: Reliability::BestEffort,
        ..QoS::default()
    };
    let subscription = listener.create_typed_subscription(&ep, &qos).unwrap();
    let wrong_type = listener
        .create_typed_subscription(&endpoint("/typed", "demo/msg/Other", 'a'), &qos)
        .unwrap();
    let wrong_hash = listener
        .create_typed_subscription(&endpoint("/typed", "demo/msg/State", 'b'), &qos)
        .unwrap();
    let publisher = talker.create_typed_publisher(&ep, &qos).unwrap();
    publisher
        .wait_for_subscribers(Duration::from_secs(5))
        .unwrap();
    publisher.publish(b"first-once").unwrap();
    assert_eq!(
        subscription.recv_timeout(Duration::from_secs(5)).unwrap(),
        b"first-once"
    );
    for i in 0u8..5 {
        publisher.publish(&[i]).unwrap();
    }
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        subscription.recv_timeout(Duration::from_secs(5)).unwrap(),
        vec![3]
    );
    assert_eq!(
        subscription.recv_timeout(Duration::from_secs(5)).unwrap(),
        vec![4]
    );
    assert_eq!(wrong_type.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    assert_eq!(wrong_hash.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    let stats = subscription.queue_stats().unwrap();
    assert_eq!(stats.depth, 2);
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.received_total, 6);
    assert_eq!(stats.dropped_total, 3);
}

#[test]
fn topic_reliability_and_congestion_reach_the_router() {
    use aos_core::transport::{QoS, Reliability, Transport};
    use zenoh::qos::{CongestionControl, Reliability as WireReliability};

    let router = Router::start();
    let talker = router.transport();
    for (index, (reliability, expected_reliability, expected_congestion)) in [
        (
            Reliability::Reliable,
            WireReliability::Reliable,
            CongestionControl::Block,
        ),
        (
            Reliability::BestEffort,
            WireReliability::BestEffort,
            CongestionControl::Drop,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let ep = endpoint(&format!("/wire_qos_{index}"), "demo/msg/State", 'a');
        // Observe an actual remote sample at the router, independently of AOS
        // Graph metadata and the adapter's requested QoS value.
        let subscriber = router
            .session
            .declare_subscriber(format!(
                "aos/0{}/{}/{}",
                ep.name, ep.type_name, ep.type_hash
            ))
            .wait()
            .unwrap();
        let publisher = talker
            .create_typed_publisher(
                &ep,
                &QoS {
                    reliability,
                    ..QoS::default()
                },
            )
            .unwrap();
        publisher
            .wait_for_subscribers(Duration::from_secs(5))
            .unwrap();
        publisher.publish(b"once").unwrap();
        let sample = subscriber
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .expect("router must receive the sample");
        assert_eq!(sample.payload().to_bytes().as_ref(), b"once");
        assert_eq!(sample.reliability(), expected_reliability);
        assert_eq!(sample.congestion_control(), expected_congestion);
    }
}

#[test]
fn transient_local_late_join_reads_publisher_cache_and_cache_dies_with_publisher() {
    use aos_core::transport::{Durability, QoS, Transport};
    let router = Router::start();
    let talker = router.transport();
    let ep = endpoint("/state", "demo/msg/State", 'a');
    let qos = QoS {
        depth: 2,
        durability: Durability::TransientLocal,
        ..QoS::default()
    };
    let publisher = talker.create_typed_publisher(&ep, &qos).unwrap();
    for i in 0u8..5 {
        publisher.publish(&[i]).unwrap();
    }
    let listener = router.transport();
    let subscription = listener.create_typed_subscription(&ep, &qos).unwrap();
    assert_eq!(
        subscription.recv_timeout(Duration::from_secs(5)).unwrap(),
        vec![3]
    );
    assert_eq!(
        subscription.recv_timeout(Duration::from_secs(5)).unwrap(),
        vec![4]
    );
    let volatile = listener
        .create_typed_subscription(&ep, &QoS::default())
        .unwrap();
    assert_eq!(
        volatile.recv_timeout(Duration::from_millis(100)),
        Err(Error::Timeout)
    );
    publisher.close().unwrap();
    let after_owner = router.transport();
    let after = after_owner.create_typed_subscription(&ep, &qos).unwrap();
    assert_eq!(
        after.recv_timeout(Duration::from_millis(200)),
        Err(Error::Timeout)
    );
}

#[test]
fn matching_wait_is_bounded_and_close_interrupts_it() {
    use aos_core::transport::{PublisherBackend, Transport};
    let router = Router::start();
    let transport = router.transport();
    let publisher: Arc<dyn PublisherBackend> =
        transport.create_publisher("/no_subscriber").unwrap().into();
    assert_eq!(
        publisher.wait_for_subscribers(Duration::from_millis(50)),
        Err(Error::Timeout)
    );
    let waiting = publisher.clone();
    let waiter = thread::spawn(move || waiting.wait_for_subscribers(Duration::from_secs(30)));
    thread::sleep(Duration::from_millis(50));
    publisher.close().unwrap();
    assert_eq!(waiter.join().unwrap(), Err(Error::Closed));
}

fn graph_node(id: &str) -> aos_core::transport::GraphEntity {
    use aos_core::transport::{EntityKind, GraphEntity};
    GraphEntity {
        id: id.into(),
        node_id: id.into(),
        node_name: "robot".into(),
        namespace: "/demo".into(),
        kind: EntityKind::Node,
        endpoint: None,
        qos: None,
    }
}

#[test]
fn graph_late_join_and_entity_lifetime_follow_liveliness() {
    use aos_core::transport::{EntityKind, QoS, Transport};
    let router = Router::start();
    let owner = router.transport();
    let node = graph_node("node1");
    let mut publisher = node.clone();
    publisher.id = "pub1".into();
    publisher.kind = EntityKind::Publisher;
    publisher.endpoint = Some(endpoint("/demo/chatter", "demo/msg/State", 'a'));
    publisher.qos = Some(QoS::default());
    let node_guard = owner.declare_graph_entity(&node).unwrap();
    let pub_guard = owner.declare_graph_entity(&publisher).unwrap();
    let observer = router.transport();
    assert!(wait_until(Duration::from_secs(5), || observer
        .graph(Duration::from_millis(300))
        .unwrap()
        .len()
        == 2));
    let entities = observer.graph(Duration::from_secs(1)).unwrap();
    assert!(entities.contains(&node));
    assert!(entities.contains(&publisher));
    let other_domain = ZenohTransport::open(peer_config(&router.endpoint), 1).unwrap();
    assert!(other_domain
        .graph(Duration::from_millis(100))
        .unwrap()
        .is_empty());
    pub_guard.close().unwrap();
    assert!(wait_until(Duration::from_secs(5), || observer
        .graph(Duration::from_millis(300))
        .unwrap()
        == vec![node.clone()]));
    drop(node_guard);
    assert!(wait_until(Duration::from_secs(5), || observer
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
}

#[test]
fn remote_graph_keeps_unsupported_qos_unknown_without_hiding_the_entity() {
    use aos_core::transport::Transport;
    let router = Router::start();
    let token = router
        .session
        .liveliness()
        .declare_token(format!(
            "aos/@lv/0/foreign/n1/e1/MP/%/%/foreign/%foreign/data%msg%Value/{}/1:2:1,10:123,0:,:,,",
            "a".repeat(64)
        ))
        .wait()
        .unwrap();
    let observer = router.transport();
    let mut found = None;
    assert!(wait_until(Duration::from_secs(5), || {
        found = observer
            .graph(Duration::from_millis(300))
            .unwrap()
            .into_iter()
            .find(|entity| entity.id == "e1");
        found.is_some()
    }));
    let found = found.unwrap();
    assert_eq!(found.endpoint.unwrap().name, "/foreign");
    assert_eq!(found.qos, None);
    drop(token);
}

#[test]
fn node_service_qos_reaches_real_queues_graph_and_node_card() {
    use aos_core::{resolve_qos, EndpointOptions, EntityKind, Interface, Node, NodeOptions};
    use serde_json::json;
    let router = Router::start();
    let provider_context = router.context();
    let caller_context = router.context();
    let provider = Node::new(
        provider_context,
        "qos_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let caller = Node::new(
        caller_context.clone(),
        "qos_caller",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let interface = Interface::from_json(json!({
        "name":"test/srv/Queue","kind":"srv",
        "request":{"type":"object","properties":{"number":{"type":"integer"}},"required":["number"],"additionalProperties":false},
        "response":{"type":"object","properties":{"number":{"type":"integer"}},"required":["number"],"additionalProperties":false}
    })).unwrap();
    let server_qos = resolve_qos(&json!({"depth":1}), "services_default", true).unwrap();
    let client_qos = resolve_qos(&json!({"depth":2}), "services_default", true).unwrap();
    let service = provider
        .create_service_with_qos(
            &interface,
            "/queued",
            server_qos.clone(),
            EndpointOptions::default(),
        )
        .unwrap();
    let client = Arc::new(
        caller
            .create_client_with_qos(
                &interface,
                "/queued",
                client_qos.clone(),
                EndpointOptions::default(),
            )
            .unwrap(),
    );
    assert!(client
        .wait_for_service(Some(Duration::from_secs(5)))
        .unwrap());
    assert_eq!(client.queue_stats().unwrap().depth, 2);
    let mut calls = Vec::new();
    for number in 1..=3 {
        let client = client.clone();
        calls.push(thread::spawn(move || {
            client.call(&json!({"number":number}), Duration::from_secs(5))
        }));
        assert!(wait_until(Duration::from_secs(5), || {
            service.queue_stats().unwrap().received_total == number
        }));
    }
    let request = service.recv_timeout(Duration::ZERO).unwrap();
    assert_eq!(request.request(), &json!({"number":3}));
    request.reply(&json!({"number":3})).unwrap();
    for (index, pending) in calls.into_iter().enumerate() {
        if index == 2 {
            assert_eq!(pending.join().unwrap(), Ok(json!({"number":3})));
        } else {
            assert!(matches!(
                pending.join().unwrap(),
                Err(Error::QueueOverflow(message))
                    if message.starts_with("SERVICE_QUEUE_OVERFLOW:")
                        && message.contains("no automatic retry")
            ));
        }
    }
    let stats = service.queue_stats().unwrap();
    assert_eq!(stats.depth, 1);
    assert_eq!(stats.received_total, 3);
    assert_eq!(stats.dropped_total, 2);
    assert_eq!(stats.queued, 0);

    let graph = caller_context.graph(Duration::from_secs(1)).unwrap();
    for (kind, profile) in [
        (EntityKind::Service, &server_qos.profile),
        (EntityKind::Client, &client_qos.profile),
    ] {
        let entity = graph
            .iter()
            .find(|entity| {
                entity.kind == kind
                    && entity
                        .endpoint
                        .as_ref()
                        .is_some_and(|ep| ep.name == "/queued")
            })
            .unwrap();
        assert_eq!(entity.qos.as_ref(), Some(profile));
        // Liveliness carries effective fields, not the remote owner's input
        // sources. It cannot claim which preset or override that owner used.
        let remote_qos = serde_json::to_value(entity).unwrap()["qos"].clone();
        assert!(remote_qos.get("sources").is_none());
        assert!(remote_qos.get("preset").is_none());
    }
    for (node, expected) in [(&provider, &server_qos), (&caller, &client_qos)] {
        let card = node.node_card();
        let entity = card["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entity| entity["name"] == "/queued")
            .unwrap();
        assert_eq!(entity["qos"], expected.effective_value());
        assert_eq!(entity["qos"]["sources"]["depth"], "override");
    }
    caller.destroy_node().unwrap();
    assert!(client.queue_stats().unwrap().closed);
}

#[test]
fn action_five_profiles_reach_graph_and_late_joiner_receives_real_status() {
    use aos_core::{
        ActionClient, ActionQos, ActionServer, ActionServerOptions, Durability, EndpointOptions,
        EntityKind, Interface, Node, NodeOptions,
    };
    use serde_json::json;
    let router = Router::start();
    let provider_context = router.context();
    let caller_context = router.context();
    let provider = Node::new(
        provider_context,
        "action_qos_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let caller = Node::new(
        caller_context.clone(),
        "action_qos_caller",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let interface = Interface::from_json(json!({
        "name":"test/action/QosCount","kind":"action",
        "goal":{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false},
        "result":{"type":"object","properties":{"total":{"type":"integer"}},"required":["total"],"additionalProperties":false,"default":{"total":0}},
        "feedback":{"type":"object","properties":{"current":{"type":"integer"}},"required":["current"],"additionalProperties":false}
    })).unwrap();
    let qos = ActionQos::from_json(&json!({
        "goal_service":{"depth":2},
        "result_service":{"depth":3},
        "cancel_service":{"depth":4},
        "feedback":{"depth":5},
        "status":{"depth":6}
    }))
    .unwrap();
    let server = ActionServer::new_with_qos(
        &provider,
        &interface,
        "/count",
        ActionServerOptions::default(),
        EndpointOptions::default(),
        qos.clone(),
    )
    .unwrap();
    let client = ActionClient::new_with_qos(
        &caller,
        &interface,
        "/count",
        EndpointOptions::default(),
        qos.clone(),
    )
    .unwrap();
    assert!(client
        .wait_for_server(Some(Duration::from_secs(5)))
        .unwrap());
    let goal = thread::scope(|scope| {
        let sent =
            scope.spawn(|| client.send_goal(&json!({"count":3}), None, Duration::from_secs(5)));
        let request = server.take_goal_request(Duration::from_secs(5)).unwrap();
        let goal = request.decide(true).unwrap().unwrap();
        assert!(sent.join().unwrap().unwrap().accepted());
        goal
    });
    goal.execute().unwrap();
    goal.succeed().unwrap();
    goal.finish(Some(&json!({"total":3}))).unwrap();

    // The actual Action status is already published before this subscriber
    // exists; a generic transient-local Topic test cannot establish this.
    let late_context = router.context();
    let late_node =
        Node::new(late_context, "late_action_qos", "/", NodeOptions::default()).unwrap();
    let late = ActionClient::new_with_qos(
        &late_node,
        &interface,
        "/count",
        EndpointOptions::default(),
        qos.clone(),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = late
            .recv_status(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if status["status_list"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["goal_info"]["goal_id"] == goal.goal_id() && item["status"] == 4)
        {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(
        late.get_result_wait(goal.goal_id(), None).unwrap(),
        json!({"status":4,"result":{"total":3}})
    );
    let profiles = [
        (
            "send_goal",
            &qos.goal_service,
            EntityKind::Service,
            EntityKind::Client,
        ),
        (
            "get_result",
            &qos.result_service,
            EntityKind::Service,
            EntityKind::Client,
        ),
        (
            "cancel_goal",
            &qos.cancel_service,
            EntityKind::Service,
            EntityKind::Client,
        ),
        (
            "feedback",
            &qos.feedback,
            EntityKind::Publisher,
            EntityKind::Subscription,
        ),
        (
            "status",
            &qos.status,
            EntityKind::Publisher,
            EntityKind::Subscription,
        ),
    ];
    // Service matching and a completed Action do not imply that every
    // liveliness token has reached the caller's router view yet.
    let mut graph = Vec::new();
    assert!(
        wait_until(Duration::from_secs(5), || {
            graph = caller_context.graph(Duration::from_millis(200)).unwrap();
            profiles
                .iter()
                .all(|(suffix, _, server_kind, client_kind)| {
                    let name = format!("/count/_action/{suffix}");
                    [(&provider, server_kind), (&caller, client_kind)]
                        .iter()
                        .all(|(owner, kind)| {
                            graph.iter().any(|entity| {
                                entity.node_id == owner.id()
                                    && entity.kind == **kind
                                    && entity.endpoint.as_ref().is_some_and(|ep| ep.name == name)
                            })
                        })
                })
        }),
        "all five server and client endpoints must become visible: {graph:?}"
    );
    for (suffix, profile, server_kind, client_kind) in profiles {
        let name = format!("/count/_action/{suffix}");
        for (owner, kind) in [(&provider, server_kind), (&caller, client_kind)] {
            let entity = graph
                .iter()
                .find(|entity| {
                    entity.node_id == owner.id()
                        && entity.kind == kind
                        && entity.endpoint.as_ref().is_some_and(|ep| ep.name == name)
                })
                .unwrap();
            assert_eq!(entity.qos.as_ref(), Some(&profile.profile));
            let card = owner.node_card();
            let entity = card["endpoints"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entity| entity["name"] == name)
                .unwrap();
            assert_eq!(entity["qos"], profile.effective_value());
        }
    }
    assert_eq!(qos.status.profile.durability, Durability::TransientLocal);
    server.close().unwrap();
}

#[test]
fn service_owned_requests_reply_once_and_errors_are_distinct() {
    use aos_core::transport::Transport;
    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint("/echo", "demo/srv/Echo", 'a');
    let service = provider.create_service(&ep).unwrap();
    let guard = provider
        .declare_graph_entity(&graph_node("provider1"))
        .unwrap();
    let client = Arc::new(router.transport());
    assert!(wait_until(Duration::from_secs(5), || !client
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
    for failing in [false, true] {
        let client = client.clone();
        let ep = ep.clone();
        let pending = thread::spawn(move || client.call(&ep, b"request", Duration::from_secs(5)));
        let request = service.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(request.payload(), b"request");
        // The request remains valid outside the receiving call and on another thread.
        thread::spawn(move || {
            request
                .reply(if failing {
                    Err(Error::Validation("bad input".into()))
                } else {
                    Ok(b"response".to_vec())
                })
                .unwrap();
            assert_eq!(request.reply(Ok(b"duplicate".to_vec())), Err(Error::Closed));
        })
        .join()
        .unwrap();
        let result = pending.join().unwrap();
        if failing {
            assert!(matches!(result, Err(Error::Remote(message)) if message.contains("bad input")));
        } else {
            assert_eq!(result, Ok(b"response".to_vec()));
        }
    }
    drop(guard);
}

#[test]
fn service_timeout_does_not_retry_and_close_wakes_requests_and_calls() {
    use aos_core::transport::{ServiceBackend, Transport};
    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint("/slow", "demo/srv/Echo", 'a');
    let service: Arc<dyn ServiceBackend> = provider.create_service(&ep).unwrap().into();
    let _guard = provider
        .declare_graph_entity(&graph_node("provider2"))
        .unwrap();
    let client = Arc::new(router.transport());
    assert!(wait_until(Duration::from_secs(5), || !client
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
    let pending = {
        let client = client.clone();
        let ep = ep.clone();
        thread::spawn(move || client.call(&ep, b"once", Duration::from_millis(200)))
    };
    let held_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(held_request.payload(), b"once");
    assert_eq!(pending.join().unwrap(), Err(Error::Timeout));
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(100)),
        Err(Error::Timeout)
    ));
    drop(held_request);
    let pending = {
        let client = client.clone();
        let ep = ep.clone();
        thread::spawn(move || client.call(&ep, b"close", Duration::from_secs(30)))
    };
    let held_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    client.close().unwrap();
    assert_eq!(pending.join().unwrap(), Err(Error::Closed));
    let waiting = service.clone();
    let receiver = thread::spawn(move || waiting.recv_timeout(Duration::from_secs(30)).err());
    service.close().unwrap();
    assert_eq!(receiver.join().unwrap(), Some(Error::Closed));
    assert_eq!(held_request.reply(Ok(Vec::new())), Err(Error::Closed));
}

#[test]
fn client_cancel_interrupts_only_its_wait_without_resubmitting() {
    use aos_core::transport::Transport;
    use std::sync::atomic::{AtomicBool, Ordering};
    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint("/cancel_wait", "demo/srv/Echo", 'a');
    let service = provider.create_service(&ep).unwrap();
    let _guard = provider
        .declare_graph_entity(&graph_node("provider3"))
        .unwrap();
    let client = Arc::new(router.transport());
    assert!(wait_until(Duration::from_secs(5), || !client
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
    let cancel = Arc::new(AtomicBool::new(false));
    let pending = {
        let client = client.clone();
        let cancel = cancel.clone();
        let ep = ep.clone();
        thread::spawn(move || {
            client.call_cancelable(&ep, b"once", Duration::from_secs(30), &cancel)
        })
    };
    let held_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(held_request.payload(), b"once");
    let started = Instant::now();
    cancel.store(true, Ordering::Release);
    assert_eq!(pending.join().unwrap(), Err(Error::Closed));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(100)),
        Err(Error::Timeout)
    ));
    // The session remains available for unrelated calls; cancellation did not close it.
    assert!(!client.graph(Duration::from_secs(1)).unwrap().is_empty());
    drop(held_request);
}

#[test]
fn service_client_factories_apply_depth_and_reject_unsupported_policies() {
    use aos_core::transport::{Durability, QoS, Reliability, Transport};
    let router = Router::start();
    let transport = router.transport();
    let ep = endpoint("/factory_qos", "demo/srv/Echo", 'a');
    let qos = QoS {
        depth: 2,
        ..QoS::default()
    };
    let service = transport.create_service_with_qos(&ep, &qos).unwrap();
    let client = transport
        .create_client_with_qos(&ep, &qos)
        .unwrap()
        .unwrap();
    assert_eq!(service.queue_stats().unwrap().depth, 2);
    assert_eq!(client.queue_stats().unwrap().depth, 2);
    for unsupported in [
        QoS {
            reliability: Reliability::BestEffort,
            ..QoS::default()
        },
        QoS {
            durability: Durability::TransientLocal,
            ..QoS::default()
        },
    ] {
        assert!(matches!(
            transport.create_service_with_qos(&ep, &unsupported),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            transport.create_client_with_qos(&ep, &unsupported),
            Err(Error::Unsupported(_))
        ));
    }
    let zero_depth = QoS {
        depth: 0,
        ..QoS::default()
    };
    assert!(matches!(
        transport.create_service_with_qos(&ep, &zero_depth),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        transport.create_client_with_qos(&ep, &zero_depth),
        Err(Error::InvalidArgument(_))
    ));
    transport.close().unwrap();
    assert!(service.queue_stats().unwrap().closed);
    assert!(client.queue_stats().unwrap().closed);
    assert_eq!(
        client.call_wait(
            b"closed",
            Some(Duration::ZERO),
            &std::sync::atomic::AtomicBool::new(false)
        ),
        Err(Error::Closed)
    );
}

#[test]
fn persistent_client_timeout_never_retries_and_transport_close_interrupts_wait() {
    use aos_core::transport::{ClientBackend, QoS, Transport};
    use std::sync::atomic::AtomicBool;
    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint("/persistent", "demo/srv/Echo", 'a');
    let service = provider.create_service(&ep).unwrap();
    let _guard = provider
        .declare_graph_entity(&graph_node("persistent_provider"))
        .unwrap();
    let transport = router.transport();
    assert!(wait_until(Duration::from_secs(5), || !transport
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
    let client: Arc<dyn ClientBackend> = transport
        .create_client_with_qos(&ep, &QoS::default())
        .unwrap()
        .unwrap()
        .into();
    assert!(matches!(
        client.call_wait(b"no-deadline", None, &AtomicBool::new(false)),
        Err(Error::Unsupported(_))
    ));
    let pending = {
        let client = client.clone();
        thread::spawn(move || {
            client.call_wait(
                b"once",
                Some(Duration::from_millis(200)),
                &AtomicBool::new(false),
            )
        })
    };
    let held_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(held_request.payload(), b"once");
    assert_eq!(pending.join().unwrap(), Err(Error::Timeout));
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(100)),
        Err(Error::Timeout)
    ));
    drop(held_request);
    let pending = {
        let client = client.clone();
        thread::spawn(move || {
            client.call_wait(
                b"close",
                Some(Duration::from_secs(30)),
                &AtomicBool::new(false),
            )
        })
    };
    let held_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(held_request.payload(), b"close");
    let started = Instant::now();
    transport.close().unwrap();
    assert_eq!(pending.join().unwrap(), Err(Error::Closed));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(service.queue_stats().unwrap().received_total, 2);
    assert_eq!(client.queue_stats().unwrap().queued, 0);
    assert!(client.queue_stats().unwrap().closed);
}

#[test]
fn persistent_get_result_wait_survives_the_default_query_timeout() {
    use aos_core::transport::{QoS, Transport};
    use std::sync::{atomic::AtomicBool, mpsc};
    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint(
        "/persistent_action/_action/get_result",
        "demo/srv/GetResult",
        'a',
    );
    let service = provider.create_service(&ep).unwrap();
    let _guard = provider
        .declare_graph_entity(&graph_node("persistent_action_provider"))
        .unwrap();
    let transport = router.transport();
    assert!(wait_until(Duration::from_secs(5), || !transport
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));
    let client = transport
        .create_client_with_qos(&ep, &QoS::default())
        .unwrap()
        .unwrap();
    let (returned, result) = mpsc::channel();
    let waiting = thread::spawn(move || {
        returned
            .send(client.call_wait(b"result-once", None, &AtomicBool::new(false)))
            .unwrap();
    });
    let request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(request.payload(), b"result-once");
    assert_eq!(
        result.recv_timeout(Duration::from_secs(11)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    request.reply(Ok(b"completed".to_vec())).unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(b"completed".to_vec())
    );
    waiting.join().unwrap();
    assert_eq!(service.queue_stats().unwrap().received_total, 1);
}

#[test]
fn get_result_wait_survives_default_query_timeout_and_close_interrupts_it() {
    use aos_core::transport::Transport;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    let router = Router::start();
    let provider = router.transport();
    let ep = endpoint("/work/_action/get_result", "demo/srv/GetResult", 'a');
    let service = provider.create_service(&ep).unwrap();
    let _guard = provider
        .declare_graph_entity(&graph_node("action_provider"))
        .unwrap();
    let client = Arc::new(router.transport());
    assert!(wait_until(Duration::from_secs(5), || !client
        .graph(Duration::from_millis(300))
        .unwrap()
        .is_empty()));

    assert!(matches!(
        client.call_wait(
            &endpoint("/ordinary", "demo/srv/Echo", 'a'),
            b"request",
            None,
            &AtomicBool::new(false),
        ),
        Err(Error::Unsupported(_))
    ));

    let (returned, result) = mpsc::channel();
    let waiting = {
        let client = client.clone();
        let ep = ep.clone();
        thread::spawn(move || {
            returned
                .send(client.call_wait(&ep, b"one-result-request", None, &AtomicBool::new(false)))
                .unwrap();
        })
    };
    let request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(request.payload(), b"one-result-request");
    // Hold the owned request beyond Zenoh's ordinary 10-second query deadline.
    assert_eq!(
        result.recv_timeout(Duration::from_secs(11)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    request.reply(Ok(b"completed".to_vec())).unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(b"completed".to_vec())
    );
    waiting.join().unwrap();
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(100)),
        Err(Error::Timeout)
    ));

    let (returned, result) = mpsc::channel();
    let waiting = {
        let client = client.clone();
        thread::spawn(move || {
            returned
                .send(client.call_wait(&ep, b"close-wait", None, &AtomicBool::new(false)))
                .unwrap();
        })
    };
    let request = service.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(request.payload(), b"close-wait");
    let started = Instant::now();
    client.close().unwrap();
    assert_eq!(
        result.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(Error::Closed)
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    waiting.join().unwrap();
    drop(request);
}
