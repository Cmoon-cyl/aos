//! Receive timing and source evidence through real pure clients and a router.
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use aos_core::transport::{SubscriptionBackend, Transport};
use aos_core::{
    Context, Durability, Endpoint, EndpointOptions, Error, Interface, Node, NodeOptions, QoS,
    ReceiveScope,
};
use aos_zenoh::ZenohTransport;
use serde_json::json;
use zenoh::Wait;

const LIMIT: Duration = Duration::from_secs(5);

struct Router {
    session: zenoh::Session,
    endpoint: String,
}

impl Router {
    fn start(timestamping: bool) -> Self {
        let config = zenoh::Config::from_json5(
            &json!({
                "mode":"router",
                "listen":{"endpoints":["tcp/127.0.0.1:0"]},
                "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}},
                "timestamping":{"enabled":timestamping}
            })
            .to_string(),
        )
        .unwrap();
        let session = zenoh::open(config).wait().unwrap();
        let endpoint = session.info().locators().wait()[0].to_string();
        Self { session, endpoint }
    }

    fn client_config(&self, timestamping: bool) -> zenoh::Config {
        zenoh::Config::from_json5(
            &json!({
                "mode":"client",
                "connect":{"endpoints":[self.endpoint],"timeout_ms":3000,"exit_on_failure":true},
                "listen":{"endpoints":[]},
                "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}},
                "timestamping":{"enabled":timestamping}
            })
            .to_string(),
        )
        .unwrap()
    }

    fn transport(&self, timestamping: bool) -> ZenohTransport {
        ZenohTransport::open(self.client_config(timestamping), 0).unwrap()
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    while !condition() {
        assert!(Instant::now() < deadline, "transport readiness deadline");
        thread::sleep(Duration::from_millis(2));
    }
}

fn wait_received(subscription: &dyn SubscriptionBackend, count: u64) {
    wait_until(|| subscription.queue_stats().unwrap().received_total >= count);
}

fn endpoint() -> Endpoint {
    Endpoint {
        name: "/metadata".into(),
        type_name: "test/msg/Metadata".into(),
        type_hash: "a".repeat(64),
    }
}

#[test]
fn callback_times_and_original_utf8_bytes_survive_delayed_dequeue() {
    let router = Router::start(true);
    let sender = router.transport(true);
    let receiver = router.transport(true);
    let qos = QoS {
        depth: 4,
        ..QoS::default()
    };
    let subscription = receiver
        .create_typed_subscription(&endpoint(), &qos)
        .unwrap();
    let publisher = sender.create_typed_publisher(&endpoint(), &qos).unwrap();
    publisher.wait_for_subscribers(LIMIT).unwrap();
    let payloads = [
        " { \"data\" : \"你好\" } \n",
        "{\"data\":\"two\"}",
        " \n {\"data\": \"third\"} \n",
    ];
    let started = Instant::now();
    let started_system = SystemTime::now();
    for (index, payload) in payloads.iter().enumerate() {
        publisher.publish(payload.as_bytes()).unwrap();
        wait_received(subscription.as_ref(), index as u64 + 1);
        if index + 1 < payloads.len() {
            thread::sleep(Duration::from_millis(60));
        }
    }
    let all_queued = Instant::now();
    let all_queued_system = SystemTime::now();
    thread::sleep(Duration::from_millis(150));
    let dequeue_started = Instant::now();
    let mut previous = None;
    for payload in payloads {
        let sample = subscription.recv_sample_timeout(Duration::ZERO).unwrap();
        assert_eq!(sample.payload, payload.as_bytes());
        assert_eq!(sample.payload_size_bytes, payload.len());
        let received = sample.received.unwrap();
        assert_eq!(received.scope, ReceiveScope::AdapterDelivery);
        assert!(received.steady >= started && received.steady <= all_queued);
        assert!(received.system >= started_system && received.system <= all_queued_system);
        assert!(dequeue_started.duration_since(received.steady) >= Duration::from_millis(150));
        if let Some(previous) = previous {
            assert!(received.steady.duration_since(previous) >= Duration::from_millis(50));
        }
        previous = Some(received.steady);
    }
    assert_eq!(subscription.queue_stats().unwrap().dropped_total, 0);
}

#[test]
fn keep_last_retains_matching_metadata_and_both_close_paths_wake_receivers() {
    let router = Router::start(true);
    for close_session in [false, true] {
        let sender = router.transport(true);
        let receiver = router.transport(true);
        let qos = QoS {
            depth: 2,
            ..QoS::default()
        };
        let subscription: Arc<dyn SubscriptionBackend> = Arc::from(
            receiver
                .create_typed_subscription(&endpoint(), &qos)
                .unwrap(),
        );
        let publisher = sender.create_typed_publisher(&endpoint(), &qos).unwrap();
        publisher.wait_for_subscribers(LIMIT).unwrap();
        let mut bounds = Vec::new();
        for byte in 0..4 {
            let before = Instant::now();
            publisher.publish(&[byte]).unwrap();
            wait_received(subscription.as_ref(), u64::from(byte) + 1);
            bounds.push((before, Instant::now()));
        }
        let queued = subscription.queue_stats().unwrap();
        assert_eq!((queued.depth, queued.queued), (2, 2));
        assert_eq!((queued.received_total, queued.dropped_total), (4, 2));
        let retained = subscription.recv_sample_timeout(Duration::ZERO).unwrap();
        assert_eq!(retained.payload, [2]);
        assert_eq!(retained.payload_size_bytes, 1);
        let time = retained.received.unwrap().steady;
        assert!(time >= bounds[2].0 && time <= bounds[2].1);
        // The old and new APIs consume the same queue, with no duplicated data.
        assert_eq!(subscription.recv_timeout(Duration::ZERO).unwrap(), [3]);
        let waiting = subscription.clone();
        let (ready, started) = mpsc::channel();
        let waiter = thread::spawn(move || {
            ready.send(()).unwrap();
            waiting.recv_sample_timeout(Duration::from_secs(30))
        });
        started.recv_timeout(LIMIT).unwrap();
        thread::sleep(Duration::from_millis(20));
        let closing = Instant::now();
        if close_session {
            receiver.close().unwrap();
        } else {
            subscription.close().unwrap();
        }
        assert!(matches!(waiter.join().unwrap(), Err(Error::Closed)));
        assert!(closing.elapsed() < Duration::from_secs(1));
        let closed = subscription.queue_stats().unwrap();
        assert!(closed.closed);
        assert_eq!(closed.queued, 0);
        assert_eq!((closed.received_total, closed.dropped_total), (4, 2));
    }
}

#[test]
fn typed_decode_preserves_serialized_size_and_still_rejects_invalid_messages() {
    let router = Router::start(true);
    let sender = router.transport(true);
    let context = Context::new(router.transport(true));
    let node = Node::new(context, "observer", "/", NodeOptions::default()).unwrap();
    let interface = Interface::from_json(json!({
        "name":"test/msg/Metadata",
        "kind":"msg",
        "message":{
            "type":"object",
            "properties":{"data":{"type":"string"}},
            "required":["data"],
            "additionalProperties":false
        }
    }))
    .unwrap();
    let subscription = node
        .create_subscription(
            &interface,
            "/metadata",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    // Use the raw transport publisher to preserve whitespace and UTF-8 bytes.
    let publisher = sender
        .create_typed_publisher(subscription.endpoint(), &QoS::default())
        .unwrap();
    publisher.wait_for_subscribers(LIMIT).unwrap();
    let payload = " \n { \"data\" : \"你好\" } \t";
    publisher.publish(payload.as_bytes()).unwrap();
    let sample = subscription.recv_sample_timeout(LIMIT).unwrap();
    assert_eq!(sample.payload, json!({"data":"你好"}));
    assert_eq!(sample.payload_size_bytes, payload.len());
    assert_ne!(
        sample.payload_size_bytes,
        serde_json::to_vec(&sample.payload).unwrap().len()
    );
    assert_eq!(
        sample.received.unwrap().scope,
        ReceiveScope::AdapterDelivery
    );
    for invalid in [b"{\"data\":42}".as_slice(), &[0xff]] {
        publisher.publish(invalid).unwrap();
        assert!(matches!(
            subscription.recv_sample_timeout(LIMIT),
            Err(Error::Validation(_))
        ));
    }
    publisher.publish(b"{\"data\":\"legacy\"}").unwrap();
    assert_eq!(
        subscription.recv_timeout(LIMIT).unwrap(),
        json!({"data":"legacy"})
    );
}

#[test]
fn router_added_hlc_is_not_publisher_time_or_clock_comparability() {
    for router_timestamping in [false, true] {
        let router = Router::start(router_timestamping);
        let sender = router.transport(false);
        let receiver = router.transport(false);
        let subscription = receiver.create_subscription("/metadata").unwrap();
        let publisher = sender.create_publisher("/metadata").unwrap();
        publisher.wait_for_subscribers(LIMIT).unwrap();
        publisher.publish(b"one").unwrap();
        let sample = subscription.recv_sample_timeout(LIMIT).unwrap();
        assert!(sample.received.is_some());
        if router_timestamping {
            let source = sample.source.unwrap();
            assert!(source.publisher_id.is_none());
            assert!(source.sequence.is_none());
            let timestamp = source.timestamp.unwrap();
            assert_eq!(timestamp.kind, "zenoh_hlc");
            assert_eq!(timestamp.origin, "router_or_publisher");
            assert_eq!(
                timestamp.clock_id,
                router.session.new_timestamp().get_id().to_string()
            );
            assert!(timestamp.value.parse::<u64>().is_ok());
            assert!(!timestamp.comparable);
        } else {
            assert!(sample.source.is_none());
        }
    }
}

#[test]
fn source_info_and_raw_hlc_are_preserved_as_separate_evidence() {
    let router = Router::start(true);
    let sender = zenoh::open(router.client_config(true)).wait().unwrap();
    let receiver = router.transport(true);
    let subscription = receiver.create_subscription("/metadata").unwrap();
    let publisher = sender
        .declare_publisher("aos/0/metadata/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH")
        .wait()
        .unwrap();
    wait_until(|| publisher.matching_status().wait().unwrap().matching());
    let timestamp = sender.new_timestamp();
    let publisher_id = publisher.id();
    publisher
        .put(b"source".as_slice())
        .timestamp(timestamp)
        .source_info(Some(zenoh::sample::SourceInfo::new(publisher_id, 73)))
        .wait()
        .unwrap();
    let sample = subscription.recv_sample_timeout(LIMIT).unwrap();
    let source = sample.source.unwrap();
    assert_eq!(
        source.publisher_id,
        Some(format!("{}/{}", publisher_id.zid(), publisher_id.eid()))
    );
    assert_eq!(source.sequence, Some(73));
    let reported = source.timestamp.unwrap();
    assert_eq!(reported.value, timestamp.get_time().as_u64().to_string());
    assert_eq!(reported.clock_id, timestamp.get_id().to_string());
    assert!(!reported.comparable);
    assert_eq!(reported.origin, "router_or_publisher");
}

#[test]
fn cached_history_has_new_delivery_time_without_claiming_new_publication() {
    let router = Router::start(true);
    let sender = router.transport(true);
    let receiver = router.transport(true);
    let qos = QoS {
        depth: 1,
        durability: Durability::TransientLocal,
        ..QoS::default()
    };
    let publisher = sender.create_typed_publisher(&endpoint(), &qos).unwrap();
    publisher.publish(b"history").unwrap();
    thread::sleep(Duration::from_millis(100));
    let subscribing = Instant::now();
    let subscription = receiver
        .create_typed_subscription(&endpoint(), &qos)
        .unwrap();
    let sample = subscription.recv_sample_timeout(LIMIT).unwrap();
    assert_eq!(sample.payload, b"history");
    let received = sample.received.unwrap();
    assert_eq!(received.scope, ReceiveScope::AdapterDelivery);
    assert!(received.steady >= subscribing);
    assert!(!sample.source.unwrap().timestamp.unwrap().comparable);
}
