//! Real adapter admission: no oversized copy, finite queued bytes, old/native
//! entry points and typed Action paths all use the same local policy.
use std::{
    thread,
    time::{Duration, Instant},
};

use aos_core::{
    transport::Transport, ActionClient, ActionObservationKind, ActionServer, ActionServerOptions,
    Context, Endpoint, EndpointOptions, Error, Interface, Node, NodeOptions, QoS, QueueLimits,
};
use aos_zenoh::ZenohTransport;
use serde_json::json;
use zenoh::Wait;

const WAIT: Duration = Duration::from_secs(5);

struct Network {
    _router: zenoh::Session,
    config: zenoh::Config,
}

impl Network {
    fn new() -> Self {
        let router = zenoh::open(
            zenoh::Config::from_json5(
                r#"{
            mode:"router", listen:{endpoints:["tcp/127.0.0.1:0"]},
            scouting:{multicast:{enabled:false},gossip:{enabled:false}}
        }"#,
            )
            .unwrap(),
        )
        .wait()
        .unwrap();
        let endpoint = router.info().locators().wait()[0].to_string();
        let config = zenoh::Config::from_json5(&json!({
            "mode":"client", "connect":{"endpoints":[endpoint],"timeout_ms":3000,"exit_on_failure":true},
            "listen":{"endpoints":[]},
            "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}
        }).to_string()).unwrap();
        Self {
            _router: router,
            config,
        }
    }
    fn transport(&self, limits: QueueLimits) -> ZenohTransport {
        ZenohTransport::open_with_queue_limits(self.config.clone(), 71, limits).unwrap()
    }
}

fn until(mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(
            Instant::now() < deadline,
            "adapter byte admission did not settle"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

fn endpoint() -> Endpoint {
    Endpoint {
        name: "/bytes".into(),
        type_name: "test/msg/Bytes".into(),
        type_hash: "a".repeat(64),
    }
}

#[test]
fn real_typed_and_legacy_subscriptions_admit_before_copy_and_bound_total_bytes() {
    for typed in [false, true] {
        let network = Network::new();
        let sender = network.transport(QueueLimits::default());
        let receiver = network.transport(QueueLimits {
            message_bytes: 8,
            queue_bytes: 10,
        });
        let (publisher, subscription) = if typed {
            (
                sender
                    .create_typed_publisher(&endpoint(), &QoS::default())
                    .unwrap(),
                receiver
                    .create_typed_subscription(&endpoint(), &QoS::default())
                    .unwrap(),
            )
        } else {
            (
                sender.create_publisher("/bytes").unwrap(),
                receiver.create_subscription("/bytes").unwrap(),
            )
        };
        publisher.wait_for_subscribers(WAIT).unwrap();
        for value in 0..3_u8 {
            publisher.publish(&[value; 3]).unwrap();
            until(|| subscription.queue_stats().unwrap().received_total == u64::from(value) + 1);
        }
        publisher.publish(&[9; 9]).unwrap();
        until(|| {
            subscription
                .queue_stats()
                .unwrap()
                .bytes
                .unwrap()
                .oversized_messages_total
                == 1
        });
        assert_eq!(
            (
                subscription.queue_stats().unwrap().queued,
                subscription.queue_stats().unwrap().dropped_total
            ),
            (3, 0)
        );
        publisher.publish(&[4; 8]).unwrap();
        until(|| subscription.queue_stats().unwrap().received_total == 4);
        let stats = subscription.queue_stats().unwrap();
        let bytes = stats.bytes.unwrap();
        assert_eq!((stats.queued, stats.dropped_total), (1, 3));
        assert_eq!(
            (
                bytes.queued_bytes,
                bytes.reserved_bytes,
                bytes.evicted_bytes_total
            ),
            (8, 0, 9)
        );
        assert_eq!(subscription.recv_timeout(Duration::ZERO).unwrap(), [4; 8]);
        subscription.close().unwrap();
        let bytes = subscription.queue_stats().unwrap().bytes.unwrap();
        assert_eq!((bytes.queued_bytes, bytes.reserved_bytes), (0, 0));
    }
}

#[test]
fn real_typed_and_legacy_publishers_reject_before_submitting_the_payload() {
    for typed in [false, true] {
        let network = Network::new();
        let transport = network.transport(QueueLimits {
            message_bytes: 8,
            queue_bytes: 10,
        });
        let (publisher, subscription) = if typed {
            (
                transport
                    .create_typed_publisher(&endpoint(), &QoS::default())
                    .unwrap(),
                transport
                    .create_typed_subscription(&endpoint(), &QoS::default())
                    .unwrap(),
            )
        } else {
            (
                transport.create_publisher("/bytes").unwrap(),
                transport.create_subscription("/bytes").unwrap(),
            )
        };
        publisher.wait_for_subscribers(WAIT).unwrap();
        assert!(
            matches!(publisher.publish(&[0; 9]), Err(Error::QueueOverflow(message))
            if message.contains("was not submitted"))
        );
        assert!(matches!(
            subscription.recv_timeout(Duration::from_millis(50)),
            Err(Error::Timeout)
        ));
        assert_eq!(subscription.queue_stats().unwrap().received_total, 0);
        publisher.publish(&[1; 8]).unwrap();
        assert_eq!(subscription.recv_timeout(WAIT).unwrap(), [1; 8]);
    }
}

#[test]
fn explicit_node_topic_and_action_observation_limits_override_only_the_local_receiver() {
    let network = Network::new();
    let context = Context::new(network.transport(QueueLimits::default()));
    let node = Node::new(
        context.clone(),
        "byte_observer",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let message = Interface::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../interfaces/String.json"),
    )
    .unwrap();
    let limits = QueueLimits {
        message_bytes: 256,
        queue_bytes: 512,
    };
    let subscription = node
        .create_subscription_with_limits(
            &message,
            "topic",
            QoS::default(),
            limits,
            EndpointOptions::default(),
        )
        .unwrap();
    let publisher = node
        .create_publisher(
            &message,
            "topic",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    publisher.wait_for_subscribers(WAIT).unwrap();
    publisher.publish(&json!({"data":"x".repeat(256)})).unwrap();
    until(|| {
        subscription
            .queue_stats()
            .unwrap()
            .bytes
            .unwrap()
            .oversized_messages_total
            == 1
    });
    publisher.publish(&json!({"data":"ok"})).unwrap();
    assert_eq!(
        subscription.recv_timeout(WAIT).unwrap(),
        json!({"data":"ok"})
    );
    let action = Interface::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../interfaces/action/Fibonacci.json"),
    )
    .unwrap();
    let feedback = node
        .create_action_observation_subscription_with_limits(
            &action,
            "observe",
            ActionObservationKind::Feedback,
            QoS::default(),
            limits,
            EndpointOptions::default(),
        )
        .unwrap();
    let feedback_writer = node
        .create_action_observation_publisher(
            &action,
            "observe",
            ActionObservationKind::Feedback,
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    feedback_writer.wait_for_subscribers(WAIT).unwrap();
    feedback_writer
        .publish(&json!({"goal_id":"a".repeat(32),"feedback":{"partial_sequence":vec![7;256]}}))
        .unwrap();
    until(|| {
        feedback
            .queue_stats()
            .unwrap()
            .bytes
            .unwrap()
            .oversized_messages_total
            == 1
    });
    let small = json!({"goal_id":"a".repeat(32),"feedback":{"partial_sequence":[1,2]}});
    feedback_writer.publish(&small).unwrap();
    assert_eq!(feedback.recv_timeout(WAIT).unwrap(), small);
    for queue in [
        subscription.queue_stats().unwrap(),
        feedback.queue_stats().unwrap(),
    ] {
        let bytes = queue.bytes.unwrap();
        assert_eq!(
            (bytes.message_limit_bytes, bytes.queue_limit_bytes),
            (256, 512)
        );
        assert_eq!((bytes.queued_bytes, bytes.reserved_bytes), (0, 0));
    }
    context.shutdown().unwrap();
}

#[test]
fn native_action_goal_feedback_and_result_paths_cannot_bypass_session_limits() {
    let network = Network::new();
    let limit = 256 * 1024;
    let provider_context = Context::new(network.transport(QueueLimits::default()));
    let client_context = Context::new(network.transport(QueueLimits {
        message_bytes: limit,
        queue_bytes: limit * 2,
    }));
    let provider = Node::new(
        provider_context.clone(),
        "byte_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let caller = Node::new(
        client_context.clone(),
        "byte_caller",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let interface = Interface::from_json(json!({
        "name":"test/action/Bytes", "kind":"action",
        "goal":{"type":"object","properties":{"data":{"type":"string"}},"required":["data"],"additionalProperties":false},
        "result":{"type":"object","properties":{"data":{"type":"string"}},"required":["data"],"additionalProperties":false,"default":{"data":""}},
        "feedback":{"type":"object","properties":{"data":{"type":"string"}},"required":["data"],"additionalProperties":false}
    })).unwrap();
    let server = ActionServer::new(
        &provider,
        &interface,
        "bytes",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client =
        ActionClient::new(&caller, &interface, "bytes", EndpointOptions::default()).unwrap();
    assert!(client.wait_for_server(Some(WAIT)).unwrap());
    let large = json!({"data":"x".repeat(limit)});
    assert!(
        matches!(client.send_goal(&large, None, WAIT), Err(Error::QueueOverflow(message))
        if message.contains("was not submitted"))
    );
    assert!(matches!(
        server.take_goal_request(Duration::from_millis(50)),
        Err(Error::Timeout)
    ));
    thread::scope(|scope| {
        let sent = scope.spawn(|| client.send_goal(&json!({"data":"small"}), None, WAIT));
        let goal = server
            .take_goal_request(WAIT)
            .unwrap()
            .decide(true)
            .unwrap()
            .unwrap();
        let accepted = sent.join().unwrap().unwrap();
        assert!(accepted.accepted());
        goal.execute().unwrap();
        goal.publish_feedback(&large).unwrap();
        goal.publish_feedback(&json!({"data":"small"})).unwrap();
        assert_eq!(
            client.recv_feedback(WAIT).unwrap()["feedback"],
            json!({"data":"small"})
        );
        goal.succeed().unwrap();
        goal.finish(Some(&large)).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (action_client, goal_id) = (&client, accepted.goal_id().to_owned());
        scope.spawn(move || {
            let _ = tx.send(action_client.get_result_wait(&goal_id, None));
        });
        let response = rx.recv_timeout(WAIT).unwrap_or_else(|error| {
            client_context.shutdown().unwrap();
            panic!("indefinite result did not settle on byte rejection: {error}");
        });
        assert!(matches!(response, Err(Error::QueueOverflow(message))
            if message.contains("REPLY_MESSAGE_TOO_LARGE") && message.contains("may have executed")));
    });
    client_context.shutdown().unwrap();
    provider_context.shutdown().unwrap();
}
