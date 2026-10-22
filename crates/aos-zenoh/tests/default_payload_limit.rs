//! Compatibility boundary: the original constructor now rejects >16 MiB
//! outgoing payloads on both byte and typed publisher entry points.
use aos_core::{transport::Transport, Endpoint, Error, QoS};
use aos_zenoh::ZenohTransport;

#[test]
fn default_session_rejects_oversized_payload_before_native_or_typed_publish() {
    let config = zenoh::Config::from_json5(
        r#"{
        mode:"peer", listen:{endpoints:["tcp/127.0.0.1:0"]},
        scouting:{multicast:{enabled:false},gossip:{enabled:false}}
    }"#,
    )
    .unwrap();
    let transport = ZenohTransport::open(config, 72).unwrap();
    let endpoint = Endpoint {
        name: "/default_bytes".into(),
        type_name: "test/msg/Bytes".into(),
        type_hash: "a".repeat(64),
    };
    let payload = vec![0; 16 * 1024 * 1024 + 1];
    let publishers = [
        transport.create_publisher("/default_bytes").unwrap(),
        transport
            .create_typed_publisher(&endpoint, &QoS::default())
            .unwrap(),
    ];
    for publisher in publishers {
        assert!(
            matches!(publisher.publish(&payload), Err(Error::QueueOverflow(message))
            if message.contains("was not submitted") && message.contains("16777216"))
        );
    }
    transport.close().unwrap();
}
