#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use aos_core::{Error, Publisher, Subscription};

pub fn config(name: &str) -> zenoh::Config {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name);
    zenoh::Config::from_file(path).expect("checked-in config must parse")
}

/// Checked-in peer configuration pointed at `endpoint`.
pub fn peer_config(endpoint: &str) -> zenoh::Config {
    let mut config = config("zenoh-session.json5");
    config
        .insert_json5("connect/endpoints", &format!(r#"["{endpoint}"]"#))
        .unwrap();
    config
}

/// A loopback port that was free when probed.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Volatile samples published before the subscription reaches the publisher
/// are lost by design, so keep publishing until one arrives or time runs out.
pub fn publish_until_received(
    publisher: &Publisher,
    subscription: &Subscription,
    payload: &[u8],
    limit: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        publisher.publish(payload).unwrap();
        match subscription.recv_timeout(Duration::from_millis(100)) {
            Ok(received) => return Some(received),
            Err(Error::Timeout) => {}
            Err(error) => panic!("receive failed: {error}"),
        }
    }
    None
}
