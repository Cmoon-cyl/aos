use std::{path::PathBuf, time::Duration};

use zenoh::Wait;
use zenoh_ext::{
    AdvancedPublisherBuilderExt, AdvancedSubscriberBuilderExt, CacheConfig, HistoryConfig,
    MissDetectionConfig, RecoveryConfig,
};

fn config(name: &str) -> zenoh::Config {
    let name = if cfg!(windows) && name == "zenoh-router.json5" {
        "zenoh-router-windows.json5"
    } else {
        name
    };
    raw_config(name)
}

fn raw_config(name: &str) -> zenoh::Config {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name);
    zenoh::Config::from_file(path).expect("checked-in config must parse with pinned Zenoh")
}

#[test]
fn windows_router_changes_only_the_listener_address() {
    let mut common = raw_config("zenoh-router.json5");
    common
        .insert_json5("listen/endpoints", r#"["tcp/0.0.0.0:7447"]"#)
        .unwrap();
    assert_eq!(
        serde_json::to_value(common).unwrap(),
        serde_json::to_value(raw_config("zenoh-router-windows.json5")).unwrap()
    );
}

#[test]
fn peer_configuration_is_accepted_by_zenoh() {
    config("zenoh-session.json5");
}

#[test]
fn all_interface_router_listener_accepts_explicit_ipv4_connections() {
    let mut router_config = config("zenoh-router.json5");
    let listener = if cfg!(windows) {
        r#"["tcp/0.0.0.0:0"]"#
    } else {
        r#"["tcp/[::]:0"]"#
    };
    router_config
        .insert_json5("listen/endpoints", listener)
        .unwrap();
    let router = zenoh::open(router_config).wait().unwrap();
    let locators = router.info().locators().wait();
    let locator = locators
        .first()
        .expect("Router listener locator")
        .to_string();
    let port = locator.rsplit(':').next().unwrap();
    let client = zenoh::open(zenoh::Config::from_json5(&format!(r#"{{
        mode:"client",connect:{{endpoints:["tcp/127.0.0.1:{port}"],timeout_ms:2000,exit_on_failure:true}},
        listen:{{endpoints:[]}},scouting:{{multicast:{{enabled:false}},gossip:{{enabled:false}}}}
    }}"#)).unwrap()).wait().unwrap();
    assert!(client
        .info()
        .routers_zid()
        .wait()
        .any(|zid| zid == router.zid()));
    client.close().wait().unwrap();
    router.close().wait().unwrap();
}

#[test]
fn default_gossip_connects_local_peers_after_a_late_router_and_survives_its_exit() {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("tcp/127.0.0.1:{}", reservation.local_addr().unwrap().port());
    drop(reservation);
    let mut peer_config = config("zenoh-session.json5");
    peer_config
        .insert_json5(
            "connect/endpoints",
            &serde_json::json!([endpoint]).to_string(),
        )
        .unwrap();
    let first = zenoh::open(peer_config.clone()).wait().unwrap();
    let second = zenoh::open(peer_config).wait().unwrap();
    assert_eq!(first.info().routers_zid().wait().count(), 0);
    let mut router_config = config("zenoh-router.json5");
    router_config
        .insert_json5(
            "listen/endpoints",
            &serde_json::json!([endpoint]).to_string(),
        )
        .unwrap();
    let router = zenoh::open(router_config).wait().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while !first
        .info()
        .peers_zid()
        .wait()
        .any(|zid| zid == second.zid())
    {
        assert!(
            std::time::Instant::now() < deadline,
            "gossip did not create a direct link"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let sub = second
        .declare_subscriber("aos/default_topology")
        .wait()
        .unwrap();
    let published = first
        .declare_publisher("aos/default_topology")
        .wait()
        .unwrap();
    while !published.matching_status().wait().unwrap().matching() {
        assert!(
            std::time::Instant::now() < deadline,
            "subscription did not propagate"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    router.close().wait().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while first.info().routers_zid().wait().count() != 0
        || second.info().routers_zid().wait().count() != 0
    {
        assert!(
            std::time::Instant::now() < deadline,
            "Router disconnect did not propagate"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    published.put("direct-after-router-exit").wait().unwrap();
    assert_eq!(
        sub.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "direct-after-router-exit"
    );
    drop(sub);
    drop(published);
    first.close().wait().unwrap();
    second.close().wait().unwrap();
}

#[test]
fn best_effort_transient_local_reaches_late_joiner() {
    check_transient_local_late_joiner(false);
}

#[test]
fn reliable_transient_local_reaches_late_joiner() {
    check_transient_local_late_joiner(true);
}

fn check_transient_local_late_joiner(reliable: bool) {
    let mut router_config = config("zenoh-router.json5");
    // Let the OS allocate the test port; the distributed sample keeps port 7447.
    router_config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .unwrap();
    let router = zenoh::open(router_config).wait().expect("router must open");
    let locators = router.info().locators().wait();
    assert_eq!(locators.len(), 1);
    assert!(locators[0].to_string().starts_with("tcp/127.0.0.1:"));
    let mut peer_config = config("zenoh-session.json5");
    peer_config
        .insert_json5("connect/endpoints", &format!(r#"["{}"]"#, locators[0]))
        .unwrap();
    let peer = zenoh::open(peer_config.clone())
        .wait()
        .expect("publisher peer must connect");
    assert!(peer
        .info()
        .routers_zid()
        .wait()
        .any(|id| id == router.info().zid().wait()));
    let mut publisher = peer
        .declare_publisher("aos/config_probe")
        .advanced()
        .publisher_detection()
        .cache(CacheConfig::default().max_samples(1));
    // Hiroz's best-effort mapping uses cache without miss detection. That path
    // requires timestamping; the reliable mapping uses sequence numbers.
    if reliable {
        publisher = publisher.sample_miss_detection(
            MissDetectionConfig::default().sporadic_heartbeat(Duration::from_millis(500)),
        );
    }
    let publisher = publisher
        .wait()
        .expect("transient-local publisher must open with the checked-in configuration");
    publisher.put("first").wait().unwrap();
    publisher.put("last").wait().unwrap();

    let late_peer = zenoh::open(peer_config)
        .wait()
        .expect("late subscriber peer must connect");
    let mut subscriber = late_peer
        .declare_subscriber("aos/config_probe")
        .advanced()
        .history(
            HistoryConfig::default()
                .detect_late_publishers()
                .max_samples(1),
        )
        .subscriber_detection();
    if reliable {
        subscriber = subscriber.recovery(RecoveryConfig::default().heartbeat());
    }
    let subscriber = subscriber.wait().expect("history subscriber must open");
    let sample = subscriber
        .recv_timeout(Duration::from_secs(5))
        .expect("history receiver must remain connected")
        .expect("late subscriber must receive the cached sample");
    assert_eq!(sample.payload().try_to_string().unwrap(), "last");
    drop(subscriber);
    drop(publisher);
    late_peer.close().wait().expect("late peer must close");
    peer.close().wait().expect("peer must close");
    router.close().wait().expect("router must close");
}
