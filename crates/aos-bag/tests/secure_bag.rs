//! Secure Bag business effects with the same native Router/compiler as the CLI.
//! Test-only tracing ties ACL decisions to actual intercepted wire messages.
#[path = "support/replay_routes.rs"]
mod replay_routes;
#[allow(dead_code)]
#[path = "../../aos-zenoh/tests/support/security.rs"]
mod support;

use aos_bag::{
    inspect, InspectOptions, PlaybackOptions, Player, RecordOptions, RecordStopReason, Recorder,
};
use aos_core::{
    Context, ContextConfigRequest, EndpointOptions, Interface, Node, NodeConfigRequest,
    NodeOptions, QoS,
};
use aos_zenoh::security::{compile_policy, open_secure_context, router_config, SecurityPolicy};
use serde_json::{json, Value};
use std::{
    fs,
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use support::{AclLogs, Certificates};
use zenoh::Wait;

const WAIT: Duration = Duration::from_secs(6);
const COUNT: u64 = 8;

fn until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "secure Bag propagation timed out"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn bag_node(target: Option<&str>) -> NodeConfigRequest {
    let mut program = json!({"name":"operator", "namespace":"/secure"});
    if let Some(target) = target {
        program["remaps"] = json!([
            {"kind":"topic", "from":"/replay/secure/allowed", "to":target}
        ]);
    }
    NodeConfigRequest {
        program,
        ..Default::default()
    }
}

fn key(topic: &str, interface: &Interface) -> String {
    format!(
        "aos/29/{}/{}/{}",
        topic.trim_start_matches('/'),
        interface.type_name(),
        interface.type_hash()
    )
}

#[test]
fn acl_denials_require_exact_key_and_adjacent_events_on_the_same_thread() {
    use std::io::Write;

    const KEY: &str = "aos/29/replay/denied/type/hash";
    let decision = |thread: u32, key: &str| {
        format!("TRACE rx ThreadId({thread}) zenoh::net::routing::interceptor::access_control: subject is unauthorized to Put (ingress) on {key}\n")
    };
    let intercepted = |thread: u32| {
        format!("TRACE rx ThreadId({thread}) zenoh::net::routing::interceptor: Msg intercepted!\n")
    };
    let mut logs = AclLogs::default();
    let text = decision(1, &format!("{KEY}/other"))
        + &intercepted(1)
        + &decision(2, KEY)
        + &intercepted(3)
        + &decision(4, KEY)
        + "TRACE rx ThreadId(4) zenoh::net::routing::interceptor::access_control: unrelated event\n"
        + &intercepted(4)
        + &decision(5, KEY) // Cache decision alone is not a wire interception.
        + &decision(6, KEY) + &intercepted(6);
    logs.write_all(text.as_bytes()).unwrap();
    let pairs = logs.message_denials(KEY, "Put (ingress)");
    assert_eq!(pairs.len(), 1);
    assert!(pairs[0].contains("ThreadId(6)"));
    assert!(logs.message_denials(KEY, "Subscriber").is_empty());
}

#[test]
fn mtls_record_and_replay_obey_current_subject_and_final_key() {
    let logs = AclLogs::install_with_routing();
    let certificates = Certificates::new();
    for name in ["server", "provider", "operator"] {
        certificates.issue(name, name, "ca", "IP:127.0.0.1,DNS:localhost", false);
    }
    let policy: SecurityPolicy = serde_json::from_str(include_str!(
        "../../../python/tests/fixtures/secure_bag_policy.json"
    ))
    .unwrap();
    let compiled = compile_policy(&policy).unwrap();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let endpoint = format!("tls/127.0.0.1:{port}");
    let router =
        zenoh::open(router_config(&certificates.router(&endpoint, "server"), &compiled).unwrap())
            .wait()
            .unwrap();
    let context = |name: &str| -> Context {
        open_secure_context(
            ContextConfigRequest::default(),
            &certificates.client(&endpoint, name, 29),
        )
        .unwrap()
    };
    // Separate native client sessions ensure the control crosses the Router.
    let provider_context = context("provider");
    let observer_context = context("provider");
    let restricted_context = context("operator");
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/secure",
        NodeOptions::default(),
    )
    .unwrap();
    let observer = Node::new(
        observer_context.clone(),
        "observer",
        "/secure",
        NodeOptions::default(),
    )
    .unwrap();
    let interface = Interface::from_json(
        serde_json::from_str(include_str!("../../../interfaces/String.json")).unwrap(),
    )
    .unwrap();
    let allowed = provider
        .create_publisher(
            &interface,
            "/secure/allowed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let denied = provider
        .create_publisher(
            &interface,
            "/secure/denied",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let denied_control = observer
        .create_subscription(
            &interface,
            "/secure/denied",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    denied.wait_for_subscribers(WAIT).unwrap();
    let directory = certificates.directory().join("recording");
    let recorder = Recorder::start(
        restricted_context.clone(),
        RecordOptions {
            directory: directory.clone(),
            node: bag_node(None),
            topics: vec!["/secure/allowed".into(), "/secure/denied".into()],
            discovery_interval_ms: 20,
            description_timeout_ms: 1000,
            ..Default::default()
        },
    )
    .unwrap();
    until(|| recorder.status().active_channels.len() == 2);
    assert!(recorder.status().unresolved.is_empty());
    allowed.wait_for_subscribers(WAIT).unwrap();
    for index in 0..COUNT {
        allowed
            .publish(&json!({"data":format!("record-allowed-{index}")}))
            .unwrap();
        denied
            .publish(&json!({"data":format!("record-denied-{index}")}))
            .unwrap();
        assert_eq!(
            denied_control.recv_timeout(WAIT).unwrap()["data"],
            format!("record-denied-{index}")
        );
    }
    until(|| recorder.status().counters.written_total == COUNT);
    // Keep the authorized source active in the same bounded observation window.
    thread::sleep(Duration::from_millis(250));
    recorder.stop(RecordStopReason::Requested).unwrap();
    let recorded = recorder.wait(WAIT).unwrap();
    assert!(recorded.status.resources_closed);
    assert!(recorded.status.cleanup_error.is_none());
    assert!(recorded.status.error.is_none());
    assert!(recorded.manifest.clean_close);
    assert!(!restricted_context.is_shutdown());
    let info = inspect(&directory, InspectOptions::default()).unwrap();
    assert_eq!(info.messages, COUNT);
    assert_eq!(info.channels.len(), 2);
    let mut actual = Vec::new();
    for shard in &info.manifest.shards {
        let bytes = fs::read(directory.join(&shard.file)).unwrap();
        for item in mcap::MessageStream::new(&bytes).unwrap() {
            let item = item.unwrap();
            if item.channel.metadata["aos.channel_role"] == "business" {
                assert_eq!(item.channel.topic, "/secure/allowed");
                actual.push(
                    serde_json::from_slice::<Value>(&item.data).unwrap()["data"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
        }
    }
    assert_eq!(
        actual,
        (0..COUNT)
            .map(|index| format!("record-allowed-{index}"))
            .collect::<Vec<_>>()
    );
    let record_denials = logs.message_denials(&key("/secure/denied", &interface), "Subscriber");
    assert!(
        !record_denials.is_empty(),
        "record denial needs an actual intercepted DeclareSubscriber: {}",
        logs.text()
    );

    let replay_allowed = observer
        .create_subscription(
            &interface,
            "/replay/allowed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let replay_denied = observer
        .create_subscription(
            &interface,
            "/replay/denied",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let control = provider
        .create_publisher(
            &interface,
            "/replay/denied",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    control.wait_for_subscribers(WAIT).unwrap();
    control
        .publish(&json!({"data":"authorized-before"}))
        .unwrap();
    assert_eq!(
        replay_denied.recv_timeout(WAIT).unwrap()["data"],
        "authorized-before"
    );
    let mut replay_reports = Vec::new();
    let denied_key = key("/replay/denied", &interface);
    for target in ["/replay/allowed", "/replay/denied"] {
        let player = Player::open(
            restricted_context.clone(),
            &directory,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                topics: vec!["/secure/allowed".into()],
                node: bag_node(Some(target)),
                rate: 1000.0,
                subscriber_wait_ms: if target == "/replay/allowed" { 1000 } else { 0 },
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(player.status().endpoints[0].target, target);
        if target == "/replay/denied" {
            // The same publish policy also denies the subscriber declaration
            // toward this client. Once InterestFinal arrives, Zenoh may drop
            // a local Put with an empty route instead of sending it to Router.
            // Wait for the actual ACL interception, not an arbitrary delay.
            until(|| {
                !logs
                    .message_denials(&denied_key, "Declare Subscriber (egress)")
                    .is_empty()
            });
        }
        player.start().unwrap();
        let report = player.wait(WAIT).unwrap();
        assert!(report.status.resources_closed);
        assert!(report.status.cleanup_error.is_none());
        assert!(report.status.error.is_none());
        assert_eq!(report.status.published_total, COUNT);
        if target == "/replay/allowed" {
            for value in &actual {
                assert_eq!(replay_allowed.recv_timeout(WAIT).unwrap()["data"], *value);
            }
        } else {
            assert!(matches!(
                replay_denied.recv_timeout(Duration::from_millis(350)),
                Err(aos_core::Error::Timeout)
            ));
        }
        replay_reports.push(json!({"target":target,"local_published_total":report.status.published_total,"resources_closed":report.status.resources_closed}));
    }
    control
        .publish(&json!({"data":"authorized-after"}))
        .unwrap();
    assert_eq!(
        replay_denied.recv_timeout(WAIT).unwrap()["data"],
        "authorized-after"
    );
    // Local publish success does not imply a wire Put. Account for every
    // playback attempt on its own worker thread and exact final key. Each
    // selected Router route still needs a real ingress interception.
    let routes = replay_routes::playback_routes(&logs.text(), &denied_key).unwrap();
    assert_eq!(routes.len(), COUNT as usize, "{}", logs.text());
    let routed = routes.iter().filter(|route| route.to_router).count();
    until(|| logs.message_denials(&denied_key, "Put (ingress)").len() >= routed);
    let play_denials = logs.message_denials(&denied_key, "Put (ingress)");
    assert_eq!(play_denials.len(), routed, "{}", logs.text());
    let interest_denials = logs.message_denials(&denied_key, "Declare Subscriber (egress)");
    assert!(!interest_denials.is_empty(), "{}", logs.text());
    for route in &routes {
        println!("denied playback local route:\n{}", route.evidence);
    }
    for pair in record_denials
        .iter()
        .chain(&interest_denials)
        .chain(&play_denials)
    {
        println!("actual Router denial:\n{pair}");
    }
    let router_zid = router.zid().to_string();
    provider.destroy_node().unwrap();
    observer.destroy_node().unwrap();
    provider_context.shutdown().unwrap();
    observer_context.shutdown().unwrap();
    restricted_context.shutdown().unwrap();
    router.close().wait().unwrap();
    let evidence = json!({
        "status":"passed", "router":"test-only same-source native TLS Router",
        "router_zid":router_zid,"router_session_closed":true,
        "policy_digest":compiled.source_digest(), "domain_id":29,
        "record":{"discovered_channels":2,"authorized_source_samples":COUNT,"denied_source_control_samples":COUNT,
            "mcap_business_samples":actual,"mcap_denied_samples":0,"resources_closed":recorded.status.resources_closed,
            "intercepted_subscription_denials":record_denials.len()},
        "replay":{"same_source":true,"reports":replay_reports,"authorized_received":COUNT,
            "denied_received":0,"denied_observation_ms":350,"intercepted_puts":play_denials.len(),
            "local_attempts":routes.len(),"selected_router_routes":routed,
            "empty_local_routes":routes.len()-routed,
            "intercepted_subscriber_egress":interest_denials.len(),
            "same_target_authorized_controls":["authorized-before","authorized-after"]}
    });
    println!(
        "secure Bag evidence: {}",
        serde_json::to_string(&evidence).unwrap()
    );
    if let Some(path) = std::env::var_os("AOS_SECURE_BAG_TRACE") {
        fs::write(path, logs.text()).unwrap();
    }
    if let Some(path) = std::env::var_os("AOS_SECURE_BAG_RESULT") {
        fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
}
