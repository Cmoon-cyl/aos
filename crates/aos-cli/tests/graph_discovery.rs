//! Real CLI + two explicitly linked Routers; no multicast or business probes.
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use aos_core::{
    transport::Transport, Context, EndpointOptions, EntityKind, GraphEntity, Interface,
    LifecycleCallbackReturn, LifecycleTransitionId, Node, NodeOptions,
};
use aos_zenoh::ZenohTransport;
use serde_json::{json, Value};
use zenoh::Wait;

struct Network {
    directory: PathBuf,
    routers: Vec<zenoh::Session>,
    provider: Context,
    raw: ZenohTransport,
}

impl Network {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("aos-graph-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let mut routers = Vec::<zenoh::Session>::new();
        let mut previous = None;
        for _ in 0..2 {
            let mut config =
                zenoh::Config::from_json5(include_str!("../../../config/zenoh-router.json5"))
                    .unwrap();
            config
                .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
                .unwrap();
            config
                .insert_json5("scouting/gossip/enabled", "false")
                .unwrap();
            if let Some(endpoint) = previous.as_ref() {
                config
                    .insert_json5("connect/endpoints", &json!([endpoint]).to_string())
                    .unwrap();
            }
            let session = zenoh::open(config).wait().unwrap();
            previous = Some(session.info().locators().wait()[0].to_string());
            routers.push(session);
        }
        let client = |router: &zenoh::Session| {
            let endpoint = router.info().locators().wait()[0].to_string();
            let mut config =
                zenoh::Config::from_json5(include_str!("../../../config/zenoh-session.json5"))
                    .unwrap();
            config.insert_json5("mode", r#""client""#).unwrap();
            config
                .insert_json5("connect/endpoints", &json!([endpoint]).to_string())
                .unwrap();
            config.insert_json5("listen/endpoints", "[]").unwrap();
            config
                .insert_json5("scouting/gossip/enabled", "false")
                .unwrap();
            config
        };
        std::fs::write(
            directory.join("observer.json5"),
            client(&routers[0]).to_string(),
        )
        .unwrap();
        let provider = Context::new(ZenohTransport::open(client(&routers[1]), 987).unwrap());
        let raw = ZenohTransport::open(client(&routers[1]), 987).unwrap();
        Self {
            directory,
            routers,
            provider,
            raw,
        }
    }

    fn command(&self, verb: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aos"));
        command
            .args(["graph", verb, "--request", "-"])
            .env("AOS_ZENOH_CONFIG", self.directory.join("observer.json5"))
            .env("AOS_DOMAIN_ID", "987")
            .env_remove("AOS_PROCESS_CONFIG")
            .env_remove("AOS_SECURITY_PROFILE")
            .env_remove("AOS_NAMESPACE")
            .env_remove("AOS_NODE_NAME")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        command
    }

    fn spawn(&self, verb: &str, request: Value) -> Reader {
        let mut child = self.command(verb).spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(request.to_string().as_bytes())
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let value = serde_json::from_str(&line.unwrap()).unwrap();
                if sender.send(value).is_err() {
                    break;
                }
            }
        });
        Reader {
            child,
            receiver,
            view: BTreeMap::new(),
            sequence: None,
            watch_id: None,
        }
    }

    fn finite(&self, verb: &str, request: Value, expected: i32) -> Value {
        let mut reader = self.spawn(verb, request);
        let value = reader
            .receiver
            .recv_timeout(Duration::from_secs(15))
            .expect("finite command output");
        assert_eq!(reader.finish(), expected, "{value}");
        value
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        let _ = self.provider.shutdown();
        let _ = self.raw.close();
        for router in &self.routers {
            let _ = router.close().wait();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct Reader {
    child: Child,
    receiver: Receiver<Value>,
    view: BTreeMap<String, Value>,
    sequence: Option<u64>,
    watch_id: Option<String>,
}

impl Reader {
    fn apply(&mut self, frame: &Value) {
        let data = &frame["data"];
        match frame["event"].as_str() {
            Some("snapshot") => {
                assert!(self.sequence.is_none());
                assert_eq!(data["sequence"], 0);
                self.sequence = Some(0);
                self.watch_id = Some(data["watch_id"].as_str().unwrap().into());
                self.view = data["nodes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|node| (node["node_id"].as_str().unwrap().into(), node.clone()))
                    .collect();
            }
            Some("delta") => {
                assert_eq!(data["sequence"].as_u64(), self.sequence.map(|n| n + 1));
                assert_eq!(data["watch_id"].as_str(), self.watch_id.as_deref());
                self.sequence = data["sequence"].as_u64();
                for id in data["removed_node_ids"].as_array().unwrap() {
                    self.view.remove(id.as_str().unwrap());
                }
                for node in data["added"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .chain(data["updated"].as_array().unwrap())
                {
                    self.view
                        .insert(node["node_id"].as_str().unwrap().into(), node.clone());
                }
            }
            Some("summary") => (),
            _ => panic!("unexpected graph frame: {frame}"),
        }
    }

    fn until(&mut self, predicate: impl Fn(&BTreeMap<String, Value>) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let frame = self
                .receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("Graph did not converge");
            self.apply(&frame);
            if predicate(&self.view) {
                return;
            }
        }
    }

    fn finish(&mut self) -> i32 {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code().unwrap_or(-1);
            }
            assert!(
                Instant::now() < deadline,
                "CLI did not stop within cleanup deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn service() -> Interface {
    Interface::from_json(json!({"name":"test/srv/Inspect","kind":"srv",
        "request":{"type":"object","additionalProperties":false},
        "response":{"type":"object","additionalProperties":false}}))
    .unwrap()
}

#[test]
fn graph_cli_converges_across_routers_without_calling_business_services() {
    const NAME: &str = "graph_cli_converges_across_routers_without_calling_business_services";
    if std::env::var("AOS_GRAPH_TEST_CHILD").as_deref() != Ok(NAME) {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", NAME, "--nocapture"])
            .env("AOS_GRAPH_TEST_CHILD", NAME);
        if std::env::var_os("ZENOH_RUNTIME").is_none() {
            command.env("ZENOH_RUNTIME", "(net: (worker_threads: 8))");
        }
        assert!(command.status().unwrap().success());
        return;
    }
    let mut network = Network::new();
    let mut watch = network.spawn(
        "watch",
        json!({"namespace":"/fleet","timeout":35,"refresh_interval":0.15,"probe_timeout":0.2}),
    );
    watch.until(|view| view.is_empty()); // Baseline before any Node exists.
    let owner = Node::new_lifecycle(
        network.provider.clone(),
        "worker",
        "/fleet",
        NodeOptions {
            display_name: Some("Worker".into()),
            summary: "Local test worker".into(),
            tags: vec!["test".into()],
        },
    )
    .unwrap();
    let id = owner.id().to_owned();
    let first = owner
        .create_service(
            &service(),
            "/fleet/inspect",
            EndpointOptions {
                summary: "Inspect a value".into(),
                side_effects: Some("read_only".into()),
                contract_version: Some("1".into()),
            },
        )
        .unwrap();
    watch.until(|view| {
        view.get(&id).is_some_and(|node| {
            node["introspection"]["status"] == "ok"
                && node["endpoints"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["name"] == "/fleet/inspect" && e["description_status"] == "current")
        })
    });
    assert_eq!(watch.view[&id]["endpoints"][0]["side_effects"], "read_only");
    assert_eq!(watch.view[&id]["metadata_sanitized"], false);
    assert!(watch.view[&id]["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["name"] != "/fleet/worker/get_node_card"));

    // Only lifecycle state changes; no Graph declaration changes.
    let mut transition = owner
        .lifecycle()
        .unwrap()
        .begin_transition(LifecycleTransitionId::Configure)
        .unwrap();
    watch.until(|view| {
        view.get(&id)
            .is_some_and(|node| node["lifecycle"]["transitioning"] == true)
    });
    assert_eq!(watch.view[&id]["metadata_sanitized"], false);
    transition
        .complete(LifecycleCallbackReturn::Success, Some(Duration::ZERO))
        .unwrap();
    watch.until(|view| {
        view.get(&id)
            .is_some_and(|node| node["lifecycle"]["current_state"]["label"] == "inactive")
    });
    assert_eq!(watch.view[&id]["metadata_sanitized"], false);
    assert!(
        watch
            .receiver
            .recv_timeout(Duration::from_millis(450))
            .is_err(),
        "unchanged refresh must not flood JSONL"
    );

    let mut late = network.spawn(
        "wait",
        json!({"endpoint_name":"/fleet/second","kind":"service","timeout":5,"settle_time":0.1,"refresh_interval":0.15}),
    );
    assert!(late
        .receiver
        .recv_timeout(Duration::from_millis(250))
        .is_err());
    let second = owner
        .create_service(&service(), "/fleet/second", EndpointOptions::default())
        .unwrap();
    let appeared = late.receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(appeared["data"]["matched"], true);
    assert_eq!(late.finish(), 0);
    watch.until(|view| {
        view.get(&id).is_some_and(|node| {
            node["endpoints"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["name"] == "/fleet/second" && e["description_status"] == "current")
        })
    });
    second.close().unwrap();
    watch.until(|view| {
        view.get(&id).is_some_and(|node| {
            node["introspection"]["status"] == "ok"
                && node["endpoints"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|e| e["name"] != "/fleet/second")
        })
    });

    let waited = network.finite(
        "wait",
        json!({"endpoint_name":"/fleet/inspect","kind":"service","timeout":3}),
        0,
    );
    assert_eq!(waited["data"]["matched"], true);
    assert_eq!(waited["data"]["match_evidence"], "introspection");
    // Introspection says nothing about the business lifecycle gate.
    assert_eq!(waited["data"]["nodes"][0]["lifecycle"]["gate_open"], false);
    assert_eq!(waited["data"]["scope"]["business_health"], "unverified");
    let absent = network.finite(
        "wait",
        json!({"node_name":"/fleet/missing","timeout":0.7,"settle_time":0.1}),
        4,
    );
    assert_eq!(absent["error"]["code"], "WAIT_TIMEOUT");
    assert_eq!(absent["error"]["last_observation"]["nodes"], json!([]));

    let duplicate = Node::new(
        network.provider.clone(),
        "worker",
        "/fleet",
        NodeOptions::default(),
    )
    .unwrap();
    watch.until(|view| {
        view.len() == 2
            && view
                .values()
                .all(|node| node["introspection"]["status"] == "ambiguous")
    });
    duplicate.destroy_node().unwrap();
    watch.until(|view| view.len() == 1 && view[&id]["introspection"]["status"] == "ok");

    // Eight permanently unresponsive advertisements precede the next healthy
    // node in ID order. Their bounded batch must not exhaust the total budget.
    let guards: Vec<_> = (0..8)
        .map(|i| {
            network
                .raw
                .declare_graph_entity(&GraphEntity {
                    id: format!("000-fake-{i}"),
                    node_id: format!("000-fake-{i}"),
                    node_name: format!("fake{i}"),
                    namespace: "/fleet".into(),
                    kind: EntityKind::Node,
                    endpoint: None,
                    qos: None,
                })
                .unwrap()
        })
        .collect();
    let snapshot = network.finite(
        "snapshot",
        json!({"namespace":"/fleet","timeout":3,"probe_timeout":0.15}),
        0,
    );
    let nodes = snapshot["data"]["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 9);
    assert!(nodes
        .iter()
        .any(|node| node["node_id"] == id && node["introspection"]["status"] == "ok"));
    assert_eq!(
        nodes
            .iter()
            .filter(|node| node["introspection"]["status"] == "unavailable")
            .count(),
        8
    );
    let limited = network.finite("snapshot", json!({"namespace":"/fleet","max_nodes":1}), 2);
    assert_eq!(limited["error"]["code"], "RESOURCE_LIMIT");
    let partial = network.finite(
        "wait",
        json!({"namespace":"/fleet","timeout":3,"probe_timeout":0.1}),
        0,
    );
    assert_eq!(partial["data"]["nodes"].as_array().unwrap().len(), 9);
    assert_eq!(partial["data"]["matched_node_ids"], json!([id]));
    assert_eq!(partial["data"]["selected_not_matched"], 8);
    drop(guards);

    let orphan = network
        .raw
        .declare_graph_entity(&GraphEntity {
            id: "orphan-service".into(),
            node_id: "orphan".into(),
            node_name: "orphan".into(),
            namespace: "/partial".into(),
            kind: EntityKind::Service,
            endpoint: Some(aos_core::Endpoint {
                name: "/partial/work".into(),
                type_name: "test/srv/Inspect".into(),
                type_hash: service().type_hash().into(),
            }),
            qos: None,
        })
        .unwrap();
    let started = Instant::now();
    let partial = network.finite("snapshot", json!({"namespace":"/partial","timeout":3}), 0);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "endpoint-only owner must not force the full timeout"
    );
    assert_eq!(
        partial["data"]["nodes"][0]["introspection"]["status"],
        "undeclared"
    );
    drop(orphan);

    // A restart must use a new ID even when its fully qualified name is stable.
    owner.destroy_node().unwrap();
    watch.until(|view| !view.contains_key(&id));
    let restarted = Node::new(
        network.provider.clone(),
        "worker",
        "/fleet",
        NodeOptions::default(),
    )
    .unwrap();
    let replacement = restarted
        .create_service(&service(), "/fleet/inspect", EndpointOptions::default())
        .unwrap();
    let next_id = restarted.id().to_owned();
    assert_ne!(id, next_id);
    watch.until(|view| {
        view.get(&next_id)
            .is_some_and(|node| node["introspection"]["status"] == "ok")
    });
    // Drop and restore the provider's Router, keeping both provider and CLI
    // sessions alive. Reconnection must recover the same instance ID.
    let listen = network.routers[1].info().locators().wait()[0].to_string();
    let connect = network.routers[0].info().locators().wait()[0].to_string();
    network.routers[1].close().wait().unwrap();
    watch.until(|view| !view.contains_key(&next_id));
    let mut config =
        zenoh::Config::from_json5(include_str!("../../../config/zenoh-router.json5")).unwrap();
    config
        .insert_json5("listen/endpoints", &json!([listen]).to_string())
        .unwrap();
    config
        .insert_json5("connect/endpoints", &json!([connect]).to_string())
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();
    network.routers[1] = zenoh::open(config).wait().unwrap();
    watch.until(|view| {
        view.get(&next_id)
            .is_some_and(|node| node["introspection"]["status"] == "ok")
    });
    assert!(
        matches!(
            replacement.recv_timeout(Duration::from_millis(20)),
            Err(aos_core::Error::Timeout)
        ),
        "discovery must not call business endpoints"
    );
    drop(first);

    // A finite watcher sends its summary and exits without a signal.
    let mut finite = network.spawn(
        "watch",
        json!({"timeout":0.6,"settle_time":0.1,"descriptions":false}),
    );
    loop {
        let frame = finite
            .receiver
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        finite.apply(&frame);
        if frame["event"] == "summary" {
            assert!(
                frame["data"]["graph_reads"].as_u64().unwrap() < 8,
                "idle watcher repeatedly rebuilt the Graph: {frame}"
            );
            break;
        }
    }
    assert_eq!(finite.finish(), 0);
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(watch.child.id() as i32, libc::SIGTERM);
        }
        assert_eq!(watch.finish(), 143);
    }
}
