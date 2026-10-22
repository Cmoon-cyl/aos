//! Standard Rust startup with a real peer, in an isolated child process so the
//! bootstrap locator never changes the environment of concurrent Rust tests.
use std::{fs, path::PathBuf, process::Command};

use aos_core::{ContextConfigRequest, Node, NodeConfigRequest, NodeOptions};
use serde_json::{json, Value};

const CHILD_MODE: &str = "AOS_TEST_PROCESS_BOOTSTRAP";
const LAUNCH_ID: &str = "606c355e-00a9-405b-9f41-c5855426dfe2";

fn bootstrap(peer: &str, generation: u32) -> Value {
    json!({
        "schema_version": 1,
        "launch_id": LAUNCH_ID,
        "process_id": "rust-worker",
        "generation": generation,
        "context": {
            "launch": {"zenoh_config": peer},
            "cli": {"domain_id": 86}
        },
        "node": {
            "launch": {"nodes": {"/declared/worker": {
                "name": "deployed", "namespace": "/runtime", "tags": ["bootstrap"]
            }}},
            "cli": {"nodes": {
                "/declared/worker": {"parameters": {"rate": 3}},
                "/runtime/deployed": {"summary": "must not match final identity"}
            }}
        }
    })
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "aos-rust-bootstrap-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn standard_opener_loads_once_and_nodes_share_the_generation() {
    let directory = Directory::new();
    let peer = directory.0.join("peer.json5");
    fs::write(
        &peer,
        r#"{
            mode: "peer",
            connect: { endpoints: [] },
            listen: { endpoints: ["tcp/127.0.0.1:0"] },
            scouting: { multicast: { enabled: false }, gossip: { enabled: false } }
        }"#,
    )
    .unwrap();
    let path = directory.0.join("bootstrap.json");
    fs::write(&path, bootstrap(peer.to_str().unwrap(), 1).to_string()).unwrap();
    run_child(&directory.0, &path, "valid");
}

#[test]
fn invalid_bootstrap_fails_before_loading_a_session_configuration() {
    let directory = Directory::new();
    let path = directory.0.join("bootstrap.json");
    fs::write(
        &path,
        r#"{"schema_version":1,"schema_version":1,"unexpected":true}"#,
    )
    .unwrap();
    run_child(&directory.0, &path, "invalid");
}

fn run_child(directory: &std::path::Path, path: &std::path::Path, mode: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bootstrap_child", "--nocapture"])
        .env(CHILD_MODE, mode)
        .env(aos_core::PROCESS_CONFIG_ENV, path)
        .env(
            "AOS_DOMAIN_ID",
            "this is not an implicit Rust configuration input",
        )
        .current_dir(directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn bootstrap_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    if mode == "invalid" {
        let error = match aos_zenoh::open_context(ContextConfigRequest {
            program: json!({"zenoh_config": "/missing-session-file.json5"}),
            ..Default::default()
        }) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("invalid bootstrap opened a Context"),
        };
        assert!(error.contains("duplicate config key"), "{error}");
        assert!(!error.contains("missing-session-file"), "{error}");
        return;
    }
    let context = aos_zenoh::open_context(ContextConfigRequest {
        program: json!({"domain_id": 84}),
        environment: [("AOS_DOMAIN_ID".into(), "85".into())].into(),
        ..Default::default()
    })
    .unwrap();
    let effective = context.configuration().unwrap().effective_value();
    assert_eq!(effective["domain_id"], json!({"value":86,"source":"cli"}));
    assert_eq!(effective["zenoh_config"]["source"], "launch");
    let identity = context.process_identity().unwrap();
    assert_eq!(identity.launch_id, LAUNCH_ID);
    assert_eq!(identity.pid, std::process::id());
    assert_eq!(identity.generation, 1);

    let path = PathBuf::from(std::env::var_os(aos_core::PROCESS_CONFIG_ENV).unwrap());
    let peer = context
        .configuration()
        .unwrap()
        .zenoh_config
        .as_ref()
        .unwrap();
    fs::write(&path, bootstrap(peer, 2).to_string()).unwrap();
    let make_request = || NodeConfigRequest {
        launch: json!({"nodes": {"/declared/worker": {"summary": "API summary"}}}),
        ..Default::default()
    };
    let regular = Node::new_with_config(
        context.clone(),
        "worker",
        "/declared",
        NodeOptions::default(),
        make_request(),
    )
    .unwrap();
    let lifecycle = Node::new_lifecycle_with_config(
        context.clone(),
        "worker",
        "/declared",
        NodeOptions::default(),
        make_request(),
    )
    .unwrap();
    for node in [&regular, &lifecycle] {
        let card = node.node_card();
        assert_eq!(card["fully_qualified_name"], "/runtime/deployed");
        assert_eq!(card["summary"], "API summary");
        assert_eq!(card["tags"], json!(["bootstrap"]));
        assert_eq!(card["process"], json!(identity));
        assert_eq!(
            card["effective_config"]["parameters"]["rate"]["source"],
            "cli"
        );
    }
    assert!(lifecycle.node_card()["lifecycle"].is_object());
    let error = Node::new_with_config(
        context.clone(),
        "worker",
        "/declared",
        NodeOptions::default(),
        NodeConfigRequest {
            launch: json!({"nodes": {"/declared/worker": {"name": "deployed"}}}),
            ..Default::default()
        },
    )
    .err()
    .expect("same-layer overlap must fail")
    .to_string();
    assert!(error.contains("overlap"), "{error}");

    let next = aos_zenoh::open_context(ContextConfigRequest::default()).unwrap();
    assert_eq!(next.process_identity().unwrap().generation, 2);
    assert_eq!(context.process_identity().unwrap().generation, 1);
    next.shutdown().unwrap();
    context.shutdown().unwrap();
}
