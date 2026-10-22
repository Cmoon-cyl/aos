use std::{fs, path::PathBuf};

use aos_core::{
    resolve_node_config, ContextConfigRequest, NameKind, NodeConfigRequest, NodeOptions,
    ProcessBootstrap, MAX_PROCESS_CONFIG_BYTES,
};
use serde_json::{json, Value};

fn document() -> Value {
    json!({
        "schema_version": 1,
        "launch_id": "606c355e-00a9-405b-9f41-c5855426dfe2",
        "process_id": "worker-1",
        "generation": 1,
        "context": {},
        "node": {}
    })
}

fn parse(value: Value) -> aos_core::Result<ProcessBootstrap> {
    ProcessBootstrap::from_json(&value.to_string())
}

#[test]
fn strict_document_rejects_unknown_duplicate_and_invalid_identity() {
    assert!(parse(document()).is_ok());
    for (field, value) in [
        ("schema_version", json!(2)),
        ("launch_id", json!("not-a-uuid")),
        ("process_id", json!("")),
        ("process_id", json!("a/b")),
        ("process_id", json!("a.b")),
        ("process_id", json!("工作")),
        ("process_id", json!("x".repeat(129))),
        ("generation", json!(0)),
        ("generation", json!(-1)),
        ("generation", json!(u64::MAX)),
        ("generation", json!(true)),
        ("unknown", json!(true)),
        ("context", json!({"unknown": {}})),
        ("node", json!({"launch": null})),
        ("node", json!({"cli": []})),
        ("context", json!({"launch": {"domain_id": false}})),
        ("context", json!({"launch": {"unknown": true}})),
        (
            "node",
            json!({"launch": {"nodes": {"unmatched": {"unknown": true}}}}),
        ),
        (
            "node",
            json!({"launch": {"remaps": [{"from": "in", "to": "out", "extra": 1}]}}),
        ),
        (
            "node",
            json!({"launch": {"parameters": {"v": {"type": "integer", "value": true}}}}),
        ),
    ] {
        let mut input = document();
        input[field] = value;
        assert!(parse(input).is_err(), "accepted invalid {field}");
    }
    for duplicate in [
        r#""generation":1,"generation":1"#,
        r#""node":{"launch":{"summary":"a","summary":"a"}}"#,
        r#""node":{"cli":{"nodes":{"worker":{"parameters":{"rate":1,"rate":1}}}}}"#,
    ] {
        let text = format!(
            r#"{{"schema_version":1,"launch_id":"606c355e-00a9-405b-9f41-c5855426dfe2","process_id":"worker","context":{{}},{duplicate}}}"#
        );
        let error = ProcessBootstrap::from_json(&text).unwrap_err().to_string();
        assert!(error.contains("duplicate config key"), "{error}");
    }
}

#[test]
fn deployment_paths_are_absolute_without_opening_referenced_files() {
    let directory = TestDirectory::new();
    for section in ["context", "node"] {
        let mut input = document();
        input[section]["config_files"] = json!(["relative.json"]);
        assert!(parse(input).is_err());
    }
    for source in ["launch", "cli"] {
        let mut input = document();
        input["context"][source] = json!({"zenoh_config": "./peer.json5"});
        assert!(parse(input).is_err());
    }
    let mut input = document();
    input["context"]["config_files"] = json!([directory.0.join("missing-context.json")]);
    input["context"]["launch"] = json!({"zenoh_config": directory.0.join("missing-peer.json5")});
    input["context"]["cli"] = json!({"zenoh_config": null});
    input["node"]["config_files"] = json!([directory.0.join("missing-node.json")]);
    assert!(
        parse(input).is_ok(),
        "pure parsing must not load references"
    );
}

#[test]
fn same_source_merge_rejects_equal_leaves_arrays_and_nested_nulls() {
    for (local, incoming) in [
        (json!({"summary":"same"}), json!({"summary":"same"})),
        (json!({"tags":[]}), json!({"tags":[]})),
        (json!({"display_name":null}), json!({"display_name":null})),
        (json!({"remaps":[]}), json!({"remaps":[]})),
        (
            json!({"nodes":{"worker":{"parameters":{"rate":1}}}}),
            json!({"nodes":{"worker":{"parameters":{"rate":1}}}}),
        ),
        (
            json!({"parameters":{"rate":{"type":"integer","value":1}}}),
            json!({"parameters":{"rate":{"type":"integer","value":1}}}),
        ),
    ] {
        for source in ["launch", "cli"] {
            let mut input = document();
            input["node"][source] = incoming.clone();
            let bootstrap = parse(input).unwrap();
            let request = if source == "launch" {
                NodeConfigRequest {
                    launch: local.clone(),
                    ..Default::default()
                }
            } else {
                NodeConfigRequest {
                    cli: local.clone(),
                    ..Default::default()
                }
            };
            let error = bootstrap.node_request(request).unwrap_err().to_string();
            assert!(error.contains("overlap"), "{error}");
        }
    }
    let mut input = document();
    input["context"]["launch"] = json!({"domain_id":1});
    let bootstrap = parse(input).unwrap();
    assert!(bootstrap
        .context_request(ContextConfigRequest {
            launch: json!({"domain_id":1}),
            ..Default::default()
        })
        .unwrap_err()
        .to_string()
        .contains("overlap"));
}

#[test]
fn disjoint_objects_and_cross_source_precedence_keep_declared_selectors() {
    let mut input = document();
    input["node"]["launch"] = json!({
        "nodes": {"worker": {"name":"renamed","namespace":"/deployed","parameters":{"rate":2}}},
        "remaps":[{"node":"/original/worker","from":"input","to":"output"}]
    });
    input["node"]["cli"] = json!({
        "nodes": {
            "/original/worker":{"parameters":{"rate":3}},
            "/deployed/renamed":{"summary":"must not select a renamed identity"}
        }
    });
    let bootstrap = parse(input).unwrap();
    let request = bootstrap.node_request(NodeConfigRequest {
        launch: json!({"nodes":{"worker":{"summary":"API summary","parameters":{"other":true}}}}),
        ..Default::default()
    }).unwrap();
    let config =
        resolve_node_config("worker", "/original", NodeOptions::default(), &request).unwrap();
    assert_eq!(config.name, "renamed");
    assert_eq!(config.namespace, "/deployed");
    assert_eq!(config.options.summary, "API summary");
    assert_eq!(
        config.effective_value()["parameters"]["rate"]["source"],
        "cli"
    );
    assert_eq!(
        config.effective_value()["parameters"]["other"]["source"],
        "launch"
    );
    assert_eq!(
        config
            .resolve_name("input", NameKind::Topic)
            .unwrap()
            .resolved_name,
        "/deployed/output",
    );
    let other =
        resolve_node_config("worker", "/sibling", NodeOptions::default(), &request).unwrap();
    assert_eq!(
        other.effective_value()["parameters"]["rate"]["source"],
        "launch"
    );
    // Composition is reusable; creating one Node does not consume the input.
    assert_eq!(
        serde_json::to_value(
            bootstrap
                .node_request(NodeConfigRequest::default())
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(
            bootstrap
                .node_request(NodeConfigRequest::default())
                .unwrap()
        )
        .unwrap()
    );
}

#[test]
fn file_order_keeps_local_first_and_does_not_deduplicate() {
    let directory = TestDirectory::new();
    let shared = directory.0.join("shared.json").to_str().unwrap().to_owned();
    let local = directory.0.join("local.json").to_str().unwrap().to_owned();
    let launch = directory.0.join("launch.json").to_str().unwrap().to_owned();
    let mut input = document();
    for section in ["context", "node"] {
        input[section]["config_files"] = json!([launch, shared]);
    }
    let bootstrap = parse(input).unwrap();
    let expected = vec![shared.clone(), local.clone(), launch, shared.clone()];
    let context = bootstrap
        .context_request(ContextConfigRequest {
            config_files: vec![shared.clone(), local.clone()],
            ..Default::default()
        })
        .unwrap();
    let node = bootstrap
        .node_request(NodeConfigRequest {
            config_files: vec![shared, local],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(context.config_files, expected);
    assert_eq!(node.config_files, expected);
}

struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("aos-bootstrap-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn file_loading_is_bounded_utf8_and_requires_an_absolute_regular_file() {
    let directory = TestDirectory::new();
    let path = directory.0.join("bootstrap.json");
    let text = document().to_string();
    fs::write(&path, &text).unwrap();
    assert_eq!(
        ProcessBootstrap::from_file(&path).unwrap().process_id,
        "worker-1"
    );
    assert!(ProcessBootstrap::from_file(std::path::Path::new("bootstrap.json")).is_err());
    assert!(ProcessBootstrap::from_file(&directory.0).is_err());
    assert!(ProcessBootstrap::from_file(&directory.0.join("missing")).is_err());
    let mut boundary = text;
    boundary.push_str(&" ".repeat(MAX_PROCESS_CONFIG_BYTES - boundary.len()));
    fs::write(&path, &boundary).unwrap();
    assert!(ProcessBootstrap::from_file(&path).is_ok());
    boundary.push(' ');
    fs::write(&path, &boundary).unwrap();
    assert!(ProcessBootstrap::from_file(&path)
        .unwrap_err()
        .to_string()
        .contains("1 MiB"));
    assert!(ProcessBootstrap::from_json(&boundary).is_err());
    fs::write(&path, [0xff, 0xfe]).unwrap();
    assert!(ProcessBootstrap::from_file(&path).is_err());
}
