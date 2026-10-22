use aos_core::{ActionInterfaces, Interface};
use aos_zenoh::security::*;
use serde_json::{json, Value};

fn node(name: &str) -> NodeIdentity {
    NodeIdentity {
        name: name.into(),
        namespace: "/robot".into(),
    }
}

fn message() -> Value {
    json!({"name":"test/msg/Value","kind":"msg","message":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})
}

fn action() -> Value {
    json!({"name":"test/action/Work","kind":"action","goal":{"type":"object"},"result":{"type":"object","default":{}},"feedback":{"type":"object"}})
}

fn endpoint(interface: Value, operations: Vec<EndpointOperation>) -> EndpointGrant {
    EndpointGrant {
        node: node("owner"),
        namespace: "/robot".into(),
        name: "/robot/value".into(),
        interface,
        operations,
        qos: Value::Null,
    }
}

fn policy() -> SecurityPolicy {
    SecurityPolicy {
        schema_version: 1,
        domain_id: 19,
        subjects: vec![SubjectPolicy {
            id: "owner".into(),
            common_name: "cert-owner".into(),
            nodes: vec![node("owner")],
            endpoints: vec![endpoint(message(), vec![EndpointOperation::Publish])],
            systems: vec![],
            graph_nodes: vec![],
        }],
    }
}

fn profile() -> ClientTlsProfile {
    let certs = std::env::temp_dir().join("aos-security-profile-fixture");
    ClientTlsProfile {
        schema_version: 1,
        domain_id: 19,
        router_endpoints: vec!["tls/localhost:7447".into()],
        root_ca_certificate: certs.join("ca.pem").to_string_lossy().into_owned(),
        certificate: certs.join("owner.pem").to_string_lossy().into_owned(),
        private_key: certs.join("owner.key").to_string_lossy().into_owned(),
        connect_timeout_ms: 3000,
        peer: None,
    }
}

#[test]
fn compiler_emits_only_authenticated_tls_default_deny_and_exact_type_keys() {
    let policy = policy();
    let compiled = compile_policy(&policy).unwrap();
    let native = compiled.access_control();
    assert_eq!(native["enabled"], true);
    assert_eq!(native["default_permission"], "deny");
    assert_eq!(
        native["subjects"][0]["cert_common_names"],
        json!(["cert-owner"])
    );
    assert_eq!(native["subjects"][0]["link_protocols"], json!(["tls"]));
    let interface = Interface::from_json(message()).unwrap();
    let key = format!(
        "aos/19/robot/value/test/msg/Value/{}",
        interface.type_hash()
    );
    let publish: Vec<_> = compiled
        .expanded_grants()
        .iter()
        .filter(|g| g.operation == "topic.publish")
        .collect();
    assert_eq!(publish.len(), 2);
    assert!(publish.iter().all(|g| g.key_expr == key));
    assert!(publish
        .iter()
        .any(|g| g.flow == Flow::Ingress && g.message == Message::Put));
    assert!(publish
        .iter()
        .any(|g| g.flow == Flow::Egress && g.message == Message::DeclareSubscriber));
    assert_eq!(compiled.domain_id(), 19);
    assert_eq!(compiled.source_digest().len(), 64);
    assert_eq!(
        serde_json::to_value(&compiled).unwrap(),
        serde_json::to_value(compile_policy(&policy).unwrap()).unwrap()
    );
}

#[test]
fn relay_delegation_is_finite_explicit_and_not_recursive() {
    let base = compile_policy(&policy()).unwrap();
    let relay = RelaySubject {
        id: "local_router".into(),
        common_name: "router-cert".into(),
        forward_subjects: vec!["owner".into()],
    };
    let expanded = with_relay_subjects(&base, std::slice::from_ref(&relay)).unwrap();
    assert_ne!(base.source_digest(), expanded.source_digest());
    let forwarded: Vec<_> = expanded
        .expanded_grants()
        .iter()
        .filter(|g| g.subject_id == "local_router")
        .collect();
    assert_eq!(forwarded.len(), base.expanded_grants().len());
    for (forwarded, source) in forwarded.iter().zip(base.expanded_grants()) {
        assert_eq!(forwarded.key_expr, source.key_expr);
        assert_eq!(forwarded.flow, source.flow);
        assert_eq!(forwarded.message, source.message);
    }
    assert!(with_relay_subjects(&expanded, std::slice::from_ref(&relay)).is_err());
    assert!(with_relay_subjects(&base, &[]).is_err());
    for (id, cn, subjects) in [
        ("owner", "router-cert", vec!["owner"]),
        ("router", "cert-owner", vec!["owner"]),
        ("router", "wild*", vec!["owner"]),
        ("router", "router-cert", vec!["unknown"]),
        ("router", "router-cert", vec!["owner", "owner"]),
    ] {
        let bad = RelaySubject {
            id: id.into(),
            common_name: cn.into(),
            forward_subjects: subjects.into_iter().map(Into::into).collect(),
        };
        assert!(with_relay_subjects(&base, &[bad]).is_err());
    }
}

#[test]
fn secure_peer_requires_loopback_policy_and_tls_on_every_link() {
    let mut peer = profile();
    peer.router_endpoints = vec!["tls/127.0.0.1:7447".into()];
    peer.peer = Some(PeerTlsOptions {
        listen_endpoint: "tls/127.0.0.1:0".into(),
        policy: policy(),
        relays: vec![RelaySubject {
            id: "router".into(),
            common_name: "router".into(),
            forward_subjects: vec!["owner".into()],
        }],
    });
    let config = client_config(&peer).unwrap();
    validate_client_config(&config, &peer).unwrap();
    let value = serde_json::to_value(&config).unwrap();
    assert_eq!(value["mode"], "peer");
    assert_eq!(value["scouting"]["gossip"]["enabled"], true);
    assert_eq!(value["access_control"]["default_permission"], "deny");
    assert_eq!(value["transport"]["link"]["protocols"], json!(["tls"]));
    for endpoint in [
        "tls/0.0.0.0:0",
        "tls/192.168.1.2:0",
        "tcp/127.0.0.1:0",
        "tls/localhost:0",
    ] {
        let mut invalid = peer.clone();
        invalid.peer.as_mut().unwrap().listen_endpoint = endpoint.into();
        assert!(client_config(&invalid).is_err());
    }
    let mut invalid = peer.clone();
    invalid.peer.as_mut().unwrap().policy.domain_id += 1;
    assert!(client_config(&invalid).is_err());
    invalid = peer.clone();
    invalid.router_endpoints = vec!["tls/remote.example:7447".into()];
    assert!(client_config(&invalid).is_err());
    invalid = peer.clone();
    invalid.connect_timeout_ms = 500;
    assert!(client_config(&invalid).is_err());
    let mut tampered = config;
    tampered
        .insert_json5("transport/link/protocols", r#"["tls","tcp"]"#)
        .unwrap();
    assert!(validate_client_config(&tampered, &peer).is_err());
}

#[test]
fn graph_read_expands_finite_policy_entities_without_granting_data() {
    let mut policy = policy();
    policy.subjects.push(SubjectPolicy {
        id: "observer".into(),
        common_name: "cert-observer".into(),
        nodes: vec![],
        endpoints: vec![],
        systems: vec![],
        graph_nodes: vec![node("owner")],
    });
    let compiled = compile_policy(&policy).unwrap();
    let grants: Vec<_> = compiled
        .expanded_grants()
        .iter()
        .filter(|g| g.subject_id == "observer")
        .collect();
    assert_eq!(
        grants
            .iter()
            .filter(|g| g.operation == "graph.read_request")
            .count(),
        2
    );
    let reads: Vec<_> = grants
        .iter()
        .filter(|g| g.operation == "graph.read")
        .collect();
    assert_eq!(reads.len(), 4, "Node, two builtins, one Topic");
    assert!(reads
        .iter()
        .all(|g| g.flow == Flow::Egress && g.message == Message::LivelinessToken));
    assert!(reads.iter().all(|g| !g.key_expr.ends_with("/**")));
    assert!(grants
        .iter()
        .all(|g| !matches!(g.message, Message::Put | Message::Reply | Message::Query)));
    policy.subjects[1].graph_nodes.push(node("absent"));
    assert!(compile_policy(&policy).is_err());
}

#[test]
fn action_send_result_and_cancel_are_independent_real_derived_service_permissions() {
    let mut policy = policy();
    policy.subjects[0].endpoints = vec![endpoint(
        action(),
        vec![EndpointOperation::SendGoal, EndpointOperation::GetResult],
    )];
    let compiled = compile_policy(&policy).unwrap();
    let types = ActionInterfaces::new(&Interface::from_json(action()).unwrap()).unwrap();
    let grants = compiled.expanded_grants();
    assert!(grants
        .iter()
        .any(|g| g.operation == "action.send_goal"
            && g.key_expr.ends_with(types.send_goal.type_hash())));
    assert!(grants.iter().any(|g| g.operation == "action.get_result"
        && g.key_expr.ends_with(types.get_result.type_hash())));
    assert!(!grants.iter().any(|g| g.key_expr.contains("cancel_goal")));
    policy.subjects[0].endpoints[0].operations = vec![EndpointOperation::CancelGoal];
    let cancel = compile_policy(&policy).unwrap();
    assert!(cancel
        .expanded_grants()
        .iter()
        .any(|g| g.operation == "action.cancel_goal"
            && g.key_expr.ends_with(types.cancel_goal.type_hash())));
    assert!(!cancel
        .expanded_grants()
        .iter()
        .any(|g| g.key_expr.contains("send_goal") || g.key_expr.contains("get_result")));
}

#[test]
fn transient_local_permission_is_bound_to_the_type_and_operation() {
    let mut policy = policy();
    policy.subjects[0].endpoints[0].qos = json!({"durability":"transient_local"});
    let compiled = compile_policy(&policy).unwrap();
    let hash = Interface::from_json(message())
        .unwrap()
        .type_hash()
        .to_owned();
    let advanced: Vec<_> = compiled
        .expanded_grants()
        .iter()
        .filter(|g| g.operation.ends_with("transient_local"))
        .collect();
    assert!(!advanced.is_empty());
    assert!(advanced.iter().all(|g| g.key_expr.contains(&hash)));
    assert!(advanced
        .iter()
        .any(|g| g.message == Message::DeclareQueryable));
    assert!(!advanced
        .iter()
        .any(|g| g.flow == Flow::Ingress && g.message == Message::Query));
}

#[test]
fn system_read_and_write_permissions_do_not_overlap() {
    let mut policy = policy();
    policy.subjects[0].systems = vec![SystemGrant {
        node: node("owner"),
        target: node("other"),
        operations: vec![
            SystemOperation::ParametersRead,
            SystemOperation::LifecycleRead,
        ],
    }];
    let compiled = compile_policy(&policy).unwrap();
    assert!(compiled
        .expanded_grants()
        .iter()
        .any(|g| g.resource == "/robot/other/get_parameters"));
    assert!(compiled
        .expanded_grants()
        .iter()
        .any(|g| g.resource == "/robot/other/get_state"));
    assert!(!compiled
        .expanded_grants()
        .iter()
        .any(|g| g.resource.contains("set_parameters") || g.resource.contains("change_state")));
    policy.subjects[0].systems[0].operations = vec![
        SystemOperation::ParametersWrite,
        SystemOperation::LifecycleChange,
    ];
    let compiled = compile_policy(&policy).unwrap();
    assert!(compiled
        .expanded_grants()
        .iter()
        .any(|g| g.resource == "/robot/other/set_parameters"));
    assert!(!compiled
        .expanded_grants()
        .iter()
        .any(|g| g.resource == "/robot/other/get_parameters"));
    policy.subjects[0].systems[0].operations = vec![SystemOperation::ParametersProvide];
    assert!(compile_policy(&policy).is_err());
}

#[test]
fn parameter_events_expansion_discloses_global_domain_topic() {
    let mut policy = policy();
    policy.subjects[0].systems = vec![SystemGrant {
        node: node("owner"),
        target: node("other"),
        operations: vec![SystemOperation::ParameterEventsRead],
    }];
    let compiled = compile_policy(&policy).unwrap();
    let events: Vec<_> = compiled
        .expanded_grants()
        .iter()
        .filter(|g| g.operation.starts_with("parameters.events"))
        .collect();
    assert!(!events.is_empty());
    assert!(events.iter().all(|g| g.resource == "/parameter_events"));
}

#[test]
fn subject_node_namespace_and_operation_boundaries_reject_ambiguous_inputs() {
    let base = policy();
    let mut invalid = Vec::new();
    let mut p = base.clone();
    p.subjects.push(p.subjects[0].clone());
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].common_name = "*".into();
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].nodes.push(node("owner"));
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].node = node("unowned");
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].name = "/robot2/value".into();
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].name = "/robot/**".into();
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].operations.clear();
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0]
        .operations
        .push(EndpointOperation::Publish);
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].operations = vec![EndpointOperation::CancelGoal];
    invalid.push(p);
    let mut p = base.clone();
    p.subjects[0].endpoints[0].qos = json!({"unknown":true});
    invalid.push(p);
    let mut p = base.clone();
    p.schema_version = 2;
    invalid.push(p);
    let mut p = base.clone();
    p.subjects.clear();
    invalid.push(p);
    for (i, policy) in invalid.iter().enumerate() {
        assert!(compile_policy(policy).is_err(), "invalid case {i}");
    }
}

#[test]
fn strict_json_rejects_unknown_duplicate_and_oversized_fields_without_echoing_secrets() {
    const SECRET: &str = "PRIVATE-KEY-CONTENT-MUST-NOT-APPEAR";
    let valid = serde_json::to_value(profile()).unwrap();
    let mut unknown = valid.clone();
    unknown[SECRET] = json!(SECRET);
    let mut wrong_type = valid.clone();
    wrong_type["domain_id"] = json!(SECRET);
    for text in [
        unknown.to_string(),
        wrong_type.to_string(),
        format!("{{\"{SECRET}\":1,\"{SECRET}\":2}}"),
        " ".repeat(MAX_SECURITY_JSON_BYTES + 1),
    ] {
        let error = ClientTlsProfile::from_json(&text).unwrap_err().to_string();
        assert!(!error.contains(SECRET));
    }
    let mut unknown = serde_json::to_value(policy()).unwrap();
    unknown["subjects"][0]["raw_acl"] = json!({});
    assert!(SecurityPolicy::from_json(&unknown.to_string()).is_err());
    unknown = serde_json::to_value(policy()).unwrap();
    unknown["subjects"][0]["endpoints"][0]["operations"] = json!(["delete"]);
    assert!(SecurityPolicy::from_json(&unknown.to_string()).is_err());
}

#[test]
fn client_profile_is_tls_only_and_rejects_endpoint_attribute_injection() {
    let base = profile();
    let config = client_config(&base).unwrap();
    validate_client_config(&config, &base).unwrap();
    let native = serde_json::to_value(config).unwrap();
    assert_eq!(native["mode"], "client");
    assert_eq!(native["transport"]["link"]["protocols"], json!(["tls"]));
    assert_eq!(native["scouting"]["multicast"]["enabled"], false);
    assert_eq!(native["transport"]["link"]["tls"]["enable_mtls"], true);
    for bad in [
        "tcp/localhost:7447",
        "tls/localhost:7447#verify_name_on_connect=false",
        "tls/localhost:0",
        "tls/localhost:7447?x=y",
        "tls/../host:7447",
        "tls/host:65536",
        "tls/host:7447;tcp/host:7447",
    ] {
        let mut p = base.clone();
        p.router_endpoints = vec![bad.into()];
        assert!(client_config(&p).is_err(), "{bad}");
    }
    let mut p = base.clone();
    p.router_endpoints.push("tls/other:7447".into());
    assert!(client_config(&p).is_err());
    let mut p = base.clone();
    p.private_key = p.certificate.clone();
    assert!(client_config(&p).is_err());
    let mut p = base.clone();
    p.private_key = "relative.pem".into();
    assert!(client_config(&p).is_err());
    let mut p = base.clone();
    p.connect_timeout_ms = 0;
    assert!(client_config(&p).is_err());
    let mut p = base;
    p.connect_timeout_ms = 60_001;
    assert!(client_config(&p).is_err());
}

#[test]
fn final_native_overrides_cannot_disable_authentication_or_add_routes() {
    let profile = profile();
    for (path, value) in [
        ("mode", "\"peer\""),
        ("scouting/multicast/enabled", "true"),
        ("scouting/gossip/enabled", "true"),
        ("transport/link/protocols", "[\"tls\",\"tcp\"]"),
        (
            "connect/endpoints",
            "[\"tls/localhost:7447\",\"tcp/localhost:7448\"]",
        ),
        ("listen/endpoints", "[\"tls/localhost:7448\"]"),
        ("transport/link/tls/enable_mtls", "false"),
        ("transport/link/tls/verify_name_on_connect", "false"),
        ("transport/link/tls/close_link_on_expiration", "false"),
        (
            "transport/link/tls/connect_private_key_base64",
            "\"U0VDUkVU\"",
        ),
        ("adminspace/enabled", "true"),
    ] {
        let mut config = client_config(&profile).unwrap();
        config.insert_json5(path, value).unwrap();
        assert!(
            validate_client_config(&config, &profile).is_err(),
            "override {path}"
        );
    }
}

#[test]
fn profile_loader_checks_the_opened_regular_file_and_rejects_symlinks() {
    let directory = std::env::temp_dir().join(format!(
        "aos-profile-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let path = directory.join("profile.json");
    std::fs::write(&path, serde_json::to_vec(&profile()).unwrap()).unwrap();
    assert_eq!(
        load_client_profile(Some(&path)).unwrap().unwrap().domain_id,
        19
    );
    assert!(load_client_profile(Some(std::path::Path::new(""))).is_err());
    assert!(load_client_profile(Some(std::path::Path::new("relative.json"))).is_err());
    assert!(load_client_profile(Some(&directory)).is_err());
    assert!(load_client_profile(Some(&directory.join("missing"))).is_err());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&path, directory.join("link.json")).unwrap();
        assert!(load_client_profile(Some(&directory.join("link.json"))).is_err());
        let fifo = directory.join("fifo");
        let c_path = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let start = std::time::Instant::now();
        assert!(load_client_profile(Some(&fifo)).is_err());
        assert!(
            start.elapsed() < std::time::Duration::from_secs(1),
            "FIFO must never block profile reading"
        );
    }
    std::fs::write(&path, " ".repeat(MAX_SECURITY_JSON_BYTES + 1)).unwrap();
    assert!(load_client_profile(Some(&path)).is_err());
    std::fs::write(&path, [0xff]).unwrap();
    assert!(load_client_profile(Some(&path)).is_err());
}

#[test]
fn compiler_bounds_subject_node_and_input_grant_expansion() {
    let mut p = policy();
    p.subjects = (0..65)
        .map(|i| SubjectPolicy {
            id: format!("id{i}"),
            common_name: format!("cn{i}"),
            nodes: vec![node(&format!("node{i}"))],
            endpoints: vec![],
            systems: vec![],
            graph_nodes: vec![],
        })
        .collect();
    assert!(compile_policy(&p).is_err());
    let mut p = policy();
    p.subjects[0].nodes = (0..257).map(|i| node(&format!("node{i}"))).collect();
    assert!(compile_policy(&p).is_err());
    let mut p = policy();
    p.subjects[0].endpoints = vec![p.subjects[0].endpoints[0].clone(); 1025];
    assert!(compile_policy(&p).is_err());
}

#[test]
fn resolved_secure_entry_rejects_final_domain_without_opening_network() {
    let resolved = aos_core::resolve_context_config(&aos_core::ContextConfigRequest {
        program: json!({"domain_id":20}),
        ..Default::default()
    })
    .unwrap();
    let error = match open_resolved_secure_context(resolved, None, &profile()) {
        Ok(_) => panic!("unauthorized domain accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("final domain differs"));
}
