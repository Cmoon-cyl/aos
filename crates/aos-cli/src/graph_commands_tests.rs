use super::*;
use aos_core::Endpoint;

fn node(id: &str, name: &str, namespace: &str) -> GraphEntity {
    GraphEntity {
        id: id.into(),
        node_id: id.into(),
        node_name: name.into(),
        namespace: namespace.into(),
        kind: EntityKind::Node,
        endpoint: None,
        qos: None,
    }
}

fn service(owner: &GraphEntity, id: &str, name: &str, hash: &str) -> GraphEntity {
    GraphEntity {
        id: id.into(),
        kind: EntityKind::Service,
        endpoint: Some(Endpoint {
            name: name.into(),
            type_name: "test/srv/Work".into(),
            type_hash: hash.into(),
        }),
        ..owner.clone()
    }
}

fn options(value: Value) -> Options {
    Options::parse("snapshot", &value).unwrap()
}

fn cache(
    nodes: &BTreeMap<String, ObservedNode>,
    side_effects: Value,
) -> BTreeMap<String, Description> {
    nodes.iter().map(|(id,node)| (id.clone(),Description {
        graph:node.clone(),attempted:Instant::now(),checked_at:1,last_success:Some(1),status:"ok",error:None,
        card:compact_card(&json!({"summary":"A worker","effective_config":{"parameter_version":7,"parameters":{"secret":"not-for-inventory"}},
            "endpoints":node.entities.iter().filter_map(|e|e.endpoint.as_ref().map(|endpoint|json!({
                "kind":kind_name(e.kind),"name":endpoint.name,"type_name":endpoint.type_name,
                "type_hash":endpoint.type_hash,"side_effects":side_effects,"schema":{"secret":"not-for-inventory"}
            }))).collect::<Vec<_>>() }))
    })).collect()
}

#[test]
fn unknown_side_effects_stay_unknown_and_secrets_are_not_projected() {
    let owner = node("n1", "worker", "/fleet");
    let nodes = observe(vec![service(&owner, "s1", "/work", "hash"), owner]);
    let view = project(&nodes, &cache(&nodes, Value::Null), &options(json!({}))).unwrap();
    assert!(view["n1"]["endpoints"][0]["side_effects"].is_null());
    assert_eq!(view["n1"]["parameter_version"], 7);
    assert!(!serde_json::to_string(&view)
        .unwrap()
        .contains("not-for-inventory"));
}

#[test]
fn duplicate_names_keep_distinct_instances_without_trusting_a_card() {
    let nodes = observe(vec![node("n1", "same", "/"), node("n2", "same", "/")]);
    let view = project(
        &nodes,
        &cache(&nodes, json!("read_only")),
        &options(json!({})),
    )
    .unwrap();
    assert_eq!(view.len(), 2);
    for item in view.values() {
        assert_eq!(item["introspection"]["status"], "ambiguous");
        assert_eq!(item["introspection"]["error_code"], "AMBIGUOUS_NODE");
        assert!(item["summary"].is_null());
    }
}

#[test]
fn filtering_a_node_does_not_hide_service_provider_conflicts() {
    let one = node("n1", "one", "/fleet");
    let two = node("n2", "two", "/other");
    let nodes = observe(vec![
        service(&one, "s1", "/shared", "h1"),
        service(&two, "s2", "/shared", "h2"),
        one,
        two,
    ]);
    let view = project(
        &nodes,
        &BTreeMap::new(),
        &options(json!({"node_name":"/fleet/one"})),
    )
    .unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(
        view["n1"]["endpoints"][0]["provider_selection"]["status"],
        "multiple_observed"
    );
    assert_eq!(
        view["n1"]["endpoints"][0]["provider_selection"]["node_count"],
        2
    );
}

#[test]
fn endpoint_changes_invalidate_metadata_and_do_not_resurrect_removed_services() {
    let owner = node("n1", "worker", "/");
    let before = observe(vec![service(&owner, "s1", "/old", "h1"), owner.clone()]);
    let descriptions = cache(&before, json!("read_only"));
    let after = observe(vec![service(&owner, "s2", "/new", "h2"), owner]);
    let view = project(&after, &descriptions, &options(json!({}))).unwrap();
    assert_eq!(view["n1"]["introspection"]["status"], "pending");
    assert_eq!(view["n1"]["endpoints"].as_array().unwrap().len(), 1);
    assert_eq!(view["n1"]["endpoints"][0]["name"], "/new");
    assert!(view["n1"]["endpoints"][0]["side_effects"].is_null());
}

#[test]
fn timestamp_refresh_is_quiet_but_metadata_and_restart_are_changes() {
    let nodes = observe(vec![node("n1", "worker", "/")]);
    let mut descriptions = cache(&nodes, Value::Null);
    let before = project(&nodes, &descriptions, &options(json!({}))).unwrap();
    descriptions.get_mut("n1").unwrap().checked_at = 99;
    let refreshed = project(&nodes, &descriptions, &options(json!({}))).unwrap();
    assert!(delta(&before, &refreshed).is_none());
    descriptions.get_mut("n1").unwrap().card["parameter_version"] = json!(8);
    let after = project(&nodes, &descriptions, &options(json!({}))).unwrap();
    assert_eq!(
        delta(&before, &after).unwrap()["updated"][0]["parameter_version"],
        8
    );
    let restarted = observe(vec![node("n2", "worker", "/")]);
    let change = delta(
        &after,
        &project(&restarted, &BTreeMap::new(), &options(json!({}))).unwrap(),
    )
    .unwrap();
    assert_eq!(change["removed_node_ids"], json!(["n1"]));
    assert_eq!(change["added"][0]["node_id"], "n2");
}

#[test]
fn namespace_boundaries_and_hard_limits_never_silently_drop_nodes() {
    let nodes = observe(vec![
        node("n1", "one", "/fleet"),
        node("n2", "two", "/fleet/sub"),
        node("n3", "three", "/fleet2"),
    ]);
    let view = project(
        &nodes,
        &BTreeMap::new(),
        &options(json!({"namespace":"/fleet"})),
    )
    .unwrap();
    assert_eq!(view.len(), 2);
    assert!(!view.contains_key("n3"));
    let error = project(&nodes, &BTreeMap::new(), &options(json!({"max_nodes":1}))).unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
}

#[test]
fn control_filter_checks_type_and_owner_not_just_a_suffix() {
    let owner = node("n1", "worker", "/");
    let business = service(&owner, "s1", "/worker/get_node_card", "h");
    assert!(!internal(&business));
    let mut builtin = business.clone();
    builtin.endpoint.as_mut().unwrap().type_name = "aos_interfaces/srv/GetNodeCard".into();
    assert!(internal(&builtin));
    builtin.endpoint.as_mut().unwrap().name = "/elsewhere/get_node_card".into();
    assert!(!internal(&builtin));
}

#[test]
fn description_clipping_is_visible_and_wait_cannot_match_without_a_selector() {
    let card = compact_card(&json!({"summary":"中".repeat(2000),"tags":vec!["tag";40]}));
    assert_eq!(card["summary"].as_str().unwrap().chars().count(), 1024);
    assert_eq!(card["tags"].as_array().unwrap().len(), 32);
    assert_eq!(card["description_truncated"], true);
    assert!(validate("wait", &json!({})).is_err());
    assert!(validate("wait", &json!({"node_name":"/n","descriptions":false})).is_err());
    assert!(validate("snapshot", &json!({"max_nodes":1.0})).is_err());
    assert!(validate("snapshot", &json!({"timeout":0.1,"settle_time":1})).is_err());
}

#[test]
fn oversized_structured_metadata_cannot_enter_the_compact_inventory() {
    let hostile = json!({"payload":"x".repeat(900_000)});
    let card = compact_card(
        &json!({"lifecycle":{"current_state":{"id":hostile,"label":hostile},
        "gate_open":hostile,"transitioning":hostile,"epoch":hostile},
        "effective_config":{"parameter_version":hostile}}),
    );
    assert!(serde_json::to_vec(&card).unwrap().len() < 1024);
    assert_eq!(card["metadata_sanitized"], true);
    assert!(card["parameter_version"].is_null());
    assert!(card["lifecycle"]["gate_open"].is_null());
    assert!(card["lifecycle"]["current_state"]["id"].is_null());
}

#[test]
fn failed_refresh_retains_only_explicitly_stale_metadata_for_the_same_graph() {
    let owner = node("n1", "worker", "/");
    let nodes = observe(vec![service(&owner, "s1", "/work", "hash"), owner]);
    let mut descriptions = cache(&nodes, json!("read_only"));
    let failed = descriptions.get_mut("n1").unwrap();
    failed.status = "unavailable";
    failed.error = Some("TIMEOUT");
    failed.checked_at = 2;
    let view = project(&nodes, &descriptions, &options(json!({}))).unwrap();
    assert_eq!(view["n1"]["introspection"]["stale"], true);
    assert_eq!(view["n1"]["introspection"]["last_success_at_unix_ms"], 1);
    assert_eq!(view["n1"]["endpoints"][0]["description_status"], "stale");
    assert_eq!(view["n1"]["endpoints"][0]["side_effects"], "read_only");
}

#[test]
fn endpoint_only_owners_are_explicit_and_cli_noise_can_be_requested() {
    let owner = node("n1", "worker", "/");
    let nodes = observe(vec![service(&owner, "s1", "/work", "hash")]);
    let view = project(&nodes, &BTreeMap::new(), &options(json!({}))).unwrap();
    assert_eq!(view["n1"]["introspection"]["status"], "undeclared");
    assert!(
        next_probe(&nodes, &BTreeMap::new(), &options(json!({})), false)
            .unwrap()
            .is_none()
    );
    let name = "aos_cli_0123456789abcdef0123456789abcdef";
    let nodes = observe(vec![
        node("cli", name, "/"),
        node("app", "aos_cli_app", "/"),
    ]);
    assert_eq!(
        project(&nodes, &BTreeMap::new(), &options(json!({})))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        project(
            &nodes,
            &BTreeMap::new(),
            &options(json!({"include_cli_nodes":true}))
        )
        .unwrap()
        .len(),
        2
    );
    assert_eq!(
        project(
            &nodes,
            &BTreeMap::new(),
            &options(json!({"node_name":format!("/{name}")}))
        )
        .unwrap()
        .len(),
        1
    );
}

#[test]
fn cli_publishers_and_subscriptions_remain_discoverable() {
    let owner = node("cli", "aos_cli_0123456789abcdef0123456789abcdef", "/");
    for kind in [EntityKind::Publisher, EntityKind::Subscription] {
        let mut endpoint = service(&owner, "endpoint", "/chatter", "hash");
        endpoint.kind = kind;
        let nodes = observe(vec![owner.clone(), endpoint]);
        for request in [
            json!({}),
            json!({"endpoint_name":"/chatter","kind":kind_name(kind)}),
        ] {
            let view = project(&nodes, &BTreeMap::new(), &options(request)).unwrap();
            assert_eq!(view["cli"]["endpoints"][0]["name"], "/chatter");
        }
    }
    let mut client = service(&owner, "client", "/work", "hash");
    client.kind = EntityKind::Client;
    let nodes = observe(vec![owner, client]);
    assert!(project(&nodes, &BTreeMap::new(), &options(json!({})))
        .unwrap()
        .is_empty());
    assert_eq!(
        project(
            &nodes,
            &BTreeMap::new(),
            &options(json!({"include_clients":true}))
        )
        .unwrap()
        .len(),
        1
    );
}
