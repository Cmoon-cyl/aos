//! Native mTLS, router ACL decisions, and actual provider-side effects.
//! Requires OpenSSL and local loopback sockets; no committed credentials.
#[path = "support/security.rs"]
mod support;

use aos_core::{
    ActionClient, ActionServer, ActionServerOptions, Context, ContextConfigRequest, Durability,
    EndpointOptions, Error, Interface, Node, NodeOptions, QoS,
};
use aos_core::{
    LifecycleCallbackReturn, LifecycleClient, LifecycleState, LifecycleTransitionSelector,
    Parameter, ParameterClient, ParameterDescriptor, ParameterValue,
};
use aos_zenoh::security::*;
use serde_json::{json, Value};
use std::{
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use support::{AclLogs, Certificates};
use zenoh::Wait;

const DOMAIN: u32 = 19;
const WAIT: Duration = Duration::from_secs(5);
const DENIED_WAIT: Duration = Duration::from_millis(250);

fn node(name: &str) -> NodeIdentity {
    NodeIdentity {
        name: name.into(),
        namespace: "/robot".into(),
    }
}
fn message() -> Value {
    json!({"name":"test/msg/Value","kind":"msg","message":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})
}
fn service() -> Value {
    json!({"name":"test/srv/Change","kind":"srv","request":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false},"response":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})
}
fn action() -> Value {
    json!({"name":"test/action/Work","kind":"action","goal":{"type":"object"},"result":{"type":"object","default":{}},"feedback":{"type":"object"}})
}
fn grant(
    owner: &str,
    name: &str,
    interface: Value,
    operations: Vec<EndpointOperation>,
    qos: Value,
) -> EndpointGrant {
    EndpointGrant {
        node: node(owner),
        namespace: "/robot".into(),
        name: name.into(),
        interface,
        operations,
        qos,
    }
}
fn subject(name: &str) -> SubjectPolicy {
    SubjectPolicy {
        id: name.into(),
        common_name: name.into(),
        nodes: vec![node(name)],
        endpoints: vec![],
        systems: vec![],
        graph_nodes: vec![],
    }
}
fn policy() -> SecurityPolicy {
    use EndpointOperation::*;
    let mut provider = subject("provider");
    provider.nodes.push(node("managed"));
    provider.systems = vec![SystemGrant {
        node: node("managed"),
        target: node("managed"),
        operations: vec![
            SystemOperation::ParametersProvide,
            SystemOperation::LifecycleProvide,
        ],
    }];
    provider.endpoints = vec![
        grant(
            "provider",
            "/robot/value",
            message(),
            vec![Publish],
            Value::Null,
        ),
        grant(
            "provider",
            "/robot/state",
            message(),
            vec![Publish],
            json!({"durability":"transient_local","depth":2}),
        ),
        grant(
            "provider",
            "/robot/change",
            service(),
            vec![Provide],
            Value::Null,
        ),
        grant(
            "provider",
            "/robot/work",
            action(),
            vec![Provide],
            Value::Null,
        ),
    ];
    let mut reader = subject("reader");
    reader.endpoints = vec![
        grant(
            "reader",
            "/robot/value",
            message(),
            vec![Subscribe],
            Value::Null,
        ),
        grant(
            "reader",
            "/robot/state",
            message(),
            vec![Subscribe],
            json!({"durability":"transient_local","depth":2}),
        ),
        grant(
            "reader",
            "/robot/work",
            action(),
            vec![SendGoal, GetResult, Feedback, Status],
            Value::Null,
        ),
    ];
    reader.graph_nodes = vec![node("provider"), node("managed")];
    reader.systems = vec![SystemGrant {
        node: node("reader"),
        target: node("managed"),
        operations: vec![
            SystemOperation::ParametersRead,
            SystemOperation::LifecycleRead,
        ],
    }];
    let mut writer = subject("writer");
    writer.graph_nodes = vec![node("provider"), node("managed")];
    writer.endpoints = vec![
        grant(
            "writer",
            "/robot/change",
            service(),
            vec![Call],
            Value::Null,
        ),
        grant(
            "writer",
            "/robot/work",
            action(),
            vec![CancelGoal],
            Value::Null,
        ),
    ];
    writer.systems = vec![SystemGrant {
        node: node("writer"),
        target: node("managed"),
        operations: vec![
            SystemOperation::ParametersWrite,
            SystemOperation::LifecycleChange,
        ],
    }];
    let mut hidden = subject("hidden");
    hidden.endpoints = vec![grant(
        "hidden",
        "/robot/hidden",
        message(),
        vec![Publish],
        Value::Null,
    )];
    SecurityPolicy {
        schema_version: 1,
        domain_id: DOMAIN,
        subjects: vec![provider, reader, writer, hidden],
    }
}

struct Router {
    session: zenoh::Session,
    endpoint: String,
}
impl Router {
    fn start(
        certificates: &Certificates,
        certificate: &str,
        policy: &CompiledSecurityPolicy,
    ) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let endpoint = format!("tls/127.0.0.1:{port}");
        let config = router_config(&certificates.router(&endpoint, certificate), policy).unwrap();
        let session = zenoh::open(config).wait().expect("secure router opens");
        Self { session, endpoint }
    }
    fn context(&self, certificates: &Certificates, name: &str) -> Context {
        open_secure_context(
            ContextConfigRequest::default(),
            &certificates.client(&self.endpoint, name, DOMAIN),
        )
        .expect("authenticated Context connects")
    }
    fn raw(&self, certificates: &Certificates, name: &str) -> zenoh::Session {
        zenoh::open(client_config(&certificates.client(&self.endpoint, name, DOMAIN)).unwrap())
            .wait()
            .expect("authenticated native session connects")
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.session.close().wait();
    }
}

fn key(name: &str, interface: &Interface, domain: u32) -> String {
    format!(
        "aos/{domain}/{}/{}/{}",
        name.trim_start_matches('/'),
        interface.type_name(),
        interface.type_hash()
    )
}
fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "native propagation deadline");
        thread::sleep(Duration::from_millis(20));
    }
}
fn rejected(profile: &ClientTlsProfile, label: &str) {
    let start = Instant::now();
    let result = zenoh::open(client_config(profile).unwrap()).wait();
    assert!(result.is_err(), "{label} unexpectedly authenticated");
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "{label} did not honor bounded connection"
    );
    println!("native TLS rejection: {label}");
}

fn no_response<T: std::fmt::Debug>(result: aos_core::Result<T>, label: &str) {
    assert!(
        matches!(result, Err(Error::Timeout | Error::NoResponse(_))),
        "{label}: {result:?}"
    );
}

fn wait_service(node: &Node, interface: &Interface, name: &str) {
    let client = node
        .create_client(interface, name, EndpointOptions::default())
        .unwrap();
    assert!(
        client.wait_for_service(Some(WAIT)).unwrap(),
        "authorized service graph declaration must be visible for {name}"
    );
}

// Graph declarations can precede data-route convergence. Only a read-only
// readiness probe is repeated; writes and goal control below run exactly once.
fn read_until_ready<T>(mut read: impl FnMut() -> aos_core::Result<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        match read() {
            Ok(value) => return value,
            Err(Error::Timeout | Error::NoResponse(_)) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("authorized read-only readiness probe failed: {error}"),
        }
    }
}

#[test]
fn native_tls_and_acl_enforce_identity_resources_and_business_effects() {
    let logs = AclLogs::install();
    let certificates = Certificates::new();
    for name in [
        "router", "provider", "reader", "writer", "hidden", "unmapped",
    ] {
        certificates.issue(name, name, "ca", "DNS:localhost,IP:127.0.0.1", false);
    }
    certificates.issue(
        "untrusted",
        "reader",
        "other_ca",
        "DNS:localhost,IP:127.0.0.1",
        false,
    );
    certificates.issue("wrong_name", "router", "ca", "DNS:unrelated.invalid", false);
    certificates.issue(
        "expired_client",
        "reader",
        "ca",
        "DNS:localhost,IP:127.0.0.1",
        true,
    );
    certificates.issue(
        "expired_router",
        "router",
        "ca",
        "DNS:localhost,IP:127.0.0.1",
        true,
    );
    let compiled = compile_policy(&policy()).unwrap();
    let router = Router::start(&certificates, "router", &compiled);

    // Establish a real valid connection before evaluating negative cases.
    let valid = router.raw(&certificates, "reader");
    assert_eq!(
        valid.info().routers_zid().wait().collect::<Vec<_>>(),
        vec![router.session.info().zid().wait()]
    );
    valid.close().wait().unwrap();
    let base = certificates.client(&router.endpoint, "reader", DOMAIN);
    let mut wrong_ca = base.clone();
    wrong_ca.root_ca_certificate = certificates.path("other_ca.pem");
    rejected(&wrong_ca, "wrong server CA");
    rejected(
        &certificates.client(&router.endpoint, "untrusted", DOMAIN),
        "untrusted client CA",
    );
    rejected(
        &certificates.client(&router.endpoint, "expired_client", DOMAIN),
        "expired client certificate",
    );
    let mut missing = base.clone();
    missing.certificate = certificates.path("missing.pem");
    rejected(&missing, "missing client certificate");
    let mut mismatched = base.clone();
    mismatched.private_key = certificates.path("writer.key");
    rejected(&mismatched, "mismatched certificate and key");
    std::fs::write(certificates.directory().join("empty.pem"), "").unwrap();
    let mut empty = base.clone();
    empty.root_ca_certificate = certificates.path("empty.pem");
    rejected(
        &empty,
        "empty explicit CA fails closed against private router",
    );
    let mut no_certificate = client_config(&base).unwrap();
    no_certificate
        .insert_json5("transport/link/tls/connect_certificate", "null")
        .unwrap();
    no_certificate
        .insert_json5("transport/link/tls/connect_private_key", "null")
        .unwrap();
    assert!(
        zenoh::open(no_certificate).wait().is_err(),
        "mTLS rejects a client with no certificate"
    );
    let mut plaintext = client_config(&base).unwrap();
    plaintext
        .insert_json5("transport/link/protocols", "[\"tcp\"]")
        .unwrap();
    plaintext
        .insert_json5(
            "connect/endpoints",
            &json!([router.endpoint.replacen("tls/", "tcp/", 1)]).to_string(),
        )
        .unwrap();
    assert!(
        zenoh::open(plaintext).wait().is_err(),
        "TLS router must not accept a plaintext client"
    );
    for (certificate, label) in [
        ("wrong_name", "wrong server SAN"),
        ("expired_router", "expired server certificate"),
    ] {
        let invalid_router = Router::start(&certificates, certificate, &compiled);
        rejected(
            &certificates.client(&invalid_router.endpoint, "reader", DOMAIN),
            label,
        );
    }

    let provider_context = router.context(&certificates, "provider");
    let reader_context = router.context(&certificates, "reader");
    let writer_context = router.context(&certificates, "writer");
    let hidden_context = router.context(&certificates, "hidden");
    let provider = Node::new(
        provider_context.clone(),
        "provider",
        "/robot",
        NodeOptions::default(),
    )
    .unwrap();
    let reader = Node::new(
        reader_context.clone(),
        "reader",
        "/robot",
        NodeOptions::default(),
    )
    .unwrap();
    let writer = Node::new(
        writer_context.clone(),
        "writer",
        "/robot",
        NodeOptions::default(),
    )
    .unwrap();
    let hidden = Node::new(
        hidden_context.clone(),
        "hidden",
        "/robot",
        NodeOptions::default(),
    )
    .unwrap();
    let watch = reader_context.watch_graph().unwrap().unwrap();
    let message = Interface::from_json(message()).unwrap();
    let publisher = provider
        .create_publisher(
            &message,
            "/robot/value",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let subscription = reader
        .create_subscription(
            &message,
            "/robot/value",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let _hidden_publisher = hidden
        .create_publisher(
            &message,
            "/robot/hidden",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    publisher.wait_for_subscribers(WAIT).unwrap();
    publisher.publish(&json!({"value":1})).unwrap();
    assert_eq!(subscription.recv_timeout(WAIT).unwrap(), json!({"value":1}));
    wait_until(|| {
        watch.snapshot().unwrap().iter().any(|entity| {
            entity
                .endpoint
                .as_ref()
                .is_some_and(|ep| ep.name == "/robot/value")
        })
    });
    let graph = reader_context.graph(WAIT).unwrap();
    assert!(graph.iter().any(|entity| entity.node_name == "provider"));
    // ACL applies at the router. A Context always sees its own local graph.
    assert!(
        graph
            .iter()
            .all(|entity| matches!(entity.node_name.as_str(), "provider" | "reader")),
        "graph must filter unauthorized remote Node metadata: {graph:?}"
    );
    assert!(watch
        .snapshot()
        .unwrap()
        .iter()
        .all(|entity| matches!(entity.node_name.as_str(), "provider" | "reader")));
    wait_until(|| logs.contains_denial("%robot/hidden"));
    println!("native ACL: graph snapshot and live watch filter unauthorized remote Nodes");

    // A valid certificate is not permission to publish, forge identity, or
    // change the domain, namespace, Interface hash, or native key.
    let attacker = router.raw(&certificates, "reader");
    let topic_key = key("/robot/value", &message, DOMAIN);
    let other_hash = format!(
        "{}/{}",
        topic_key.rsplit_once('/').unwrap().0,
        "0".repeat(64)
    );
    let attempts = [
        topic_key.clone(),
        key("/robot/value", &message, DOMAIN + 1),
        key("/robot2/value", &message, DOMAIN),
        other_hash,
    ];
    for attempt in &attempts {
        attacker
            .declare_publisher(attempt.as_str())
            .wait()
            .unwrap()
            .put(br#"{"value":999}"#.to_vec())
            .wait()
            .unwrap();
    }
    assert_eq!(subscription.recv_timeout(DENIED_WAIT), Err(Error::Timeout));
    for attempt in &attempts {
        wait_until(|| logs.contains_denial(attempt));
    }
    let forged = attacker
        .liveliness()
        .declare_token(format!(
            "aos/@lv/{DOMAIN}/forged/foreign/node/NN/%/%robot/provider"
        ))
        .wait()
        .unwrap();
    wait_until(|| logs.contains_denial("/forged/foreign/node/NN/"));
    assert!(!reader_context
        .graph(WAIT)
        .unwrap()
        .iter()
        .any(|entity| entity.id == "node"));
    forged.undeclare().wait().unwrap();
    let unmapped = router.raw(&certificates, "unmapped");
    unmapped
        .put(topic_key.as_str(), br#"{"value":888}"#.to_vec())
        .wait()
        .unwrap();
    assert_eq!(subscription.recv_timeout(DENIED_WAIT), Err(Error::Timeout));
    unmapped.close().wait().unwrap();
    publisher.publish(&json!({"value":2})).unwrap();
    assert_eq!(
        subscription.recv_timeout(WAIT).unwrap(),
        json!({"value":2}),
        "authorized route still works after rejected attempts"
    );
    let unauthorized_reader = router.raw(&certificates, "hidden");
    let denied_subscription = unauthorized_reader
        .declare_subscriber(topic_key.as_str())
        .wait()
        .unwrap();
    publisher.publish(&json!({"value":3})).unwrap();
    assert_eq!(subscription.recv_timeout(WAIT).unwrap(), json!({"value":3}));
    assert!(denied_subscription
        .recv_timeout(DENIED_WAIT)
        .unwrap()
        .is_none());
    unauthorized_reader.close().wait().unwrap();
    println!("native ACL: Topic deny decisions and subscriber no-effect confirmed");

    // Service denial is checked at the provider queue and business state,
    // then a distinct authorized identity performs the same operation.
    let service_interface = Interface::from_json(service()).unwrap();
    let service = provider
        .create_service(
            &service_interface,
            "/robot/change",
            EndpointOptions::default(),
        )
        .unwrap();
    let denied = reader
        .create_client(
            &service_interface,
            "/robot/change",
            EndpointOptions::default(),
        )
        .unwrap();
    let allowed = writer
        .create_client(
            &service_interface,
            "/robot/change",
            EndpointOptions::default(),
        )
        .unwrap();
    wait_service(&writer, &service_interface, "/robot/change");
    let mut business_value = 1;
    no_response(
        denied.call(&json!({"value":999}), DENIED_WAIT),
        "unauthorized service call",
    );
    assert!(matches!(
        service.recv_timeout(DENIED_WAIT),
        Err(Error::Timeout)
    ));
    assert_eq!(business_value, 1);
    wait_until(|| logs.contains_denial(&key("/robot/change", &service_interface, DOMAIN)));
    thread::scope(|scope| {
        let response = scope.spawn(|| allowed.call(&json!({"value":7}), WAIT));
        let request = service.recv_timeout(WAIT).unwrap();
        business_value = request.request()["value"].as_i64().unwrap();
        request.reply(&json!({"value":business_value})).unwrap();
        assert_eq!(response.join().unwrap().unwrap(), json!({"value":7}));
    });
    assert_eq!(business_value, 7);
    println!("native ACL: Service deny/no-effect and authorized mutation confirmed");

    // System service writes require distinct permissions even when a subject
    // can read the same Node. Assert the authoritative state and callback queue.
    let managed = Node::new_lifecycle(
        provider_context.clone(),
        "managed",
        "/robot",
        NodeOptions::default(),
    )
    .unwrap();
    let store = managed.parameters();
    store
        .declare(
            "value",
            ParameterValue::Integer(1),
            ParameterDescriptor::default(),
            false,
        )
        .unwrap();
    let parameters = managed.enable_parameter_services().unwrap();
    let parameter_reader = ParameterClient::new(&reader, "/robot/managed").unwrap();
    let parameter_writer = ParameterClient::new(&writer, "/robot/managed").unwrap();
    wait_service(
        &reader,
        &aos_core::ParameterServiceKind::GetParameters.interface(),
        "/robot/managed/get_parameters",
    );
    wait_service(
        &writer,
        &aos_core::ParameterServiceKind::SetParametersAtomically.interface(),
        "/robot/managed/set_parameters_atomically",
    );
    thread::scope(|scope| {
        let received = scope
            .spawn(|| read_until_ready(|| parameter_reader.get(&["value".into()], DENIED_WAIT)));
        while !received.is_finished() {
            match parameters.recv_timeout(Duration::from_millis(50)) {
                Ok(request) => {
                    request.execute(|_| Ok(())).unwrap();
                }
                Err(Error::Timeout) => {}
                Err(error) => panic!("parameter read queue failed: {error}"),
            }
        }
        assert_eq!(
            received.join().unwrap().value.unwrap(),
            [ParameterValue::Integer(1)]
        );
    });
    let version = store.version().unwrap();
    no_response(
        parameter_reader.set_atomically(
            &[Parameter::new("value", ParameterValue::Integer(999))],
            DENIED_WAIT,
        ),
        "unauthorized parameter write",
    );
    assert!(matches!(
        parameters.recv_timeout(DENIED_WAIT),
        Err(Error::Timeout)
    ));
    assert_eq!(store.version().unwrap(), version);
    assert_eq!(store.get("value").unwrap(), ParameterValue::Integer(1));
    wait_until(|| logs.contains_denial("/robot/managed/set_parameters_atomically/"));
    thread::scope(|scope| {
        let changed = scope.spawn(|| {
            parameter_writer
                .set_atomically(&[Parameter::new("value", ParameterValue::Integer(7))], WAIT)
        });
        parameters
            .recv_timeout(WAIT)
            .unwrap()
            .execute(|_| Ok(()))
            .unwrap();
        assert!(changed.join().unwrap().unwrap().successful);
    });
    assert_eq!(store.get("value").unwrap(), ParameterValue::Integer(7));
    let lifecycle = managed.lifecycle().unwrap();
    let lifecycle_server = managed.enable_lifecycle_services().unwrap();
    let lifecycle_reader = LifecycleClient::new(&reader, "/robot/managed").unwrap();
    let lifecycle_writer = LifecycleClient::new(&writer, "/robot/managed").unwrap();
    wait_service(
        &reader,
        &aos_core::LifecycleServiceKind::GetState.interface(),
        "/robot/managed/get_state",
    );
    wait_service(
        &writer,
        &aos_core::LifecycleServiceKind::ChangeState.interface(),
        "/robot/managed/change_state",
    );
    assert_eq!(
        read_until_ready(|| lifecycle_reader.state(DENIED_WAIT))
            .current_state
            .id,
        LifecycleState::Unconfigured as u8
    );
    let configure = || LifecycleTransitionSelector {
        id: 0,
        label: "configure".into(),
    };
    no_response(
        lifecycle_reader.change_state(configure(), None, DENIED_WAIT),
        "unauthorized lifecycle change",
    );
    assert!(matches!(
        lifecycle_server.take_change_request(DENIED_WAIT),
        Err(Error::Timeout)
    ));
    assert_eq!(
        lifecycle.current_state().unwrap(),
        LifecycleState::Unconfigured
    );
    wait_until(|| logs.contains_denial("/robot/managed/change_state/"));
    thread::scope(|scope| {
        let changed = scope.spawn(|| lifecycle_writer.change_state(configure(), None, WAIT));
        let request = lifecycle_server.take_change_request(WAIT).unwrap();
        let report = request
            .begin_transition()
            .unwrap()
            .complete(LifecycleCallbackReturn::Success, None)
            .unwrap()
            .report
            .unwrap();
        request.reply(&report).unwrap();
        assert!(changed.join().unwrap().unwrap().success);
    });
    assert_eq!(lifecycle.current_state().unwrap(), LifecycleState::Inactive);
    println!("native ACL: Parameter and Lifecycle read/write separation and authoritative no-effect confirmed");

    let transient = QoS {
        durability: Durability::TransientLocal,
        depth: 2,
        ..QoS::default()
    };
    let state = provider
        .create_publisher(
            &message,
            "/robot/state",
            transient.clone(),
            EndpointOptions::default(),
        )
        .unwrap();
    // Complete an actual delivery for every publication before declaring the
    // late subscriber, so an in-flight live sample cannot masquerade as history.
    let live_state = reader
        .create_subscription(
            &message,
            "/robot/state",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    state.wait_for_subscribers(WAIT).unwrap();
    for value in [1, 2, 3] {
        state.publish(&json!({"value":value})).unwrap();
        assert_eq!(
            live_state.recv_timeout(WAIT).unwrap(),
            json!({"value":value})
        );
    }
    live_state.close().unwrap();
    let late = reader
        .create_subscription(
            &message,
            "/robot/state",
            transient,
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(late.recv_timeout(WAIT).unwrap(), json!({"value":2}));
    assert_eq!(late.recv_timeout(WAIT).unwrap(), json!({"value":3}));
    println!("native ACL: transient-local late history confirmed");

    let action = Interface::from_json(action()).unwrap();
    let server = ActionServer::new(
        &provider,
        &action,
        "/robot/work",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client =
        ActionClient::new(&reader, &action, "/robot/work", EndpointOptions::default()).unwrap();
    let cancel_client =
        ActionClient::new(&writer, &action, "/robot/work", EndpointOptions::default()).unwrap();
    let action_types = aos_core::ActionInterfaces::new(&action).unwrap();
    wait_service(
        &reader,
        &action_types.send_goal,
        "/robot/work/_action/send_goal",
    );
    wait_service(
        &writer,
        &action_types.cancel_goal,
        "/robot/work/_action/cancel_goal",
    );
    let goal = thread::scope(|scope| {
        let submitted = scope.spawn(|| client.send_goal(&json!({}), None, WAIT));
        let request = server.take_goal_request(WAIT).unwrap();
        let goal = request.decide(true).unwrap().unwrap();
        assert!(submitted.join().unwrap().unwrap().accepted());
        goal
    });
    goal.execute().unwrap();
    no_response(
        client.cancel_goal(goal.goal_id(), DENIED_WAIT),
        "unauthorized goal cancel",
    );
    assert!(
        matches!(server.take_cancel_request(DENIED_WAIT), Err(Error::Timeout)),
        "unauthorized cancel must not reach provider"
    );
    wait_until(|| logs.contains_denial("/robot/work/_action/cancel_goal/"));
    thread::scope(|scope| {
        let canceled = scope.spawn(|| cancel_client.cancel_goal(goal.goal_id(), WAIT));
        let request = server.take_cancel_request(WAIT).unwrap();
        request.respond(&[goal.goal_id().to_owned()]).unwrap();
        assert_eq!(canceled.join().unwrap().unwrap()["return_code"], 0);
    });
    goal.canceled().unwrap();
    goal.finish(Some(&json!({}))).unwrap();
    assert_eq!(
        client.get_result(goal.goal_id(), WAIT).unwrap()["status"],
        5
    );

    // Cache construction logs decisions for every message kind. Require an
    // immediately following interceptor-chain drop marker on the same thread,
    // for both cached Put/declare and uncached Query interception paths.
    for (key, operation) in [
        (topic_key.clone(), "Put (ingress)"),
        (topic_key.clone(), "Subscriber (ingress)"),
        (
            key("/robot/change", &service_interface, DOMAIN),
            "Query (ingress)",
        ),
        (
            key(
                "/robot/managed/set_parameters_atomically",
                &aos_core::ParameterServiceKind::SetParametersAtomically.interface(),
                DOMAIN,
            ),
            "Query (ingress)",
        ),
        (
            key(
                "/robot/managed/change_state",
                &aos_core::LifecycleServiceKind::ChangeState.interface(),
                DOMAIN,
            ),
            "Query (ingress)",
        ),
        (
            key(
                "/robot/work/_action/cancel_goal",
                &action_types.cancel_goal,
                DOMAIN,
            ),
            "Query (ingress)",
        ),
    ] {
        let evidence = logs.message_denials(&key, operation);
        assert!(
            !evidence.is_empty(),
            "missing actual router denial for {operation} {key}"
        );
    }

    publisher.close().unwrap();
    wait_until(|| {
        !watch.snapshot().unwrap().iter().any(|entity| {
            entity
                .endpoint
                .as_ref()
                .is_some_and(|ep| ep.name == "/robot/value")
                && entity.node_name == "provider"
        })
    });

    attacker.close().wait().unwrap();
    for context in [
        &provider_context,
        &reader_context,
        &writer_context,
        &hidden_context,
    ] {
        context.shutdown().unwrap();
    }
    assert!(matches!(watch.snapshot(), Err(Error::Closed)));
    println!("native TLS/ACL: valid mTLS, CA/SAN/expiry/key rejection, graph snapshot/watch filtering, typed Topic denial, Service no-effect + authorized effect, TL history, Action cancel separation, shutdown PASS");
}
