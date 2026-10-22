//! C16/C17 native boundary closure: real expiry, individual operations, and
//! certificate identity. Requires OpenSSL and loopback; commits no credentials.
#![cfg(unix)]

#[path = "support/security_boundaries.rs"]
mod boundary_support;
#[path = "support/security.rs"]
mod support;

use aos_core::{
    ActionClient, ActionInterfaces, ActionServer, ActionServerOptions, Context,
    ContextConfigRequest, EndpointOptions, EntityKind, Error, Interface, Node, NodeOptions, QoS,
};
use aos_zenoh::security::*;
use serde_json::{json, Value};
use std::{
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};
use support::{AclLogs, Certificates};
use zenoh::{query::ReplyKeyExpr, Wait};

const DOMAIN: u32 = 29;
const WAIT: Duration = Duration::from_secs(5);
const DENIED: Duration = Duration::from_millis(300);
const ACTION_NAME: &str = "/robot/work";

fn identity(name: &str) -> NodeIdentity {
    NodeIdentity {
        name: name.into(),
        namespace: "/robot".into(),
    }
}
fn subject(name: &str) -> SubjectPolicy {
    SubjectPolicy {
        id: name.into(),
        common_name: name.into(),
        nodes: vec![identity(name)],
        endpoints: vec![],
        systems: vec![],
        graph_nodes: vec![],
    }
}
fn message() -> Value {
    json!({"name":"boundary/msg/Value","kind":"msg","message":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})
}
fn service() -> Value {
    json!({"name":"boundary/srv/Change","kind":"srv","request":{"type":"object","properties":{"value":{"type":"integer"},"subject":{"type":"string"},"common_name":{"type":"string"},"node":{"type":"string"}},"required":["value"],"additionalProperties":false},"response":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}})
}
fn action() -> Value {
    json!({"name":"boundary/action/Work","kind":"action","goal":{"type":"object"},"result":{"type":"object","default":{}},"feedback":{"type":"object"}})
}
fn grant(
    owner: &str,
    name: &str,
    interface: Value,
    operations: Vec<EndpointOperation>,
) -> EndpointGrant {
    EndpointGrant {
        node: identity(owner),
        namespace: "/robot".into(),
        name: name.into(),
        interface,
        operations,
        qos: Value::Null,
    }
}

fn policy() -> SecurityPolicy {
    use EndpointOperation::*;
    let mut provider = subject("provider");
    provider.endpoints = vec![
        grant("provider", "/robot/value", message(), vec![Publish]),
        grant("provider", "/robot/change", service(), vec![Provide]),
        grant("provider", "/robot/reply_probe", service(), vec![Provide]),
        grant("provider", "/robot/reply_target", service(), vec![Provide]),
        grant("provider", ACTION_NAME, action(), vec![Provide]),
    ];
    let mut allowed = subject("allowed");
    allowed.endpoints = vec![
        grant("allowed", "/robot/value", message(), vec![Subscribe]),
        grant("allowed", "/robot/change", service(), vec![Call]),
        grant("allowed", "/robot/reply_probe", service(), vec![Call]),
        grant("allowed", "/robot/reply_target", service(), vec![Call]),
        grant(
            "allowed",
            ACTION_NAME,
            action(),
            vec![SendGoal, GetResult, CancelGoal, Feedback, Status],
        ),
    ];
    allowed.systems = vec![SystemGrant {
        node: identity("allowed"),
        target: identity("provider"),
        operations: vec![SystemOperation::Describe],
    }];
    allowed.graph_nodes = vec![
        identity("provider"),
        identity("restricted"),
        identity("allowed"),
    ];
    let mut restricted = subject("restricted");
    restricted.endpoints = vec![grant(
        "restricted",
        "/robot/reply_probe",
        service(),
        vec![Provide],
    )];
    let mut subjects = vec![provider, allowed, restricted];
    for (name, omitted) in [
        ("no_goal", SendGoal),
        ("no_result", GetResult),
        ("no_feedback", Feedback),
        ("no_status", Status),
    ] {
        let mut limited = subject(name);
        limited.endpoints.push(grant(
            name,
            ACTION_NAME,
            action(),
            [SendGoal, GetResult, CancelGoal, Feedback, Status]
                .into_iter()
                .filter(|op| *op != omitted)
                .collect(),
        ));
        limited.graph_nodes.push(identity("provider"));
        subjects.push(limited);
    }
    SecurityPolicy {
        schema_version: 1,
        domain_id: DOMAIN,
        subjects,
    }
}

struct Router {
    session: zenoh::Session,
    endpoint: String,
}
impl Router {
    fn start(certs: &Certificates, name: &str, policy: &CompiledSecurityPolicy) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let endpoint = format!("tls/127.0.0.1:{port}");
        let session = zenoh::open(router_config(&certs.router(&endpoint, name), policy).unwrap())
            .wait()
            .unwrap();
        Self { session, endpoint }
    }
    fn raw(&self, certs: &Certificates, name: &str) -> zenoh::Session {
        zenoh::open(client_config(&certs.client(&self.endpoint, name, DOMAIN)).unwrap())
            .wait()
            .unwrap()
    }
    fn context(&self, certs: &Certificates, name: &str) -> Context {
        open_secure_context(
            ContextConfigRequest::default(),
            &certs.client(&self.endpoint, name, DOMAIN),
        )
        .unwrap()
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        let _ = self.session.close().wait();
    }
}
fn key(name: &str, interface: &Interface) -> String {
    format!(
        "aos/{DOMAIN}/{}/{}/{}",
        name.trim_start_matches('/'),
        interface.type_name(),
        interface.type_hash()
    )
}
fn wait_until(label: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "deadline: {label}");
        thread::sleep(Duration::from_millis(10));
    }
}
fn no_response<T>(result: aos_core::Result<T>, label: &str) {
    match result {
        Err(Error::Timeout | Error::NoResponse(_)) => {}
        Err(error) => panic!("{label}: unexpected error: {error}"),
        Ok(_) => panic!("{label}: unauthorized request succeeded"),
    }
}
fn denied(logs: &AclLogs, key: &str, operation: &str, before: usize, label: &str) {
    let deadline = Instant::now() + WAIT;
    while logs.message_denials(key, operation).len() <= before {
        if Instant::now() >= deadline {
            let text = logs.text();
            let relevant: Vec<_> = text
                .lines()
                .filter(|line| {
                    line.contains(key)
                        || line.contains("Msg intercepted!")
                        || line.contains("did not match any configured ACL subject")
                })
                .rev()
                .take(30)
                .collect();
            panic!("deadline: {label}; last relevant router events: {relevant:#?}");
        }
        thread::sleep(Duration::from_millis(10));
    }
    let evidence = logs.message_denials(key, operation);
    assert!(logs.contains_denial(key));
    println!("DENIAL {label}:\n{}", evidence[before]);
}
fn read_ready<T>(mut read: impl FnMut() -> aos_core::Result<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        match read() {
            Ok(value) => return value,
            Err(Error::Timeout | Error::NoResponse(_)) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(error) => panic!("read-only readiness failed: {error}"),
        }
    }
}
fn node(context: &Context, name: &str) -> Node {
    Node::new(context.clone(), name, "/robot", NodeOptions::default()).unwrap()
}
fn native_read(session: &zenoh::Session, key: &str, timeout: Duration) -> Option<String> {
    let replies = session
        .get(key.to_owned())
        .accept_replies(ReplyKeyExpr::Any)
        .timeout(timeout)
        .wait()
        .unwrap();
    // Disconnection after QueryFinal and timeout both mean no successful reply.
    replies
        .recv_timeout(timeout + DENIED)
        .ok()
        .flatten()
        .map(|reply| {
            reply
                .result()
                .unwrap()
                .payload()
                .try_to_string()
                .unwrap()
                .into_owned()
        })
}
fn native_read_ready(session: &zenoh::Session, key: &str) -> String {
    let mut value = None;
    wait_until("native read-only readiness", || {
        value = native_read(session, key, DENIED);
        value.is_some()
    });
    value.unwrap()
}

fn live_expiry(certs: &Certificates, compiled: &CompiledSecurityPolicy) {
    let typed_key = key("/robot/value", &Interface::from_json(message()).unwrap());
    // Client-leaf expiry: both publishers use the same policy subject and the
    // same live router. Only the short-lived certificate expires.
    let router = Router::start(certs, "router", compiled);
    let long_publisher = router.raw(certs, "provider");
    let receiver = router.raw(certs, "allowed");
    let subscriber = receiver
        .declare_subscriber(typed_key.clone())
        .wait()
        .unwrap();
    let control = long_publisher
        .declare_publisher(typed_key.clone())
        .wait()
        .unwrap();
    let expires = boundary_support::issue_short(certs, "short_client", "provider");
    let short = router.raw(certs, "short_client");
    let publisher = short.declare_publisher(typed_key.clone()).wait().unwrap();
    wait_until("short client has an actual subscriber", || {
        publisher.matching_status().wait().unwrap().matching()
    });
    publisher.put("before-client-expiry").wait().unwrap();
    assert_eq!(
        subscriber
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "before-client-expiry"
    );
    assert!(SystemTime::now() < expires);
    // TLS expiry closes the socket first; router metadata can remain until
    // Zenoh 1.10.1's default 10-second link lease expires. Budget that cleanup
    // after certificate expiry, rather than from the earlier positive probe.
    let limit = Instant::now()
        + expires
            .duration_since(SystemTime::now())
            .unwrap_or_default()
        + Duration::from_secs(12);
    while short.info().routers_zid().wait().next().is_some() {
        assert!(
            Instant::now() < limit,
            "existing client link survived certificate expiry"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(SystemTime::now() + Duration::from_secs(1) >= expires);
    let _ = publisher.put("after-client-expiry").wait();
    assert!(subscriber.recv_timeout(DENIED).unwrap().is_none());
    control.put("long-client-still-live").wait().unwrap();
    assert_eq!(
        subscriber
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "long-client-still-live"
    );
    assert!(zenoh::open(
        client_config(&certs.client(&router.endpoint, "short_client", DOMAIN)).unwrap()
    )
    .wait()
    .is_err());
    short.close().wait().unwrap();
    long_publisher.close().wait().unwrap();
    receiver.close().wait().unwrap();
    println!("PASS C16 live client leaf expiry: before delivery, existing link gone, after delivery absent, long-lived same-subject control live, expired reconnect rejected");

    // Server-leaf expiry has an independent long-lived router positive control.
    let control_sender = router.raw(certs, "provider");
    let control_receiver = router.raw(certs, "allowed");
    let control_sub = control_receiver
        .declare_subscriber(typed_key.clone())
        .wait()
        .unwrap();
    let control_pub = control_sender
        .declare_publisher(typed_key.clone())
        .wait()
        .unwrap();
    let expires = boundary_support::issue_short(certs, "short_router", "router");
    let expiring_router = Router::start(certs, "short_router", compiled);
    let sender = expiring_router.raw(certs, "provider");
    let receiver = expiring_router.raw(certs, "allowed");
    let subscriber = receiver
        .declare_subscriber(typed_key.clone())
        .wait()
        .unwrap();
    let publisher = sender.declare_publisher(typed_key).wait().unwrap();
    wait_until("short router data route", || {
        publisher.matching_status().wait().unwrap().matching()
    });
    publisher.put("before-router-expiry").wait().unwrap();
    assert_eq!(
        subscriber
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "before-router-expiry"
    );
    assert!(SystemTime::now() < expires);
    let limit = Instant::now()
        + expires
            .duration_since(SystemTime::now())
            .unwrap_or_default()
        + Duration::from_secs(12);
    while sender.info().routers_zid().wait().next().is_some()
        || receiver.info().routers_zid().wait().next().is_some()
    {
        assert!(
            Instant::now() < limit,
            "existing server link survived certificate expiry"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(SystemTime::now() + Duration::from_secs(1) >= expires);
    let _ = publisher.put("after-router-expiry").wait();
    assert!(subscriber.recv_timeout(DENIED).unwrap().is_none());
    wait_until("long router data route", || {
        control_pub.matching_status().wait().unwrap().matching()
    });
    control_pub.put("long-router-still-live").wait().unwrap();
    assert_eq!(
        control_sub
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "long-router-still-live"
    );
    assert!(zenoh::open(
        client_config(&certs.client(&expiring_router.endpoint, "provider", DOMAIN)).unwrap()
    )
    .wait()
    .is_err());
    for session in [&sender, &receiver, &control_sender, &control_receiver] {
        session.close().wait().unwrap();
    }
    println!("PASS C16 live server leaf expiry: before delivery, both existing links gone, after delivery absent, long-lived router control live, expired reconnect rejected");
}

fn service_identity_and_description(
    certs: &Certificates,
    compiled: &CompiledSecurityPolicy,
    logs: &AclLogs,
) {
    let router = Router::start(certs, "router", compiled);
    let provider_context = router.context(certs, "provider");
    let allowed_context = router.context(certs, "allowed");
    let restricted_context = router.context(certs, "restricted");
    let provider = node(&provider_context, "provider");
    let allowed = node(&allowed_context, "allowed");
    let restricted = node(&restricted_context, "restricted");
    let interface = Interface::from_json(service()).unwrap();
    let change_key = key("/robot/change", &interface);
    let service = provider
        .create_service(&interface, "/robot/change", EndpointOptions::default())
        .unwrap();
    let allowed_client = allowed
        .create_client(&interface, "/robot/change", EndpointOptions::default())
        .unwrap();
    assert!(allowed_client.wait_for_service(Some(WAIT)).unwrap());
    let watch = allowed_context.watch_graph().unwrap().unwrap();
    wait_until("authorized provider graph", || {
        watch
            .snapshot()
            .unwrap()
            .iter()
            .any(|entry| entry.node_name == "provider")
    });
    let attacker = router.raw(certs, "restricted");
    let before = logs
        .message_denials(&change_key, "Queryable (ingress)")
        .len();
    let blocked_provider = attacker
        .declare_queryable(change_key.clone())
        .wait()
        .unwrap();
    denied(
        logs,
        &change_key,
        "Queryable (ingress)",
        before,
        "Service provide declaration",
    );

    let denied_client = restricted
        .create_client(&interface, "/robot/change", EndpointOptions::default())
        .unwrap();
    let forged =
        json!({"value":999,"subject":"allowed","common_name":"allowed","node":"/robot/allowed"});
    // This is valid application data; schema rejection cannot explain absence.
    interface.validate("request", &forged).unwrap();
    let before = logs.message_denials(&change_key, "Query (ingress)").len();
    no_response(
        denied_client.call(&forged, DENIED),
        "payload cannot confer certificate identity",
    );
    denied(
        logs,
        &change_key,
        "Query (ingress)",
        before,
        "payload identity spoof",
    );
    assert!(matches!(service.recv_timeout(DENIED), Err(Error::Timeout)));
    let mut received = 0;
    thread::scope(|scope| {
        let result = scope.spawn(|| allowed_client.call(&json!({"value":7}), WAIT));
        let request = service.recv_timeout(WAIT).unwrap();
        received += 1;
        assert_eq!(request.request()["value"], 7);
        request.reply(&json!({"value":7})).unwrap();
        assert_eq!(result.join().unwrap().unwrap(), json!({"value":7}));
    });
    assert_eq!(received, 1);
    assert!(blocked_provider.recv_timeout(DENIED).unwrap().is_none());
    println!("PASS C17 Service provide + payload identity: unauthorized provider received 0; spoofed caller executed 0; authorized caller executed once");

    // ReplyKeyExpr::Any deliberately permits a different response key locally.
    // The router, not the provider API, must reject the unauthorized Reply.
    let caller = router.raw(certs, "allowed");
    let probe_key = key("/robot/reply_probe", &interface);
    let target_key = key("/robot/reply_target", &interface);
    let wrong_key = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mode = wrong_key.clone();
    let calls = attempts.clone();
    let reply_probe = probe_key.clone();
    let reply_target = target_key.clone();
    let provider_a = attacker
        .declare_queryable(probe_key.clone())
        .callback(move |query| {
            calls.fetch_add(1, Ordering::SeqCst);
            let (key, payload) = if mode.load(Ordering::SeqCst) {
                (reply_target.clone(), "forbidden-reply")
            } else {
                (reply_probe.clone(), "ready")
            };
            query
                .reply(key, payload)
                .wait()
                .expect("native reply passes local key validation");
        })
        .wait()
        .unwrap();
    assert_eq!(native_read_ready(&caller, &probe_key), "ready");
    let count = attempts.load(Ordering::SeqCst);
    let before = logs.message_denials(&target_key, "Reply (ingress)").len();
    wrong_key.store(true, Ordering::SeqCst);
    assert_eq!(native_read(&caller, &probe_key, DENIED), None);
    assert_eq!(attempts.load(Ordering::SeqCst), count + 1);
    denied(
        logs,
        &target_key,
        "Reply (ingress)",
        before,
        "Service reply to ungranted resource",
    );
    provider_a.undeclare().wait().unwrap();
    let legitimate = router.raw(certs, "provider");
    let reply_target = target_key.clone();
    let provider_b = legitimate
        .declare_queryable(probe_key.clone())
        .callback(move |query| {
            query
                .reply(reply_target.clone(), "authorized-reply")
                .wait()
                .unwrap();
        })
        .wait()
        .unwrap();
    assert_eq!(native_read_ready(&caller, &probe_key), "authorized-reply");
    println!("PASS C17 Service Reply: exact forbidden reply attempt accepted locally, intercepted by router and absent at caller; authorized same-key reply delivered");

    let card = read_ready(|| allowed_context.get_node_card("/robot/provider", DENIED));
    assert_eq!(card["fully_qualified_name"], "/robot/provider");
    let description = read_ready(|| {
        allowed_context.get_type_description(
            "/robot/provider",
            interface.type_name(),
            Some(interface.type_hash()),
            DENIED,
        )
    });
    assert_eq!(description.type_hash(), interface.type_hash());
    for (suffix, descriptor_interface, description) in [
        ("get_node_card", aos_core::node_card_interface(), false),
        (
            "get_type_description",
            aos_core::type_description_interface(),
            true,
        ),
    ] {
        let descriptor_key = key(&format!("/robot/provider/{suffix}"), &descriptor_interface);
        let before = logs
            .message_denials(&descriptor_key, "Query (ingress)")
            .len();
        if description {
            no_response(
                restricted_context.get_type_description(
                    "/robot/provider",
                    interface.type_name(),
                    Some(interface.type_hash()),
                    DENIED,
                ),
                "type description denied",
            );
        } else {
            no_response(
                restricted_context.get_node_card("/robot/provider", DENIED),
                "node description denied",
            );
        }
        denied(logs, &descriptor_key, "Query (ingress)", before, suffix);
    }
    println!("PASS C17 descriptor services: both authorized descriptors match provider; both unauthorized reads intercepted with no response");

    let anonymous = router.raw(certs, "without_cn");
    assert_eq!(
        anonymous.info().routers_zid().wait().count(),
        1,
        "missing CN fixture completes trusted TLS so ACL is exercised"
    );
    let anonymous_zid = anonymous.zid().to_string();
    wait_until("missing CN has no mapped ACL subject", || {
        logs.text().lines().any(|line| {
            line.contains(&anonymous_zid)
                && line.contains("did not match any configured ACL subject")
                && line.contains("Deny")
        })
    });
    // An uncached Query with zero authenticated subjects returns the default
    // decision without a per-key trace in Zenoh 1.10.1. Subscribe exercises an
    // actual keyed declaration without first needing an allowed remote route.
    let topic_interface = Interface::from_json(message()).unwrap();
    let topic_key = key("/robot/value", &topic_interface);
    let observer = allowed
        .create_subscription(
            &topic_interface,
            "/robot/value",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let legitimate_pub = provider
        .create_publisher(
            &topic_interface,
            "/robot/value",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    legitimate_pub.wait_for_subscribers(WAIT).unwrap();
    legitimate_pub.publish(&json!({"value":1})).unwrap();
    assert_eq!(observer.recv_timeout(WAIT).unwrap(), json!({"value":1}));
    let before = logs
        .message_denials(&topic_key, "Subscriber (ingress)")
        .len();
    let anonymous_sub = anonymous
        .declare_subscriber(topic_key.clone())
        .wait()
        .unwrap();
    denied(
        logs,
        &topic_key,
        "Subscriber (ingress)",
        before,
        "trusted certificate without CN",
    );
    let replies = anonymous
        .get(change_key.clone())
        .payload(serde_json::to_vec(&forged).unwrap())
        .timeout(DENIED)
        .wait()
        .unwrap();
    assert!(replies
        .recv_timeout(DENIED + DENIED)
        .ok()
        .flatten()
        .is_none());
    assert!(matches!(service.recv_timeout(DENIED), Err(Error::Timeout)));
    assert_eq!(received, 1);
    legitimate_pub.publish(&json!({"value":2})).unwrap();
    assert_eq!(observer.recv_timeout(WAIT).unwrap(), json!({"value":2}));
    assert!(anonymous_sub.recv_timeout(DENIED).unwrap().is_none());
    println!("PASS C17 missing CN: explicit CA validates TLS, no ACL subject inferred, actual Subscriber intercepted/no delivery, Query no response or provider execution, mapped-CN Topic control live before and after");

    provider_b.undeclare().wait().unwrap();
    blocked_provider.undeclare().wait().unwrap();
    for session in [&anonymous, &legitimate, &caller, &attacker] {
        session.close().wait().unwrap();
    }
    for context in [&restricted_context, &allowed_context, &provider_context] {
        context.shutdown().unwrap();
    }
}

fn entity_registration(router: &Router, certs: &Certificates, allowed: &Context, logs: &AclLogs) {
    // The existing Action topology already owns all seven entity kinds. A
    // valid observed entity is the template and positive control for each
    // forged token; resource/type fields stay unchanged, certificate changes.
    let kinds = [
        ("NN", EntityKind::Node, "provider"),
        ("MP", EntityKind::Publisher, "provider"),
        ("MS", EntityKind::Subscription, "allowed"),
        ("SC", EntityKind::Client, "allowed"),
        ("SS", EntityKind::Service, "provider"),
        ("AS", EntityKind::ActionServer, "provider"),
        ("AC", EntityKind::ActionClient, "allowed"),
    ];
    let watch = allowed.watch_graph().unwrap().unwrap();
    wait_until("legitimate controls for seven entity kinds", || {
        let snapshot = watch.snapshot().unwrap();
        kinds.iter().all(|(_, kind, owner)| {
            snapshot
                .iter()
                .any(|entity| entity.kind == *kind && entity.node_name == *owner)
        })
    });
    let snapshot = watch.snapshot().unwrap();
    let attacker = router.raw(certs, "restricted");
    for (tag, kind, owner) in kinds {
        let original = snapshot
            .iter()
            .find(|entity| entity.kind == kind && entity.node_name == owner)
            .unwrap();
        let forged_id = format!("forged_{tag}");
        let mut forged_key = format!(
            "aos/@lv/{DOMAIN}/{}/forged_node/{forged_id}/{tag}/%/{}/{}",
            attacker.zid(),
            original.namespace.replace('/', "%"),
            original.node_name
        );
        if let Some(endpoint) = &original.endpoint {
            forged_key.push_str(&format!(
                "/{}/{}/{}/%",
                endpoint.name.replace('/', "%"),
                endpoint.type_name.replace('/', "%"),
                endpoint.type_hash
            ));
        }
        let before = logs
            .message_denials(&forged_key, "Liveliness Token (ingress)")
            .len();
        let token = attacker
            .liveliness()
            .declare_token(forged_key.clone())
            .wait()
            .unwrap();
        denied(
            logs,
            &forged_key,
            "Liveliness Token (ingress)",
            before,
            &format!("forged {tag} entity registration"),
        );
        thread::sleep(DENIED);
        let after = watch.snapshot().unwrap();
        assert!(!after.iter().any(|entity| entity.id == forged_id));
        assert!(after.iter().any(|entity| entity.id == original.id));
        token.undeclare().wait().unwrap();
    }
    attacker.close().wait().unwrap();
    println!("PASS C17 entity registration: all seven Node/Publisher/Subscription/Client/Service/ActionServer/ActionClient forgeries intercepted/absent; each exact legitimate resource remains visible");
}

fn action_operations(certs: &Certificates, compiled: &CompiledSecurityPolicy, logs: &AclLogs) {
    let router = Router::start(certs, "router", compiled);
    let provider_context = router.context(certs, "provider");
    provider_context.enable_log_capture(2048).unwrap();
    let provider = node(&provider_context, "provider");
    let interface = Interface::from_json(action()).unwrap();
    let types = ActionInterfaces::new(&interface).unwrap();
    let server = ActionServer::new(
        &provider,
        &interface,
        ACTION_NAME,
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let names = [
        "allowed",
        "no_goal",
        "no_result",
        "no_feedback",
        "no_status",
    ];
    let contexts: Vec<_> = names
        .iter()
        .map(|name| router.context(certs, name))
        .collect();
    let nodes: Vec<_> = contexts
        .iter()
        .zip(names)
        .map(|(context, name)| node(context, name))
        .collect();
    let feedback_key = key(&format!("{ACTION_NAME}/_action/feedback"), &types.feedback);
    let status_key = key(&format!("{ACTION_NAME}/_action/status"), &types.status);
    let before_feedback = logs
        .message_denials(&feedback_key, "Subscriber (ingress)")
        .len();
    let before_status = logs
        .message_denials(&status_key, "Subscriber (ingress)")
        .len();
    let clients: Vec<_> = nodes
        .iter()
        .map(|node| {
            ActionClient::new(node, &interface, ACTION_NAME, EndpointOptions::default()).unwrap()
        })
        .collect();
    for client in &clients {
        assert!(client.wait_for_server(Some(WAIT)).unwrap());
    }
    denied(
        logs,
        &feedback_key,
        "Subscriber (ingress)",
        before_feedback,
        "Action feedback permission omitted",
    );
    denied(
        logs,
        &status_key,
        "Subscriber (ingress)",
        before_status,
        "Action status permission omitted",
    );
    let goal_key = key(
        &format!("{ACTION_NAME}/_action/send_goal"),
        &types.send_goal,
    );
    let before = logs.message_denials(&goal_key, "Query (ingress)").len();
    no_response(
        clients[1].send_goal(
            &json!({"value":999}),
            Some("11111111111111111111111111111111"),
            DENIED,
        ),
        "Action send_goal denied",
    );
    denied(
        logs,
        &goal_key,
        "Query (ingress)",
        before,
        "Action send_goal permission omitted",
    );
    assert!(matches!(
        server.take_goal_request(DENIED),
        Err(Error::Timeout)
    ));
    let mut admissions = 0;
    let goal = thread::scope(|scope| {
        let sent = scope.spawn(|| {
            clients[0].send_goal(
                &json!({"value":7}),
                Some("22222222222222222222222222222222"),
                WAIT,
            )
        });
        let request = server.take_goal_request(WAIT).unwrap();
        admissions += 1;
        assert_eq!(request.request(), &json!({"value":7}));
        let goal = request.decide(true).unwrap().unwrap();
        assert!(sent.join().unwrap().unwrap().accepted());
        goal
    });
    goal.execute().unwrap();
    // Graph discovery does not wait for every volatile feedback subscription
    // to reach the router. Exercise the live feedback path until each allowed
    // subject receives it; the denied subject must receive none of the probes.
    let mut received = [false; 5];
    wait_until("authorized Action feedback", || {
        goal.publish_feedback(&json!({"progress":7})).unwrap();
        for index in [0, 1, 2, 4] {
            if !received[index] {
                if let Ok(value) = clients[index].recv_feedback(DENIED) {
                    assert_eq!(value["feedback"], json!({"progress":7}), "{}", names[index]);
                    received[index] = true;
                }
            }
        }
        [0, 1, 2, 4].into_iter().all(|index| received[index])
    });
    assert_eq!(clients[3].recv_feedback(DENIED), Err(Error::Timeout));
    // The no-feedback subject still receives provider status, and no-status
    // still receives feedback. These are independent permissions, not no-ACL subjects.
    for index in [0, 1, 2, 3] {
        let mut has_goal = false;
        wait_until("authorized Action status", || {
            if let Ok(value) = clients[index].recv_status(DENIED) {
                has_goal = value["status_list"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["goal_info"]["goal_id"] == goal.goal_id());
            }
            has_goal
        });
    }
    assert_eq!(clients[4].recv_status(DENIED), Err(Error::Timeout));
    goal.succeed().unwrap();
    goal.finish(Some(&json!({"value":7}))).unwrap();
    assert_eq!(goal.status(), 4);
    provider_context.drain_telemetry();
    let result_key = key(
        &format!("{ACTION_NAME}/_action/get_result"),
        &types.get_result,
    );
    let before = logs.message_denials(&result_key, "Query (ingress)").len();
    no_response(
        clients[2].get_result(goal.goal_id(), DENIED),
        "Action get_result denied",
    );
    denied(
        logs,
        &result_key,
        "Query (ingress)",
        before,
        "Action get_result permission omitted",
    );
    assert!(!provider_context
        .drain_telemetry()
        .iter()
        .any(|record| record.event.event == "result_sent"
            && record.event.goal_id.as_deref() == Some(goal.goal_id())));
    assert_eq!(provider_context.telemetry_snapshot().log_dropped_total, 0);
    for index in [0, 1, 3, 4] {
        assert_eq!(
            clients[index].get_result(goal.goal_id(), WAIT).unwrap(),
            json!({"status":4,"result":{"value":7}})
        );
    }
    assert_eq!(admissions, 1);
    assert!(matches!(
        server.take_goal_request(DENIED),
        Err(Error::Timeout)
    ));
    assert_eq!(goal.status(), 4);
    println!("PASS C17 Action: four subjects each omit one permission; send_goal intercepted/admitted 0, get_result intercepted/provider result_sent 0, feedback/status intercepted and no receive; allowed complementary operations delivered; one authorized goal admitted/executed/succeeded");
    entity_registration(&router, certs, &contexts[0], logs);
    for context in &contexts {
        context.shutdown().unwrap();
    }
    provider_context.shutdown().unwrap();
}

#[test]
fn native_security_boundaries_have_denial_and_positive_business_controls() {
    let logs = AclLogs::install();
    let certs = Certificates::new();
    for name in [
        "router",
        "provider",
        "allowed",
        "restricted",
        "no_goal",
        "no_result",
        "no_feedback",
        "no_status",
    ] {
        certs.issue(name, name, "ca", "DNS:localhost,IP:127.0.0.1", false);
    }
    boundary_support::issue_without_cn(&certs, "without_cn");
    let compiled = compile_policy(&policy()).unwrap();
    println!("policy source digest: {}", compiled.source_digest());
    live_expiry(&certs, &compiled);
    service_identity_and_description(&certs, &compiled, &logs);
    action_operations(&certs, &compiled, &logs);
    let credential_directory = certs.directory().to_path_buf();
    drop(certs);
    assert!(!credential_directory.exists());
    println!("PASS native C16/C17 boundary groups; temporary credential directory deleted and absence verified");
}
