//! Native TLS links, gossip-created local peer links, ACLs and Router recovery.
#[path = "support/security.rs"]
#[allow(dead_code)]
mod support;

use aos_core::Interface;
use aos_zenoh::security::*;
use serde_json::{json, Value};
use std::{
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use support::{AclLogs, Certificates};
use zenoh::Wait;

const WAIT: Duration = Duration::from_secs(8);

fn until(label: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !ready() {
        assert!(Instant::now() < deadline, "deadline: {label}");
        thread::sleep(Duration::from_millis(20));
    }
}

fn endpoint() -> String {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    format!("tls/127.0.0.1:{port}")
}

fn identity(name: &str) -> NodeIdentity {
    NodeIdentity {
        name: name.into(),
        namespace: "/topology".into(),
    }
}

fn interface(kind: &str) -> Value {
    if kind == "msg" {
        json!({"name":"topology/msg/Value","kind":"msg","message":{"type":"object"}})
    } else {
        json!({"name":"topology/srv/Echo","kind":"srv","request":{"type":"object"},"response":{"type":"object"}})
    }
}

fn policy() -> SecurityPolicy {
    let subjects = ["publisher", "reader"]
        .into_iter()
        .map(|name| {
            let publishing = name == "publisher";
            SubjectPolicy {
                id: name.into(),
                common_name: name.into(),
                nodes: vec![identity(name)],
                graph_nodes: vec![identity("publisher"), identity("reader")],
                systems: vec![],
                endpoints: vec![
                    EndpointGrant {
                        node: identity(name),
                        namespace: "/topology".into(),
                        name: "/topology/value".into(),
                        interface: interface("msg"),
                        qos: Value::Null,
                        operations: if publishing {
                            vec![EndpointOperation::Publish, EndpointOperation::Subscribe]
                        } else {
                            vec![EndpointOperation::Subscribe]
                        },
                    },
                    EndpointGrant {
                        node: identity(name),
                        namespace: "/topology".into(),
                        name: "/topology/reverse".into(),
                        interface: interface("msg"),
                        qos: Value::Null,
                        operations: vec![if publishing {
                            EndpointOperation::Subscribe
                        } else {
                            EndpointOperation::Publish
                        }],
                    },
                    EndpointGrant {
                        node: identity(name),
                        namespace: "/topology".into(),
                        name: "/topology/echo".into(),
                        interface: interface("srv"),
                        qos: Value::Null,
                        operations: vec![if publishing {
                            EndpointOperation::Provide
                        } else {
                            EndpointOperation::Call
                        }],
                    },
                ],
            }
        })
        .collect();
    SecurityPolicy {
        schema_version: 1,
        domain_id: 57,
        subjects,
    }
}

fn relay(name: &str) -> RelaySubject {
    RelaySubject {
        id: name.into(),
        common_name: name.into(),
        forward_subjects: vec!["publisher".into(), "reader".into()],
    }
}

fn peer(certs: &Certificates, router: &str, name: &str) -> ClientTlsProfile {
    let mut profile = certs.client(router, name, 57);
    profile.connect_timeout_ms = 3000;
    profile.peer = Some(PeerTlsOptions {
        listen_endpoint: "tls/127.0.0.1:0".into(),
        policy: policy(),
        relays: vec![relay("router_a")],
    });
    profile
}

fn router(
    certs: &Certificates,
    name: &str,
    listen: &str,
    connect: Vec<String>,
) -> RouterTlsProfile {
    let mut profile = certs.router(listen, name);
    profile.federation = Some(RouterFederation {
        connect_endpoints: connect,
        relays: vec![relay(if name == "router_a" {
            "router_b"
        } else {
            "router_a"
        })],
    });
    profile
}

fn key(kind: &str, name: &str) -> String {
    let interface = Interface::from_json(interface(kind)).unwrap();
    format!(
        "aos/57/topology/{name}/{}/{}",
        interface.type_name(),
        interface.type_hash()
    )
}

#[test]
fn secure_local_gossip_and_router_federation_preserve_data_and_denials() {
    let logs = AclLogs::install();
    let certs = Certificates::new();
    for name in ["router_a", "router_b", "publisher", "reader", "intruder"] {
        certs.issue(name, name, "ca", "IP:127.0.0.1", false);
    }
    let compiled = compile_policy(&policy()).unwrap();
    let a = endpoint();
    // Local gossip is independent of federation and needs no invented relay
    // identities in the Router policy. The peer trusts its actual local Router.
    let mut a_profile = certs.router(&a, "router_a");
    a_profile.local_peers = true;
    assert!(a_profile.federation.is_none());
    // Peers open before their Router and reconnect in the background.
    let publisher = zenoh::open(client_config(&peer(&certs, &a, "publisher")).unwrap())
        .wait()
        .unwrap();
    assert_eq!(publisher.info().routers_zid().wait().count(), 0);
    let reader = zenoh::open(client_config(&peer(&certs, &a, "reader")).unwrap())
        .wait()
        .unwrap();
    let router_a = zenoh::open(router_config(&a_profile, &compiled).unwrap())
        .wait()
        .unwrap();
    until("local Router attaches", || {
        publisher
            .info()
            .routers_zid()
            .wait()
            .any(|z| z == router_a.zid())
    });
    until("gossip creates a direct peer link", || {
        publisher
            .info()
            .peers_zid()
            .wait()
            .any(|z| z == reader.zid())
    });
    let topic = key("msg", "value");
    let sub = reader.declare_subscriber(&topic).wait().unwrap();
    let published = publisher.declare_publisher(&topic).wait().unwrap();
    until("direct subscription propagates", || {
        published.matching_status().wait().unwrap().matching()
    });
    router_a.close().wait().unwrap();
    until("Router is absent", || {
        publisher.info().routers_zid().wait().count() == 0
            && reader.info().routers_zid().wait().count() == 0
    });
    published.put("local-with-router-down").wait().unwrap();
    assert_eq!(
        sub.recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "local-with-router-down"
    );
    // Rejoin the same Router and introduce a signed but ungranted identity.
    let router_a = zenoh::open(router_config(&a_profile, &compiled).unwrap())
        .wait()
        .unwrap();
    let intruder = zenoh::open(client_config(&peer(&certs, &a, "intruder")).unwrap())
        .wait()
        .unwrap();
    until("intruder has an authenticated peer link", || {
        intruder
            .info()
            .peers_zid()
            .wait()
            .any(|z| z == reader.zid())
    });
    router_a.close().wait().unwrap();
    until("Router is absent for ACL check", || {
        reader.info().routers_zid().wait().count() == 0
    });
    let forbidden = intruder.declare_subscriber(&topic).wait().unwrap();
    thread::sleep(Duration::from_millis(100));
    published.put("not-for-intruder").wait().unwrap();
    assert!(forbidden
        .recv_timeout(Duration::from_millis(400))
        .unwrap()
        .is_none());
    assert!(sub.recv_timeout(WAIT).unwrap().is_some());
    intruder.put(&topic, "forbidden-direct-put").wait().unwrap();
    assert!(sub
        .recv_timeout(Duration::from_millis(400))
        .unwrap()
        .is_none());
    until("actual direct peer declaration denial", || {
        logs.message_denials(&topic, "Subscriber (ingress)")
            .iter()
            .any(|line| line.contains(&format!("{} is unauthorized", intruder.zid())))
    });
    published.put("authorized-after-denial").wait().unwrap();
    assert_eq!(
        sub.recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "authorized-after-denial"
    );
    drop(sub);
    drop(published);
    drop(forbidden);
    intruder.close().wait().unwrap();
    reader.close().wait().unwrap();
    publisher.close().wait().unwrap();

    // A secure peer at A and a strict client at B cannot form a direct peer
    // bypass: the client has no listener and the peer never autoconnects Routers.
    let a_profile = router(&certs, "router_a", &a, vec![]);
    let b = endpoint();
    let b_profile = router(&certs, "router_b", &b, vec![a.clone()]);
    let router_b = zenoh::open(router_config(&b_profile, &compiled).unwrap())
        .wait()
        .unwrap();
    assert_eq!(router_b.info().routers_zid().wait().count(), 0);
    let router_a = zenoh::open(router_config(&a_profile, &compiled).unwrap())
        .wait()
        .unwrap();
    until("late Router federation link", || {
        router_b
            .info()
            .routers_zid()
            .wait()
            .any(|z| z == router_a.zid())
    });
    let publisher = zenoh::open(client_config(&peer(&certs, &a, "publisher")).unwrap())
        .wait()
        .unwrap();
    let reader = zenoh::open(client_config(&certs.client(&b, "reader", 57)).unwrap())
        .wait()
        .unwrap();
    assert_eq!(publisher.info().peers_zid().wait().count(), 0);
    assert_eq!(reader.info().peers_zid().wait().count(), 0);
    let sub = reader.declare_subscriber(&topic).wait().unwrap();
    let published = publisher.declare_publisher(&topic).wait().unwrap();
    until("federated subscription propagates", || {
        published.matching_status().wait().unwrap().matching()
    });
    published.put("across-routers").wait().unwrap();
    assert_eq!(
        sub.recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "across-routers"
    );
    let reverse = key("msg", "reverse");
    let reverse_sub = publisher.declare_subscriber(&reverse).wait().unwrap();
    let reverse_pub = reader.declare_publisher(&reverse).wait().unwrap();
    until("reverse federated subscription propagates", || {
        reverse_pub.matching_status().wait().unwrap().matching()
    });
    reverse_pub.put("reverse-across-routers").wait().unwrap();
    assert_eq!(
        reverse_sub
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "reverse-across-routers"
    );
    drop(reverse_sub);
    drop(reverse_pub);
    let echo = key("srv", "echo");
    let queryable = publisher
        .declare_queryable(&echo)
        .complete(true)
        .wait()
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    let replies = reader
        .get(&echo)
        .payload("request-across-routers")
        .timeout(WAIT)
        .wait()
        .unwrap();
    let query = queryable
        .recv_timeout(WAIT)
        .unwrap()
        .expect("query crosses Router link");
    assert_eq!(
        query.payload().unwrap().try_to_string().unwrap(),
        "request-across-routers"
    );
    query.reply(&echo, "reply-across-routers").wait().unwrap();
    // Complete the provider query before awaiting consolidated replies.
    drop(query);
    assert_eq!(
        replies
            .recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .result()
            .as_ref()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "reply-across-routers"
    );
    let graph_key = compiled
        .expanded_grants()
        .iter()
        .find(|g| {
            g.subject_id == "publisher"
                && g.operation == "graph.register"
                && g.message == Message::LivelinessToken
        })
        .unwrap()
        .key_expr
        .replace('*', "fixture");
    let graph = reader
        .liveliness()
        .declare_subscriber(&graph_key)
        .history(true)
        .wait()
        .unwrap();
    let token = publisher
        .liveliness()
        .declare_token(&graph_key)
        .wait()
        .unwrap();
    assert!(graph.recv_timeout(WAIT).unwrap().is_some());
    token.undeclare().wait().unwrap();
    assert_eq!(
        graph.recv_timeout(WAIT).unwrap().unwrap().kind(),
        zenoh::sample::SampleKind::Delete
    );
    // Reader has no publish right, even though both Routers can forward the
    // publisher's finite grants. The edge Router still checks its real CN.
    let control = publisher.declare_subscriber(&topic).wait().unwrap();
    reader
        .put(&topic, "forbidden-federated-put")
        .wait()
        .unwrap();
    // Native Zenoh delivers a session's own publication locally. Router ACLs
    // govern the network hop, not APIs called inside that same process.
    assert_eq!(
        sub.recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "forbidden-federated-put"
    );
    assert!(control
        .recv_timeout(Duration::from_millis(400))
        .unwrap()
        .is_none());
    until("federated edge denial", || {
        logs.message_denials(&topic, "Put (ingress)")
            .iter()
            .any(|line| line.contains(&format!("{} is unauthorized", reader.zid())))
    });
    drop(control);
    drop(queryable);
    drop(graph);
    router_a.close().wait().unwrap();
    until("federation disconnect observed", || {
        router_b.info().routers_zid().wait().count() == 0
    });
    let router_a = zenoh::open(router_config(&a_profile, &compiled).unwrap())
        .wait()
        .unwrap();
    until("federation recovers", || {
        router_b
            .info()
            .routers_zid()
            .wait()
            .any(|z| z == router_a.zid())
    });
    until("existing publisher reconnects and reannounces", || {
        publisher
            .info()
            .routers_zid()
            .wait()
            .any(|z| z == router_a.zid())
            && published.matching_status().wait().unwrap().matching()
    });
    published
        .put("business-after-router-restart")
        .wait()
        .unwrap();
    assert_eq!(
        sub.recv_timeout(WAIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "business-after-router-restart"
    );
    drop(sub);
    drop(published);
    reader.close().wait().unwrap();
    publisher.close().wait().unwrap();
    router_b.close().wait().unwrap();
    router_a.close().wait().unwrap();
}
