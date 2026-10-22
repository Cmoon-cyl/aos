//! Readiness must preserve its network discovery interest across local polls.
#[path = "support/delay_proxy.rs"]
mod delay_proxy;
mod support;

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use aos_core::transport::GraphWatchBackend;
use aos_core::{
    ActionClient, ActionServer, ActionServerOptions, Context, EndpointOptions, EntityKind, Error,
    GraphEntity, Interface, Node, NodeOptions, QoS,
};
use aos_zenoh::ZenohTransport;
use delay_proxy::DelayProxy;
use serde_json::json;
use zenoh::Wait;

const LIMIT: Duration = Duration::from_secs(5);
const DELAY: Duration = Duration::from_millis(120);

struct Router {
    _session: zenoh::Session,
    endpoint: String,
}

impl Router {
    fn start() -> Self {
        Self::start_at("tcp/127.0.0.1:0")
    }

    fn start_at(endpoint: &str) -> Self {
        let mut config = support::config("zenoh-router.json5");
        config
            .insert_json5("listen/endpoints", &json!([endpoint]).to_string())
            .unwrap();
        let session = zenoh::open(config).wait().unwrap();
        let endpoint = session.info().locators().wait()[0].to_string();
        Self {
            _session: session,
            endpoint,
        }
    }

    fn context(&self) -> Context {
        Context::new(ZenohTransport::open(support::peer_config(&self.endpoint), 0).unwrap())
    }

    fn delayed_context(&self) -> (DelayProxy, Context) {
        let proxy = DelayProxy::start(&self.endpoint);
        let mut config = support::peer_config(&proxy.endpoint);
        // A client with no listening/gossip route cannot bypass the proxy by
        // discovering a direct peer connection through the router.
        config.insert_json5("mode", "\"client\"").unwrap();
        config.insert_json5("listen/endpoints", "[]").unwrap();
        config
            .insert_json5("scouting/gossip/enabled", "false")
            .unwrap();
        let context = Context::new(ZenohTransport::open(config, 0).unwrap());
        (proxy, context)
    }
}

fn node(context: Context, name: &str) -> Node {
    Node::new(context, name, "/", NodeOptions::default()).unwrap()
}

fn service_interface() -> Interface {
    Interface::from_json(json!({
        "name":"test/srv/Ready","kind":"srv",
        "request":{"type":"object","properties":{},"additionalProperties":false},
        "response":{"type":"object","properties":{},"additionalProperties":false}
    }))
    .unwrap()
}

fn action_interface() -> Interface {
    Interface::from_json(json!({
        "name":"test/action/Ready","kind":"action",
        "goal":{"type":"object","properties":{},"additionalProperties":false},
        "result":{"type":"object","properties":{},"additionalProperties":false,"default":{}},
        "feedback":{"type":"object","properties":{},"additionalProperties":false}
    }))
    .unwrap()
}

fn message_interface() -> Interface {
    Interface::from_json(json!({
        "name":"test/msg/Ready","kind":"msg",
        "message":{"type":"object","properties":{},"additionalProperties":false}
    }))
    .unwrap()
}

fn graph_until(
    watch: &dyn GraphWatchBackend,
    mut condition: impl FnMut(&[GraphEntity]) -> bool,
) -> Vec<GraphEntity> {
    let started = Instant::now();
    loop {
        let entities = watch.snapshot().unwrap();
        if condition(&entities) {
            return entities;
        }
        assert!(
            started.elapsed() < LIMIT,
            "graph condition not met: {entities:?}"
        );
        match watch.wait_for_change(LIMIT.saturating_sub(started.elapsed())) {
            Ok(()) | Err(Error::Timeout) => {}
            Err(error) => panic!("graph watch: {error}"),
        }
    }
}

#[test]
fn service_readiness_survives_network_delay_longer_than_local_poll() {
    let router = Router::start();
    let provider = node(router.context(), "provider");
    let interface = service_interface();
    let service = provider
        .create_service(&interface, "/ready", EndpointOptions::default())
        .unwrap();
    let (proxy, context) = router.delayed_context();
    let caller = node(context.clone(), "caller");
    let client = caller
        .create_client(&interface, "/ready", EndpointOptions::default())
        .unwrap();
    proxy.set_delay(DELAY);

    // Reproduce the old short-query failure on the exact same live transport.
    for _ in 0..3 {
        let short = context.graph(Duration::from_millis(50)).unwrap();
        assert!(!short.iter().any(|entity| {
            entity.kind == EntityKind::Service
                && entity.endpoint.as_ref() == Some(client.endpoint())
        }));
    }
    // Timed-out GETs can still teach Zenoh's local token cache when their
    // delayed declarations arrive. Use a fresh observer to measure discovery
    // latency rather than accidentally measuring that now-warm cache.
    let (fresh_proxy, fresh_context) = router.delayed_context();
    let fresh_caller = node(fresh_context, "fresh_caller");
    let fresh_client = fresh_caller
        .create_client(&interface, "/ready", EndpointOptions::default())
        .unwrap();
    fresh_proxy.set_delay(DELAY);
    let started = Instant::now();
    assert!(fresh_client.wait_for_service(Some(LIMIT)).unwrap());
    eprintln!(
        "readiness TCP delay: {}ms each way; three 50ms GETs missed; fresh watch ready in {:?}",
        DELAY.as_millis(),
        started.elapsed()
    );
    assert!(started.elapsed() >= DELAY);
    assert!(started.elapsed() < LIMIT);
    assert_eq!(service.queue_stats().unwrap().received_total, 0);
    assert!(matches!(
        service.recv_timeout(Duration::ZERO),
        Err(Error::Timeout)
    ));
}

#[test]
fn service_readiness_waits_for_a_late_provider_without_deadline() {
    let router = Router::start();
    let provider = node(router.context(), "late_provider");
    let (proxy, context) = router.delayed_context();
    let caller = node(context, "late_caller");
    let interface = service_interface();
    let client = caller
        .create_client(&interface, "/late", EndpointOptions::default())
        .unwrap();
    proxy.set_delay(DELAY);
    thread::scope(|scope| {
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiting_client = &client;
        let waiting = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(waiting_client.wait_for_service(None)).unwrap();
        });
        started_rx.recv().unwrap();
        thread::sleep(Duration::from_millis(150));
        let service = provider
            .create_service(&interface, "/late", EndpointOptions::default())
            .unwrap();
        let result = done_rx.recv_timeout(LIMIT);
        if result.is_err() {
            client.close().unwrap();
        }
        waiting.join().unwrap();
        assert_eq!(result.unwrap(), Ok(true));
        assert_eq!(service.queue_stats().unwrap().received_total, 0);
    });
}

#[test]
fn action_readiness_waits_for_a_late_provider_without_admission() {
    let router = Router::start();
    let provider = node(router.context(), "action_provider");
    let (proxy, context) = router.delayed_context();
    let caller = node(context, "action_caller");
    let interface = action_interface();
    let client =
        ActionClient::new(&caller, &interface, "/act", EndpointOptions::default()).unwrap();
    proxy.set_delay(DELAY);
    thread::scope(|scope| {
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiting_client = &client;
        let waiting = scope.spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(waiting_client.wait_for_server(None)).unwrap();
        });
        started_rx.recv().unwrap();
        thread::sleep(Duration::from_millis(150));
        let server = ActionServer::new(
            &provider,
            &interface,
            "/act",
            ActionServerOptions::default(),
            EndpointOptions::default(),
        )
        .unwrap();
        let result = done_rx.recv_timeout(LIMIT);
        if result.is_err() {
            client.close().unwrap();
        }
        waiting.join().unwrap();
        assert_eq!(result.unwrap(), Ok(true));
        assert!(matches!(
            server.take_goal_request(Duration::ZERO),
            Err(Error::Timeout)
        ));
    });
}

#[test]
fn graph_watch_tracks_existing_late_and_removed_tokens() {
    let router = Router::start();
    let provider = node(router.context(), "graph_provider");
    let interface = message_interface();
    let first = provider
        .create_subscription(
            &interface,
            "/observed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let (proxy, context) = router.delayed_context();
    proxy.set_delay(DELAY);
    let watch = context.watch_graph().unwrap().unwrap();
    let count = |entities: &[GraphEntity]| {
        entities
            .iter()
            .filter(|entity| {
                entity.kind == EntityKind::Subscription
                    && entity
                        .endpoint
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.name == "/observed")
            })
            .count()
    };
    graph_until(watch.as_ref(), |entities| count(entities) == 1);
    let second = provider
        .create_subscription(
            &interface,
            "/observed",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    graph_until(watch.as_ref(), |entities| count(entities) == 2);
    first.close().unwrap();
    graph_until(watch.as_ref(), |entities| count(entities) == 1);
    second.close().unwrap();
    graph_until(watch.as_ref(), |entities| count(entities) == 0);
}

#[test]
fn finite_readiness_budget_expires_and_rejects_wrong_identity() {
    let router = Router::start();
    let provider = node(router.context(), "wrong_provider");
    let interface = service_interface();
    let mut wrong_definition = interface.definition().clone();
    wrong_definition["request"]["properties"] = json!({"other":{"type":"integer"}});
    let wrong_interface = Interface::from_json(wrong_definition).unwrap();
    let _wrong_hash = provider
        .create_service(&wrong_interface, "/absent", EndpointOptions::default())
        .unwrap();
    let _wrong_name = provider
        .create_service(&interface, "/other", EndpointOptions::default())
        .unwrap();
    let (proxy, context) = router.delayed_context();
    let caller = node(context, "deadline_caller");
    let client = caller
        .create_client(&interface, "/absent", EndpointOptions::default())
        .unwrap();
    proxy.set_delay(DELAY);
    let budget = Duration::from_millis(700);
    let started = Instant::now();
    assert!(!client.wait_for_service(Some(budget)).unwrap());
    assert!(started.elapsed() >= budget);
    assert!(started.elapsed() < budget + Duration::from_millis(300));
    let started = Instant::now();
    assert!(!client.wait_for_service(Some(Duration::ZERO)).unwrap());
    assert!(started.elapsed() < Duration::from_millis(100));
}

#[test]
fn service_and_action_readiness_wake_on_client_node_and_context_close() {
    let router = Router::start();
    for action in [false, true] {
        for close in ["entity", "node", "context"] {
            let (proxy, context) = router.delayed_context();
            let caller = node(context.clone(), "close_caller");
            let service = caller
                .create_client(
                    &service_interface(),
                    "/absent_service",
                    EndpointOptions::default(),
                )
                .unwrap();
            let action_client = ActionClient::new(
                &caller,
                &action_interface(),
                "/absent_action",
                EndpointOptions::default(),
            )
            .unwrap();
            proxy.set_delay(DELAY);
            thread::scope(|scope| {
                let (started_tx, started_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let waiting_action = &action_client;
                let waiting_service = &service;
                scope.spawn(move || {
                    started_tx.send(()).unwrap();
                    let result = if action {
                        waiting_action.wait_for_server(None)
                    } else {
                        waiting_service.wait_for_service(None)
                    };
                    done_tx.send(result).unwrap();
                });
                started_rx.recv().unwrap();
                thread::sleep(Duration::from_millis(100));
                let started = Instant::now();
                match close {
                    "entity" if action => action_client.close().unwrap(),
                    "entity" => service.close().unwrap(),
                    "node" => caller.destroy_node().unwrap(),
                    "context" => context.shutdown().unwrap(),
                    _ => unreachable!(),
                }
                assert_eq!(
                    done_rx.recv_timeout(Duration::from_millis(500)).unwrap(),
                    Err(Error::Closed),
                    "{action}/{close}"
                );
                assert!(started.elapsed() < Duration::from_millis(700));
            });
        }
    }
}

#[test]
fn context_close_wakes_graph_watch_without_a_polling_interval() {
    let router = Router::start();
    let context = router.context();
    let watch: Arc<dyn GraphWatchBackend> = context.watch_graph().unwrap().unwrap().into();
    let waiter = watch.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(waiter.wait_for_change(Duration::from_secs(60)))
            .unwrap();
    });
    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(50));
    context.shutdown().unwrap();
    assert_eq!(
        done_rx.recv_timeout(Duration::from_millis(500)).unwrap(),
        Err(Error::Closed)
    );
    worker.join().unwrap();
    assert_eq!(watch.snapshot(), Err(Error::Closed));
    assert!(matches!(context.watch_graph(), Err(Error::Closed)));
}

#[test]
fn typed_publisher_matching_keeps_total_budget_under_network_delay() {
    let router = Router::start();
    let provider = node(router.context(), "subscriber");
    let (proxy, context) = router.delayed_context();
    let caller = node(context, "publisher");
    let interface = message_interface();
    let publisher = caller
        .create_publisher(
            &interface,
            "/topic",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    proxy.set_delay(DELAY);
    thread::scope(|scope| {
        let (started_tx, started_rx) = mpsc::channel();
        let waiting = scope.spawn(move || {
            started_tx.send(()).unwrap();
            publisher.wait_for_subscribers(LIMIT)
        });
        started_rx.recv().unwrap();
        thread::sleep(Duration::from_millis(150));
        let subscription = provider
            .create_subscription(
                &interface,
                "/topic",
                QoS::default(),
                EndpointOptions::default(),
            )
            .unwrap();
        assert_eq!(waiting.join().unwrap(), Ok(()));
        assert_eq!(subscription.queue_stats().unwrap().received_total, 0);
    });
}

#[test]
fn same_session_service_and_action_readiness_observe_local_tokens() {
    let router = Router::start();
    let context = router.context();
    let provider = node(context.clone(), "same_session_provider");
    let caller = node(context, "same_session_caller");
    let interface = service_interface();
    let service = provider
        .create_service(&interface, "/local", EndpointOptions::default())
        .unwrap();
    let client = caller
        .create_client(&interface, "/local", EndpointOptions::default())
        .unwrap();
    assert!(client.wait_for_service(Some(LIMIT)).unwrap());
    assert_eq!(service.queue_stats().unwrap().received_total, 0);

    let interface = action_interface();
    let server = ActionServer::new(
        &provider,
        &interface,
        "/local_action",
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    let client = ActionClient::new(
        &caller,
        &interface,
        "/local_action",
        EndpointOptions::default(),
    )
    .unwrap();
    assert!(client.wait_for_server(Some(LIMIT)).unwrap());
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Timeout)
    ));
}

#[test]
fn recreating_a_watch_after_remote_removal_does_not_replay_a_dead_provider() {
    assert_watch_recreation_after_removal(false);
}

#[test]
fn recreating_a_client_watch_after_remote_removal_does_not_replay_a_dead_provider() {
    assert_watch_recreation_after_removal(true);
}

fn assert_watch_recreation_after_removal(client_mode: bool) {
    let router = Router::start();
    let provider = node(router.context(), "removed_provider");
    let interface = service_interface();
    let service = provider
        .create_service(&interface, "/removed", EndpointOptions::default())
        .unwrap();
    let (_proxy, context) = if client_mode {
        // Keep the same interest-filtered client profile as cross-machine
        // deployment. No artificial delay is needed for the cache-drop gate.
        let (proxy, context) = router.delayed_context();
        (Some(proxy), context)
    } else {
        (None, router.context())
    };
    let caller = node(context.clone(), "repeat_caller");
    let client = caller
        .create_client(&interface, "/removed", EndpointOptions::default())
        .unwrap();
    // This creates then drops watch A. No graph watch remains on this session.
    assert!(client.wait_for_service(Some(LIMIT)).unwrap());
    let oracle_context = router.context();
    let oracle = oracle_context.watch_graph().unwrap().unwrap();
    let exists = |entities: &[GraphEntity]| {
        entities.iter().any(|entity| {
            entity.kind == EntityKind::Service
                && entity.endpoint.as_ref() == Some(client.endpoint())
        })
    };
    graph_until(oracle.as_ref(), exists);
    service.close().unwrap();
    // A different session witnesses removal before the caller probes again.
    // That is not a synchronization barrier for the caller's remote cache:
    // its first observation may still be true while Delete is in flight.
    graph_until(oracle.as_ref(), |entities| !exists(entities));
    let first = client
        .wait_for_service(Some(Duration::from_millis(500)))
        .unwrap();
    eprintln!("watch recreation: client_mode={client_mode}, first_observation={first}");

    // Keep B alive through its history replay and a later network marker.
    // On this ordered session, the marker snapshot must already be absent.
    let watch = context.watch_graph().unwrap().unwrap();
    let _marker = provider
        .create_service(&interface, "/after_removal", EndpointOptions::default())
        .unwrap();
    let current = graph_until(watch.as_ref(), |entities| {
        entities.iter().any(|entity| {
            entity.kind == EntityKind::Service
                && entity
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.name == "/after_removal")
        })
    });
    assert!(!exists(&current));
    drop(watch);

    // After B has observed removal, a new watch C must retain that result.
    // A new marker proves C has processed live traffic, not only an initially
    // empty snapshot before its history task ran.
    let recreated = context.watch_graph().unwrap().unwrap();
    let _recreated_marker = provider
        .create_service(&interface, "/after_recreation", EndpointOptions::default())
        .unwrap();
    let current = graph_until(recreated.as_ref(), |entities| {
        entities.iter().any(|entity| {
            entity.kind == EntityKind::Service
                && entity
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.name == "/after_recreation")
        })
    });
    assert!(!exists(&current));
}

#[test]
fn removal_during_watch_startup_leaves_no_stale_entity_after_history() {
    let router = Router::start();
    let provider = node(router.context(), "startup_provider");
    let (proxy, context) = router.delayed_context();
    let interface = service_interface();
    proxy.set_delay(DELAY);
    for round in 0..8 {
        let target = format!("/during_startup_{round}");
        let service = provider
            .create_service(&interface, &target, EndpointOptions::default())
            .unwrap();
        // Prime the same session's history cache, then keep that observer
        // while the next history subscriber starts and the provider withdraws.
        let primed = context.watch_graph().unwrap().unwrap();
        graph_until(primed.as_ref(), |entities| {
            entities.iter().any(|entity| {
                entity
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.name == target)
            })
        });
        let watch = thread::scope(|scope| {
            let starting = scope.spawn(|| context.watch_graph().unwrap().unwrap());
            service.close().unwrap();
            starting.join().unwrap()
        });
        let marker = format!("/after_startup_{round}");
        let marker_service = provider
            .create_service(&interface, &marker, EndpointOptions::default())
            .unwrap();
        let current = graph_until(watch.as_ref(), |entities| {
            entities.iter().any(|entity| {
                entity
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.name == marker)
            })
        });
        assert!(!current.iter().any(|entity| {
            entity
                .endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.name == target)
        }));
        marker_service.close().unwrap();
    }
}

#[test]
fn graph_watch_created_before_router_connection_receives_current_tokens() {
    let endpoint = format!("tcp/127.0.0.1:{}", support::free_port());
    let mut config = support::peer_config(&endpoint);
    // Peer mode can retry a missing router in the background. Disable startup
    // barriers and all other routes, so the watch necessarily predates it.
    config
        .insert_json5("connect/exit_on_failure", "false")
        .unwrap();
    config
        .insert_json5("open/return_conditions/connect_scouted", "false")
        .unwrap();
    config
        .insert_json5("open/return_conditions/declares", "false")
        .unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
        .insert_json5("scouting/gossip/autoconnect", r#"{"router":[],"peer":[]}"#)
        .unwrap();
    let transport = ZenohTransport::open(config, 0).unwrap();
    assert!(transport.router_zids().is_empty());
    let context = Context::new(transport);
    let watch = context.watch_graph().unwrap().unwrap();
    assert!(watch.snapshot().unwrap().is_empty());
    let router = Router::start_at(&endpoint);
    let _token = router
        ._session
        .liveliness()
        .declare_token("aos/@lv/0/router/n1/late_router_token/NN/%/%/late_router")
        .wait()
        .unwrap();
    let ready = |entities: &[GraphEntity]| {
        entities
            .iter()
            .any(|entity| entity.id == "late_router_token")
    };
    let started = Instant::now();
    loop {
        let current = watch.snapshot().unwrap();
        if ready(&current) {
            break;
        }
        if started.elapsed() >= LIMIT {
            panic!(
                "late router graph missing: peers={}, watch={current:?}, GET={:?}",
                router._session.info().peers_zid().wait().count(),
                context.graph(Duration::from_secs(1))
            );
        }
        let _ = watch.wait_for_change(Duration::from_millis(100));
    }
}

#[test]
fn graph_watch_alone_keeps_its_context_alive_until_drop() {
    let router = Router::start();
    let context = router.context();
    let watch = context.watch_graph().unwrap().unwrap();
    drop(context);
    assert!(watch.snapshot().is_ok());
    let provider = node(router.context(), "owned_context_provider");
    let _service = provider
        .create_service(&service_interface(), "/owned", EndpointOptions::default())
        .unwrap();
    graph_until(watch.as_ref(), |entities| {
        entities.iter().any(|entity| {
            entity.kind == EntityKind::Service
                && entity
                    .endpoint
                    .as_ref()
                    .is_some_and(|endpoint| endpoint.name == "/owned")
        })
    });
    assert_eq!(router._session.info().peers_zid().wait().count(), 2);
    drop(watch);
    let started = Instant::now();
    while router._session.info().peers_zid().wait().count() != 1 {
        assert!(started.elapsed() < LIMIT, "watch retained the last Context");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn repeated_short_readiness_calls_keep_each_call_budget() {
    let router = Router::start();
    let provider = node(router.context(), "short_call_provider");
    let interface = service_interface();
    let service = provider
        .create_service(&interface, "/short_calls", EndpointOptions::default())
        .unwrap();
    let (proxy, context) = router.delayed_context();
    let caller = node(context, "short_call_caller");
    let client = caller
        .create_client(&interface, "/short_calls", EndpointOptions::default())
        .unwrap();
    proxy.set_delay(DELAY);
    let budget = Duration::from_millis(100);
    let mut attempts = Vec::new();
    for _ in 0..20 {
        let started = Instant::now();
        let ready = client.wait_for_service(Some(budget)).unwrap();
        let elapsed = started.elapsed();
        if !ready {
            assert!(elapsed >= budget);
        }
        assert!(elapsed < budget + Duration::from_millis(300));
        attempts.push((ready, elapsed.as_millis()));
        if ready {
            break;
        }
    }
    let started = Instant::now();
    assert!(client.wait_for_service(Some(LIMIT)).unwrap());
    eprintln!(
        "short readiness calls with {}ms one-way TCP delay: attempts={attempts:?}; full-budget wait={:?}",
        DELAY.as_millis(),
        started.elapsed()
    );
    assert_eq!(service.queue_stats().unwrap().received_total, 0);
}
