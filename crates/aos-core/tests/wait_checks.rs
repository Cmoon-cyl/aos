//! Discovery-only checks: one watch per wait and caller checks outside locks.
//! Real transport and Python signal behavior live in test_platform_resources.py.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use aos_core::{transport::*, *};

#[derive(Default)]
struct State {
    graph: Mutex<BTreeMap<String, GraphEntity>>,
    created: AtomicUsize,
    dropped: AtomicUsize,
}

struct Discovery(Arc<State>);
struct Watch(Arc<State>);
struct Guard(Arc<State>, String);
struct QuietService;
struct QuietSubscription;

impl GraphGuard for Guard {
    fn close(&self) -> Result<()> {
        self.0.graph.lock().unwrap().remove(&self.1);
        Ok(())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl GraphWatchBackend for Watch {
    fn snapshot(&self) -> Result<Vec<GraphEntity>> {
        Ok(self.0.graph.lock().unwrap().values().cloned().collect())
    }
    fn wait_for_change(&self, timeout: Duration) -> Result<()> {
        std::thread::sleep(timeout);
        Err(Error::Timeout)
    }
}
impl Drop for Watch {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl ServiceBackend for QuietService {
    fn recv_timeout(&self, _: Duration) -> Result<Box<dyn RequestBackend>> {
        Err(Error::Closed)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl SubscriptionBackend for QuietSubscription {
    fn recv_timeout(&self, _: Duration) -> Result<Vec<u8>> {
        Err(Error::Closed)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl Transport for Discovery {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("discovery-only fixture".into()))
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        Ok(Box::new(QuietSubscription))
    }
    fn create_typed_subscription(
        &self,
        _: &Endpoint,
        _: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        Ok(Box::new(QuietSubscription))
    }
    fn create_service(&self, _: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        Ok(Box::new(QuietService))
    }
    fn declare_graph_entity(&self, entity: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        self.0
            .graph
            .lock()
            .unwrap()
            .insert(entity.id.clone(), entity.clone());
        Ok(Box::new(Guard(self.0.clone(), entity.id.clone())))
    }
    fn watch_graph(&self) -> Result<Option<Box<dyn GraphWatchBackend>>> {
        self.0.created.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Box::new(Watch(self.0.clone()))))
    }
    fn close(&self) -> Result<()> {
        self.0.graph.lock().unwrap().clear();
        Ok(())
    }
}

fn fixture() -> (Arc<State>, Context, Node) {
    let state = Arc::new(State::default());
    let context = Context::new(Discovery(state.clone()));
    let node = Node::new(context.clone(), "checks", "/", NodeOptions::default()).unwrap();
    (state, context, node)
}
fn interface(path: &str) -> Interface {
    Interface::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../interfaces")
            .join(path),
    )
    .unwrap()
}
fn assert_watches(state: &State, count: usize) {
    assert_eq!(state.created.load(Ordering::SeqCst), count);
    assert_eq!(state.dropped.load(Ordering::SeqCst), count);
}

#[test]
fn interrupted_service_wait_releases_one_watch_and_keeps_the_client_usable() {
    let (state, _context, node) = fixture();
    let interface = interface("AddTwoInts.json");
    let client = node
        .create_client(&interface, "absent", EndpointOptions::default())
        .unwrap();
    let caller = std::thread::current().id();
    let mut checks = 0;
    let interruption = Error::Transport("caller interruption".into());
    assert_eq!(
        client.wait_for_service_with_check(None, || {
            assert_eq!(std::thread::current().id(), caller);
            assert!(state.graph.try_lock().is_ok());
            assert_eq!(state.created.load(Ordering::SeqCst), 1);
            assert_eq!(state.dropped.load(Ordering::SeqCst), 0);
            checks += 1;
            if checks == 3 {
                Err(interruption.clone())
            } else {
                Ok(())
            }
        }),
        Err(interruption)
    );
    assert_eq!(checks, 3);
    assert_watches(&state, 1);
    let _provider = node
        .create_service(&interface, "absent", EndpointOptions::default())
        .unwrap();
    assert!(client.wait_for_service(Some(Duration::ZERO)).unwrap());
    assert_watches(&state, 2);
}

#[test]
fn action_wait_check_can_close_the_client_without_overwriting_its_error() {
    let (state, _context, node) = fixture();
    let client = ActionClient::new(
        &node,
        &interface("action/Fibonacci.json"),
        "absent",
        EndpointOptions::default(),
    )
    .unwrap();
    let mut checks = 0;
    let interruption = Error::Transport("caller interruption after close".into());
    assert_eq!(
        client.wait_for_server_with_check(None, || {
            checks += 1;
            if checks == 3 {
                client.close().unwrap();
                Err(interruption.clone())
            } else {
                Ok(())
            }
        }),
        Err(interruption)
    );
    assert_eq!(checks, 3);
    assert_watches(&state, 1);
    assert_eq!(
        client.wait_for_server(Some(Duration::ZERO)),
        Err(Error::Closed)
    );
}

#[test]
fn readiness_check_can_close_its_context_without_holding_resource_locks() {
    let (state, context, node) = fixture();
    let client = node
        .create_client(
            &interface("AddTwoInts.json"),
            "absent",
            EndpointOptions::default(),
        )
        .unwrap();
    assert_eq!(
        client.wait_for_service_with_check(None, || context.shutdown()),
        Err(Error::Closed)
    );
    assert_watches(&state, 1);
    assert!(context.is_shutdown());
}

#[test]
fn node_bounded_subscription_does_not_fall_back_to_a_legacy_adapter() {
    let (state, _context, node) = fixture();
    let interface = interface("String.json");
    let _ordinary = node
        .create_subscription(
            &interface,
            "ordinary",
            QoS::default(),
            EndpointOptions::default(),
        )
        .unwrap();
    let before = state.graph.lock().unwrap().len();
    assert!(matches!(
        node.create_subscription_with_limits(
            &interface,
            "bounded",
            QoS::default(),
            QueueLimits::default(),
            EndpointOptions::default()
        ),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(state.graph.lock().unwrap().len(), before);
}
