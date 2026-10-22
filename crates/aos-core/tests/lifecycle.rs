use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, Barrier, Condvar, Mutex, Weak,
};
use std::thread;
use std::time::{Duration, Instant};

use aos_core::transport::{PublisherBackend, SubscriptionBackend, Transport};
use aos_core::{Context, Error, Publisher, Result, Subscription};

#[derive(Default)]
struct Queue {
    data: Mutex<(bool, VecDeque<Vec<u8>>)>,
    ready: Condvar,
}

impl Queue {
    fn close(&self) {
        self.data.lock().unwrap().0 = true;
        self.ready.notify_all();
    }
}

#[derive(Default)]
struct Session {
    closed: AtomicBool,
    closes: AtomicUsize,
    publisher_closes: AtomicUsize,
    subscription_closes: AtomicUsize,
    subscriptions: Mutex<Vec<(String, Weak<Queue>)>>,
    receive_started: Mutex<Option<mpsc::Sender<()>>>,
    fail_publish: AtomicBool,
    fail_declare: AtomicBool,
    fail_close: AtomicBool,
}

impl Session {
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}

struct TestTransport {
    session: Arc<Session>,
    declaration_gate: Option<Arc<Barrier>>,
}

impl Transport for TestTransport {
    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
        self.session.check()?;
        if self.session.fail_declare.load(Ordering::SeqCst) {
            return Err(Error::Transport("declaration failed".into()));
        }
        let publisher = TestPublisher {
            session: self.session.clone(),
            topic: topic.into(),
            closed: AtomicBool::new(false),
        };
        if let Some(gate) = &self.declaration_gate {
            gate.wait();
            gate.wait();
        }
        Ok(Box::new(publisher))
    }

    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
        let mut subscriptions = self.session.subscriptions.lock().unwrap();
        self.session.check()?;
        let queue = Arc::new(Queue::default());
        subscriptions.push((topic.into(), Arc::downgrade(&queue)));
        drop(subscriptions);
        if let Some(gate) = &self.declaration_gate {
            gate.wait();
            gate.wait();
        }
        Ok(Box::new(TestSubscription {
            session: self.session.clone(),
            queue,
        }))
    }

    fn close(&self) -> Result<()> {
        self.session.closes.fetch_add(1, Ordering::SeqCst);
        self.session.closed.store(true, Ordering::SeqCst);
        for (_, queue) in self.session.subscriptions.lock().unwrap().iter() {
            if let Some(queue) = queue.upgrade() {
                queue.close();
            }
        }
        if self.session.fail_close.load(Ordering::SeqCst) {
            Err(Error::Transport("session cleanup failed".into()))
        } else {
            Ok(())
        }
    }
}

struct TestPublisher {
    session: Arc<Session>,
    topic: String,
    closed: AtomicBool,
}

impl PublisherBackend for TestPublisher {
    fn publish(&self, payload: &[u8]) -> Result<()> {
        self.session.check()?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        if self.session.fail_publish.load(Ordering::SeqCst) {
            return Err(Error::Transport("publish failed".into()));
        }
        for (topic, queue) in self.session.subscriptions.lock().unwrap().iter() {
            if topic == &self.topic {
                if let Some(queue) = queue.upgrade() {
                    let mut state = queue.data.lock().unwrap();
                    if !state.0 {
                        state.1.push_back(payload.to_vec());
                        queue.ready.notify_one();
                    }
                }
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        self.session.publisher_closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for TestPublisher {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct TestSubscription {
    session: Arc<Session>,
    queue: Arc<Queue>,
}

impl SubscriptionBackend for TestSubscription {
    fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        if let Some(started) = self.session.receive_started.lock().unwrap().as_ref() {
            started.send(()).unwrap();
        }
        let deadline = Instant::now() + timeout;
        let mut state = self.queue.data.lock().unwrap();
        loop {
            if state.0 {
                return Err(Error::Closed);
            }
            if let Some(message) = state.1.pop_front() {
                return Ok(message);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            state = self.queue.ready.wait_timeout(state, remaining).unwrap().0;
        }
    }

    fn close(&self) -> Result<()> {
        let mut state = self.queue.data.lock().unwrap();
        if state.0 {
            return Err(Error::Closed);
        }
        state.0 = true;
        self.session
            .subscription_closes
            .fetch_add(1, Ordering::SeqCst);
        self.queue.ready.notify_all();
        Ok(())
    }
}

impl Drop for TestSubscription {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn fixture() -> (Context, Arc<Session>) {
    let session = Arc::new(Session::default());
    let context = Context::new(TestTransport {
        session: session.clone(),
        declaration_gate: None,
    });
    (context, session)
}

#[test]
fn binding_handles_are_send_sync_static() {
    fn assert_handle<T: Send + Sync + 'static>() {}
    assert_handle::<Context>();
    assert_handle::<Publisher>();
    assert_handle::<Subscription>();
}

#[test]
fn opaque_bytes_round_trip_and_topics_stay_separate() {
    let (context, _) = fixture();
    let request = context.create_subscription("/echo/request").unwrap();
    let response = context.create_subscription("/echo/response").unwrap();
    let client = context.create_publisher("/echo/request").unwrap();
    let provider = context.create_publisher("/echo/response").unwrap();
    let bytes = b"\0\xff{\"value\":1}";
    client.publish(bytes).unwrap();
    assert_eq!(response.recv_timeout(Duration::ZERO), Err(Error::Timeout));
    let received = request.recv_timeout(Duration::ZERO).unwrap();
    provider.publish(&received).unwrap();
    assert_eq!(response.recv_timeout(Duration::ZERO).unwrap(), bytes);
    assert_eq!(request.recv_timeout(Duration::ZERO), Err(Error::Timeout));
}

#[test]
fn rejects_non_absolute_or_middleware_topic_names() {
    let (context, _) = fixture();
    for topic in [
        "", "/", "echo", "/echo/", "//echo", "/1echo", "/e-cho", "/a/*", "/a/**", "/é",
    ] {
        assert!(matches!(
            context.create_publisher(topic),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            context.create_subscription(topic),
            Err(Error::InvalidArgument(_))
        ));
    }
    context.create_publisher("/_echo/a_1").unwrap();
}

#[test]
fn handles_keep_context_alive_until_last_owner_drops() {
    let (context, session) = fixture();
    let clone = context.clone();
    let publisher = context.create_publisher("/echo").unwrap();
    let subscription = context.create_subscription("/echo").unwrap();
    drop(context);
    drop(clone);
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    publisher.publish(b"still alive").unwrap();
    assert_eq!(
        subscription.recv_timeout(Duration::ZERO).unwrap(),
        b"still alive"
    );
    drop(publisher);
    assert_eq!(session.publisher_closes.load(Ordering::SeqCst), 1);
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
    drop(subscription);
    assert_eq!(session.subscription_closes.load(Ordering::SeqCst), 1);
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn explicit_shutdown_invalidates_all_handles_and_clones() {
    let (context, session) = fixture();
    let clone = context.clone();
    let publisher = context.create_publisher("/echo").unwrap();
    let subscription = context.create_subscription("/echo").unwrap();
    publisher.publish(b"queued before shutdown").unwrap();
    context.shutdown().unwrap();
    assert!(clone.is_shutdown());
    assert_eq!(clone.shutdown(), Err(Error::Closed));
    assert!(matches!(
        clone.create_publisher("/echo"),
        Err(Error::Closed)
    ));
    assert!(matches!(
        clone.create_subscription("/echo"),
        Err(Error::Closed)
    ));
    assert_eq!(publisher.publish(b"late"), Err(Error::Closed));
    assert_eq!(
        subscription.recv_timeout(Duration::ZERO),
        Err(Error::Closed)
    );
    drop((context, clone, publisher, subscription));
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn entity_close_does_not_close_context_or_siblings() {
    let (context, session) = fixture();
    let publisher = context.create_publisher("/echo").unwrap();
    let subscription = context.create_subscription("/echo").unwrap();
    publisher.close().unwrap();
    assert_eq!(publisher.close(), Err(Error::Closed));
    assert_eq!(publisher.publish(b"late"), Err(Error::Closed));
    subscription.close().unwrap();
    assert_eq!(subscription.close(), Err(Error::Closed));
    assert_eq!(
        subscription.recv_timeout(Duration::ZERO),
        Err(Error::Closed)
    );
    assert!(!context.is_shutdown());
    context
        .create_publisher("/other")
        .unwrap()
        .publish(b"ok")
        .unwrap();
    assert_eq!(session.closes.load(Ordering::SeqCst), 0);
}

#[test]
fn backend_errors_are_distinct_and_failed_shutdown_is_terminal() {
    let (context, session) = fixture();
    session.fail_declare.store(true, Ordering::SeqCst);
    assert!(
        matches!(context.create_publisher("/echo"), Err(Error::Transport(message)) if message == "declaration failed")
    );
    session.fail_declare.store(false, Ordering::SeqCst);
    let publisher = context.create_publisher("/echo").unwrap();
    session.fail_publish.store(true, Ordering::SeqCst);
    assert_eq!(
        publisher.publish(b"test"),
        Err(Error::Transport("publish failed".into()))
    );
    session.fail_close.store(true, Ordering::SeqCst);
    assert_eq!(
        context.shutdown(),
        Err(Error::Transport("session cleanup failed".into()))
    );
    assert!(context.is_shutdown());
    assert_eq!(context.shutdown(), Err(Error::Closed));
    assert_eq!(publisher.publish(b"late"), Err(Error::Closed));
    drop((publisher, context));
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
}

fn receive_interruption(close_context: bool) {
    let (context, session) = fixture();
    let subscription = Arc::new(context.create_subscription("/echo").unwrap());
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    *session.receive_started.lock().unwrap() = Some(started_tx);
    let receiver = subscription.clone();
    let thread = thread::spawn(move || {
        done_tx
            .send(receiver.recv_timeout(Duration::from_secs(30)))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    if close_context {
        context.shutdown().unwrap();
    } else {
        subscription.close().unwrap();
    }
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        Err(Error::Closed)
    );
    thread.join().unwrap();
}

#[test]
fn shutdown_interrupts_pending_receive() {
    receive_interruption(true);
}

#[test]
fn subscription_close_interrupts_pending_receive() {
    receive_interruption(false);
}

#[test]
fn concurrent_shutdown_has_exactly_one_cleanup_caller() {
    let (context, session) = fixture();
    let gate = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let context = context.clone();
            let gate = gate.clone();
            thread::spawn(move || {
                gate.wait();
                context.shutdown()
            })
        })
        .collect();
    gate.wait();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results.iter().filter(|r| **r == Err(Error::Closed)).count(),
        1
    );
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn shutdown_racing_declaration_cleans_up_the_losing_handle() {
    for publisher in [true, false] {
        let session = Arc::new(Session::default());
        let gate = Arc::new(Barrier::new(2));
        let context = Context::new(TestTransport {
            session: session.clone(),
            declaration_gate: Some(gate.clone()),
        });
        let creator = context.clone();
        let thread = thread::spawn(move || {
            if publisher {
                creator.create_publisher("/echo").map(|_| ())
            } else {
                creator.create_subscription("/echo").map(|_| ())
            }
        });
        gate.wait();
        context.shutdown().unwrap();
        gate.wait();
        assert_eq!(thread.join().unwrap(), Err(Error::Closed));
        if publisher {
            assert_eq!(session.publisher_closes.load(Ordering::SeqCst), 1);
        }
        assert!(session
            .subscriptions
            .lock()
            .unwrap()
            .iter()
            .all(|(_, queue)| queue.upgrade().is_none()));
    }
}
