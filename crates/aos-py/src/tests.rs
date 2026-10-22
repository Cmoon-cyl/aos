//! Binding lifecycle tests through an embedded interpreter.
//!
//! `MemoryTransport` is a test-only aos-core adapter that can inject failures
//! and observe waiters. It proves Python object ownership, GIL release and
//! error mapping, not communication; `python/tests/test_zenoh_session.py`
//! covers real Zenoh sessions through the installed wheel.

use std::collections::{HashMap, VecDeque};
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use aos_core::transport::{PublisherBackend, SubscriptionBackend, Transport};
use aos_core::{Context, Error, Result};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::context::PyContext;

#[derive(Default)]
struct Queue {
    state: Mutex<(bool, VecDeque<Vec<u8>>)>,
    ready: Condvar,
}

impl Queue {
    fn close(&self) {
        self.state.lock().unwrap().0 = true;
        self.ready.notify_all();
    }
}

#[derive(Default)]
struct Session {
    closed: AtomicBool,
    closes: AtomicUsize,
    fail_publish: AtomicBool,
    fail_close: AtomicBool,
    waiting: Mutex<usize>,
    waiting_changed: Condvar,
    queues: Mutex<HashMap<String, Vec<Weak<Queue>>>>,
}

impl Session {
    fn check(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn wait_for_receivers(&self, count: usize) {
        let waiting = self.waiting.lock().unwrap();
        let (_guard, timeout) = self
            .waiting_changed
            .wait_timeout_while(waiting, Duration::from_secs(5), |n| *n < count)
            .unwrap();
        assert!(!timeout.timed_out(), "receiver did not start waiting");
    }
}

struct MemoryTransport(Arc<Session>);

impl Transport for MemoryTransport {
    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
        self.0.check()?;
        Ok(Box::new(MemoryPublisher {
            session: self.0.clone(),
            topic: topic.into(),
        }))
    }

    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
        self.0.check()?;
        let queue = Arc::new(Queue::default());
        let mut queues = self.0.queues.lock().unwrap();
        queues
            .entry(topic.into())
            .or_default()
            .push(Arc::downgrade(&queue));
        Ok(Box::new(MemorySubscription {
            session: self.0.clone(),
            queue,
        }))
    }

    fn close(&self) -> Result<()> {
        self.0.closed.store(true, Ordering::SeqCst);
        self.0.closes.fetch_add(1, Ordering::SeqCst);
        for queue in self.0.queues.lock().unwrap().values().flatten() {
            if let Some(queue) = queue.upgrade() {
                queue.close();
            }
        }
        if self.0.fail_close.load(Ordering::SeqCst) {
            Err(Error::Transport("session close failed".into()))
        } else {
            Ok(())
        }
    }
}

struct MemoryPublisher {
    session: Arc<Session>,
    topic: String,
}

impl PublisherBackend for MemoryPublisher {
    fn publish(&self, payload: &[u8]) -> Result<()> {
        self.session.check()?;
        if self.session.fail_publish.load(Ordering::SeqCst) {
            return Err(Error::Transport("link down".into()));
        }
        let queues = self.session.queues.lock().unwrap();
        for queue in queues.get(&self.topic).into_iter().flatten() {
            if let Some(queue) = queue.upgrade() {
                queue.state.lock().unwrap().1.push_back(payload.to_vec());
                queue.ready.notify_one();
            }
        }
        Ok(())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}

struct MemorySubscription {
    session: Arc<Session>,
    queue: Arc<Queue>,
}

impl SubscriptionBackend for MemorySubscription {
    fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.queue.state.lock().unwrap();
        *self.session.waiting.lock().unwrap() += 1;
        self.session.waiting_changed.notify_all();
        let result = loop {
            if state.0 {
                break Err(Error::Closed);
            }
            if let Some(data) = state.1.pop_front() {
                break Ok(data);
            }
            let now = Instant::now();
            if now >= deadline {
                break Err(Error::Timeout);
            }
            state = self
                .queue
                .ready
                .wait_timeout(state, deadline - now)
                .unwrap()
                .0;
        };
        *self.session.waiting.lock().unwrap() -= 1;
        result
    }

    fn close(&self) -> Result<()> {
        self.queue.close();
        Ok(())
    }
}

/// Run `code` with `aos` bound to a fresh native module and `ctx` to a new
/// Context over `session`.
fn run(session: &Arc<Session>, code: &str) {
    Python::attach(|py| {
        let globals = globals(py, session);
        run_in(py, &globals, code);
    });
}

fn globals<'py>(py: Python<'py>, session: &Arc<Session>) -> Bound<'py, PyDict> {
    let module = PyModule::new(py, "_native").unwrap();
    crate::errors::register(&module).unwrap();
    crate::context::register(&module).unwrap();
    let context = PyContext::from_core(Context::new(MemoryTransport(session.clone())));
    let globals = PyDict::new(py);
    globals.set_item("aos", module).unwrap();
    globals
        .set_item("ctx", Py::new(py, context).unwrap())
        .unwrap();
    globals
}

fn run_in(py: Python<'_>, globals: &Bound<'_, PyDict>, code: &str) {
    let code = CString::new(code).expect("embedded Python code must not contain null bytes");
    if let Err(error) = py.run(&code, Some(globals), None) {
        error.print(py);
        panic!("embedded Python failed: {error}");
    }
}

fn session() -> Arc<Session> {
    Python::initialize();
    Arc::new(Session::default())
}

#[test]
fn echo_bytes_through_owned_handles() {
    let session = session();
    run(
        &session,
        r#"
sub = ctx.create_subscription("/echo/request")
pub = ctx.create_publisher("/echo/request")
pub.publish(b"\x00\xffpayload")
pub.publish(b"")
assert sub.recv(1.0) == b"\x00\xffpayload"
assert sub.recv(0) == b""
assert type(ctx).__module__ == "aos_sdk" and type(ctx).__name__ == "Context"
"#,
    );
}

#[test]
fn core_errors_map_to_distinct_python_categories() {
    let session = session();
    // Both phases use one Context. Recreating it over the same test Session
    // lets collection of the first Context close the second one's transport.
    Python::attach(|py| {
        let globals = globals(py, &session);
        run_in(
            py,
            &globals,
            r#"
def raises(cls, fn, *args):
    try:
        fn(*args)
    except cls as error:
        return error
    raise AssertionError(f"{fn} did not raise {cls.__name__}")

raises(aos.InvalidArgumentError, ctx.create_publisher, "relative")
raises(aos.InvalidArgumentError, ctx.create_subscription, "/bad-name")
sub = ctx.create_subscription("/echo/request")
for bad in (-1.0, float("nan"), float("inf")):
    raises(aos.InvalidArgumentError, sub.recv, bad)
raises(TypeError, sub.recv, "1")
timeout = raises(aos.WaitTimeoutError, sub.recv, 0)
assert not isinstance(timeout, aos.CommunicationError)
pub = ctx.create_publisher("/echo/request")
raises(TypeError, pub.publish, "text is not bytes")
import gc
gc.collect()
"#,
        );
        session.fail_publish.store(true, Ordering::SeqCst);
        run_in(
            py,
            &globals,
            r#"
error = None
try:
    ctx.create_publisher("/echo/request").publish(b"x")
except aos.CommunicationError as caught:
    error = caught
assert error is not None and "link down" in str(error), error
"#,
        );
    });
}

#[test]
fn resource_refusals_preserve_specific_and_legacy_catch_categories() {
    use crate::errors::{InvalidArgumentError, ResourceLimitError};
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "resource_errors").unwrap();
        crate::errors::register(&module).unwrap();
        assert!(module
            .getattr("ResourceLimitError")
            .unwrap()
            .is(py.get_type::<ResourceLimitError>()));
        let resource =
            crate::errors::to_py_err(Error::ResourceLimit("interface schema nodes".into()));
        assert!(resource.is_instance_of::<ResourceLimitError>(py));
        assert!(resource.is_instance_of::<InvalidArgumentError>(py));
        assert!(resource.to_string().contains("interface schema nodes"));
        for error in [
            Error::InvalidArgument("bad limit".into()),
            Error::Validation("wrong type".into()),
        ] {
            let error = crate::errors::to_py_err(error);
            assert!(error.is_instance_of::<InvalidArgumentError>(py));
            assert!(!error.is_instance_of::<ResourceLimitError>(py));
        }
    });
}

#[test]
fn reply_queue_failures_preserve_specific_and_legacy_catch_categories() {
    use crate::errors::{
        CommunicationError, NoResponseError, QueueOverflowError, WaitTimeoutError,
    };

    Python::initialize();
    Python::attach(|py| {
        let overflow = crate::errors::to_py_err(Error::QueueOverflow("reply queue evicted".into()));
        assert!(overflow.is_instance_of::<QueueOverflowError>(py));
        assert!(overflow.is_instance_of::<CommunicationError>(py));
        assert!(!overflow.is_instance_of::<WaitTimeoutError>(py));
        assert!(overflow.to_string().contains("reply queue evicted"));
        let no_response = crate::errors::to_py_err(Error::NoResponse("query finished".into()));
        assert!(no_response.is_instance_of::<NoResponseError>(py));
        assert!(no_response.is_instance_of::<CommunicationError>(py));
        assert!(!no_response.is_instance_of::<WaitTimeoutError>(py));
        assert!(no_response.to_string().contains("query finished"));
    });
}

#[test]
fn shutdown_is_terminal_and_repeated_shutdown_is_invalid_state() {
    let session = session();
    run(
        &session,
        r#"
pub = ctx.create_publisher("/echo/request")
assert ctx.ok()
ctx.shutdown()
assert not ctx.ok()
for fn, args in ((ctx.shutdown, ()), (ctx.create_publisher, ("/a",)),
                 (ctx.create_subscription, ("/a",)), (pub.publish, (b"x",))):
    try:
        fn(*args)
    except aos.InvalidStateError:
        pass
    else:
        raise AssertionError(f"{fn} succeeded after shutdown")
# Releasing an entity after shutdown is safe: the adapter may report success
# or that it is already closed.
try:
    pub.destroy()
except aos.InvalidStateError:
    pass
"#,
    );
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
}

#[test]
fn blocked_recv_releases_gil_and_wakes_on_shutdown() {
    let session = session();
    let globals = Python::attach(|py| {
        let globals = globals(py, &session);
        run_in(
            py,
            &globals,
            r#"
import threading, time
sub = ctx.create_subscription("/echo/request")
outcome = {}
def wait():
    start = time.monotonic()
    try:
        sub.recv(30.0)
    except BaseException as error:
        outcome["error"] = error
    outcome["elapsed"] = time.monotonic() - start
worker = threading.Thread(target=wait)
worker.start()
"#,
        );
        globals.unbind()
    });
    // The worker is now inside the adapter wait. If recv held the GIL, this
    // thread could not run Python until the 30 s deadline. In that regression
    // `worker.start()` itself cannot return, so the test fails earlier, in
    // `wait_for_receivers`, rather than at the assertions below.
    session.wait_for_receivers(1);
    let started = Instant::now();
    Python::attach(|py| {
        run_in(
            py,
            globals.bind(py),
            r#"
ctx.shutdown()
worker.join(5)
assert not worker.is_alive()
assert isinstance(outcome["error"], aos.InvalidStateError), outcome
assert outcome["elapsed"] < 5, outcome
"#,
        );
    });
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[test]
fn destroy_wakes_only_that_subscription() {
    let session = session();
    let globals = Python::attach(|py| {
        let globals = globals(py, &session);
        run_in(
            py,
            &globals,
            r#"
import threading
first = ctx.create_subscription("/echo/request")
second = ctx.create_subscription("/echo/request")
outcome = {}
def wait():
    try:
        first.recv(30.0)
    except BaseException as error:
        outcome["error"] = error
worker = threading.Thread(target=wait)
worker.start()
"#,
        );
        globals.unbind()
    });
    session.wait_for_receivers(1);
    Python::attach(|py| {
        run_in(
            py,
            globals.bind(py),
            r#"
first.destroy()
worker.join(5)
assert not worker.is_alive()
assert isinstance(outcome["error"], aos.InvalidStateError), outcome
ctx.create_publisher("/echo/request").publish(b"still open")
assert second.recv(1.0) == b"still open"
assert ctx.ok()
"#,
        );
    });
}

#[test]
fn entities_keep_session_alive_until_last_python_reference() {
    let session = session();
    run(
        &session,
        r#"
sub = ctx.create_subscription("/echo/request")
pub = ctx.create_publisher("/echo/request")
del ctx
pub.publish(b"after context drop")
assert sub.recv(1.0) == b"after context drop"
del pub
"#,
    );
    // `sub` lived until `run` dropped its globals; the last Python handle
    // closes the session exactly once.
    assert_eq!(session.closes.load(Ordering::SeqCst), 1);
    assert!(session.closed.load(Ordering::SeqCst));
}

#[test]
fn context_managers_tolerate_prior_close_but_report_cleanup_failure() {
    let session = session();
    run(
        &session,
        r#"
with ctx as entered:
    assert entered is ctx
    with ctx.create_publisher("/echo/request") as pub:
        pub.destroy()
    entered.shutdown()
assert not ctx.ok()
"#,
    );
    let failing = self::session();
    failing.fail_close.store(true, Ordering::SeqCst);
    run(
        &failing,
        r#"
try:
    with ctx:
        raise KeyError("body")
except aos.CommunicationError as error:
    assert isinstance(error.__context__, KeyError), error.__context__
else:
    raise AssertionError("cleanup failure was swallowed")
assert not ctx.ok()
"#,
    );
}

#[test]
fn python_creates_entities_only_through_a_context() {
    let session = session();
    run(
        &session,
        r#"
for cls in (aos.Publisher, aos.Subscription):
    try:
        cls()
    except TypeError:
        pass
    else:
        raise AssertionError(f"{cls.__name__}() created a handle")
try:
    aos.Context(0)
except TypeError:
    pass
else:
    raise AssertionError("Context options must be keyword-only")
"#,
    );
}

#[test]
fn embedded_default_config_is_the_local_router_peer_profile() {
    let config = zenoh::Config::from_json5(crate::context::DEFAULT_SESSION_CONFIG)
        .expect("embedded session config must parse");
    assert_eq!(config.get_json("mode").unwrap(), r#""peer""#);
    assert_eq!(
        config.get_json("connect/endpoints").unwrap(),
        r#"["tcp/localhost:7447"]"#
    );
}

#[test]
fn public_interface_resource_exports_and_stubs_match_native_behavior() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "_native").unwrap();
        crate::_native(&module).unwrap();
        let globals = PyDict::new(py);
        globals.set_item("resource_native", module).unwrap();
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python");
        globals
            .set_item("resource_source", source.to_str().unwrap())
            .unwrap();
        run_in(
            py,
            &globals,
            r#"
import importlib.util
import pathlib
import sys
import unittest

saved_modules = {key: value for key, value in sys.modules.copy().items()
                 if key == "aos_sdk" or key.startswith("aos_sdk.")}
for key in saved_modules:
    del sys.modules[key]
saved_path = list(sys.path)
try:
    sys.path.insert(0, resource_source)
    sys.modules["aos_sdk._native"] = resource_native
    path = pathlib.Path(resource_source) / "tests" / "test_interface_resources.py"
    spec = importlib.util.spec_from_file_location("aos_resource_contract_tests", path)
    tests = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tests)
    suite = unittest.defaultTestLoader.loadTestsFromModule(tests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    assert result.wasSuccessful(), (result.errors, result.failures)
finally:
    for key in list(sys.modules):
        if key == "aos_sdk" or key.startswith("aos_sdk."):
            del sys.modules[key]
    sys.modules.update(saved_modules)
    sys.path[:] = saved_path
"#,
        );
    });
}
