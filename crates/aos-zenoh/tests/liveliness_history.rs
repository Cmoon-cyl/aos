//! Regression gates for the pinned Zenoh history/live ordering patch.
// Compile the exact vendored helper, including its panic/drop/reentry unit
// tests, without adding the upstream crate's unrelated dev dependencies.
#[path = "../../../vendor/zenoh/src/api/liveliness_history.rs"]
mod history_gate;
mod support;

use std::process::Command;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use zenoh::{sample::SampleKind, Wait};

const LIMIT: Duration = Duration::from_secs(10);
const CHILD_TEST_ENV: &str = "AOS_HISTORY_TEST_CHILD";

// Each network test gets its own process-global Zenoh runtime. Configure only
// the child before it starts, without changing a multithreaded harness's
// environment or replacing a caller-supplied ZENOH_RUNTIME.
fn ran_in_child(name: &str) -> bool {
    if std::env::var_os(CHILD_TEST_ENV).as_deref() == Some(std::ffi::OsStr::new(name)) {
        return false;
    }
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", name, "--nocapture"])
        .env(CHILD_TEST_ENV, name);
    if std::env::var_os("ZENOH_RUNTIME").is_none() {
        command.env("ZENOH_RUNTIME", "(net: (worker_threads: 8))");
    }
    let output = command.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{name} failed in its isolated runtime:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout
            .lines()
            .any(|line| line == format!("test {name} ... ok"))
            && stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;"),
        "{name} did not run exactly once in its isolated runtime:\n{stdout}\n{stderr}"
    );
    eprintln!("{stderr}");
    true
}

struct Router {
    session: zenoh::Session,
    endpoint: String,
}

impl Router {
    fn start(endpoint: &str) -> Self {
        let mut config = support::config("zenoh-router.json5");
        config
            .insert_json5("listen/endpoints", &json!([endpoint]).to_string())
            .unwrap();
        let session = zenoh::open(config).wait().unwrap();
        let endpoint = session.info().locators().wait()[0].to_string();
        Self { session, endpoint }
    }

    fn session(&self) -> Arc<zenoh::Session> {
        let mut config = support::peer_config(&self.endpoint);
        config.insert_json5("mode", "\"client\"").unwrap();
        // The router is the sole route, including in the reconnect test.
        config.insert_json5("listen/endpoints", "[]").unwrap();
        config
            .insert_json5("scouting/gossip/enabled", "false")
            .unwrap();
        Arc::new(zenoh::open(config).wait().unwrap())
    }
}

fn next_event(events: &mpsc::Receiver<(String, SampleKind)>, key: &str, kind: SampleKind) {
    let deadline = Instant::now() + LIMIT;
    loop {
        let event = events
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("liveliness event did not arrive");
        if event == (key.to_owned(), kind) {
            return;
        }
    }
}

// Release a held history callback on every exit, including assertion panic.
struct Release(Option<mpsc::Sender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

#[test]
fn blocked_history_put_is_delivered_before_a_later_live_delete() {
    if ran_in_child("blocked_history_put_is_delivered_before_a_later_live_delete") {
        return;
    }
    let router = Router::start("tcp/127.0.0.1:0");
    let provider = router.session();
    let observer = router.session();
    let target = "aos/history_barrier/target";
    let marker = "aos/history_barrier/after_delete";
    let token = provider.liveliness().declare_token(target).wait().unwrap();

    let (witness_tx, witness_rx) = mpsc::channel();
    let witness = observer
        .liveliness()
        .declare_subscriber("aos/history_barrier/**")
        .history(true)
        .callback(move |sample| {
            witness_tx
                .send((sample.key_expr().to_string(), sample.kind()))
                .unwrap();
        })
        .wait()
        .unwrap();
    // Prime the observer's remote token cache before creating history B.
    next_event(&witness_rx, target, SampleKind::Put);

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let release = Release(Some(release_tx));
    let (applied_tx, applied_rx) = mpsc::channel();
    let history = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| {
            if sample.kind() == SampleKind::Put {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv_timeout(LIMIT).unwrap();
            }
            applied_tx.send(sample.kind()).unwrap();
        })
        .wait()
        .unwrap();
    entered_rx.recv_timeout(LIMIT).unwrap();

    let (future_tx, future_rx) = mpsc::channel();
    let future_witness = observer
        .liveliness()
        .declare_subscriber(target)
        .history(false)
        .callback(move |sample| future_tx.send(sample.kind()).unwrap())
        .wait()
        .unwrap();
    token.undeclare().wait().unwrap();
    assert_eq!(future_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Delete);
    next_event(&witness_rx, target, SampleKind::Delete);
    // A later declaration on the same ordered TCP session is an ingress
    // barrier. The witness callback does not use history B's blocked callback.
    // The future-only witness alone is insufficient: the suffix-based Delete
    // path traverses a HashMap, not insertion-ordered resource subscribers.
    let marker_token = provider.liveliness().declare_token(marker).wait().unwrap();
    next_event(&witness_rx, marker, SampleKind::Put);
    let before_release: Vec<_> = applied_rx.try_iter().collect();
    drop(release);

    let mut applied = before_release.clone();
    while applied.len() < 2 {
        applied.push(applied_rx.recv_timeout(LIMIT).unwrap());
    }
    eprintln!("history barrier: before_release={before_release:?}, applied={applied:?}");
    assert_eq!(
        applied,
        [SampleKind::Put, SampleKind::Delete],
        "a cached history Put must not overwrite a later live Delete"
    );
    history.undeclare().wait().unwrap();
    future_witness.undeclare().wait().unwrap();
    witness.undeclare().wait().unwrap();
    marker_token.undeclare().wait().unwrap();
}

#[test]
fn history_and_live_callbacks_can_reenter_the_session() {
    if ran_in_child("history_and_live_callbacks_can_reenter_the_session") {
        return;
    }
    let router = Router::start("tcp/127.0.0.1:0");
    let provider = router.session();
    let observer = router.session();
    let target = "aos/history_reentry/target";
    let token = provider.liveliness().declare_token(target).wait().unwrap();
    let (primed_tx, primed_rx) = mpsc::channel();
    let prime = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| primed_tx.send(sample.kind()).unwrap())
        .wait()
        .unwrap();
    assert_eq!(primed_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);

    let (done_tx, done_rx) = mpsc::channel();
    let callback_session = observer.clone();
    let watch = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| {
            // Declaration and undeclaration both acquire session state.
            let nested = callback_session
                .declare_subscriber("aos/history_reentry/nested")
                .wait()
                .unwrap();
            nested.undeclare().wait().unwrap();
            done_tx.send(sample.kind()).unwrap();
        })
        .wait()
        .unwrap();
    assert_eq!(done_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);
    token.undeclare().wait().unwrap();
    assert_eq!(done_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Delete);
    watch.undeclare().wait().unwrap();
    prime.undeclare().wait().unwrap();
}

struct CallbackDropped(mpsc::Sender<()>);

impl Drop for CallbackDropped {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn undeclare_during_history_releases_the_callback_and_pending_delete() {
    if ran_in_child("undeclare_during_history_releases_the_callback_and_pending_delete") {
        return;
    }
    close_during_history(false);
}

#[test]
fn session_close_during_history_releases_the_callback_and_pending_delete() {
    if ran_in_child("session_close_during_history_releases_the_callback_and_pending_delete") {
        return;
    }
    close_during_history(true);
}

fn close_during_history(close_session: bool) {
    let router = Router::start("tcp/127.0.0.1:0");
    let provider = router.session();
    let observer = router.session();
    let target = "aos/history_close/target";
    let marker = "aos/history_close/after_delete";
    let token = provider.liveliness().declare_token(target).wait().unwrap();
    let (prime_tx, prime_rx) = mpsc::channel();
    let prime = observer
        .liveliness()
        .declare_subscriber("aos/history_close/**")
        .history(true)
        .callback(move |sample| {
            prime_tx
                .send((sample.key_expr().to_string(), sample.kind()))
                .unwrap()
        })
        .wait()
        .unwrap();
    next_event(&prime_rx, target, SampleKind::Put);

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let release = Release(Some(release_tx));
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let callback_dropped = CallbackDropped(dropped_tx);
    let (applied_tx, applied_rx) = mpsc::channel();
    let watch = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| {
            let _keep_alive = &callback_dropped;
            if sample.kind() == SampleKind::Put {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv_timeout(LIMIT).unwrap();
            }
            applied_tx.send(sample.kind()).unwrap();
        })
        .wait()
        .unwrap();
    entered_rx.recv_timeout(LIMIT).unwrap();
    token.undeclare().wait().unwrap();
    next_event(&prime_rx, target, SampleKind::Delete);
    // Delete and this later marker cross the same ordered provider session.
    // Seeing prime's Delete alone would not prove that watch's callback had
    // already queued Delete, because subscriber iteration order is unspecified.
    let _marker_token = provider.liveliness().declare_token(marker).wait().unwrap();
    next_event(&prime_rx, marker, SampleKind::Put);

    if close_session {
        let closing = observer.clone();
        let (closed_tx, closed_rx) = mpsc::channel();
        let close = std::thread::spawn(move || {
            let result = closing.close().wait();
            closed_tx.send(result.is_ok()).unwrap();
        });
        let deadline = Instant::now() + LIMIT;
        while !observer.is_closed() {
            assert!(Instant::now() < deadline, "session close did not begin");
            std::thread::yield_now();
        }
        // This Put is already running. Delete is still queued behind it and
        // must be discarded; generally, one already-dequeued sample may finish.
        drop(release);
        assert!(closed_rx.recv_timeout(LIMIT).unwrap());
        close.join().unwrap();
        drop(watch);
        drop(prime);
    } else {
        watch.undeclare().wait().unwrap();
        drop(release);
        prime.undeclare().wait().unwrap();
    }
    dropped_rx.recv_timeout(LIMIT).unwrap();
    assert_eq!(applied_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);
    assert!(matches!(
        applied_rx.recv_timeout(LIMIT),
        Err(mpsc::RecvTimeoutError::Disconnected)
    ));
}

#[test]
fn the_same_session_and_token_reappear_after_a_real_router_disconnect() {
    if ran_in_child("the_same_session_and_token_reappear_after_a_real_router_disconnect") {
        return;
    }
    let router = Router::start("tcp/127.0.0.1:0");
    let endpoint = router.endpoint.clone();
    let provider = router.session();
    let observer = router.session();
    let provider_zid = provider.zid();
    let observer_zid = observer.zid();
    let target = "aos/history_reconnect/same_token";
    let token = provider.liveliness().declare_token(target).wait().unwrap();
    let (prime_tx, prime_rx) = mpsc::channel();
    let prime = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| prime_tx.send(sample.kind()).unwrap())
        .wait()
        .unwrap();
    assert_eq!(prime_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);

    let (events_tx, events_rx) = mpsc::channel();
    let watch = observer
        .liveliness()
        .declare_subscriber(target)
        .history(true)
        .callback(move |sample| events_tx.send(sample.kind()).unwrap())
        .wait()
        .unwrap();
    assert_eq!(events_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);
    prime.undeclare().wait().unwrap();

    router.session.close().wait().unwrap();
    assert_eq!(events_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Delete);
    let replacement = Router::start(&endpoint);
    assert_eq!(events_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Put);
    assert_eq!(provider.zid(), provider_zid);
    assert_eq!(observer.zid(), observer_zid);
    // The original token handle remains declared on the original provider.
    token.undeclare().wait().unwrap();
    assert_eq!(events_rx.recv_timeout(LIMIT).unwrap(), SampleKind::Delete);
    watch.undeclare().wait().unwrap();
    replacement.session.close().wait().unwrap();
}
