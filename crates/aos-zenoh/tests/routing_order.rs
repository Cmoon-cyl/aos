//! Deterministic regressions for routing-generated Current/Delete ordering.
//! The shared-runtime initializer uses the existing transitive internal feature
//! from zenoh-ext/unstable; this test adds no dependency feature.
//! These tests use public callbacks and no scheduler sleeps or routing hooks.

use std::collections::BTreeSet;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use zenoh::internal::runtime::{DynamicRuntime, Runtime, RuntimeBuilder, ZRuntime};
use zenoh::{sample::SampleKind, Wait};

const LIMIT: Duration = Duration::from_secs(5);
const CHILD_TEST_ENV: &str = "AOS_ROUTING_ORDER_TEST_CHILD";

fn ran_in_child(name: &str) -> bool {
    ran_in_child_with_evidence(name, &[])
}

fn ran_in_child_with_evidence(name: &str, required: &[&str]) -> bool {
    if std::env::var(CHILD_TEST_ENV).as_deref() == Ok(name) {
        return false;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD_TEST_ENV, name)
        // Path evidence below matches plain tracing fields. ANSI styling can
        // otherwise split a field name from its brace or equals sign.
        .env("NO_COLOR", "1")
        .env(
            "RUST_LOG",
            if required.is_empty() {
                "error"
            } else {
                "error,zenoh::net::routing::hat::router=trace"
            },
        )
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{name} failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout
            .lines()
            .any(|line| line == format!("test {name} ... ok"))
            && stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;"),
        "{name} did not execute exactly once:\n{stdout}\n{stderr}"
    );
    for evidence in required {
        assert!(
            stdout.contains(evidence) || stderr.contains(evidence),
            "{name} did not traverse {evidence}:\n{stdout}\n{stderr}"
        );
        let first = stdout
            .lines()
            .chain(stderr.lines())
            .find(|line| line.contains(evidence))
            .unwrap();
        eprintln!("routing path proof ({evidence}): {first}");
    }
    eprintln!("{stderr}");
    true
}

#[derive(Clone, Copy, Debug)]
enum Change {
    Withdraw,
    ProviderClose,
    ObserverClose,
}

struct Release(Option<mpsc::Sender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[test]
fn generated_current_batch_cannot_overtake_a_later_local_withdrawal() {
    if !ran_in_child("generated_current_batch_cannot_overtake_a_later_local_withdrawal") {
        blocked_current(Change::Withdraw);
    }
}

#[test]
fn provider_close_withdrawals_follow_its_pending_current_batch() {
    if !ran_in_child("provider_close_withdrawals_follow_its_pending_current_batch") {
        blocked_current(Change::ProviderClose);
    }
}

#[test]
fn observer_close_prevents_new_callbacks_without_waiting_for_an_inflight_callback() {
    if !ran_in_child(
        "observer_close_prevents_new_callbacks_without_waiting_for_an_inflight_callback",
    ) {
        blocked_current(Change::ObserverClose);
    }
}

fn blocked_current(change: Change) {
    let mut config = zenoh::Config::default();
    config.insert_json5("mode", "\"router\"").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();
    let runtime = ZRuntime::Application
        .block_on(RuntimeBuilder::new(config).build())
        .unwrap();
    // These are separate API sessions sharing one routing gateway, just as
    // upstream's plugin initializer permits. A provider undeclare therefore
    // updates the same routing tables synchronously before wait() returns.
    // Public init takes DynamicRuntime. InitBuilder converts that to a
    // GenericRuntime with static_runtime=None, so close removes only this face.
    let dynamic: DynamicRuntime = runtime.clone().into();
    let provider = zenoh::session::init(dynamic.clone()).wait().unwrap();
    let observer = zenoh::session::init(dynamic).wait().unwrap();
    let mut tokens: Vec<_> = ["diagnostic/order/a", "diagnostic/order/b"]
        .into_iter()
        .map(|key| {
            (
                key.to_owned(),
                provider.liveliness().declare_token(key).wait().unwrap(),
            )
        })
        .collect();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Release(Some(release_tx));
    let release_rx = Mutex::new(release_rx);
    let first = AtomicBool::new(true);
    let current = Arc::new(Mutex::new(BTreeSet::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let started = starts.clone();
    let observed = current.clone();
    let recorded = events.clone();
    let (watch, target) = thread::scope(|scope| {
        let opening = scope.spawn(|| {
            observer
                .liveliness()
                .declare_subscriber("diagnostic/order/**")
                .history(true)
                .callback(move |sample| {
                    let key = sample.key_expr().to_string();
                    started.lock().unwrap().push((key.clone(), sample.kind()));
                    if sample.kind() == SampleKind::Put && first.swap(false, Ordering::SeqCst) {
                        entered_tx.send(key.clone()).unwrap();
                        release_rx.lock().unwrap().recv_timeout(LIMIT).unwrap();
                    }
                    recorded.lock().unwrap().push((key.clone(), sample.kind()));
                    match sample.kind() {
                        SampleKind::Put => {
                            observed.lock().unwrap().insert(key);
                        }
                        SampleKind::Delete => {
                            observed.lock().unwrap().remove(&key);
                        }
                    }
                })
                .wait()
                .unwrap()
        });
        // The initial Current batch has been generated under the routing lock,
        // and is paused while emitting its first token. Select the other token,
        // so its Current declaration is still in that generated batch.
        let held = entered_rx.recv_timeout(LIMIT).unwrap();
        let target_index = tokens.iter().position(|(key, _)| key != &held).unwrap();
        let target = tokens[target_index].0.clone();
        eprintln!("routing barrier: change={change:?}; held={held}; target={target}");
        match change {
            Change::Withdraw => tokens.remove(target_index).1.undeclare().wait().unwrap(),
            Change::ProviderClose => {
                provider.close().wait().unwrap();
                assert!(
                    !runtime.is_closed(),
                    "plugin close must keep the shared runtime alive"
                );
                assert!(!observer.is_closed());
            }
            Change::ObserverClose => {
                observer.close().wait().unwrap();
                assert!(observer.is_closed());
                assert!(
                    !runtime.is_closed(),
                    "plugin close must keep the shared runtime alive"
                );
                assert!(!provider.is_closed());
                assert_eq!(starts.lock().unwrap().len(), 1);
                let later = provider
                    .liveliness()
                    .declare_token("diagnostic/order/after_observer_close")
                    .wait()
                    .unwrap();
                later.undeclare().wait().unwrap();
            }
        }
        eprintln!(
            "routing barrier: withdrawal returned; before_release={:?}",
            events.lock().unwrap()
        );
        assert!(
            !starts
                .lock()
                .unwrap()
                .contains(&(target.clone(), SampleKind::Delete)),
            "the pending Delete must not begin before releasing Current"
        );
        assert!(
            !events
                .lock()
                .unwrap()
                .contains(&(target.clone(), SampleKind::Delete)),
            "the pending Delete must not complete before releasing Current"
        );
        drop(release);
        (opening.join().unwrap(), target)
    });
    match change {
        Change::ObserverClose => {
            assert_eq!(
                starts.lock().unwrap().len(),
                1,
                "no callback may begin after observer close"
            );
            assert_eq!(
                events.lock().unwrap().len(),
                1,
                "the already running callback may finish after close"
            );
            drop(watch);
        }
        Change::Withdraw | Change::ProviderClose => {
            let marker_session = if provider.is_closed() {
                &observer
            } else {
                &provider
            };
            let marker = marker_session
                .liveliness()
                .declare_token("diagnostic/order/after_withdrawal")
                .wait()
                .unwrap();
            let marker_snapshot = current.lock().unwrap().clone();
            eprintln!(
                "routing barrier: change={change:?}; events={:?}; target={target}; marker_snapshot={marker_snapshot:?}",
                events.lock().unwrap()
            );
            assert!(marker_snapshot.contains("diagnostic/order/after_withdrawal"));
            assert!(
                !marker_snapshot.contains(&target),
                "a generated Current declaration resurrected a withdrawn token"
            );
            if matches!(change, Change::ProviderClose) {
                assert!(!marker_snapshot.contains("diagnostic/order/a"));
                assert!(!marker_snapshot.contains("diagnostic/order/b"));
                let events = events.lock().unwrap();
                for key in ["diagnostic/order/a", "diagnostic/order/b"] {
                    let put = events
                        .iter()
                        .position(|event| event == &(key.to_owned(), SampleKind::Put))
                        .unwrap();
                    let delete = events
                        .iter()
                        .position(|event| event == &(key.to_owned(), SampleKind::Delete))
                        .unwrap();
                    assert!(
                        put < delete,
                        "close withdrawal must follow the pending current put"
                    );
                }
            }
            marker.undeclare().wait().unwrap();
            watch.undeclare().wait().unwrap();
        }
    }
    drop(tokens);
    observer.close().wait().unwrap();
    provider.close().wait().unwrap();
    runtime.close().wait().unwrap();
}

fn router_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("mode", "\"router\"").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();
    config
}

fn build_runtime(config: zenoh::Config) -> Runtime {
    ZRuntime::Application
        .block_on(RuntimeBuilder::new(config).build())
        .unwrap()
}

fn plugin_session(runtime: &Runtime) -> zenoh::Session {
    let dynamic: DynamicRuntime = runtime.clone().into();
    zenoh::session::init(dynamic).wait().unwrap()
}

#[test]
fn same_lane_blocking_get_is_a_timeout_contract_violation() {
    if !ran_in_child("same_lane_blocking_get_is_a_timeout_contract_violation") {
        blocking_get_contract(false);
    }
}

#[test]
fn joining_another_thread_that_waits_on_the_callback_lane_also_times_out() {
    if !ran_in_child("joining_another_thread_that_waits_on_the_callback_lane_also_times_out") {
        blocking_get_contract(true);
    }
}

fn blocking_get_contract(other_thread: bool) {
    let runtime = build_runtime(router_config());
    let provider = plugin_session(&runtime);
    let observer = Arc::new(plugin_session(&runtime));
    let answer = provider
        .liveliness()
        .declare_token("diagnostic/reentry/answer")
        .wait()
        .unwrap();
    let callback_session = observer.clone();
    let query_timeout = Duration::from_millis(80);
    let (result_tx, result_rx) = mpsc::channel();
    let watch = observer
        .liveliness()
        .declare_subscriber("diagnostic/reentry/trigger")
        .callback(move |sample| {
            if sample.kind() != SampleKind::Put {
                return;
            }
            let query = || {
                let started = Instant::now();
                let replies = callback_session
                    .liveliness()
                    .get("diagnostic/reentry/answer")
                    .timeout(query_timeout)
                    .wait()
                    .unwrap();
                let error = replies
                    .recv_timeout(LIMIT)
                    .unwrap()
                    .expect("query timeout must fire")
                    .into_result()
                    .expect_err("the callback cannot synchronously receive its own lane's answer");
                assert_eq!(error.payload().try_to_string().unwrap(), "Timeout");
                assert!(started.elapsed() >= query_timeout);
                started.elapsed()
            };
            let elapsed = if other_thread {
                thread::scope(|scope| scope.spawn(query).join().unwrap())
            } else {
                query()
            };
            result_tx.send(elapsed).unwrap();
        })
        .wait()
        .unwrap();
    let trigger = provider
        .liveliness()
        .declare_token("diagnostic/reentry/trigger")
        .wait()
        .unwrap();
    let elapsed = result_rx.recv_timeout(LIMIT).unwrap();
    eprintln!("same-lane contract: other_thread={other_thread}; timeout={query_timeout:?}; observed={elapsed:?}");
    // The same query works after the callback returns and the FIFO is released.
    let replies = observer
        .liveliness()
        .get("diagnostic/reentry/answer")
        .timeout(LIMIT)
        .wait()
        .unwrap();
    let result = replies
        .recv_timeout(LIMIT)
        .unwrap()
        .unwrap()
        .into_result()
        .unwrap();
    assert_eq!(result.key_expr().as_str(), "diagnostic/reentry/answer");
    assert!(
        replies.recv_timeout(LIMIT).is_err(),
        "DeclareFinal closes the successful query"
    );
    trigger.undeclare().wait().unwrap();
    watch.undeclare().wait().unwrap();
    answer.undeclare().wait().unwrap();
    // The timed-out Current reply arrived after the query was removed. With
    // no Future interest on this key, Zenoh caches it in remote_tokens and
    // never receives its withdrawal. Fix the contract's observable boundary:
    // a fresh history watch still replays this ghost after provider withdrawal.
    let history = observer
        .liveliness()
        .declare_subscriber("diagnostic/reentry/answer")
        .history(true)
        .wait()
        .unwrap();
    let ghost = history.recv_timeout(LIMIT).unwrap().unwrap();
    assert_eq!(ghost.key_expr().as_str(), "diagnostic/reentry/answer");
    assert_eq!(ghost.kind(), SampleKind::Put);
    eprintln!(
        "same-lane contract: late Current reply remains a history ghost after provider withdrawal"
    );
    history.undeclare().wait().unwrap();
    observer.close().wait().unwrap();
    provider.close().wait().unwrap();
    runtime.close().wait().unwrap();
}

fn start_router(mut config: zenoh::Config) -> Runtime {
    config
        .insert_json5("listen/endpoints", "[\"tcp/127.0.0.1:0\"]")
        .unwrap();
    let mut runtime = build_runtime(config);
    ZRuntime::Application.block_on(runtime.start()).unwrap();
    assert!(!runtime.get_locators().is_empty());
    runtime
}

fn receive_event(receiver: &mpsc::Receiver<(String, SampleKind)>, key: &str, kind: SampleKind) {
    let deadline = Instant::now() + LIMIT;
    loop {
        let event = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if event == (key.to_owned(), kind) {
            break;
        }
    }
}

#[test]
fn token_drop_during_unwind_delivers_delete_without_a_later_declaration() {
    if ran_in_child("token_drop_during_unwind_delivers_delete_without_a_later_declaration") {
        return;
    }
    let runtime = build_runtime(router_config());
    let provider = plugin_session(&runtime);
    let observer = plugin_session(&runtime);
    let key = "diagnostic/unwind/token";
    let watch = observer
        .liveliness()
        .declare_subscriber(key)
        .wait()
        .unwrap();
    let token = provider.liveliness().declare_token(key).wait().unwrap();
    assert_eq!(
        watch.recv_timeout(LIMIT).unwrap().unwrap().kind(),
        SampleKind::Put
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _token = token;
        panic!("application panic dropping a live token")
    }));
    assert_eq!(
        result.unwrap_err().downcast_ref::<&str>(),
        Some(&"application panic dropping a live token")
    );
    // Deliberately make no new Declare/Get/Close call before observing Delete.
    let deleted = watch.recv_timeout(LIMIT).unwrap().unwrap();
    assert_eq!(deleted.kind(), SampleKind::Delete);
    assert_eq!(deleted.key_expr().as_str(), key);
    eprintln!(
        "unwind proof: original panic returned; token Delete arrived without a later declaration"
    );
    watch.undeclare().wait().unwrap();
    observer.close().wait().unwrap();
    provider.close().wait().unwrap();
    runtime.close().wait().unwrap();
}

#[test]
fn subscriber_drop_during_unwind_delivers_unmatch_without_a_later_declaration() {
    if ran_in_child("subscriber_drop_during_unwind_delivers_unmatch_without_a_later_declaration") {
        return;
    }
    let runtime = build_runtime(router_config());
    let provider = plugin_session(&runtime);
    let observer = plugin_session(&runtime);
    let key = "diagnostic/unwind/subscriber";
    let publisher = provider.declare_publisher(key).wait().unwrap();
    let matches = publisher.matching_listener().wait().unwrap();
    let subscriber = observer.declare_subscriber(key).wait().unwrap();
    assert!(matches.recv_timeout(LIMIT).unwrap().unwrap().matching());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _subscriber = subscriber;
        panic!("application panic dropping a live subscriber")
    }));
    assert_eq!(
        result.unwrap_err().downcast_ref::<&str>(),
        Some(&"application panic dropping a live subscriber")
    );
    // No later Declare/Get/Close is allowed to rescue an abandoned lane.
    assert!(!matches.recv_timeout(LIMIT).unwrap().unwrap().matching());
    eprintln!(
        "unwind proof: original panic returned; Subscriber unmatch arrived without a later declaration"
    );
    matches.undeclare().wait().unwrap();
    publisher.undeclare().wait().unwrap();
    observer.close().wait().unwrap();
    provider.close().wait().unwrap();
    runtime.close().wait().unwrap();
}

#[test]
fn transport_rx_survives_a_declaration_callback_panic() {
    if ran_in_child("transport_rx_survives_a_declaration_callback_panic") {
        return;
    }
    let first = start_router(router_config());
    let mut config = router_config();
    config
        .insert_json5(
            "connect/endpoints",
            &format!("[\"{}\"]", first.get_locators()[0]),
        )
        .unwrap();
    let second = start_router(config);
    let provider = plugin_session(&first);
    let observer = plugin_session(&second);
    let (events_tx, events_rx) = mpsc::channel();
    let watch = observer
        .liveliness()
        .declare_subscriber("diagnostic/rx-panic/**")
        .history(true)
        .callback(move |sample| {
            events_tx
                .send((sample.key_expr().to_string(), sample.kind()))
                .unwrap();
            if sample.kind() == SampleKind::Put && sample.key_expr().as_str().ends_with("/first") {
                panic!("declaration callback panic on transport RX")
            }
        })
        .wait()
        .unwrap();
    let token1 = provider
        .liveliness()
        .declare_token("diagnostic/rx-panic/first")
        .wait()
        .unwrap();
    receive_event(&events_rx, "diagnostic/rx-panic/first", SampleKind::Put);
    let token2 = provider
        .liveliness()
        .declare_token("diagnostic/rx-panic/second")
        .wait()
        .unwrap();
    receive_event(&events_rx, "diagnostic/rx-panic/second", SampleKind::Put);
    token1.undeclare().wait().unwrap();
    receive_event(&events_rx, "diagnostic/rx-panic/first", SampleKind::Delete);
    token2.undeclare().wait().unwrap();
    receive_event(&events_rx, "diagnostic/rx-panic/second", SampleKind::Delete);
    eprintln!("transport panic proof: subsequent Put and both Deletes reached the observer");
    watch.undeclare().wait().unwrap();
    observer.close().wait().unwrap();
    provider.close().wait().unwrap();
    second.close().wait().unwrap();
    first.close().wait().unwrap();
}

#[test]
fn two_routers_exchange_tokens_subscribers_queryables_and_linkstate_oam() {
    if ran_in_child_with_evidence(
        "two_routers_exchange_tokens_subscribers_queryables_and_linkstate_oam",
        &["handle_oam{", "linkstate=LinkStateList", "Compute trees"],
    ) {
        return;
    }
    zenoh::init_log_from_env_or("error,zenoh::net::routing::hat::router=trace");
    let first = start_router(router_config());
    let provider = plugin_session(&first);
    let (provider_tx, provider_rx) = mpsc::channel();
    let provider_watch = provider
        .liveliness()
        .declare_subscriber("diagnostic/routers/**")
        .history(true)
        .callback(move |sample| {
            provider_tx
                .send((sample.key_expr().to_string(), sample.kind()))
                .unwrap();
        })
        .wait()
        .unwrap();
    // Seed all three sourced entity kinds before the router link exists.
    // OAM/tree computation must make these existing entities reachable.
    let reverse_key = "diagnostic/routers/preexisting_subscriber";
    let reverse_subscriber = provider.declare_subscriber(reverse_key).wait().unwrap();
    let query_key = "diagnostic/routers/query";
    let queryable = provider
        .declare_queryable(query_key)
        .complete(true)
        .callback(move |query| {
            query.reply(query_key, "router answer").wait().unwrap();
        })
        .wait()
        .unwrap();
    let provider_ready = provider
        .liveliness()
        .declare_token("diagnostic/routers/provider_ready")
        .wait()
        .unwrap();
    eprintln!("router tree proof: token/queryable/subscriber exist before connecting router B");
    let mut config = router_config();
    config
        .insert_json5(
            "connect/endpoints",
            &serde_json::to_string(&vec![first.get_locators()[0].to_string()]).unwrap(),
        )
        .unwrap();
    let second = start_router(config);
    let observer = plugin_session(&second);
    assert_ne!(provider.zid(), observer.zid());
    let (observer_tx, observer_rx) = mpsc::channel();
    let observer_watch = observer
        .liveliness()
        .declare_subscriber("diagnostic/routers/**")
        .history(true)
        .callback(move |sample| {
            observer_tx
                .send((sample.key_expr().to_string(), sample.kind()))
                .unwrap();
        })
        .wait()
        .unwrap();
    let data_key = "diagnostic/routers/data";
    let subscriber = observer.declare_subscriber(data_key).wait().unwrap();
    let observer_ready = observer
        .liveliness()
        .declare_token("diagnostic/routers/observer_ready")
        .wait()
        .unwrap();
    receive_event(
        &provider_rx,
        "diagnostic/routers/observer_ready",
        SampleKind::Put,
    );
    receive_event(
        &observer_rx,
        "diagnostic/routers/provider_ready",
        SampleKind::Put,
    );
    observer
        .put(reverse_key, "preexisting subscriber payload")
        .wait()
        .unwrap();
    assert_eq!(
        reverse_subscriber
            .recv_timeout(LIMIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "preexisting subscriber payload"
    );
    provider.put(data_key, "router payload").wait().unwrap();
    assert_eq!(
        subscriber
            .recv_timeout(LIMIT)
            .unwrap()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "router payload"
    );
    let replies = observer.get(query_key).timeout(LIMIT).wait().unwrap();
    assert_eq!(
        replies
            .recv_timeout(LIMIT)
            .unwrap()
            .unwrap()
            .into_result()
            .unwrap()
            .payload()
            .try_to_string()
            .unwrap(),
        "router answer"
    );
    for round in 0..8 {
        let key = format!("diagnostic/routers/churn/{round}");
        let token = provider.liveliness().declare_token(&key).wait().unwrap();
        receive_event(&observer_rx, &key, SampleKind::Put);
        let replies = observer
            .liveliness()
            .get(&key)
            .timeout(LIMIT)
            .wait()
            .unwrap();
        assert_eq!(
            replies
                .recv_timeout(LIMIT)
                .unwrap()
                .unwrap()
                .into_result()
                .unwrap()
                .key_expr()
                .as_str(),
            key
        );
        assert!(
            replies.recv_timeout(LIMIT).is_err(),
            "query must end with DeclareFinal"
        );
        token.undeclare().wait().unwrap();
        receive_event(&observer_rx, &key, SampleKind::Delete);
    }
    queryable.undeclare().wait().unwrap();
    subscriber.undeclare().wait().unwrap();
    reverse_subscriber.undeclare().wait().unwrap();
    provider_ready.undeclare().wait().unwrap();
    receive_event(
        &observer_rx,
        "diagnostic/routers/provider_ready",
        SampleKind::Delete,
    );
    observer_ready.undeclare().wait().unwrap();
    receive_event(
        &provider_rx,
        "diagnostic/routers/observer_ready",
        SampleKind::Delete,
    );
    provider_watch.undeclare().wait().unwrap();
    observer_watch.undeclare().wait().unwrap();
    provider.close().wait().unwrap();
    observer.close().wait().unwrap();
    first.close().wait().unwrap();
    second.close().wait().unwrap();
}

#[test]
fn an_acl_rejected_network_interest_receives_final_without_query_timeout() {
    if !ran_in_child("an_acl_rejected_network_interest_receives_final_without_query_timeout") {
        let mut config = router_config();
        config
            .insert_json5(
                "access_control",
                r#"{
            enabled: true, default_permission: "allow",
            rules: [{ id: "deny_query", permission: "deny", messages: ["liveliness_query"],
                      flows: ["ingress"], key_exprs: ["diagnostic/acl/denied/**"] }],
            subjects: [{ id: "all" }], policies: [{ rules: ["deny_query"], subjects: ["all"] }]
        }"#,
            )
            .unwrap();
        let runtime = start_router(config);
        let provider = plugin_session(&runtime);
        let allowed = provider
            .liveliness()
            .declare_token("diagnostic/acl/allowed/token")
            .wait()
            .unwrap();
        let denied = provider
            .liveliness()
            .declare_token("diagnostic/acl/denied/token")
            .wait()
            .unwrap();
        let mut config = zenoh::Config::default();
        config.insert_json5("mode", "\"client\"").unwrap();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        config
            .insert_json5(
                "connect/endpoints",
                &serde_json::to_string(&vec![runtime.get_locators()[0].to_string()]).unwrap(),
            )
            .unwrap();
        let client = zenoh::open(config).wait().unwrap();
        let replies = client
            .liveliness()
            .get("diagnostic/acl/allowed/token")
            .timeout(LIMIT)
            .wait()
            .unwrap();
        assert_eq!(
            replies
                .recv_timeout(LIMIT)
                .unwrap()
                .unwrap()
                .into_result()
                .unwrap()
                .key_expr()
                .as_str(),
            "diagnostic/acl/allowed/token"
        );
        assert!(replies.recv_timeout(LIMIT).is_err());
        // A 30-second query timer cannot produce this <=5-second disconnect.
        // It must be the DeclareFinal sent by DeMux's rejected-Interest arm.
        let replies = client
            .liveliness()
            .get("diagnostic/acl/denied/token")
            .timeout(Duration::from_secs(30))
            .wait()
            .unwrap();
        assert!(
            replies.recv_timeout(LIMIT).is_err(),
            "ACL rejection must close the query with zero replies before timeout"
        );
        eprintln!("ACL network proof: allowed token returned; denied interest finalized with zero replies");
        client.close().wait().unwrap();
        allowed.undeclare().wait().unwrap();
        denied.undeclare().wait().unwrap();
        provider.close().wait().unwrap();
        runtime.close().wait().unwrap();
    }
}
