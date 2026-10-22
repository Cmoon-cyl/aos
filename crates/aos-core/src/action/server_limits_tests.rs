//! Default-limit regressions deliberately use only the old public options.
//! They also execute against the old implementation without weakening asserts.
use super::*;
use crate::action::resource_test_support::{fixture, request, ReplyCapture, State};
use std::sync::atomic::Ordering;

fn server() -> (Arc<State>, crate::Context, Node, ActionServer) {
    server_options(ActionServerOptions::default())
}
fn server_options(
    options: ActionServerOptions,
) -> (Arc<State>, crate::Context, Node, ActionServer) {
    let (transport, context, node, _) = fixture();
    let interface = Interface::from_json(json!({
        "name":"test/action/ResourceBytes","kind":"action",
        "goal":{"type":"string"},
        "result":{"type":"string","default":""},
        "feedback":{"type":"object"}
    }))
    .unwrap();
    let server = ActionServer::new(
        &node,
        &interface,
        "/resources",
        options,
        EndpointOptions::default(),
    )
    .unwrap();
    (transport, context, node, server)
}

fn small_limits() -> ActionResourceLimits {
    ActionResourceLimits {
        max_goals: 4,
        max_goal_bytes: 8,
        max_result_bytes: 8,
        max_result_cache_bytes: 8,
    }
}

fn expire(goal: &ServerGoalHandle) {
    goal.goal.runtime.lock().unwrap().terminal_at = Some(Instant::now() - Duration::from_secs(11));
}

#[test]
fn pending_goal_drop_rejection_duplicate_and_closed_decision_release_slots() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: ActionResourceLimits {
            max_goals: 2,
            ..small_limits()
        },
        ..Default::default()
    });
    let id = "a".repeat(32);
    transport.goal_request(&id, json!(""));
    let first = server.take_goal_request(Duration::ZERO).unwrap();
    transport.goal_request(&id, json!(""));
    let duplicate = server.take_goal_request(Duration::ZERO).unwrap();
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 2);
    let accepted = first.decide(true).unwrap().unwrap();
    assert!(duplicate.decide(true).unwrap().is_none());
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 1);
    for rejected in [true, false] {
        transport.goal_request(&"b".repeat(32), json!(""));
        let request = server.take_goal_request(Duration::ZERO).unwrap();
        if rejected {
            assert!(request.decide(false).unwrap().is_none());
        }
        drop(request);
        assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 1);
    }
    accepted.execute().unwrap();
    accepted.succeed().unwrap();
    accepted.finish(Some(&json!(""))).unwrap();
    expire(&accepted);
    server.inner.expire_goals();
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 0);
    transport.goal_request(&"c".repeat(32), json!(""));
    let pending = server.take_goal_request(Duration::ZERO).unwrap();
    server.close().unwrap();
    // A taken admission is owned by the application, until decided or dropped.
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 1);
    assert!(matches!(pending.decide(true), Err(Error::Closed)));
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 0);
}

#[test]
fn failed_admission_reply_keeps_the_authoritative_goal_bounded_until_close() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: ActionResourceLimits {
            max_goals: 1,
            ..small_limits()
        },
        ..Default::default()
    });
    let capture = transport.goal_request(&"a".repeat(32), json!(""));
    *capture.failure.lock().unwrap() = Some(Error::Closed);
    assert!(matches!(
        server
            .take_goal_request(Duration::ZERO)
            .unwrap()
            .decide(true),
        Err(Error::Closed)
    ));
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 1);
    assert_eq!(server.inner.registry.lock().unwrap().goals.len(), 1);
    let rejected = transport.goal_request(&"b".repeat(32), json!(""));
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(rejected.result().unwrap().unwrap()["accepted"], false);
    server.close().unwrap();
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 0);
}

#[test]
fn exact_goal_byte_limit_counts_utf8_and_json_escaping_before_admission() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: small_limits(),
        ..Default::default()
    });
    let exact = json!("汉\nx"); // 2 quotes + 3 UTF-8 + 2 escaped LF + 1 ASCII.
    assert_eq!(serde_json::to_vec(&exact).unwrap().len(), 8);
    transport.goal_request(&"a".repeat(32), exact.clone());
    let request = server.take_goal_request(Duration::ZERO).unwrap();
    assert_eq!(request.request(), &exact);
    let rejected = transport.goal_request(&"b".repeat(32), json!("汉\nxy"));
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(rejected.result().unwrap().unwrap()["accepted"], false);
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 1);
    drop(request);
    assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 0);
    server.close().unwrap();
}

#[test]
fn small_cache_budget_preserves_earlier_results_and_is_reusable_after_expiry() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: small_limits(),
        ..Default::default()
    });
    let first = admit(&server, &transport, &"a".repeat(32));
    let second = admit(&server, &transport, &"b".repeat(32));
    let third = admit(&server, &transport, &"c".repeat(32));
    for goal in [&first, &second, &third] {
        goal.execute().unwrap();
        goal.succeed().unwrap();
    }
    first.finish(Some(&json!("ab"))).unwrap();
    second.finish(Some(&json!("cd"))).unwrap();
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
    assert_remote(third.finish(Some(&json!(""))), "ACTION_RESULT_CACHE_LIMIT");
    assert_eq!(
        result_request(&server, first.goal_id())
            .result()
            .unwrap()
            .unwrap()["result"],
        "ab"
    );
    assert_eq!(
        result_request(&server, second.goal_id())
            .result()
            .unwrap()
            .unwrap()["result"],
        "cd"
    );
    assert!(matches!(
        first.finish(Some(&json!(""))),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
    expire(&first);
    server.inner.expire_goals();
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 4);
    let next = admit(&server, &transport, &"d".repeat(32));
    next.execute().unwrap();
    next.succeed().unwrap();
    next.finish(Some(&json!("ef"))).unwrap();
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
    server.close().unwrap();
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
}

#[test]
fn error_results_are_bounded_and_preserve_each_legal_terminal_state() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: small_limits(),
        ..Default::default()
    });
    for (index, terminal) in [
        GoalStatus::Succeeded,
        GoalStatus::Canceled,
        GoalStatus::Aborted,
    ]
    .into_iter()
    .enumerate()
    {
        let goal = admit(&server, &transport, &format!("{index:032x}"));
        goal.execute().unwrap();
        match terminal {
            GoalStatus::Succeeded => goal.succeed().unwrap(),
            GoalStatus::Canceled => {
                goal.update(GoalEvent::CancelGoal).unwrap();
                goal.canceled().unwrap();
            }
            _ => goal.abort().unwrap(),
        }
        assert_remote(
            goal.finish_error("longer than eight bytes"),
            "ACTION_RESULT_TOO_LARGE",
        );
        assert_eq!(goal.status(), terminal as u8);
    }
    let active = admit(&server, &transport, &"d".repeat(32));
    active.execute().unwrap();
    active.finish_error("汉🙂").unwrap();
    assert_eq!(active.status(), GoalStatus::Aborted as u8);
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 7);
    assert!(matches!(result_request(&server, active.goal_id()).result(),
        Some(Err(Error::Remote(message))) if message.contains("汉🙂")));
    server.close().unwrap();
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
}

#[test]
fn schema_error_and_default_result_are_bounded_before_retention() {
    let (transport, _context, _node, server) = server_options(ActionServerOptions {
        resource_limits: small_limits(),
        ..Default::default()
    });
    let goal = admit(&server, &transport, &"a".repeat(32));
    goal.execute().unwrap();
    goal.succeed().unwrap();
    assert_remote(goal.finish(Some(&json!(false))), "ACTION_RESULT_TOO_LARGE");
    assert_eq!(goal.status(), GoalStatus::Succeeded as u8);
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
    server.close().unwrap();

    let (transport, context, node, _) = fixture();
    let interface = Interface::from_json(json!({
        "name":"test/action/LargeDefault", "kind":"action",
        "goal":{"type":"string"}, "feedback":{"type":"object"},
        "result":{"type":"string","default":"123456789"}
    }))
    .unwrap();
    let server = ActionServer::new(
        &node,
        &interface,
        "/resources",
        ActionServerOptions {
            resource_limits: small_limits(),
            ..Default::default()
        },
        EndpointOptions::default(),
    )
    .unwrap();
    assert!(matches!(result_request(&server, &"e".repeat(32)).result(),
        Some(Err(Error::Remote(message))) if message.contains("ACTION_RESULT_TOO_LARGE")));
    let goal = admit(&server, &transport, &"f".repeat(32));
    goal.execute().unwrap();
    goal.succeed().unwrap();
    assert_remote(goal.finish(None), "ACTION_RESULT_TOO_LARGE");
    assert_eq!(goal.status(), GoalStatus::Succeeded as u8);
    assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
    server.close().unwrap();
    drop(context);
}

#[test]
fn blocked_replies_retain_bytes_after_expiry_or_close_until_transport_returns() {
    use std::sync::mpsc;
    for close in [false, true] {
        for cached in [false, true] {
            let (transport, _context, _node, server) = server_options(ActionServerOptions {
                resource_limits: small_limits(),
                ..Default::default()
            });
            let goal = admit(&server, &transport, &"a".repeat(32));
            goal.execute().unwrap();
            goal.succeed().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let release_rx = Mutex::new(release_rx);
            let (request, capture) = request(json!({"goal_id":goal.goal_id()}));
            *capture.failure.lock().unwrap() = Some(Error::Closed);
            *capture.reply_hook.lock().unwrap() = Some(Arc::new(move || {
                entered_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap();
            }));
            let request = server
                .inner
                .get_result
                .decode_request(request, None)
                .unwrap();
            if cached {
                goal.finish(Some(&json!("123456"))).unwrap();
            } else {
                server.inner.process_result(request);
                let worker_goal = goal.clone();
                let worker = std::thread::spawn(move || worker_goal.finish(Some(&json!("123456"))));
                entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
                if close {
                    server.close().unwrap();
                } else {
                    expire(&goal);
                    server.inner.expire_goals();
                }
                assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
                release_tx.send(()).unwrap();
                worker.join().unwrap().unwrap();
                assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
                if !close {
                    server.close().unwrap();
                }
                continue;
            }
            let inner = server.inner.clone();
            let worker = std::thread::spawn(move || inner.process_result(request));
            entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            if close {
                server.close().unwrap();
            } else {
                expire(&goal);
                server.inner.expire_goals();
            }
            assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 8);
            release_tx.send(()).unwrap();
            worker.join().unwrap();
            assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
            if !close {
                server.close().unwrap();
            }
        }
    }
}

#[test]
fn resource_error_releases_the_managed_action_lease_and_allows_deactivation() {
    use crate::{LifecycleCallbackReturn::Success, LifecycleState, LifecycleTransitionId};
    let (transport, context, _ordinary, _) = fixture();
    let node = Node::new_lifecycle(context, "managed", "/", Default::default()).unwrap();
    let lifecycle = node.lifecycle().unwrap();
    for transition in [
        LifecycleTransitionId::Configure,
        LifecycleTransitionId::Activate,
    ] {
        lifecycle
            .begin_transition(transition)
            .unwrap()
            .complete(Success, None)
            .unwrap();
    }
    let interface = Interface::from_json(json!({
        "name":"test/action/ManagedBytes","kind":"action",
        "goal":{"type":"string"},"result":{"type":"string","default":""},
        "feedback":{"type":"object"}
    }))
    .unwrap();
    let server = ActionServer::new(
        &node,
        &interface,
        "/resources",
        ActionServerOptions {
            resource_limits: small_limits(),
            ..Default::default()
        },
        EndpointOptions::default(),
    )
    .unwrap();
    let goal = admit(&server, &transport, &"a".repeat(32));
    assert_eq!(lifecycle.snapshot().unwrap().in_flight.len(), 1);
    goal.execute().unwrap();
    goal.succeed().unwrap();
    assert_remote(
        goal.finish(Some(&json!("1234567"))),
        "ACTION_RESULT_TOO_LARGE",
    );
    assert!(lifecycle.snapshot().unwrap().in_flight.is_empty());
    let report = lifecycle
        .begin_transition(LifecycleTransitionId::Deactivate)
        .unwrap()
        .complete(Success, Some(Duration::from_millis(100)))
        .unwrap()
        .report
        .unwrap();
    assert!(report.successful);
    assert_eq!(lifecycle.current_state().unwrap(), LifecycleState::Inactive);
    assert_eq!(goal.status(), GoalStatus::Succeeded as u8);
    server.close().unwrap();
}

#[test]
fn concurrent_finishes_and_close_release_all_slots_and_byte_reservations() {
    use std::sync::Barrier;
    for _ in 0..16 {
        let (transport, _context, _node, server) = server_options(ActionServerOptions {
            resource_limits: small_limits(),
            ..Default::default()
        });
        let goal = admit(&server, &transport, &"a".repeat(32));
        goal.execute().unwrap();
        goal.succeed().unwrap();
        let replies = (0..4)
            .map(|_| result_request(&server, goal.goal_id()))
            .collect::<Vec<_>>();
        let start = Barrier::new(9);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let start = &start;
                let goal = &goal;
                scope.spawn(move || {
                    start.wait();
                    let result = goal.finish(Some(&json!("123456")));
                    assert!(matches!(
                        result,
                        Ok(()) | Err(Error::Closed | Error::InvalidArgument(_) | Error::Remote(_))
                    ));
                });
            }
            start.wait();
            server.close().unwrap();
        });
        assert_eq!(server.inner.result_bytes.used.load(Ordering::Acquire), 0);
        assert_eq!(server.inner.goal_slots.used.load(Ordering::Acquire), 0);
        assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
        for reply in replies {
            // A close may reach the Service before a detached finish waiter.
            // That path reports Closed locally; it must never reply twice.
            assert!(reply.replies.lock().unwrap().len() <= 1);
        }
    }
}
fn admit(server: &ActionServer, transport: &State, id: &str) -> ServerGoalHandle {
    transport.goal_request(id, json!(""));
    server
        .take_goal_request(Duration::ZERO)
        .unwrap()
        .decide(true)
        .unwrap()
        .unwrap()
}
fn result_request(server: &ActionServer, id: &str) -> Arc<ReplyCapture> {
    let (request, capture) = request(json!({"goal_id":id}));
    server.inner.process_result(
        server
            .inner
            .get_result
            .decode_request(request, None)
            .unwrap(),
    );
    capture
}
fn assert_remote(result: Result<()>, code: &str) {
    assert!(
        matches!(&result, Err(Error::Remote(message)) if message.contains(code)),
        "expected {code}, got {result:?}"
    );
}

#[test]
fn default_goal_slots_include_pending_admissions_and_registered_goals() {
    let (transport, _context, _node, server) = server();
    let registered = admit(&server, &transport, &format!("{:032x}", 0));
    let mut pending = Vec::new();
    for index in 1..1024 {
        transport.goal_request(&format!("{index:032x}"), json!(""));
        pending.push(server.take_goal_request(Duration::ZERO).unwrap());
    }
    let rejected = transport.goal_request(&format!("{:032x}", 1024), json!(""));
    let response = server.take_goal_request(Duration::ZERO);
    assert!(
        matches!(response, Err(Error::Timeout)),
        "a full provider must reject before handing a request to the admission callback"
    );
    assert_eq!(rejected.result().unwrap().unwrap()["accepted"], false);
    assert_eq!(registered.status(), GoalStatus::Accepted as u8);
    assert!(!registered.is_cancel_requested());
    drop(pending.pop());
    transport.goal_request(&format!("{:032x}", 1025), json!(""));
    let next = server.take_goal_request(Duration::ZERO).unwrap();
    assert!(next.decide(false).unwrap().is_none());
    server.close().unwrap();
}

#[test]
fn default_oversized_goal_is_rejected_before_the_admission_callback() {
    let (transport, _context, _node, server) = server();
    let rejected = transport.goal_request(&"1".repeat(32), json!("x".repeat(1 << 20)));
    assert!(matches!(
        server.take_goal_request(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert_eq!(rejected.result().unwrap().unwrap()["accepted"], false);
    assert!(server.inner.registry.lock().unwrap().goals.is_empty());
    let accepted = admit(&server, &transport, &"2".repeat(32));
    assert!(accepted.is_active());
    server.close().unwrap();
}

#[test]
fn default_oversized_result_preserves_terminal_and_replies_a_stable_resource_error() {
    let (transport, _context, _node, server) = server();
    let goal = admit(&server, &transport, &"3".repeat(32));
    goal.execute().unwrap();
    goal.succeed().unwrap();
    let pending = result_request(&server, goal.goal_id());
    assert!(pending.result().is_none());
    assert_remote(
        goal.finish(Some(&json!("x".repeat(1 << 20)))),
        "ACTION_RESULT_TOO_LARGE",
    );
    assert_eq!(goal.status(), GoalStatus::Succeeded as u8);
    let first = pending.result().unwrap();
    let next = result_request(&server, goal.goal_id()).result().unwrap();
    assert_eq!(first, next);
    assert!(
        matches!(first, Err(Error::Remote(message)) if message.contains("ACTION_RESULT_TOO_LARGE"))
    );
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
    server.close().unwrap();
}

#[test]
fn default_total_result_budget_does_not_evict_unexpired_results() {
    let (transport, _context, _node, server) = server();
    // A JSON string including its two quotes is exactly one MiB.
    let value = json!("x".repeat((1 << 20) - 2));
    assert_eq!(serde_json::to_vec(&value).unwrap().len(), 1 << 20);
    for index in 0..64 {
        let goal = admit(&server, &transport, &format!("{index:032x}"));
        goal.execute().unwrap();
        goal.succeed().unwrap();
        goal.finish(Some(&value)).unwrap();
    }
    let next = admit(&server, &transport, &format!("{:032x}", 64));
    next.execute().unwrap();
    next.succeed().unwrap();
    assert_remote(next.finish(Some(&value)), "ACTION_RESULT_CACHE_LIMIT");
    assert_eq!(next.status(), GoalStatus::Succeeded as u8);
    let first = result_request(&server, &format!("{:032x}", 0));
    assert_eq!(first.result().unwrap().unwrap()["result"], value);
    server.close().unwrap();
}

#[test]
fn expiry_clears_the_internal_result_even_when_application_holds_a_goal_handle() {
    let (transport, _context, _node, server) = server();
    let goal = admit(&server, &transport, &"4".repeat(32));
    goal.execute().unwrap();
    goal.succeed().unwrap();
    goal.finish(Some(&json!("kept"))).unwrap();
    goal.goal.runtime.lock().unwrap().terminal_at = Some(Instant::now() - Duration::from_secs(11));
    server.inner.expire_goals();
    let state = goal.goal.runtime.lock().unwrap();
    assert!(state.expired);
    assert!(
        state.result.is_none(),
        "application handle must not pin expired internal results"
    );
    drop(state);
    assert_eq!(goal.request(), &json!(""));
    server.close().unwrap();
}

#[test]
fn close_clears_the_internal_result_even_when_application_holds_a_goal_handle() {
    let (transport, _context, _node, server) = server();
    let goal = admit(&server, &transport, &"5".repeat(32));
    goal.execute().unwrap();
    goal.succeed().unwrap();
    goal.finish(Some(&json!("kept"))).unwrap();
    server.close().unwrap();
    let state = goal.goal.runtime.lock().unwrap();
    assert!(state.expired);
    assert!(
        state.result.is_none(),
        "application handle must not pin closed internal results"
    );
    drop(state);
    assert_eq!(goal.request(), &json!(""));
}
