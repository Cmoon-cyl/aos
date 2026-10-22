use super::*;
use crate::action::resource_test_support::{fixture, request, ReplyCapture};
use std::sync::atomic::Ordering;

fn server(options: ActionServerOptions) -> (crate::Context, Node, ActionServer) {
    let (_, context, node, interface) = fixture();
    let server = ActionServer::new(
        &node,
        &interface,
        "/resources",
        options,
        EndpointOptions::default(),
    )
    .unwrap();
    (context, node, server)
}
fn install_goal(server: &ActionServer, id: &str) -> ServerGoalHandle {
    let goal = Arc::new(GoalRecord {
        info: GoalInfo {
            goal_id: id.into(),
            stamp: Timestamp::default(),
        },
        request: json!({}),
        request_id: None,
        runtime: Mutex::new(GoalRuntime {
            status: GoalStatus::Executing,
            execution_scheduled: true,
            result: None,
            waiters: Vec::new(),
            terminal_at: None,
            expired: false,
            lifecycle_lease: None,
            goal_slot: Some(server.inner.goal_slots.acquire().unwrap()),
        }),
    });
    server
        .inner
        .registry
        .lock()
        .unwrap()
        .goals
        .insert(id.into(), goal.clone());
    ServerGoalHandle {
        server: Arc::downgrade(&server.inner),
        goal,
    }
}
fn pending_result(server: &ActionServer, id: &str) -> Arc<ReplyCapture> {
    let (request, capture) = request(json!({"goal_id":id}));
    let request = server
        .inner
        .get_result
        .decode_request(request, None)
        .unwrap();
    server.inner.process_result(request);
    capture
}

// This regression uses only the pre-limit API, so it also runs unchanged
// against the old implementation and proves the formerly unbounded retention.
#[test]
fn default_result_waiters_are_bounded_across_goals_without_changing_goal_state() {
    let (_context, _node, server) = server(ActionServerOptions::default());
    let first = install_goal(&server, &"1".repeat(32));
    let second = install_goal(&server, &"2".repeat(32));
    let mut admitted = Vec::new();
    for index in 0..1024 {
        let goal = if index % 2 == 0 { &first } else { &second };
        admitted.push(pending_result(&server, goal.goal_id()));
    }
    assert!(admitted.iter().all(|reply| reply.result().is_none()));
    let rejected = pending_result(&server, first.goal_id());
    assert!(
        matches!(rejected.result(), Some(Err(Error::Remote(message)))
        if message == "ACTION_RESULT_WAITER_LIMIT")
    );
    assert_eq!(first.goal.runtime.lock().unwrap().waiters.len(), 512);
    assert_eq!(second.goal.runtime.lock().unwrap().waiters.len(), 512);
    assert_eq!(first.status(), GoalStatus::Executing as u8);
    assert_eq!(second.status(), GoalStatus::Executing as u8);
    first.succeed().unwrap();
    first.finish(Some(&json!({}))).unwrap();
    for (index, reply) in admitted.iter().enumerate() {
        assert_eq!(reply.result().is_some(), index % 2 == 0);
    }
    let next = pending_result(&server, second.goal_id());
    assert!(next.result().is_none());
    server.close().unwrap();
    assert!(next.result().is_some());
    assert!(admitted
        .iter()
        .all(|reply| reply.replies.lock().unwrap().len() == 1));
}

#[test]
fn waiter_limit_accepts_only_the_documented_range() {
    assert_eq!(ActionServerOptions::default().max_result_waiters, 1024);
    for limit in [0, 65_537, usize::MAX] {
        let (_, _context, node, interface) = fixture();
        assert!(matches!(
            ActionServer::new(
                &node,
                &interface,
                "/invalid",
                ActionServerOptions {
                    max_result_waiters: limit,
                    ..Default::default()
                },
                EndpointOptions::default(),
            ),
            Err(Error::InvalidArgument(_))
        ));
    }
    for limit in [1, 65_536] {
        let (_context, _node, server) = server(ActionServerOptions {
            max_result_waiters: limit,
            ..Default::default()
        });
        assert_eq!(server.inner.result_waiters.limit, limit);
        server.close().unwrap();
    }
}

#[test]
fn result_failure_expiry_and_unknown_or_cached_replies_account_for_permits() {
    let (_context, _node, server) = server(ActionServerOptions {
        max_result_waiters: 1,
        result_timeout: Duration::from_secs(1),
        ..Default::default()
    });
    let first = install_goal(&server, &"5".repeat(32));
    let second = install_goal(&server, &"6".repeat(32));
    let first_reply = pending_result(&server, first.goal_id());
    // A departed client does not create a transport cancellation notification.
    // Its unanswered request stays counted until the provider settles it.
    *first_reply.failure.lock().unwrap() = Some(Error::Closed);
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 1);
    let unknown = pending_result(&server, &"f".repeat(32));
    assert_eq!(unknown.result().unwrap().unwrap()["status"], 0);
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 1);
    first.finish_error("business failed").unwrap();
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
    assert!(matches!(first_reply.result(), Some(Err(Error::Remote(_)))));

    let second_reply = pending_result(&server, second.goal_id());
    let cached = pending_result(&server, first.goal_id());
    assert!(matches!(cached.result(), Some(Err(Error::Remote(_)))));
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 1);
    {
        let mut state = second.goal.runtime.lock().unwrap();
        state.status = GoalStatus::Succeeded;
        state.terminal_at = Some(Instant::now() - Duration::from_secs(2));
    }
    server.inner.expire_goals();
    assert_eq!(second_reply.result().unwrap().unwrap()["status"], 0);
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
    assert!(second.goal.runtime.lock().unwrap().expired);
    assert!(second.goal.runtime.lock().unwrap().waiters.is_empty());
    server.close().unwrap();
}

#[test]
fn a_blocked_terminal_reply_holds_its_permit_until_the_reply_returns() {
    use std::sync::mpsc;
    let (_context, _node, server) = server(ActionServerOptions {
        max_result_waiters: 1,
        ..Default::default()
    });
    let first = install_goal(&server, &"7".repeat(32));
    let second = install_goal(&server, &"8".repeat(32));
    let reply = pending_result(&server, first.goal_id());
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    *reply.reply_hook.lock().unwrap() = Some(Arc::new(move || {
        entered_tx.send(()).unwrap();
        release_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
    }));
    let worker = std::thread::spawn(move || first.finish_error("finished"));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 1);
    let rejected = pending_result(&server, second.goal_id());
    assert!(
        matches!(rejected.result(), Some(Err(Error::Remote(message)))
        if message == "ACTION_RESULT_WAITER_LIMIT")
    );
    release_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
    let admitted = pending_result(&server, second.goal_id());
    assert!(admitted.result().is_none());
    server.close().unwrap();
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
}

#[test]
fn close_racing_result_admission_releases_all_permits_without_changing_goals() {
    use std::sync::Barrier;
    for _ in 0..32 {
        let (_context, _node, server) = server(ActionServerOptions {
            max_result_waiters: 3,
            ..Default::default()
        });
        let goal = install_goal(&server, &"9".repeat(32));
        let mut requests = Vec::new();
        for _ in 0..8 {
            let (request, _) = request(json!({"goal_id":goal.goal_id()}));
            requests.push(
                server
                    .inner
                    .get_result
                    .decode_request(request, None)
                    .unwrap(),
            );
        }
        let start = Barrier::new(9);
        std::thread::scope(|scope| {
            for request in requests {
                let server = &server;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    server.inner.process_result(request);
                    assert!(server.inner.result_waiters.used.load(Ordering::Acquire) <= 3);
                });
            }
            start.wait();
            server.close().unwrap();
        });
        assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
        assert!(server.inner.registry.lock().unwrap().goals.is_empty());
        let state = goal.goal.runtime.lock().unwrap();
        assert!(state.expired && state.waiters.is_empty());
        assert_eq!(state.status, GoalStatus::Executing);
    }
}

#[test]
fn terminal_completion_racing_admission_replies_once_and_releases_every_permit() {
    use std::sync::Barrier;
    let (_context, _node, server) = server(ActionServerOptions {
        max_result_waiters: 4,
        ..Default::default()
    });
    let goal = install_goal(&server, &"a".repeat(32));
    let mut requests = Vec::new();
    let mut replies = Vec::new();
    for _ in 0..16 {
        let (request, capture) = request(json!({"goal_id":goal.goal_id()}));
        requests.push(
            server
                .inner
                .get_result
                .decode_request(request, None)
                .unwrap(),
        );
        replies.push(capture);
    }
    let start = Barrier::new(17);
    std::thread::scope(|scope| {
        for request in requests {
            let server = &server;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                server.inner.process_result(request);
            });
        }
        start.wait();
        goal.succeed().unwrap();
        goal.finish(Some(&json!({}))).unwrap();
    });
    assert_eq!(server.inner.result_waiters.used.load(Ordering::Acquire), 0);
    assert!(goal.goal.runtime.lock().unwrap().waiters.is_empty());
    for reply in replies {
        assert_eq!(reply.replies.lock().unwrap().len(), 1);
    }
    assert_eq!(goal.status(), GoalStatus::Succeeded as u8);
    server.close().unwrap();
}
