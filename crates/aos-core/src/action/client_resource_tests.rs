use super::*;
use crate::action::resource_test_support::fixture;
use std::sync::mpsc;

const WAIT: Duration = Duration::from_secs(2);

#[test]
fn observations_are_retained_only_by_live_handles_and_inflight_sends() {
    let (state, _context, node, interface) = fixture();
    let client =
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap();
    for index in 0..256 {
        let id = format!("{index:032x}");
        let goal = client.send_goal(&json!({}), Some(&id), WAIT).unwrap();
        assert_eq!(goal.status(), GoalStatus::Unknown as u8);
        state.status(&[(&id, GoalStatus::Executing as u8)]);
        client.recv_status(WAIT).unwrap();
        assert_eq!(goal.status(), GoalStatus::Executing as u8);
        drop(goal);
        assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
    }
    for index in 256..512 {
        let id = format!("{index:032x}");
        state.status(&[(&id, GoalStatus::Succeeded as u8)]);
        client.recv_status(WAIT).unwrap();
        client.get_result(&id, WAIT).unwrap();
    }
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
}

#[test]
fn clones_same_uuid_and_independent_provider_snapshots_preserve_live_observations() {
    let (state, _context, node, interface) = fixture();
    let client =
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap();
    let first_id = "1".repeat(32);
    let second_id = "2".repeat(32);
    let first = client.send_goal(&json!({}), Some(&first_id), WAIT).unwrap();
    let clone = first.clone();
    let same_uuid = client.send_goal(&json!({}), Some(&first_id), WAIT).unwrap();
    let second = client
        .send_goal(&json!({}), Some(&second_id), WAIT)
        .unwrap();
    state.status(&[(&first_id, 2)]);
    client.recv_status(WAIT).unwrap();
    state.status(&[(&second_id, 3)]);
    client.recv_status(WAIT).unwrap();
    assert_eq!(first.status(), 2);
    assert_eq!(clone.status(), 2);
    assert_eq!(same_uuid.status(), 2);
    assert_eq!(second.status(), 3);
    drop(first);
    drop(clone);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 2);
    same_uuid.get_result(WAIT).unwrap();
    assert_eq!(same_uuid.status(), 4);
    drop(same_uuid);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 1);
    let fresh = client.send_goal(&json!({}), Some(&first_id), WAIT).unwrap();
    assert_eq!(fresh.status(), GoalStatus::Unknown as u8);
    drop(fresh);
    drop(second);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
}

#[test]
fn status_arriving_before_send_reply_reaches_the_returned_goal_handle() {
    let (state, _context, node, interface) = fixture();
    let client = Arc::new(
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap(),
    );
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    *state.send_hook.lock().unwrap() = Some(Arc::new(move |_| {
        entered_tx.send(()).unwrap();
        release_rx
            .lock()
            .unwrap()
            .recv_timeout(WAIT)
            .map_err(|_| Error::Timeout)
    }));
    let id = "3".repeat(32);
    let worker_client = client.clone();
    let worker_id = id.clone();
    let worker =
        std::thread::spawn(move || worker_client.send_goal(&json!({}), Some(&worker_id), WAIT));
    entered_rx.recv_timeout(WAIT).unwrap();
    state.status(&[(&id, 2)]);
    client.recv_status(WAIT).unwrap();
    release_tx.send(()).unwrap();
    let goal = worker.join().unwrap().unwrap();
    assert_eq!(goal.status(), 2);
    drop(goal);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
}

#[test]
fn failed_send_and_explicit_close_do_not_forge_or_leak_status() {
    let (state, _context, node, interface) = fixture();
    let client =
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap();
    *state.send_error.lock().unwrap() = Some(Error::Timeout);
    assert!(matches!(
        client.send_goal(&json!({}), None, WAIT),
        Err(Error::Timeout)
    ));
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
    *state.send_error.lock().unwrap() = None;
    let goal = client.send_goal(&json!({}), None, WAIT).unwrap();
    state.status(&[(goal.goal_id(), 2)]);
    client.recv_status(WAIT).unwrap();
    client.close().unwrap();
    assert_eq!(goal.status(), 2);
    drop(client);
    assert_eq!(goal.status(), GoalStatus::Unknown as u8);
}

#[test]
fn failed_send_for_a_live_uuid_preserves_the_existing_observation() {
    let (state, _context, node, interface) = fixture();
    let client =
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap();
    let goal = client.send_goal(&json!({}), None, WAIT).unwrap();
    state.status(&[(goal.goal_id(), 2)]);
    client.recv_status(WAIT).unwrap();
    *state.send_error.lock().unwrap() = Some(Error::Timeout);
    assert!(matches!(
        client.send_goal(&json!({}), Some(goal.goal_id()), WAIT),
        Err(Error::Timeout)
    ));
    assert_eq!(goal.status(), 2);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 1);
    drop(goal);
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
}

#[test]
fn racing_last_drop_and_same_uuid_sends_leave_no_dead_index_entries() {
    let (state, _context, node, interface) = fixture();
    let client =
        ActionClient::new(&node, &interface, "/resources", EndpointOptions::default()).unwrap();
    let id = "4".repeat(32);
    std::thread::scope(|scope| {
        let client = &client;
        let id = &id;
        scope.spawn(move || {
            for _ in 0..256 {
                drop(client.send_goal(&json!({}), Some(id), WAIT).unwrap());
            }
        });
        scope.spawn(move || {
            for _ in 0..256 {
                let goal = client.send_goal(&json!({}), Some(id), WAIT).unwrap();
                state.status(&[(id, 2)]);
                client.recv_status(WAIT).unwrap();
                assert_eq!(goal.status(), 2);
                drop(goal);
            }
        });
    });
    assert_eq!(client.inner.observed.lock().unwrap().len(), 0);
}
