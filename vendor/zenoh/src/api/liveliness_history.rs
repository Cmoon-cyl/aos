// Copyright (c) 2026 AOS contributors
// SPDX-License-Identifier: Apache-2.0
//
// Local fix for Zenoh 1.10.1 history/live callback ordering. See
// upstream/zenoh-history-gate.md in the AOS source distribution.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

enum State<T> {
    // Live events received after the session's history snapshot are held
    // behind that snapshot until the replay and this queue have both drained.
    Replaying(VecDeque<T>),
    Live,
    Closed,
}

pub(crate) struct HistoryGate<T> {
    state: Mutex<State<T>>,
    callback: Box<dyn Fn(T) + Send + Sync>,
}

pub(crate) struct HistoryReplay<T> {
    gate: Arc<HistoryGate<T>>,
    completed: bool,
}

impl<T> HistoryGate<T> {
    pub(crate) fn new(
        callback: impl Fn(T) + Send + Sync + 'static,
    ) -> (Arc<Self>, HistoryReplay<T>) {
        let gate = Arc::new(Self {
            state: Mutex::new(State::Replaying(VecDeque::new())),
            callback: Box::new(callback),
        });
        let replay = HistoryReplay {
            gate: gate.clone(),
            completed: false,
        };
        (gate, replay)
    }

    pub(crate) fn push(&self, sample: T) {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            State::Replaying(pending) => pending.push_back(sample),
            State::Live => {
                drop(state);
                // User code never runs while either the gate or session lock
                // is held. Normal live callbacks retain their existing policy.
                (self.callback)(sample);
            }
            State::Closed => {}
        }
    }

    pub(crate) fn close(&self) {
        let previous = {
            let mut state = self.state.lock().unwrap();
            std::mem::replace(&mut *state, State::Closed)
        };
        // Release queued samples outside the gate lock.
        drop(previous);
    }

    fn recover_aborted_replay(&self) {
        let previous = {
            let mut state = self.state.lock().unwrap();
            if matches!(*state, State::Replaying(_)) {
                Some(std::mem::replace(&mut *state, State::Live))
            } else {
                // Explicit subscriber/session closure must remain closed.
                None
            }
        };
        // A callback panic makes the incomplete replay unusable. Discard its
        // remaining queue outside the lock, but allow later live events.
        drop(previous);
    }
}

impl<T> HistoryReplay<T> {
    pub(crate) fn finish(mut self, history: impl IntoIterator<Item = T>) {
        for sample in history {
            if matches!(*self.gate.state.lock().unwrap(), State::Closed) {
                return;
            }
            (self.gate.callback)(sample);
        }
        self.drain_pending();
    }

    fn drain_pending(&mut self) {
        loop {
            let sample = {
                let mut state = self.gate.state.lock().unwrap();
                match &mut *state {
                    State::Replaying(pending) => match pending.pop_front() {
                        Some(sample) => sample,
                        None => {
                            *state = State::Live;
                            self.completed = true;
                            return;
                        }
                    },
                    State::Closed => return,
                    State::Live => unreachable!("one replay owner"),
                }
            };
            (self.gate.callback)(sample);
        }
    }
}

impl<T> Drop for HistoryReplay<T> {
    fn drop(&mut self) {
        if !self.completed {
            // The recovery guard also covers a panic while draining during
            // normal Drop. Never invoke user code while already unwinding.
            let recovery = ReplayRecovery(self.gate.clone());
            if !std::thread::panicking() {
                self.drain_pending();
            }
            drop(recovery);
        }
    }
}

struct ReplayRecovery<T>(Arc<HistoryGate<T>>);

impl<T> Drop for ReplayRecovery<T> {
    fn drop(&mut self) {
        self.0.recover_aborted_replay();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Barrier, Weak};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn history_precedes_live_delete_with_a_deterministic_barrier() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = events.clone();
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        let inside = entered.clone();
        let resume = proceed.clone();
        let (gate, replay) = HistoryGate::new(move |event| {
            if event == "history_put" {
                inside.wait();
                resume.wait();
            }
            recorded.lock().unwrap().push(event);
        });
        let worker = thread::spawn(move || replay.finish(["history_put"]));
        entered.wait();
        gate.push("live_delete");
        gate.push("live_put_same_key");
        assert!(events.lock().unwrap().is_empty());
        proceed.wait();
        worker.join().unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            ["history_put", "live_delete", "live_put_same_key"]
        );
        gate.push("live_after_replay");
        assert_eq!(events.lock().unwrap().last(), Some(&"live_after_replay"));
    }

    #[test]
    fn callback_can_reenter_without_a_gate_lock() {
        let owner: Arc<Mutex<Weak<HistoryGate<u8>>>> = Arc::default();
        let inside = owner.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = events.clone();
        let (gate, replay) = HistoryGate::new(move |event| {
            recorded.lock().unwrap().push(event);
            if event == 1 {
                inside.lock().unwrap().upgrade().unwrap().push(2);
            }
        });
        *owner.lock().unwrap() = Arc::downgrade(&gate);
        replay.finish([1]);
        assert_eq!(*events.lock().unwrap(), [1, 2]);
    }

    #[test]
    fn panic_releases_buffered_payloads_and_allows_future_live_events() {
        let first = std::sync::atomic::AtomicBool::new(true);
        let delivered = Arc::new(Mutex::new(0));
        let observed = delivered.clone();
        let (gate, replay) = HistoryGate::new(move |_: Arc<()>| {
            if first.swap(false, std::sync::atomic::Ordering::Relaxed) {
                panic!("callback failed");
            }
            *observed.lock().unwrap() += 1;
        });
        let payload = Arc::new(());
        gate.push(payload.clone());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            replay.finish([Arc::new(())]);
        }));
        assert!(outcome.is_err());
        assert_eq!(Arc::strong_count(&payload), 1);
        gate.push(payload.clone());
        assert_eq!(Arc::strong_count(&payload), 1);
        assert_eq!(*delivered.lock().unwrap(), 1);
        assert!(matches!(*gate.state.lock().unwrap(), State::Live));
    }

    #[test]
    fn dropping_an_unpolled_replay_drains_pending_samples_and_opens_gate() {
        let delivered = Arc::new(Mutex::new(0));
        let observed = delivered.clone();
        let (gate, replay) = HistoryGate::new(move |_: Arc<()>| *observed.lock().unwrap() += 1);
        let payload = Arc::new(());
        gate.push(payload.clone());
        drop(replay);
        assert_eq!(Arc::strong_count(&payload), 1);
        assert_eq!(*delivered.lock().unwrap(), 1);
        gate.push(payload.clone());
        assert_eq!(Arc::strong_count(&payload), 1);
        assert_eq!(*delivered.lock().unwrap(), 2);
    }

    #[test]
    fn events_arriving_during_drain_stay_in_fifo_order() {
        let owner: Arc<Mutex<Weak<HistoryGate<u8>>>> = Arc::default();
        let inside = owner.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let recorded = events.clone();
        let (gate, replay) = HistoryGate::new(move |event| {
            recorded.lock().unwrap().push(event);
            let gate = inside.lock().unwrap().upgrade().unwrap();
            assert!(
                gate.state.try_lock().is_ok(),
                "callback must not hold gate lock"
            );
            if event == 2 {
                gate.push(4);
            }
        });
        *owner.lock().unwrap() = Arc::downgrade(&gate);
        gate.push(2);
        gate.push(3);
        replay.finish([1]);
        assert_eq!(*events.lock().unwrap(), [1, 2, 3, 4]);
    }

    #[test]
    fn panic_during_drop_drain_also_opens_gate() {
        let first = std::sync::atomic::AtomicBool::new(true);
        let (gate, replay) = HistoryGate::new(move |_: Arc<()>| {
            if first.swap(false, std::sync::atomic::Ordering::Relaxed) {
                panic!("callback failed during cancellation");
            }
        });
        let payload = Arc::new(());
        gate.push(payload.clone());
        gate.push(payload.clone());
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(replay);
        }));
        assert!(outcome.is_err());
        assert_eq!(Arc::strong_count(&payload), 1);
        gate.push(payload.clone());
        assert_eq!(Arc::strong_count(&payload), 1);
        assert!(matches!(*gate.state.lock().unwrap(), State::Live));
    }

    #[test]
    fn close_during_replay_discards_pending_events_without_waiting_for_callback() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let proceed = Arc::new(Barrier::new(2));
        let resume = proceed.clone();
        let (gate, replay) = HistoryGate::new(move |_: Arc<()>| {
            entered_tx.send(()).unwrap();
            resume.wait();
        });
        let worker = thread::spawn(move || replay.finish([Arc::new(()), Arc::new(())]));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let payload = Arc::new(());
        gate.push(payload.clone());
        gate.close();
        assert_eq!(Arc::strong_count(&payload), 1);
        proceed.wait();
        worker.join().unwrap();
        assert!(
            entered_rx.try_recv().is_err(),
            "closed replay must stop before the next callback"
        );
        let weak = Arc::downgrade(&gate);
        drop(gate);
        assert!(weak.upgrade().is_none(), "close must not retain the gate");
    }
}
