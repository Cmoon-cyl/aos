//
// Copyright (c) 2026 AOS contributors
//
// SPDX-License-Identifier: Apache-2.0
//

//! Per-destination FIFO for routing declarations.
//!
//! Producers enqueue while holding the routing control lock. Begin a batch
//! before acquiring routing locks and drain it after releasing them. A batch
//! may claim only destinations it touched; it never steals unrelated work.
//! The short queue mutex is never held while sending or dropping a destination.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::Rc,
    sync::{Arc, Mutex, MutexGuard},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

struct Lane<K: ?Sized, T> {
    destination: Arc<K>,
    pending: VecDeque<T>,
    owner_since: Option<Instant>,
    high_water: usize,
}

struct Retired<K: ?Sized, T> {
    // Keep both payloads and the destination alive until routing locks and the
    // queue mutex have been released. Their destructors may call other code.
    _destination: Arc<K>,
    _pending: VecDeque<T>,
}

struct Batch<K: ?Sized, T> {
    background: bool,
    destinations: Vec<Arc<K>>,
    seen: HashSet<usize>,
    retired: Vec<Retired<K, T>>,
}

impl<K: ?Sized, T> Default for Batch<K, T> {
    fn default() -> Self {
        Self {
            background: false,
            destinations: Vec::new(),
            seen: HashSet::new(),
            retired: Vec::new(),
        }
    }
}

struct State<K: ?Sized, T> {
    lanes: HashMap<usize, Lane<K, T>>,
    // A synchronous production scope never crosses an await. The guard is
    // !Send; nested calls on the same thread get independent touched sets.
    batches: HashMap<ThreadId, Vec<Batch<K, T>>>,
    pending: usize,
    high_water: usize,
    last_warning: Option<Instant>,
}

#[derive(Clone, Copy)]
struct WarningPolicy {
    pending: usize,
    owner_age: Duration,
    interval: Duration,
}

impl Default for WarningPolicy {
    fn default() -> Self {
        Self {
            pending: 1_024,
            owner_age: Duration::from_secs(1),
            interval: Duration::from_secs(30),
        }
    }
}

/// Production counters: these are not removed in non-test builds.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueWarning {
    pub(crate) destination: usize,
    pub(crate) lane_pending: usize,
    pub(crate) lane_high_water: usize,
    pub(crate) owner_age: Duration,
    pub(crate) total_pending: usize,
    pub(crate) total_high_water: usize,
}

pub(crate) struct DeclareOutbox<K: ?Sized, T> {
    state: Mutex<State<K, T>>,
    warning_policy: WarningPolicy,
    #[cfg(test)]
    batch_starts: std::sync::atomic::AtomicUsize,
}

impl<K: ?Sized, T> Default for DeclareOutbox<K, T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                lanes: HashMap::new(),
                batches: HashMap::new(),
                pending: 0,
                high_water: 0,
                last_warning: None,
            }),
            warning_policy: WarningPolicy::default(),
            #[cfg(test)]
            batch_starts: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<K: ?Sized, T> DeclareOutbox<K, T> {
    fn lock(&self) -> MutexGuard<'_, State<K, T>> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn begin_batch(&self) -> DeclarationBatch<'_, K, T> {
        self.begin_batch_with_mode(false)
    }

    pub(crate) fn begin_background_batch(&self) -> DeclarationBatch<'_, K, T> {
        self.begin_batch_with_mode(true)
    }

    fn begin_batch_with_mode(&self, background: bool) -> DeclarationBatch<'_, K, T> {
        #[cfg(test)]
        self.batch_starts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let thread = thread::current().id();
        let mut state = self.lock();
        let stack = state.batches.entry(thread).or_default();
        // Transport scopes can enter ordinary Face producers synchronously.
        // Every nested batch inherits their callback-panic isolation boundary.
        let background = background || stack.last().is_some_and(|batch| batch.background);
        stack.push(Batch {
            background,
            ..Batch::default()
        });
        DeclarationBatch {
            outbox: self,
            thread,
            active: true,
            background,
            _not_send: PhantomData,
        }
    }

    /// Generation and enqueue must share the same routing critical section.
    /// The batch retains each touched destination Arc, so even after close an
    /// old batch cannot accidentally claim an unrelated reused address.
    pub(crate) fn enqueue(&self, destination: &Arc<K>, item: T) -> Option<QueueWarning> {
        let id = Arc::as_ptr(destination) as *const () as usize;
        let mut state = self.lock();
        let batch = state
            .batches
            .get_mut(&thread::current().id())
            .and_then(|stack| stack.last_mut())
            .expect("routing declarations require an active production batch");
        if batch.seen.insert(id) {
            batch.destinations.push(destination.clone());
        }
        let lane = state.lanes.entry(id).or_insert_with(|| Lane {
            destination: destination.clone(),
            pending: VecDeque::new(),
            owner_since: None,
            high_water: 0,
        });
        lane.pending.push_back(item);
        lane.high_water = lane.high_water.max(lane.pending.len());
        state.pending += 1;
        state.high_water = state.high_water.max(state.pending);
        self.warning(&mut state, id)
    }

    /// Called after removing a destination face under the control lock. Queued
    /// delivery to this closed face is useless. Its in-flight callback may
    /// finish; no pending item is retained behind it. Drops happen at batch end.
    pub(crate) fn discard(&self, destination: &Arc<K>) {
        let id = Arc::as_ptr(destination) as *const () as usize;
        let mut state = self.lock();
        let Some(lane) = state.lanes.get_mut(&id) else {
            return;
        };
        let pending = std::mem::take(&mut lane.pending);
        let count = pending.len();
        let retired = Retired {
            _destination: lane.destination.clone(),
            _pending: pending,
        };
        if lane.owner_since.is_none() {
            // The retired value and the production batch still retain Arcs.
            state.lanes.remove(&id);
        }
        state.pending -= count;
        state
            .batches
            .get_mut(&thread::current().id())
            .and_then(|stack| stack.last_mut())
            .expect("discarding a face requires an active production batch")
            .retired
            .push(retired);
    }

    fn warning(&self, state: &mut State<K, T>, id: usize) -> Option<QueueWarning> {
        let lane = state.lanes.get_mut(&id)?;
        let now = Instant::now();
        let owner_age = lane
            .owner_since
            .map(|start| now.duration_since(start))
            .unwrap_or_default();
        if (lane.pending.len() < self.warning_policy.pending
            && !(lane.owner_since.is_some() && owner_age >= self.warning_policy.owner_age))
            || state
                .last_warning
                .is_some_and(|last| now.duration_since(last) < self.warning_policy.interval)
        {
            return None;
        }
        // Rate-limit across this outbox, including repeated creation/removal
        // of the same lane. No per-closed-destination tombstones are retained.
        state.last_warning = Some(now);
        Some(QueueWarning {
            destination: id,
            lane_pending: lane.pending.len(),
            lane_high_water: lane.high_water,
            owner_age,
            total_pending: state.pending,
            total_high_water: state.high_water,
        })
    }

    /// Never waits for another owner or claims a lane absent from this batch.
    /// A busy lane is owned until empty. Enqueue, empty check, owner release
    /// and removal share one mutex, preventing lost wakeups and split owners.
    fn drain(
        &self,
        destinations: Vec<Arc<K>>,
        mut send: impl FnMut(&Arc<K>, T),
        mut report: impl FnMut(&Arc<K>, QueueWarning),
    ) {
        let mut first_panic = None;
        for keep_alive in destinations {
            let id = Arc::as_ptr(&keep_alive) as *const () as usize;
            {
                let mut state = self.lock();
                let Some(lane) = state.lanes.get_mut(&id) else {
                    continue;
                };
                if lane.owner_since.is_some() {
                    continue;
                }
                lane.owner_since = Some(Instant::now());
            }
            let mut owner = LaneOwner {
                outbox: self,
                destination: id,
                keep_alive,
                active: true,
            };
            loop {
                let (item, warning) = {
                    let mut state = self.lock();
                    let warning = self.warning(&mut state, id);
                    let lane = state
                        .lanes
                        .get_mut(&id)
                        .expect("a draining lane remains indexed");
                    let item = lane.pending.pop_front();
                    if item.is_some() {
                        state.pending -= 1;
                    } else {
                        owner.active = false;
                        state.lanes.remove(&id);
                    }
                    (item, warning)
                };
                if let Some(warning) = warning {
                    if let Err(panic) =
                        catch_unwind(AssertUnwindSafe(|| report(&owner.keep_alive, warning)))
                    {
                        // A diagnostic sink must not abandon the popped item
                        // or any other destination touched by this producer.
                        if first_panic.is_none() {
                            first_panic = Some(panic);
                        }
                    }
                }
                let Some(item) = item else { break };
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| send(&owner.keep_alive, item)))
                {
                    // Finish this batch's FIFO work before restoring the first
                    // panic. Unrelated producers' lanes are never visited.
                    if first_panic.is_none() {
                        first_panic = Some(panic)
                    }
                }
            }
        }
        if let Some(panic) = first_panic {
            resume_unwind(panic)
        }
    }

    #[cfg(test)]
    fn counts(&self) -> (usize, usize, usize) {
        let state = self.lock();
        (state.lanes.len(), state.pending, state.high_water)
    }

    #[cfg(test)]
    pub(crate) fn batch_starts(&self) -> usize {
        self.batch_starts.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub(crate) struct DeclarationBatch<'a, K: ?Sized, T> {
    outbox: &'a DeclareOutbox<K, T>,
    thread: ThreadId,
    active: bool,
    background: bool,
    _not_send: PhantomData<Rc<()>>,
}

impl<K: ?Sized, T> DeclarationBatch<'_, K, T> {
    fn finish(&mut self) -> Batch<K, T> {
        self.active = false;
        let mut state = self.outbox.lock();
        let stack = state
            .batches
            .get_mut(&self.thread)
            .expect("an active batch remains registered");
        let batch = stack
            .pop()
            .expect("production batches finish in reverse order");
        if stack.is_empty() {
            state.batches.remove(&self.thread);
        }
        batch
    }

    /// Pop this production scope before calling user code, so nested scopes
    /// cannot add destinations to the already running outer drainer.
    pub(crate) fn drain(
        mut self,
        send: impl FnMut(&Arc<K>, T),
        report: impl FnMut(&Arc<K>, QueueWarning),
    ) {
        let Batch {
            destinations,
            retired,
            ..
        } = self.finish();
        drop(retired);
        self.outbox.drain(destinations, send, report);
    }

    /// Background callers report the panic and keep their worker alive. The
    /// ordinary drain still finishes touched FIFO work and releases owners.
    pub(crate) fn drain_background(
        self,
        send: impl FnMut(&Arc<K>, T),
        report: impl FnMut(&Arc<K>, QueueWarning),
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        catch_unwind(AssertUnwindSafe(|| self.drain(send, report)))
    }

    /// Production Drop guards must dispatch even while a caller is unwinding.
    /// Suppress a second callback panic in that case, preserving the original
    /// panic and the invariant that every generated idle lane has a drainer.
    pub(crate) fn drain_in_scope(
        self,
        send: impl FnMut(&Arc<K>, T),
        report: impl FnMut(&Arc<K>, QueueWarning),
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        if self.background || thread::panicking() {
            self.drain_background(send, report)
        } else {
            self.drain(send, report);
            Ok(())
        }
    }
}

impl<K: ?Sized, T> Drop for DeclarationBatch<'_, K, T> {
    fn drop(&mut self) {
        if self.active {
            // This fallback only removes an abandoned generic scope. The
            // production Drop guard always calls drain_in_scope, even during
            // unwinding. Retired values and Arcs drop outside the queue mutex.
            drop(self.finish());
        }
    }
}

struct LaneOwner<'a, K: ?Sized, T> {
    outbox: &'a DeclareOutbox<K, T>,
    destination: usize,
    keep_alive: Arc<K>,
    active: bool,
}

impl<K: ?Sized, T> Drop for LaneOwner<'_, K, T> {
    fn drop(&mut self) {
        if self.active {
            if let Some(lane) = self.outbox.lock().lanes.get_mut(&self.destination) {
                lane.owner_since = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::{mpsc, Arc, Mutex},
        thread,
        time::Duration,
    };

    use super::{DeclareOutbox, WarningPolicy};
    const LIMIT: Duration = Duration::from_secs(5);

    fn unwind_dispatch(callback_panics: bool) {
        struct EnqueueOnDrop<'a> {
            outbox: &'a DeclareOutbox<(), usize>,
            lane: &'a Arc<()>,
            delivered: &'a Mutex<Vec<usize>>,
            callback_panics: bool,
        }
        impl Drop for EnqueueOnDrop<'_> {
            fn drop(&mut self) {
                assert!(thread::panicking());
                let batch = self.outbox.begin_batch();
                for item in [1, 2, 3] {
                    let _ = self.outbox.enqueue(self.lane, item);
                }
                let result = batch.drain_in_scope(
                    |_, item| {
                        self.delivered.lock().unwrap().push(item);
                        if self.callback_panics && item == 1 {
                            panic!("secondary callback panic")
                        }
                    },
                    |_, _| {},
                );
                assert_eq!(result.is_err(), self.callback_panics);
            }
        }
        let outbox = DeclareOutbox::default();
        let lane = Arc::new(());
        let delivered = Mutex::new(Vec::new());
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = EnqueueOnDrop {
                outbox: &outbox,
                lane: &lane,
                delivered: &delivered,
                callback_panics,
            };
            panic!("original application panic")
        }));
        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"original application panic")
        );
        assert_eq!(*delivered.lock().unwrap(), [1, 2, 3]);
        // No later declaration is allowed to rescue an abandoned lane.
        assert_eq!(outbox.counts(), (0, 0, 3));
        assert!(outbox.lock().batches.is_empty());
    }

    #[test]
    fn unwind_drop_dispatches_its_generated_batch_without_later_producers() {
        unwind_dispatch(false);
    }

    #[test]
    fn unwind_drop_catches_a_second_callback_panic_and_preserves_the_original() {
        unwind_dispatch(true);
    }

    #[test]
    fn nested_face_batch_inherits_background_panic_isolation() {
        let outbox = DeclareOutbox::default();
        let lane = Arc::new(());
        let outer = outbox.begin_background_batch();
        let nested = outbox.begin_batch();
        let _ = outbox.enqueue(&lane, 1);
        assert!(nested
            .drain_in_scope(|_, _| panic!("transport callback panic"), |_, _| {})
            .is_err());
        outer
            .drain_in_scope(|_, _| unreachable!(), |_, _| {})
            .unwrap();
        assert_eq!(outbox.counts(), (0, 0, 1));
        // Isolation ends with the transport scope; ordinary callers still
        // receive their callback panic after all touched work is dispatched.
        let ordinary = outbox.begin_batch();
        let _ = outbox.enqueue(&lane, 2);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = ordinary.drain_in_scope(|_, _| panic!("ordinary callback panic"), |_, _| {});
        }));
        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"ordinary callback panic")
        );
        assert_eq!(outbox.counts(), (0, 0, 1));
        assert_eq!(outbox.batch_starts(), 3);
    }

    #[test]
    fn reentry_enqueues_behind_pending_items_without_recursive_delivery() {
        let outbox = DeclareOutbox::default();
        let lane = Arc::new(());
        let events = Mutex::new(Vec::new());
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&lane, 1);
        let _ = outbox.enqueue(&lane, 2);
        batch.drain(
            |_, item| {
                events.lock().unwrap().push((item, "begin"));
                if item == 1 {
                    let nested = outbox.begin_batch();
                    let _ = outbox.enqueue(&lane, 3);
                    nested.drain(
                        |_, _| panic!("a busy lane must not be drained recursively"),
                        |_, _| {},
                    );
                }
                events.lock().unwrap().push((item, "end"));
            },
            |_, _| {},
        );
        assert_eq!(
            *events.lock().unwrap(),
            [
                (1, "begin"),
                (1, "end"),
                (2, "begin"),
                (2, "end"),
                (3, "begin"),
                (3, "end")
            ]
        );
        assert_eq!(outbox.counts(), (0, 0, 2));
    }

    #[test]
    fn an_independent_lane_progresses_while_another_callback_is_blocked() {
        let outbox = Arc::new(DeclareOutbox::default());
        let first = Arc::new(());
        let second = Arc::new(());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocked = outbox.clone();
        let worker = thread::spawn(move || {
            let batch = blocked.begin_batch();
            let _ = blocked.enqueue(&first, 1);
            batch.drain(
                |_, item| {
                    assert_eq!(item, 1);
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(LIMIT).unwrap();
                },
                |_, _| {},
            );
        });
        entered_rx.recv_timeout(LIMIT).unwrap();
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&second, 2);
        let mut delivered = vec![];
        batch.drain(|_, item| delivered.push(item), |_, _| {});
        assert_eq!(delivered, [2]);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(outbox.counts(), (0, 0, 1));
    }

    #[test]
    fn a_producer_never_claims_an_unrelated_pending_lane() {
        let outbox = Arc::new(DeclareOutbox::default());
        let first = Arc::new(());
        let second = Arc::new(());
        let (queued_tx, queued_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let parked = outbox.clone();
        let worker = thread::spawn(move || {
            let batch = parked.begin_batch();
            let _ = parked.enqueue(&first, 1);
            queued_tx.send(()).unwrap();
            // A real scheduling gap after routing unlock and before drain.
            release_rx.recv_timeout(LIMIT).unwrap();
            let mut delivered = vec![];
            batch.drain(|_, item| delivered.push(item), |_, _| {});
            assert_eq!(delivered, [1]);
        });
        queued_rx.recv_timeout(LIMIT).unwrap();
        outbox.begin_batch().drain(
            |_, _| panic!("an empty producer must never steal work"),
            |_, _| {},
        );
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&second, 2);
        let mut delivered = vec![];
        batch.drain(|_, item| delivered.push(item), |_, _| {});
        assert_eq!(delivered, [2]);
        assert_eq!(outbox.counts(), (1, 1, 2));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(outbox.counts(), (0, 0, 2));
    }

    #[test]
    fn panic_finishes_touched_lanes_and_restores_the_first_panic_without_a_stuck_owner() {
        let outbox = DeclareOutbox::default();
        let lane = Arc::new(());
        let batch = outbox.begin_batch();
        for item in [1, 2, 3] {
            let _ = outbox.enqueue(&lane, item);
        }
        let mut delivered = vec![];
        let result = catch_unwind(AssertUnwindSafe(|| {
            batch.drain(
                |_, item| {
                    delivered.push(item);
                    if item == 1 {
                        panic!("first callback panic")
                    }
                    if item == 2 {
                        panic!("second callback panic")
                    }
                },
                |_, _| {},
            );
        }));
        assert_eq!(delivered, [1, 2, 3]);
        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"first callback panic")
        );
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&lane, 4);
        batch.drain(|_, item| delivered.push(item), |_, _| {});
        assert_eq!(delivered, [1, 2, 3, 4]);
        assert_eq!(outbox.counts(), (0, 0, 3));
    }

    #[test]
    fn diagnostic_panic_delivers_the_popped_item_and_remaining_destinations() {
        let outbox = DeclareOutbox {
            warning_policy: WarningPolicy {
                owner_age: Duration::ZERO,
                ..WarningPolicy::default()
            },
            ..DeclareOutbox::default()
        };
        let first = Arc::new(());
        let second = Arc::new(());
        let batch = outbox.begin_batch();
        for item in [1, 2] {
            let _ = outbox.enqueue(&first, item);
        }
        let _ = outbox.enqueue(&second, 3);
        let mut delivered = Vec::new();
        let result = catch_unwind(AssertUnwindSafe(|| {
            batch.drain(
                |_, item| delivered.push(item),
                |_, _| panic!("diagnostic sink panic"),
            );
        }));
        assert_eq!(delivered, [1, 2, 3]);
        assert_eq!(
            result.unwrap_err().downcast_ref::<&str>(),
            Some(&"diagnostic sink panic")
        );
        assert_eq!(outbox.counts(), (0, 0, 3));
        let next = outbox.begin_batch();
        let _ = outbox.enqueue(&first, 4);
        next.drain(|_, item| delivered.push(item), |_, _| {});
        assert_eq!(delivered, [1, 2, 3, 4]);
        assert_eq!(outbox.counts(), (0, 0, 3));
    }

    #[test]
    fn background_dispatch_isolates_panic_and_allows_the_next_worker_iteration() {
        let outbox = DeclareOutbox::default();
        let lane = Arc::new(());
        let mut delivered = vec![];
        let mut panics = 0;
        for iteration in 1..=2 {
            let batch = outbox.begin_batch();
            let _ = outbox.enqueue(&lane, iteration);
            if batch
                .drain_background(
                    |_, item| {
                        delivered.push(item);
                        if item == 1 {
                            panic!("callback panic in background worker")
                        }
                    },
                    |_, _| {},
                )
                .is_err()
            {
                panics += 1
            }
        }
        assert_eq!(delivered, [1, 2]);
        assert_eq!(panics, 1);
        assert_eq!(outbox.counts(), (0, 0, 1));
    }

    #[test]
    fn an_empty_lane_releases_its_destination_after_the_inflight_send() {
        let outbox = DeclareOutbox::default();
        let destination = Arc::new(());
        let weak = Arc::downgrade(&destination);
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&destination, ());
        drop(destination);
        assert!(weak.upgrade().is_some());
        batch.drain(|_, ()| assert!(weak.upgrade().is_some()), |_, _| {});
        assert!(weak.upgrade().is_none());
        assert_eq!(outbox.counts(), (0, 0, 1));
    }

    #[test]
    fn a_concurrent_enqueue_and_reentry_survive_the_active_callback_panicking() {
        let outbox = Arc::new(DeclareOutbox::default());
        let destination = Arc::new(());
        let received = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let draining = outbox.clone();
        let recorded = received.clone();
        let lane = destination.clone();
        let worker = thread::spawn(move || {
            let batch = draining.begin_batch();
            let _ = draining.enqueue(&lane, 1);
            catch_unwind(AssertUnwindSafe(|| {
                batch.drain(
                    |_, item| {
                        recorded.lock().unwrap().push(item);
                        if item == 1 {
                            entered_tx.send(()).unwrap();
                            release_rx.recv_timeout(LIMIT).unwrap();
                            let nested = draining.begin_batch();
                            let _ = draining.enqueue(&lane, 3);
                            nested.drain(
                                |_, _| panic!("reentry must not take the active lane"),
                                |_, _| {},
                            );
                            panic!("active callback panic");
                        }
                    },
                    |_, _| {},
                );
            }))
        });
        entered_rx.recv_timeout(LIMIT).unwrap();
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&destination, 2);
        batch.drain(
            |_, _| panic!("the first callback still owns this lane"),
            |_, _| {},
        );
        release_tx.send(()).unwrap();
        assert!(worker.join().unwrap().is_err());
        assert_eq!(*received.lock().unwrap(), [1, 2, 3]);
        assert_eq!(outbox.counts(), (0, 0, 2));
    }

    #[test]
    fn closing_a_busy_destination_drops_pending_items_after_unlock() {
        struct Pending(mpsc::Sender<()>);
        impl Drop for Pending {
            fn drop(&mut self) {
                self.0.send(()).unwrap();
            }
        }
        let outbox = Arc::new(DeclareOutbox::default());
        let destination = Arc::new(());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let draining = outbox.clone();
        let lane = destination.clone();
        let worker = thread::spawn(move || {
            let batch = draining.begin_batch();
            let _ = draining.enqueue(&lane, None::<Pending>);
            batch.drain(
                |_, item| {
                    assert!(
                        item.is_none(),
                        "closed destinations must not receive pending work"
                    );
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(LIMIT).unwrap();
                },
                |_, _| {},
            );
        });
        entered_rx.recv_timeout(LIMIT).unwrap();
        let batch = outbox.begin_batch();
        let _ = outbox.enqueue(&destination, Some(Pending(dropped_tx)));
        outbox.discard(&destination);
        assert!(
            dropped_rx.try_recv().is_err(),
            "payload drops must be deferred past routing unlock"
        );
        assert_eq!(outbox.counts(), (1, 0, 1));
        batch.drain(|_, _| panic!("closed queue is empty"), |_, _| {});
        dropped_rx.recv_timeout(LIMIT).unwrap();
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(outbox.counts(), (0, 0, 1));
    }

    #[test]
    fn a_retired_idle_lane_cannot_be_stolen_or_retain_its_destination() {
        let outbox = DeclareOutbox::default();
        let destination = Arc::new(());
        let weak = Arc::downgrade(&destination);
        let pending = outbox.begin_batch();
        let _ = outbox.enqueue(&destination, 1);
        let closing = outbox.begin_batch();
        outbox.discard(&destination);
        closing.drain(
            |_, _| panic!("a close batch cannot revive retired work"),
            |_, _| {},
        );
        drop(destination);
        assert!(
            weak.upgrade().is_some(),
            "the original batch prevents address reuse"
        );
        pending.drain(
            |_, _| panic!("the original batch sees an absent lane"),
            |_, _| {},
        );
        assert!(weak.upgrade().is_none());
        assert_eq!(outbox.counts(), (0, 0, 1));
    }

    #[test]
    fn production_counters_warn_at_backlog_threshold_and_rate_limit() {
        let outbox = DeclareOutbox {
            warning_policy: WarningPolicy {
                pending: 2,
                ..WarningPolicy::default()
            },
            ..DeclareOutbox::default()
        };
        let destination = Arc::new(());
        let batch = outbox.begin_batch();
        assert!(outbox.enqueue(&destination, 1).is_none());
        let warning = outbox.enqueue(&destination, 2).unwrap();
        assert_eq!((warning.lane_pending, warning.lane_high_water), (2, 2));
        assert_eq!((warning.total_pending, warning.total_high_water), (2, 2));
        assert_eq!(warning.owner_age, Duration::ZERO);
        assert_eq!(warning.destination, Arc::as_ptr(&destination) as usize);
        assert!(
            outbox.enqueue(&destination, 3).is_none(),
            "warnings are rate limited across the outbox"
        );
        batch.drain(|_, _| {}, |_, _| panic!("rate limit also covers draining"));
        assert_eq!(outbox.counts(), (0, 0, 3));
        let recreated = outbox.begin_batch();
        for item in [4, 5] {
            assert!(
                outbox.enqueue(&destination, item).is_none(),
                "removing and recreating a lane must not reset the warning interval"
            );
        }
        recreated.drain(
            |_, _| {},
            |_, _| panic!("recreated lanes share the warning interval"),
        );
        assert_eq!(outbox.counts(), (0, 0, 3));
    }

    #[test]
    fn owner_age_is_reported_by_production_observability() {
        let outbox = DeclareOutbox {
            warning_policy: WarningPolicy {
                owner_age: Duration::ZERO,
                ..WarningPolicy::default()
            },
            ..DeclareOutbox::default()
        };
        let destination = Arc::new(());
        let batch = outbox.begin_batch();
        assert!(outbox.enqueue(&destination, 1).is_none());
        let mut notices = vec![];
        batch.drain(|_, _| {}, |_, notice| notices.push(notice));
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].lane_pending, 1);
    }

    #[test]
    fn concurrent_producers_converge_after_a_blocked_callback_and_report_high_water() {
        let outbox = Arc::new(DeclareOutbox::default());
        let lane = Arc::new(());
        let received = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let draining = outbox.clone();
        let recorded = received.clone();
        let first_lane = lane.clone();
        let worker = thread::spawn(move || {
            let batch = draining.begin_batch();
            let _ = draining.enqueue(&first_lane, (usize::MAX, 0));
            batch.drain(
                |_, item| {
                    if item.0 == usize::MAX {
                        entered_tx.send(()).unwrap();
                        release_rx.recv_timeout(LIMIT).unwrap();
                    } else {
                        recorded.lock().unwrap().push(item);
                    }
                },
                |_, _| {},
            );
        });
        entered_rx.recv_timeout(LIMIT).unwrap();
        let control = Mutex::new(());
        let expected = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for producer in 0..4 {
                let (outbox, control, expected, lane) = (&outbox, &control, &expected, &lane);
                scope.spawn(move || {
                    for sequence in 0..2_000 {
                        let batch = outbox.begin_batch();
                        {
                            let _control = control.lock().unwrap();
                            let _ = outbox.enqueue(lane, (producer, sequence));
                            expected.lock().unwrap().push((producer, sequence));
                        }
                        batch.drain(
                            |_, _| panic!("the other thread still owns this lane"),
                            |_, _| {},
                        );
                    }
                });
            }
        });
        assert_eq!(outbox.counts(), (1, 8_000, 8_000));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(*received.lock().unwrap(), *expected.lock().unwrap());
        assert_eq!(outbox.counts(), (0, 0, 8_000));
        eprintln!("routing_outbox pressure: high_water=8000 pending=0 lanes=0 delivered=8000");
    }
}
