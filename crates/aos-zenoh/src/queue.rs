//! Bounded middleware queues. Callbacks enqueue owned data; user code runs elsewhere.
use aos_core::{transport::QueueStats, Error, QueueLimits, Result};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::byte_budget::{ByteBudget, Rejection};

/// Session shutdown wakes Topic, Service, reply and discovery queues uniformly.
pub(crate) trait CloseWake: Send + Sync {
    fn wake_close(&self);
}

/// Keep_last receive queue filled by the Zenoh callback.
///
/// The callback only updates state under a short lock. Evicted items are
/// returned or dropped after unlocking, since dropping a Query can emit a
/// protocol message. No user code runs in the callback. Receivers wait on
/// their own threads; close wakes all of them.
pub(crate) struct Queue<T> {
    state: Mutex<QueueState<T>>,
    ready: Condvar,
    depth: usize,
}

struct QueueState<T> {
    items: VecDeque<Weighted<T>>,
    closed: bool,
    finished: Option<Instant>,
    failure: Option<Error>,
    received_total: u64,
    dropped_total: u64,
    bytes: Option<ByteBudget>,
    reservations: usize,
}

struct Weighted<T> {
    payload: T,
    bytes: usize,
}

struct Reservation<'a, T> {
    queue: &'a Queue<T>,
    bytes: usize,
    active: bool,
}

impl<T> Reservation<'_, T> {
    fn commit(mut self, payload: T) -> std::result::Result<(), Rejection> {
        let mut state = self.queue.lock();
        state.reservations -= 1;
        self.active = false;
        if state.closed {
            state.bytes.as_mut().unwrap().release(self.bytes);
            drop(state);
            drop(payload);
            return Err(Rejection::Closed);
        }
        state.bytes.as_mut().unwrap().commit(self.bytes);
        state.received_total = state.received_total.saturating_add(1);
        state.items.push_back(Weighted {
            payload,
            bytes: self.bytes,
        });
        drop(state);
        self.queue.ready.notify_one();
        Ok(())
    }
}

impl<T> Drop for Reservation<'_, T> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.queue.lock();
            state.reservations -= 1;
            state.bytes.as_mut().unwrap().release(self.bytes);
        }
    }
}

impl<T> Queue<T> {
    pub(crate) fn new(depth: usize) -> Self {
        Self::with_budget(depth, None)
    }

    pub(crate) fn with_limits(depth: usize, limits: QueueLimits) -> Self {
        Self::with_budget(depth, Some(ByteBudget::new(limits)))
    }

    fn with_budget(depth: usize, bytes: Option<ByteBudget>) -> Self {
        assert!(depth > 0);
        Self {
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                closed: false,
                finished: None,
                failure: None,
                received_total: 0,
                dropped_total: 0,
                bytes,
                reservations: 0,
            }),
            ready: Condvar::new(),
            depth,
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn push(&self, payload: T) {
        drop(self.push_returning_evicted(payload));
    }

    /// The caller may reply to an evicted request after the queue lock is released.
    pub(crate) fn push_returning_evicted(&self, payload: T) -> Option<T> {
        let mut state = self.lock();
        assert!(
            state.bytes.is_none(),
            "bounded queues require byte admission"
        );
        if state.closed {
            drop(state);
            return None;
        }
        state.received_total = state.received_total.saturating_add(1);
        let evicted = if state.items.len() == self.depth {
            state.dropped_total = state.dropped_total.saturating_add(1);
            state.items.pop_front().map(|item| item.payload)
        } else {
            None
        };
        state.items.push_back(Weighted { payload, bytes: 0 });
        drop(state);
        self.ready.notify_one();
        evicted
    }

    /// Admit before constructing/copying an owned payload. Evictions are
    /// handled outside the lock and before the new copy. The guard releases
    /// reserved bytes on panic, close, or any other early exit.
    pub(crate) fn try_push_with(
        &self,
        bytes: usize,
        build: impl FnOnce() -> T,
        mut evict: impl FnMut(T),
    ) -> std::result::Result<(), Rejection> {
        let (reservation, evicted) = self.reserve(bytes)?;
        for payload in evicted {
            evict(payload);
        }
        reservation.commit(build())
    }

    fn reserve(
        &self,
        bytes: usize,
    ) -> std::result::Result<(Reservation<'_, T>, Vec<T>), Rejection> {
        let mut state = self.lock();
        if state.closed {
            return Err(Rejection::Closed);
        }
        state.bytes.as_mut().unwrap().check_message(bytes)?;
        if state.reservations == self.depth
            || !state.bytes.as_ref().unwrap().reservation_fits(bytes)
        {
            state.bytes.as_mut().unwrap().reject_capacity();
            return Err(Rejection::Capacity);
        }
        let mut evicted = Vec::new();
        while state.items.len() + state.reservations >= self.depth
            || !state.bytes.as_ref().unwrap().fits(bytes)
        {
            let item = state
                .items
                .pop_front()
                .expect("only queued items can be evicted");
            state.bytes.as_mut().unwrap().remove(item.bytes, true);
            state.dropped_total = state.dropped_total.saturating_add(1);
            evicted.push(item.payload);
        }
        state.reservations += 1;
        state.bytes.as_mut().unwrap().reserve(bytes);
        drop(state);
        Ok((
            Reservation {
                queue: self,
                bytes,
                active: true,
            },
            evicted,
        ))
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.lock().closed
    }

    pub(crate) fn stats(&self) -> QueueStats {
        let state = self.lock();
        QueueStats {
            depth: self.depth,
            queued: state.items.len(),
            received_total: state.received_total,
            dropped_total: state.dropped_total,
            closed: state.closed,
            bytes: state.bytes.as_ref().map(ByteBudget::stats),
        }
    }

    pub(crate) fn finished_at(&self) -> Option<Instant> {
        self.lock().finished
    }

    pub(crate) fn finish(&self) {
        self.lock().finished.get_or_insert_with(Instant::now);
        self.ready.notify_all();
    }

    /// A bounded, local terminal error for a one-reply helper. Unlike a
    /// received payload, this carries no untrusted remote bytes.
    pub(crate) fn fail(&self, error: Error) {
        let mut state = self.lock();
        if !state.closed {
            state.failure.get_or_insert(error);
        }
        drop(state);
        self.ready.notify_all();
    }

    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        let items = std::mem::take(&mut state.items);
        if let Some(bytes) = &mut state.bytes {
            bytes.clear();
        }
        drop(state);
        self.ready.notify_all();
        drop(items);
    }

    pub(crate) fn recv_timeout(&self, timeout: Duration) -> Result<T> {
        // A deadline beyond Instant's range waits until data or close.
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.lock();
        loop {
            if state.closed {
                return Err(Error::Closed);
            }
            if let Some(item) = state.items.pop_front() {
                if let Some(bytes) = &mut state.bytes {
                    bytes.remove(item.bytes, false);
                }
                return Ok(item.payload);
            }
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if state.finished.is_some() {
                return Err(Error::Timeout);
            }
            let remaining = match deadline {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()),
                None => Duration::MAX,
            };
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }
            state = self
                .ready
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

impl<T: Send> CloseWake for Queue<T> {
    fn wake_close(&self) {
        self.close();
    }
}

/// A callback owns this marker. Zenoh dropping its handler marks query completion.
pub(crate) struct FinishOnDrop<T>(pub(crate) Arc<Queue<T>>);
impl<T> FinishOnDrop<T> {
    pub(crate) fn push(&self, item: T) {
        self.0.push(item);
    }
}
impl<T> Drop for FinishOnDrop<T> {
    fn drop(&mut self) {
        self.0.finish();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Weak;
    use std::thread;

    use super::*;

    #[test]
    fn queue_keeps_last_depth_samples() {
        let queue = Queue::new(10);
        for i in 0..10 + 5 {
            queue.push(vec![i as u8]);
        }
        let received: Vec<u8> = (0..10)
            .map(|_| queue.recv_timeout(Duration::ZERO).unwrap()[0])
            .collect();
        assert_eq!(received, (5_u8..15).collect::<Vec<_>>());
        assert_eq!(queue.recv_timeout(Duration::ZERO), Err(Error::Timeout));
        assert_eq!(
            queue.stats(),
            QueueStats {
                depth: 10,
                queued: 0,
                received_total: 15,
                dropped_total: 5,
                closed: false,
                bytes: None,
            }
        );
    }

    #[test]
    fn close_clearing_and_late_arrivals_are_not_overflow() {
        let queue = Queue::new(1);
        queue.push(1);
        queue.push(2);
        queue.close();
        queue.push(3);
        assert_eq!(
            queue.stats(),
            QueueStats {
                depth: 1,
                queued: 0,
                received_total: 2,
                dropped_total: 1,
                closed: true,
                bytes: None,
            }
        );
    }

    #[test]
    fn eviction_close_and_late_arrivals_drop_payloads_outside_queue_lock() {
        struct Probe {
            queue: Weak<Queue<Probe>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let queue = self.queue.upgrade().unwrap();
                assert!(queue.state.try_lock().is_ok(), "payload dropped under lock");
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
        let queue = Arc::new(Queue::new(1));
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = || Probe {
            queue: Arc::downgrade(&queue),
            drops: drops.clone(),
        };
        queue.push(probe());
        queue.push(probe());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        queue.close();
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        queue.push(probe());
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn close_wakes_every_waiter_and_drops_late_samples() {
        let queue = Arc::new(Queue::new(10));
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let queue = queue.clone();
                thread::spawn(move || {
                    let started = Instant::now();
                    (
                        queue.recv_timeout(Duration::from_secs(30)),
                        started.elapsed(),
                    )
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(100));
        queue.close();
        for waiter in waiters {
            let (result, elapsed) = waiter.join().unwrap();
            assert_eq!(result, Err(Error::Closed));
            assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
        }
        queue.push(b"late".to_vec());
        assert_eq!(queue.recv_timeout(Duration::ZERO), Err(Error::Closed));
    }

    #[test]
    fn unbounded_timeout_still_returns_data() {
        let queue = Arc::new(Queue::new(10));
        let sink = queue.clone();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            sink.push(b"x".to_vec());
        });
        assert_eq!(queue.recv_timeout(Duration::MAX), Ok(b"x".to_vec()));
        sender.join().unwrap();
    }

    #[test]
    fn byte_admission_rejects_before_copy_and_evicts_enough_old_payloads() {
        let queue = Queue::with_limits(
            10,
            QueueLimits {
                message_bytes: 8,
                queue_bytes: 10,
            },
        );
        for value in 0..3_u8 {
            queue.try_push_with(3, || vec![value; 3], drop).unwrap();
        }
        assert_eq!(
            queue.try_push_with(9, || panic!("oversized payload must not be copied"), drop),
            Err(Rejection::Oversized)
        );
        assert_eq!(queue.stats().queued, 3);
        assert_eq!(queue.stats().dropped_total, 0);
        let mut evicted = Vec::new();
        queue
            .try_push_with(8, || vec![3; 8], |item| evicted.push(item))
            .unwrap();
        assert_eq!(evicted, vec![vec![0; 3], vec![1; 3], vec![2; 3]]);
        let stats = queue.stats();
        let bytes = stats.bytes.unwrap();
        assert_eq!(
            (stats.queued, stats.received_total, stats.dropped_total),
            (1, 4, 3)
        );
        assert_eq!((bytes.queued_bytes, bytes.reserved_bytes), (8, 0));
        assert_eq!(
            (bytes.received_bytes_total, bytes.evicted_bytes_total),
            (17, 9)
        );
        assert_eq!(
            (bytes.oversized_messages_total, bytes.oversized_bytes_total),
            (1, 9)
        );
        assert_eq!(queue.recv_timeout(Duration::ZERO).unwrap(), vec![3; 8]);
        queue.close();
        let bytes = queue.stats().bytes.unwrap();
        assert_eq!(
            (
                bytes.queued_bytes,
                bytes.reserved_bytes,
                bytes.evicted_bytes_total
            ),
            (0, 0, 9)
        );
    }

    #[test]
    fn concurrent_copy_reservations_are_bounded_and_close_releases_them() {
        use std::sync::Barrier;
        let queue = Arc::new(Queue::with_limits(
            4,
            QueueLimits {
                message_bytes: 8,
                queue_bytes: 8,
            },
        ));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let worker = {
            let (queue, entered, release) = (queue.clone(), entered.clone(), release.clone());
            thread::spawn(move || {
                queue.try_push_with(
                    8,
                    || {
                        assert!(queue.state.try_lock().is_ok());
                        entered.wait();
                        release.wait();
                        vec![0; 8]
                    },
                    drop,
                )
            })
        };
        entered.wait();
        assert_eq!(queue.stats().bytes.unwrap().reserved_bytes, 8);
        assert_eq!(
            queue.try_push_with(1, || panic!("full reservation must not copy"), drop),
            Err(Rejection::Capacity)
        );
        queue.close();
        assert_eq!(queue.stats().bytes.unwrap().reserved_bytes, 8);
        release.wait();
        assert_eq!(worker.join().unwrap(), Err(Rejection::Closed));
        let stats = queue.stats();
        let bytes = stats.bytes.unwrap();
        assert_eq!(
            (stats.queued, stats.received_total, stats.dropped_total),
            (0, 0, 0)
        );
        assert_eq!(
            (
                bytes.queued_bytes,
                bytes.reserved_bytes,
                bytes.capacity_rejected_total
            ),
            (0, 0, 1)
        );
    }

    #[test]
    fn a_failed_copy_releases_its_reservation_and_payload_drops_are_unlocked() {
        struct Probe(Weak<Queue<Probe>>, Arc<AtomicUsize>);
        impl Drop for Probe {
            fn drop(&mut self) {
                assert!(self.0.upgrade().unwrap().state.try_lock().is_ok());
                self.1.fetch_add(1, Ordering::Relaxed);
            }
        }
        let queue = Arc::new(Queue::with_limits(
            1,
            QueueLimits {
                message_bytes: 4,
                queue_bytes: 4,
            },
        ));
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = || Probe(Arc::downgrade(&queue), drops.clone());
        queue.try_push_with(4, probe, drop).unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.try_push_with(4, || panic!("injected copy failure"), drop)
        }));
        assert!(failed.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(queue.stats().bytes.unwrap().reserved_bytes, 0);
        queue.try_push_with(4, probe, drop).unwrap();
        queue.close();
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert_eq!(queue.stats().bytes.unwrap().queued_bytes, 0);
    }
}
