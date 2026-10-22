//! A Client's completed replies share one KEEP_LAST queue. Pending queries
//! consume no depth. Eviction wakes the affected caller with a local error.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use aos_core::{transport::QueueStats, Error, QueueLimits, Result};

use crate::byte_budget::{ByteBudget, Rejection};
use crate::queue::CloseWake;

/// Keep remote error bytes opaque until dequeue: lossy UTF-8 conversion can
/// expand their size and must not run inside the queue's payload budget.
pub(crate) enum ReplyValue {
    Success(Vec<u8>),
    Remote(Vec<u8>),
    Timeout,
    ProviderResponseTooLarge,
}

impl ReplyValue {
    pub(crate) fn into_result(self) -> Result<Vec<u8>> {
        match self {
            Self::Success(payload) => Ok(payload),
            Self::Remote(payload) => Err(Error::Remote(
                String::from_utf8_lossy(&payload).into_owned(),
            )),
            Self::Timeout => Err(Error::Timeout),
            Self::ProviderResponseTooLarge => Err(Error::QueueOverflow(
                "SERVICE_REPLY_TOO_LARGE: the provider rejected an oversized response; \
                 provider work may have executed; no automatic retry"
                    .into(),
            )),
        }
    }
}

struct Completed {
    id: u64,
    bytes: usize,
    reply: ReplyValue,
}

pub(crate) struct ReplyQueue {
    state: Mutex<State>,
    ready: Condvar,
    depth: usize,
}

struct State {
    next_id: u64,
    queries: HashMap<u64, QueryState>,
    completed: VecDeque<Completed>,
    received_total: u64,
    dropped_total: u64,
    closed: bool,
    bytes: ByteBudget,
    reservations: usize,
}

#[derive(Clone, Copy)]
enum QueryState {
    Pending,
    Reserved,
    Ready,
    Evicted,
    Rejected(Rejection),
    Finished(Instant),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReplyEvent {
    Response(Result<Vec<u8>>),
    Finished(Instant),
}

struct Reservation<'a> {
    queue: &'a ReplyQueue,
    id: u64,
    bytes: usize,
    active: bool,
}

impl Reservation<'_> {
    fn commit(mut self, reply: ReplyValue) -> std::result::Result<(), Rejection> {
        let mut state = self.queue.lock();
        state.reservations -= 1;
        self.active = false;
        if state.closed || !matches!(state.queries.get(&self.id), Some(QueryState::Reserved)) {
            state.bytes.release(self.bytes);
            drop(state);
            drop(reply);
            return Err(Rejection::Closed);
        }
        state.bytes.commit(self.bytes);
        state.received_total = state.received_total.saturating_add(1);
        state.completed.push_back(Completed {
            id: self.id,
            bytes: self.bytes,
            reply,
        });
        state.queries.insert(self.id, QueryState::Ready);
        drop(state);
        self.queue.ready.notify_all();
        Ok(())
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.queue.lock();
            state.reservations -= 1;
            state.bytes.release(self.bytes);
            if matches!(state.queries.get(&self.id), Some(QueryState::Reserved)) {
                state
                    .queries
                    .insert(self.id, QueryState::Finished(Instant::now()));
            }
            drop(state);
            self.queue.ready.notify_all();
        }
    }
}

impl ReplyQueue {
    pub(crate) fn with_limits(depth: usize, limits: QueueLimits) -> Self {
        assert!(depth > 0);
        Self {
            state: Mutex::new(State {
                next_id: 0,
                queries: HashMap::new(),
                completed: VecDeque::new(),
                received_total: 0,
                dropped_total: 0,
                closed: false,
                bytes: ByteBudget::new(limits),
                reservations: 0,
            }),
            ready: Condvar::new(),
            depth,
        }
    }

    #[cfg(test)]
    fn new(depth: usize) -> Self {
        Self::with_limits(depth, QueueLimits::default())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn register(&self) -> Result<u64> {
        let mut state = self.lock();
        if state.closed {
            return Err(Error::Closed);
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| Error::Transport("client query IDs exhausted".into()))?;
        state.queries.insert(id, QueryState::Pending);
        Ok(id)
    }

    /// Reserve before copying a Zenoh payload. Oversize or concurrent-copy
    /// exhaustion settles this waiter explicitly without occupying reply depth.
    pub(crate) fn try_push_with(
        &self,
        id: u64,
        bytes: usize,
        build: impl FnOnce() -> ReplyValue,
    ) -> std::result::Result<(), Rejection> {
        let (reservation, evicted) = self.reserve(id, bytes)?;
        drop(evicted);
        reservation.commit(build())
    }

    fn reserve(
        &self,
        id: u64,
        bytes: usize,
    ) -> std::result::Result<(Reservation<'_>, Vec<Completed>), Rejection> {
        let mut state = self.lock();
        if state.closed || !matches!(state.queries.get(&id), Some(QueryState::Pending)) {
            return Err(Rejection::Closed);
        }
        let rejected = match state.bytes.check_message(bytes) {
            Err(rejection) => Some(rejection),
            Ok(()) if state.reservations == self.depth || !state.bytes.reservation_fits(bytes) => {
                state.bytes.reject_capacity();
                Some(Rejection::Capacity)
            }
            Ok(()) => None,
        };
        if let Some(rejection) = rejected {
            state.queries.insert(id, QueryState::Rejected(rejection));
            drop(state);
            self.ready.notify_all();
            return Err(rejection);
        }
        let mut evicted = Vec::new();
        while state.completed.len() + state.reservations >= self.depth || !state.bytes.fits(bytes) {
            let item = state
                .completed
                .pop_front()
                .expect("only queued replies can be evicted");
            state.bytes.remove(item.bytes, true);
            state.queries.insert(item.id, QueryState::Evicted);
            state.dropped_total = state.dropped_total.saturating_add(1);
            evicted.push(item);
        }
        state.bytes.reserve(bytes);
        state.reservations += 1;
        state.queries.insert(id, QueryState::Reserved);
        drop(state);
        self.ready.notify_all();
        Ok((
            Reservation {
                queue: self,
                id,
                bytes,
                active: true,
            },
            evicted,
        ))
    }

    #[cfg(test)]
    fn push(&self, id: u64, reply: Result<Vec<u8>>) {
        let payload = reply.expect("successful test reply");
        let _ = self.try_push_with(id, payload.len(), || ReplyValue::Success(payload));
    }

    fn finish(&self, id: u64) {
        let mut state = self.lock();
        if matches!(state.queries.get(&id), Some(QueryState::Pending)) {
            state
                .queries
                .insert(id, QueryState::Finished(Instant::now()));
            self.ready.notify_all();
        }
    }

    pub(crate) fn remove(&self, id: u64) {
        let mut state = self.lock();
        state.queries.remove(&id);
        let removed = state
            .completed
            .iter()
            .position(|item| item.id == id)
            .and_then(|index| state.completed.remove(index));
        if let Some(item) = &removed {
            state.bytes.remove(item.bytes, false);
        }
        drop(state);
        drop(removed);
    }

    pub(crate) fn recv_timeout(&self, id: u64, timeout: Duration) -> Result<ReplyEvent> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.lock();
        loop {
            if state.closed {
                return Err(Error::Closed);
            }
            match state.queries.get(&id).copied() {
                Some(QueryState::Ready) => {
                    let index = state
                        .completed
                        .iter()
                        .position(|item| item.id == id)
                        .expect("a ready query owns one queued reply");
                    let item = state.completed.remove(index).unwrap();
                    state.bytes.remove(item.bytes, false);
                    state.queries.remove(&id);
                    drop(state);
                    return Ok(ReplyEvent::Response(item.reply.into_result()));
                }
                Some(QueryState::Evicted) => {
                    state.queries.remove(&id);
                    return Err(Error::QueueOverflow(
                        "client reply queue evicted an unconsumed reply; the request was sent and \
                         the provider may have executed it; no automatic retry"
                            .into(),
                    ));
                }
                Some(QueryState::Rejected(rejection)) => {
                    state.queries.remove(&id);
                    return Err(rejection.local_reply_error());
                }
                Some(QueryState::Finished(finished_at)) => {
                    state.queries.remove(&id);
                    return Ok(ReplyEvent::Finished(finished_at));
                }
                None => return Err(Error::Closed),
                Some(QueryState::Pending | QueryState::Reserved) => {}
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

    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        let completed = std::mem::take(&mut state.completed);
        state.bytes.clear();
        state.queries.clear();
        drop(state);
        self.ready.notify_all();
        drop(completed);
    }

    pub(crate) fn stats(&self) -> QueueStats {
        let state = self.lock();
        QueueStats {
            depth: self.depth,
            queued: state.completed.len(),
            received_total: state.received_total,
            dropped_total: state.dropped_total,
            closed: state.closed,
            bytes: Some(state.bytes.stats()),
        }
    }
}

impl CloseWake for ReplyQueue {
    fn wake_close(&self) {
        self.close();
    }
}

pub(crate) struct FinishReplyOnDrop {
    pub(crate) queue: Arc<ReplyQueue>,
    pub(crate) id: u64,
}

impl Drop for FinishReplyOnDrop {
    fn drop(&mut self) {
        self.queue.finish(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn pending_queries_do_not_consume_depth_and_replies_keep_last() {
        let queue = ReplyQueue::new(2);
        let ids: Vec<_> = (0..5).map(|_| queue.register().unwrap()).collect();
        assert_eq!(queue.stats().queued, 0);
        assert_eq!(queue.stats().dropped_total, 0);
        for id in &ids {
            queue.push(*id, Ok(vec![*id as u8]));
        }
        assert_eq!(queue.stats().queued, 2);
        assert_eq!(queue.stats().received_total, 5);
        assert_eq!(queue.stats().dropped_total, 3);
        for id in &ids[..3] {
            assert!(matches!(
                queue.recv_timeout(*id, Duration::ZERO),
                Err(Error::QueueOverflow(message)) if message.contains("no automatic retry")
            ));
        }
        for id in &ids[3..] {
            assert_eq!(
                queue.recv_timeout(*id, Duration::ZERO),
                Ok(ReplyEvent::Response(Ok(vec![*id as u8])))
            );
        }
        assert!(queue.lock().queries.is_empty());
    }

    #[test]
    fn reply_correlation_first_winner_and_removal_are_stable() {
        let queue = ReplyQueue::new(2);
        let first = queue.register().unwrap();
        let second = queue.register().unwrap();
        queue.push(second, Ok(b"second".to_vec()));
        queue.push(second, Ok(b"duplicate".to_vec()));
        queue.push(first, Ok(b"first".to_vec()));
        queue.finish(first);
        assert_eq!(
            queue.recv_timeout(first, Duration::ZERO),
            Ok(ReplyEvent::Response(Ok(b"first".to_vec())))
        );
        queue.remove(second);
        queue.push(second, Ok(b"late".to_vec()));
        assert_eq!(queue.stats().queued, 0);
        assert_eq!(queue.stats().received_total, 2);
        assert_eq!(queue.stats().dropped_total, 0);
        assert!(queue.lock().queries.is_empty());
    }

    #[test]
    fn close_wakes_pending_and_completed_waiters_without_reporting_loss() {
        let queue = Arc::new(ReplyQueue::new(1));
        let pending = queue.register().unwrap();
        let completed = queue.register().unwrap();
        queue.push(completed, Ok(vec![1]));
        let waiter_queue = queue.clone();
        let waiter =
            thread::spawn(move || waiter_queue.recv_timeout(pending, Duration::from_secs(30)));
        queue.close();
        assert_eq!(waiter.join().unwrap(), Err(Error::Closed));
        assert_eq!(
            queue.recv_timeout(completed, Duration::ZERO),
            Err(Error::Closed)
        );
        assert_eq!(queue.register(), Err(Error::Closed));
        assert_eq!(queue.stats().queued, 0);
        assert_eq!(queue.stats().received_total, 1);
        assert_eq!(queue.stats().dropped_total, 0);
    }

    #[test]
    fn finished_without_reply_is_distinct_from_a_wait_poll_timeout() {
        let queue = Arc::new(ReplyQueue::new(1));
        let id = queue.register().unwrap();
        assert_eq!(queue.recv_timeout(id, Duration::ZERO), Err(Error::Timeout));
        drop(FinishReplyOnDrop {
            queue: queue.clone(),
            id,
        });
        assert!(matches!(
            queue.recv_timeout(id, Duration::ZERO),
            Ok(ReplyEvent::Finished(_))
        ));
    }

    #[test]
    fn reply_bytes_evict_old_waiters_and_oversize_settles_only_its_own_waiter() {
        let queue = ReplyQueue::with_limits(
            10,
            QueueLimits {
                message_bytes: 8,
                queue_bytes: 10,
            },
        );
        let ids: Vec<_> = (0..5).map(|_| queue.register().unwrap()).collect();
        for id in &ids[..3] {
            queue
                .try_push_with(*id, 3, || ReplyValue::Success(vec![*id as u8; 3]))
                .unwrap();
        }
        assert_eq!(
            queue.try_push_with(ids[3], 9, || panic!("oversize copied")),
            Err(Rejection::Oversized)
        );
        assert_eq!(queue.stats().queued, 3);
        assert!(
            matches!(queue.recv_timeout(ids[3], Duration::ZERO), Err(Error::QueueOverflow(message))
            if message.contains("REPLY_MESSAGE_TOO_LARGE") && message.contains("may have executed"))
        );
        queue
            .try_push_with(ids[4], 8, || ReplyValue::Success(vec![4; 8]))
            .unwrap();
        for id in &ids[..3] {
            assert!(matches!(
                queue.recv_timeout(*id, Duration::ZERO),
                Err(Error::QueueOverflow(_))
            ));
        }
        assert_eq!(
            queue.recv_timeout(ids[4], Duration::ZERO),
            Ok(ReplyEvent::Response(Ok(vec![4; 8])))
        );
        let bytes = queue.stats().bytes.unwrap();
        assert_eq!((bytes.queued_bytes, bytes.reserved_bytes), (0, 0));
        assert_eq!(
            (bytes.received_bytes_total, bytes.evicted_bytes_total),
            (17, 9)
        );
        assert_eq!(
            (bytes.oversized_messages_total, bytes.oversized_bytes_total),
            (1, 9)
        );
        assert!(queue.lock().queries.is_empty());
    }

    #[test]
    fn reply_reservations_reject_without_evicting_when_copies_alone_exhaust_capacity() {
        use std::sync::Barrier;
        for close in [false, true] {
            let queue = Arc::new(ReplyQueue::with_limits(
                4,
                QueueLimits {
                    message_bytes: 8,
                    queue_bytes: 12,
                },
            ));
            let old = queue.register().unwrap();
            queue
                .try_push_with(old, 2, || ReplyValue::Success(vec![1; 2]))
                .unwrap();
            let copying = queue.register().unwrap();
            let rejected = queue.register().unwrap();
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let worker = {
                let (queue, entered, release) = (queue.clone(), entered.clone(), release.clone());
                thread::spawn(move || {
                    queue.try_push_with(copying, 8, || {
                        assert!(queue.state.try_lock().is_ok());
                        entered.wait();
                        release.wait();
                        ReplyValue::Success(vec![2; 8])
                    })
                })
            };
            entered.wait();
            assert_eq!(
                queue.try_push_with(rejected, 5, || panic!("full reservation copied")),
                Err(Rejection::Capacity)
            );
            assert_eq!((queue.stats().queued, queue.stats().dropped_total), (1, 0));
            assert!(
                matches!(queue.recv_timeout(rejected, Duration::ZERO), Err(Error::QueueOverflow(message))
                if message.contains("REPLY_QUEUE_CAPACITY"))
            );
            if close {
                queue.close();
            } else {
                queue.remove(copying);
                assert_eq!(
                    queue.recv_timeout(old, Duration::ZERO),
                    Ok(ReplyEvent::Response(Ok(vec![1; 2])))
                );
            }
            assert_eq!(queue.stats().bytes.unwrap().reserved_bytes, 8);
            release.wait();
            assert_eq!(worker.join().unwrap(), Err(Rejection::Closed));
            let bytes = queue.stats().bytes.unwrap();
            assert_eq!((bytes.queued_bytes, bytes.reserved_bytes), (0, 0));
            assert!(queue.lock().queries.is_empty());
        }
    }

    #[test]
    fn remote_error_conversion_happens_after_dequeue_and_failed_copy_cannot_strand_a_waiter() {
        let queue = ReplyQueue::with_limits(
            1,
            QueueLimits {
                message_bytes: 8,
                queue_bytes: 8,
            },
        );
        let id = queue.register().unwrap();
        queue
            .try_push_with(id, 8, || ReplyValue::Remote(vec![0xff; 8]))
            .unwrap();
        assert_eq!(queue.stats().bytes.unwrap().queued_bytes, 8);
        assert!(
            matches!(queue.recv_timeout(id, Duration::ZERO), Ok(ReplyEvent::Response(Err(Error::Remote(text))))
            if text.len() == 24)
        );
        assert_eq!(queue.stats().bytes.unwrap().queued_bytes, 0);
        let id = queue.register().unwrap();
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.try_push_with(id, 8, || panic!("injected reply copy failure"))
        }));
        assert!(failed.is_err());
        assert_eq!(queue.stats().bytes.unwrap().reserved_bytes, 0);
        assert!(matches!(
            queue.recv_timeout(id, Duration::ZERO),
            Ok(ReplyEvent::Finished(_))
        ));
        assert!(queue.lock().queries.is_empty());
    }
}
