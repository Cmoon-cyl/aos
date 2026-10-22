//! Payload accounting shared by receive and correlated-reply queues.
//! All operations run under the owning queue's lock; copying runs outside it.
use aos_core::{Error, QueueByteStats, QueueLimits};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rejection {
    Closed,
    Oversized,
    Capacity,
}

impl Rejection {
    pub(crate) fn local_reply_error(self) -> Error {
        match self {
            Self::Closed => Error::Closed,
            Self::Oversized => Error::QueueOverflow(
                "REPLY_MESSAGE_TOO_LARGE: the response exceeded the local message limit; \
                 the request was sent and the provider may have executed it; no automatic retry"
                    .into(),
            ),
            Self::Capacity => Error::QueueOverflow(
                "REPLY_QUEUE_CAPACITY: in-progress replies exhausted the local queue budget; \
                 the request was sent and the provider may have executed it; no automatic retry"
                    .into(),
            ),
        }
    }
}

pub(crate) fn check_send(limits: QueueLimits, bytes: usize, scope: &str) -> aos_core::Result<()> {
    if bytes > limits.message_bytes {
        Err(Error::QueueOverflow(format!(
            "{scope}_MESSAGE_TOO_LARGE: {bytes} payload bytes exceed the local limit of {}; \
             this payload was not submitted; no automatic retry",
            limits.message_bytes
        )))
    } else {
        Ok(())
    }
}

pub(crate) struct ByteBudget {
    stats: QueueByteStats,
}

impl ByteBudget {
    pub(crate) fn new(limits: QueueLimits) -> Self {
        limits.validate().expect("validated QueueLimits");
        Self {
            stats: QueueByteStats {
                message_limit_bytes: limits.message_bytes,
                queue_limit_bytes: limits.queue_bytes,
                queued_bytes: 0,
                reserved_bytes: 0,
                received_bytes_total: 0,
                evicted_bytes_total: 0,
                oversized_messages_total: 0,
                oversized_bytes_total: 0,
                capacity_rejected_total: 0,
            },
        }
    }

    pub(crate) fn check_message(&mut self, bytes: usize) -> Result<(), Rejection> {
        if bytes > self.stats.message_limit_bytes {
            self.stats.oversized_messages_total =
                self.stats.oversized_messages_total.saturating_add(1);
            self.stats.oversized_bytes_total = self
                .stats
                .oversized_bytes_total
                .saturating_add(bytes as u64);
            Err(Rejection::Oversized)
        } else {
            Ok(())
        }
    }

    pub(crate) fn reservation_fits(&self, bytes: usize) -> bool {
        bytes <= self.stats.queue_limit_bytes - self.stats.reserved_bytes
    }

    pub(crate) fn fits(&self, bytes: usize) -> bool {
        bytes <= self.stats.queue_limit_bytes - self.stats.reserved_bytes - self.stats.queued_bytes
    }

    pub(crate) fn reject_capacity(&mut self) {
        self.stats.capacity_rejected_total = self.stats.capacity_rejected_total.saturating_add(1);
    }

    pub(crate) fn reserve(&mut self, bytes: usize) {
        self.stats.reserved_bytes += bytes;
        self.assert_bound();
    }

    pub(crate) fn release(&mut self, bytes: usize) {
        self.stats.reserved_bytes -= bytes;
    }

    pub(crate) fn commit(&mut self, bytes: usize) {
        self.release(bytes);
        self.stats.queued_bytes += bytes;
        self.stats.received_bytes_total =
            self.stats.received_bytes_total.saturating_add(bytes as u64);
        self.assert_bound();
    }

    pub(crate) fn remove(&mut self, bytes: usize, evicted: bool) {
        self.stats.queued_bytes -= bytes;
        if evicted {
            self.stats.evicted_bytes_total =
                self.stats.evicted_bytes_total.saturating_add(bytes as u64);
        }
    }

    pub(crate) fn clear(&mut self) {
        // Copies already in progress still own their reservation until their
        // RAII guard exits; close does not pretend those bytes disappeared.
        self.stats.queued_bytes = 0;
    }

    pub(crate) fn stats(&self) -> QueueByteStats {
        self.stats
    }

    fn assert_bound(&self) {
        debug_assert!(
            self.stats.queued_bytes + self.stats.reserved_bytes <= self.stats.queue_limit_bytes
        );
    }
}
