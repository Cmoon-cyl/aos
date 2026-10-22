//! Receive metadata travels with a sample through every local queue. It is
//! captured by the adapter, never reconstructed from an observer's dequeue.
use std::time::{Instant, SystemTime};

use serde::Serialize;

/// One owned payload and the facts recorded when the transport delivered it.
#[derive(Debug, Clone)]
pub struct Sample<T> {
    pub payload: T,
    /// Original serialized business payload bytes, before JSON decoding or
    /// projection. This excludes transport framing and is not network traffic.
    pub payload_size_bytes: usize,
    /// None for backends that cannot report the actual delivery time.
    pub received: Option<ReceiveMetadata>,
    /// Optional transport-reported source evidence, not authenticated identity.
    pub source: Option<SourceMetadata>,
}

impl Sample<Vec<u8>> {
    /// Compatibility path for a backend that only implements byte reception.
    /// Dequeue time is deliberately not substituted for delivery time.
    pub fn without_receive_metadata(payload: Vec<u8>) -> Self {
        Self {
            payload_size_bytes: payload.len(),
            payload,
            received: None,
            source: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReceiveMetadata {
    /// Process-local monotonic instant, suitable for receive interval metrics.
    pub steady: Instant,
    /// The local system clock sampled at delivery. It can jump and does not
    /// establish synchronization with another machine.
    pub system: SystemTime,
    pub scope: ReceiveScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiveScope {
    /// Entry into the adapter's delivery callback, before its receive queue.
    /// For Advanced pub/sub this includes history and recovery delivery. It
    /// is neither network ingress time nor the original publication time.
    AdapterDelivery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceMetadata {
    pub timestamp: Option<SourceTimestamp>,
    /// Publisher entity identity reported by the transport, when present.
    /// This is distinct from the timestamp's clock identity.
    pub publisher_id: Option<String>,
    pub sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceTimestamp {
    /// Transport-defined clock domain, for example `zenoh_hlc`.
    pub kind: String,
    /// Lossless transport representation, not a rounded floating point time.
    /// For Zenoh this is the decimal NTP64 integer, including logical bits.
    pub value: String,
    pub clock_id: String,
    /// Whether comparison with the local system clock has been established.
    /// Merely receiving a timestamp is not evidence of comparability.
    pub comparable: bool,
    /// Known origin or its uncertainty. Zenoh timestamps may be inserted or
    /// replaced by a router, so its adapter reports `router_or_publisher`.
    pub origin: String,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::transport::SubscriptionBackend;
    use crate::{Error, Result};

    struct LegacyBackend;

    impl SubscriptionBackend for LegacyBackend {
        fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
            if timeout.is_zero() {
                return Err(Error::Timeout);
            }
            Ok(" { \"data\" : \"你好\" } \n".as_bytes().to_vec())
        }

        fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn legacy_backend_retains_exact_bytes_without_inventing_receive_time() {
        let sample = LegacyBackend
            .recv_sample_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(sample.payload, " { \"data\" : \"你好\" } \n".as_bytes());
        assert_eq!(sample.payload_size_bytes, sample.payload.len());
        assert!(sample.received.is_none());
        assert!(sample.source.is_none());
        assert!(matches!(
            LegacyBackend.recv_sample_timeout(Duration::ZERO),
            Err(Error::Timeout)
        ));
    }
}
