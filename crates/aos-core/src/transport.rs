//! Adapter boundary for the P0 byte echo probe.
//!
//! Implementations own their session and entities. All methods may race with
//! close, and must reject work once the backend is closed. In particular,
//! declaration racing with session close must either fail or return an entity
//! that is already invalidated. Core's checks cannot replace this backend rule.
//!
//! Close must release resources and wake pending receives, even when it reports
//! a cleanup error. It must be bounded and must not wait for user code. Drop
//! must also release resources, including partially constructed entities.
//! No method may execute application callbacks: receive returns owned data to
//! the caller, which dispatches it after the call returns.

use std::{sync::atomic::AtomicBool, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    pub name: String,
    pub type_name: String,
    pub type_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reliability {
    Reliable,
    BestEffort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Volatile,
    TransientLocal,
}

/// MVP supports keep_last only. Other history/deadline/lifespan policies
/// must be rejected by language bindings, not silently discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QoS {
    pub depth: usize,
    pub reliability: Reliability,
    pub durability: Durability,
}

impl Serialize for QoS {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("QoS", 4)?;
        state.serialize_field("history", "keep_last")?;
        state.serialize_field("depth", &self.depth)?;
        state.serialize_field("reliability", &self.reliability)?;
        state.serialize_field("durability", &self.durability)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for QoS {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            #[serde(default)]
            history: Option<String>,
            depth: usize,
            reliability: Reliability,
            durability: Durability,
        }
        let fields = Fields::deserialize(deserializer)?;
        if fields.history.as_deref().is_some_and(|h| h != "keep_last") || fields.depth == 0 {
            return Err(serde::de::Error::custom(
                "QoS requires keep_last and positive depth",
            ));
        }
        Ok(Self {
            depth: fields.depth,
            reliability: fields.reliability,
            durability: fields.durability,
        })
    }
}

impl Default for QoS {
    fn default() -> Self {
        Self {
            depth: 10,
            reliability: Reliability::Reliable,
            durability: Durability::Volatile,
        }
    }
}

impl QoS {
    pub fn validate(&self) -> Result<()> {
        if self.depth == 0 {
            return Err(Error::InvalidArgument("QoS depth must be positive".into()));
        }
        Ok(())
    }

    pub fn validate_service(&self) -> Result<()> {
        self.validate()?;
        if self.reliability != Reliability::Reliable || self.durability != Durability::Volatile {
            return Err(Error::Unsupported(
                "Service QoS requires reliable reliability and volatile durability".into(),
            ));
        }
        Ok(())
    }
}

/// Local payload limits for adapter-owned queues, independent of wire QoS and
/// graph discovery. They do not bound middleware caches, dequeued application
/// values, metadata, or aggregate process memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueLimits {
    pub message_bytes: usize,
    pub queue_bytes: usize,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            message_bytes: 16 * 1024 * 1024,
            queue_bytes: 256 * 1024 * 1024,
        }
    }
}

impl QueueLimits {
    pub const MAX_QUEUE_BYTES: usize = 1024 * 1024 * 1024;

    pub fn validate(&self) -> Result<()> {
        if self.message_bytes == 0
            || self.message_bytes > self.queue_bytes
            || self.queue_bytes > Self::MAX_QUEUE_BYTES
        {
            return Err(Error::InvalidArgument(
                "QueueLimits requires 0 < message_bytes <= queue_bytes <= 1 GiB".into(),
            ));
        }
        Ok(())
    }
}

/// Serialized payload bytes in one adapter queue. Reservations cover copies
/// currently in progress. received_bytes_total counts committed payloads;
/// evicted_bytes_total counts KEEP_LAST evictions, not dequeue or close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueByteStats {
    pub message_limit_bytes: usize,
    pub queue_limit_bytes: usize,
    pub queued_bytes: usize,
    pub reserved_bytes: usize,
    pub received_bytes_total: u64,
    pub evicted_bytes_total: u64,
    pub oversized_messages_total: u64,
    pub oversized_bytes_total: u64,
    pub capacity_rejected_total: u64,
}

/// Local queue observations, not network loss statistics. received_total counts
/// enqueues before close; dropped_total counts KEEP_LAST evictions, excluding
/// the explicit clearing of a queue at close. Rejected oversized messages and
/// reservation failures are separate byte counters, not KEEP_LAST evictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub depth: usize,
    pub queued: usize,
    pub received_total: u64,
    pub dropped_total: u64,
    pub closed: bool,
    /// None means the backend does not report a byte budget; it does not mean
    /// zero bytes or an enforced limit. Old serialized observations stay valid.
    #[serde(default)]
    pub bytes: Option<QueueByteStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Node,
    Publisher,
    Subscription,
    Client,
    Service,
    ActionClient,
    ActionServer,
}

/// One independently owned graph declaration. The id is unique per entity;
/// node_id identifies its owner even when two nodes use the same name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEntity {
    pub id: String,
    pub node_id: String,
    pub node_name: String,
    pub namespace: String,
    pub kind: EntityKind,
    pub endpoint: Option<Endpoint>,
    pub qos: Option<QoS>,
}

pub trait GraphGuard: Send + Sync + 'static {
    fn close(&self) -> Result<()>;
}

/// An owned view of currently discovered entities, including existing entities
/// and subsequent declarations/removals. Dropping the watch releases its
/// discovery interest. Session close invalidates it and wakes pending waits.
///
/// Snapshot and waiting are local operations: short waits must not shorten or
/// restart the network discovery interest. Notifications may coalesce; callers
/// must inspect the current snapshot after waking, rather than count wakeups.
pub trait GraphWatchBackend: Send + Sync + 'static {
    fn snapshot(&self) -> Result<Vec<GraphEntity>>;
    /// Wait for a change, returning Timeout if no notification arrives within
    /// the local deadline, or Closed after session shutdown. Zero only polls.
    fn wait_for_change(&self, timeout: Duration) -> Result<()>;
}

/// Requests own their transport correlation and may outlive the receive call.
/// Dropping an unanswered request releases it; reply settles at most once.
pub trait RequestBackend: Send + Sync + 'static {
    fn payload(&self) -> &[u8];
    fn reply(&self, response: Result<Vec<u8>>) -> Result<()>;
    /// A diagnostic ID actually received on this request, when supported.
    /// It is not an authenticated identity or an authorization credential.
    fn request_id(&self) -> Option<&str> {
        None
    }
}

pub trait ServiceBackend: Send + Sync + 'static {
    fn recv_timeout(&self, timeout: Duration) -> Result<Box<dyn RequestBackend>>;
    fn close(&self) -> Result<()>;
    fn queue_stats(&self) -> Option<QueueStats> {
        None
    }
}

/// A persistent client owns its completed-reply KEEP_LAST queue. Each call is
/// submitted once; evicting an unconsumed reply ends its waiter explicitly.
pub trait ClientBackend: Send + Sync + 'static {
    fn call_wait(
        &self,
        payload: &[u8],
        timeout: Option<Duration>,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>>;
    fn queue_stats(&self) -> Option<QueueStats> {
        None
    }
    fn close(&self) -> Result<()>;
}

/// One process-local session, configured and opened by the adapter before use.
///
/// Domain isolation, middleware keys and networking configuration belong to
/// the adapter. Topic arguments are absolute AOS names, never middleware keys.
/// The P0 profile is volatile byte delivery; it offers no schema validation,
/// discovery, retained history or subscriber-processing acknowledgement.
pub trait Transport: Send + Sync + 'static {
    /// The adapter and its owning Context share one diagnostic accumulator.
    /// Legacy backends may return None; Context then owns a local fallback.
    fn telemetry(&self) -> Option<crate::Telemetry> {
        None
    }

    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>>;
    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>>;

    fn create_typed_publisher(
        &self,
        _endpoint: &Endpoint,
        _qos: &QoS,
    ) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("typed publisher".into()))
    }
    fn create_typed_subscription(
        &self,
        _endpoint: &Endpoint,
        _qos: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("typed subscription".into()))
    }
    /// An explicit bounded declaration must never fall back to an unbounded
    /// custom adapter. Implementations enforce limits before copying payloads.
    fn create_typed_subscription_with_limits(
        &self,
        _endpoint: &Endpoint,
        _qos: &QoS,
        limits: QueueLimits,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        limits.validate()?;
        Err(Error::Unsupported(
            "typed subscription with local QueueLimits".into(),
        ))
    }
    fn create_service(&self, _endpoint: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        Err(Error::Unsupported("service".into()))
    }
    fn create_service_with_qos(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn ServiceBackend>> {
        qos.validate_service()?;
        if qos != &QoS::default() {
            return Err(Error::Unsupported(
                "non-default Service QoS on this backend".into(),
            ));
        }
        self.create_service(endpoint)
    }
    /// A legacy backend may return None only for the default profile; core
    /// then uses call_wait. Non-default profiles must never become a no-op.
    fn create_client_with_qos(
        &self,
        _endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Option<Box<dyn ClientBackend>>> {
        qos.validate_service()?;
        if qos != &QoS::default() {
            return Err(Error::Unsupported(
                "non-default Client QoS on this backend".into(),
            ));
        }
        Ok(None)
    }
    /// One request, no automatic retry. A timeout does not imply no execution.
    fn call(&self, _endpoint: &Endpoint, _payload: &[u8], _timeout: Duration) -> Result<Vec<u8>> {
        Err(Error::Unsupported("service call".into()))
    }
    /// One request with a tighter local reply-byte limit. Native adapters
    /// should reject before copying a reply. This legacy fallback can only
    /// reject after call returns its Vec, but before a caller decodes it.
    fn call_bounded(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Vec<u8>> {
        if max_response_bytes == 0 {
            return Err(Error::InvalidArgument(
                "reply byte limit must be positive".into(),
            ));
        }
        let response = self.call(endpoint, payload, timeout)?;
        if response.len() > max_response_bytes {
            return Err(Error::QueueOverflow(format!(
                "service reply exceeds {max_response_bytes} bytes"
            )));
        }
        Ok(response)
    }
    /// A single request whose local wait can be canceled by entity destruction.
    /// The adapter must observe cancellation promptly and release query state;
    /// canceling this wait does not cancel provider execution and never retries.
    fn call_cancelable(
        &self,
        _endpoint: &Endpoint,
        _payload: &[u8],
        _timeout: Duration,
        _cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        Err(Error::Unsupported("cancelable service call".into()))
    }

    /// A single service invocation with an optional application deadline.
    /// None never permits retrying the invocation. An adapter may follow a
    /// documented middleware protocol maximum (for example rmw_zenoh's Action
    /// GetResult policy); it must not invent an arbitrary application deadline.
    /// Unsupported endpoint/backend combinations must report Unsupported.
    fn call_wait(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Option<Duration>,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        match timeout {
            Some(timeout) => self.call_cancelable(endpoint, payload, timeout, cancel),
            None => Err(Error::Unsupported("service wait without deadline".into())),
        }
    }

    fn declare_graph_entity(&self, _entity: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        Err(Error::Unsupported("graph declaration".into()))
    }
    fn graph(&self, _timeout: Duration) -> Result<Vec<GraphEntity>> {
        Err(Error::Unsupported("graph discovery".into()))
    }
    /// None preserves the snapshot-polling fallback for legacy/custom backends.
    /// Network adapters should provide a watch so local cancellation checks do
    /// not repeatedly truncate discovery queries.
    fn watch_graph(&self) -> Result<Option<Box<dyn GraphWatchBackend>>> {
        Ok(None)
    }

    /// Invalidate all entities and interrupt their pending receives.
    ///
    /// The first core shutdown caller invokes this once. Closing an individual
    /// entity after session close must be safe (success or `Error::Closed`).
    fn close(&self) -> Result<()>;
}

/// An owned publisher. Dropping it must undeclare its backend entity.
pub trait PublisherBackend: Send + Sync + 'static {
    /// Submit bytes once. Success does not mean a subscriber processed them.
    /// No automatic retry is permitted.
    fn publish(&self, payload: &[u8]) -> Result<()>;
    fn wait_for_subscribers(&self, _timeout: Duration) -> Result<()> {
        Err(Error::Unsupported("publisher matching".into()))
    }
    fn close(&self) -> Result<()>;
}

/// An owned subscription. Dropping it must undeclare its backend entity.
pub trait SubscriptionBackend: Send + Sync + 'static {
    /// Wait at most `timeout`; zero polls without waiting.
    ///
    /// Return `Error::Timeout` when no data arrives, `Error::Closed` when the
    /// entity/session closes, and `Error::Transport` on communication failure.
    /// Concurrent receivers compete for messages; each message is taken once.
    /// Close must wake all receivers without waiting for this deadline.
    fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>>;
    /// Receive the same single sample with transport metadata. Legacy
    /// backends remain compatible and explicitly report missing receive
    /// times; the default must not fabricate them after dequeue.
    fn recv_sample_timeout(&self, timeout: Duration) -> Result<crate::Sample<Vec<u8>>> {
        self.recv_timeout(timeout)
            .map(crate::Sample::without_receive_metadata)
    }
    fn close(&self) -> Result<()>;
    fn queue_stats(&self) -> Option<QueueStats> {
        None
    }
}
