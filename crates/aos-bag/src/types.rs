use crate::{BagError, BagResult};
use aos_core::{
    ActionInterfaces, ActionObservationKind, Durability, Interface, InterfaceKind,
    NodeConfigRequest, QoS, Reliability, Sample,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const PROFILE: &str = "aos-json-v1";
pub const FORMAT_VERSION: u32 = 1;
pub(crate) const METADATA_TOPIC: &str = "/_aos_bag/sample_metadata";
pub(crate) const RECORDING_METADATA: &str = "aos.recording";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReaderLimits {
    pub record_bytes: usize,
    pub schema_bytes: usize,
    pub descriptor_bytes: usize,
    pub channels: usize,
    pub shards: usize,
    pub manifest_bytes: usize,
    pub messages_per_shard: u64,
    pub chunk_records: usize,
    /// Accounted playback index budget, including shared descriptors, local
    /// shard maps, chunk locations and business-channel membership lists.
    pub index_bytes: usize,
}
impl Default for ReaderLimits {
    fn default() -> Self {
        Self {
            record_bytes: 8 * 1024 * 1024,
            schema_bytes: 1024 * 1024,
            descriptor_bytes: 16 * 1024 * 1024,
            channels: 4096,
            shards: 256,
            manifest_bytes: 4 * 1024 * 1024,
            messages_per_shard: 1_000_000,
            chunk_records: 65_536,
            index_bytes: 256 * 1024 * 1024,
        }
    }
}
impl ReaderLimits {
    pub(crate) fn validate(&self) -> BagResult<()> {
        if self.record_bytes == 0
            || self.record_bytes > 64 * 1024 * 1024
            || self.schema_bytes == 0
            || self.schema_bytes > self.record_bytes
            || self.descriptor_bytes < self.schema_bytes
            || self.channels == 0
            || self.channels > 32_000
            || self.shards == 0
            || self.shards > 4096
            || self.manifest_bytes == 0
            || self.manifest_bytes > 64 * 1024 * 1024
            || self.messages_per_shard == 0
            || self.chunk_records == 0
            || self.chunk_records > 1_000_000
            || self.index_bytes == 0
        {
            return Err(BagError::invalid("invalid reader resource limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InspectOptions {
    /// Allows an explicitly incomplete manifest, but does not bypass container,
    /// payload, hash, pair, or CRC validation. Recover truncated files first.
    pub allow_incomplete: bool,
    pub limits: ReaderLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageOptions {
    pub chunk_bytes: usize,
    pub chunk_interval_ms: u64,
    pub shard_bytes: u64,
    pub shard_interval_ms: u64,
    pub total_bytes: u64,
    pub messages_per_shard: u64,
    pub chunks_per_shard: usize,
    pub limits: ReaderLimits,
}
impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            chunk_bytes: 4 * 1024 * 1024,
            chunk_interval_ms: 1000,
            shard_bytes: 256 * 1024 * 1024,
            shard_interval_ms: 600_000,
            total_bytes: 4 * 1024 * 1024 * 1024,
            messages_per_shard: 65_536,
            chunks_per_shard: 1024,
            limits: ReaderLimits::default(),
        }
    }
}
impl StorageOptions {
    pub(crate) fn validate(&self) -> BagResult<()> {
        self.limits.validate()?;
        if self.chunk_bytes < 4096
            || self.chunk_bytes > self.limits.record_bytes.saturating_sub(1024)
            || self.chunk_interval_ms == 0
            || self.shard_interval_ms == 0
            || self.shard_bytes < self.chunk_bytes as u64 + 32 * 1024
            || self.total_bytes
                < self
                    .shard_bytes
                    .saturating_add(2 * self.limits.manifest_bytes as u64)
            || self.messages_per_shard == 0
            || self.messages_per_shard > self.limits.messages_per_shard
            || self.chunks_per_shard == 0
            || self.chunks_per_shard > 65_536
        {
            return Err(BagError::invalid("invalid storage resource limits"));
        }
        checked_duration(self.chunk_interval_ms)?;
        checked_duration(self.shard_interval_ms)?;
        if self.playback_index_upper_bound() > self.limits.index_bytes {
            return Err(BagError::invalid(
                "index_bytes is too small for the configured storage limits",
            ));
        }
        Ok(())
    }
    /// The same accounting units used by the indexed reader. Descriptor
    /// bytes are global writer limits and therefore charged only once.
    pub fn playback_index_upper_bound(&self) -> usize {
        let messages = usize::try_from(self.messages_per_shard).unwrap_or(usize::MAX);
        let chunks = self.chunks_per_shard.min(messages);
        let memberships = messages.min(chunks.saturating_mul(self.limits.channels / 2));
        self.limits
            .descriptor_bytes
            .saturating_mul(4)
            .saturating_add(self.limits.channels.saturating_mul(512))
            .saturating_add(self.limits.manifest_bytes.saturating_mul(2))
            .saturating_add(
                self.limits.shards.saturating_mul(
                    self.limits
                        .channels
                        .saturating_mul(2)
                        .saturating_mul(64)
                        .saturating_add(chunks.saturating_mul(64))
                        .saturating_add(memberships.saturating_mul(2))
                        .saturating_add(512),
                ),
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecordOptions {
    pub directory: PathBuf,
    pub node: NodeConfigRequest,
    pub topics: Vec<String>,
    pub all_topics: bool,
    pub exclude: Vec<String>,
    pub qos: QoS,
    pub topic_qos: BTreeMap<String, QoS>,
    pub discovery_interval_ms: u64,
    pub description_timeout_ms: u64,
    pub control_timeout_ms: u64,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    /// Per-endpoint adapter payload budget, including in-progress copies.
    /// Admission also requires depth * message_bytes <= this budget.
    /// This is not an aggregate recorder or process-memory limit.
    pub adapter_queue_bytes: usize,
    pub message_bytes: usize,
    pub metadata_bytes: usize,
    pub storage: StorageOptions,
}
impl Default for RecordOptions {
    fn default() -> Self {
        Self {
            directory: PathBuf::new(),
            node: NodeConfigRequest::default(),
            topics: Vec::new(),
            all_topics: false,
            exclude: Vec::new(),
            qos: QoS {
                depth: 256,
                reliability: Reliability::BestEffort,
                durability: Durability::Volatile,
            },
            topic_qos: BTreeMap::new(),
            discovery_interval_ms: 100,
            description_timeout_ms: 200,
            control_timeout_ms: 2000,
            queue_messages: 1024,
            queue_bytes: 32 * 1024 * 1024,
            adapter_queue_bytes: 256 * 1024 * 1024,
            message_bytes: 1024 * 1024,
            metadata_bytes: 64 * 1024,
            storage: StorageOptions::default(),
        }
    }
}
impl RecordOptions {
    pub fn validate(&self) -> BagResult<()> {
        self.storage.validate()?;
        if self.directory.as_os_str().is_empty()
            || self.all_topics == !self.topics.is_empty()
            || self.queue_messages == 0
            || self.queue_bytes == 0
            || self.message_bytes == 0
            || self.metadata_bytes == 0
            || self.queue_messages > 1_000_000
            || self.queue_bytes > 1024 * 1024 * 1024
            || self.adapter_queue_bytes == 0
            || self.adapter_queue_bytes > 1024 * 1024 * 1024
            || self
                .message_bytes
                .checked_add(self.metadata_bytes)
                .and_then(|n| n.checked_add(62))
                .is_none_or(|n| n > self.storage.chunk_bytes)
            || self.discovery_interval_ms == 0
            || self.description_timeout_ms == 0
            || self.control_timeout_ms == 0
        {
            return Err(BagError::invalid(
                "record requires a new directory, either topics or all_topics, and finite positive limits",
            ));
        }
        checked_duration(self.discovery_interval_ms)?;
        checked_duration(self.description_timeout_ms)?;
        checked_duration(self.control_timeout_ms)?;
        self.validate_qos_budget(&self.qos)?;
        validate_names(&self.topics)?;
        validate_names(&self.exclude)?;
        for (name, qos) in &self.topic_qos {
            aos_core::names::validate_absolute_name(name)?;
            self.validate_qos_budget(qos)?;
        }
        Ok(())
    }
    pub(crate) fn validate_qos_budget(&self, qos: &QoS) -> BagResult<()> {
        qos.validate()?;
        if qos
            .depth
            .checked_mul(self.message_bytes)
            .is_none_or(|bytes| bytes > self.adapter_queue_bytes)
        {
            return Err(BagError::invalid(
                "endpoint depth * message_bytes exceeds the per-endpoint adapter_queue_bytes admission budget",
            ));
        }
        Ok(())
    }
    pub(crate) fn selected(&self, topic: &str) -> bool {
        (self.all_topics || self.topics.iter().any(|name| name == topic))
            && !self.exclude.iter().any(|name| name == topic)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecoveryOptions {
    pub storage: StorageOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionObservation {
    pub base: String,
    pub kind: ActionObservationKind,
    pub interface: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRole {
    Business,
    SampleMetadata,
}

/// Immutable channel facts. A changed QoS snapshot, type, time source, or
/// sequence epoch has a different id and is never an in-place metadata update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelInfo {
    pub id: String,
    pub topic: String,
    pub type_name: String,
    pub type_hash: String,
    pub interface: Value,
    pub offered_qos: Vec<QoS>,
    pub record_qos: QoS,
    pub log_time_domain: String,
    pub publish_time_source: String,
    pub sequence_epoch: u32,
    pub action: Option<ActionObservation>,
    pub role: ChannelRole,
    pub companion_for: Option<String>,
}
impl ChannelInfo {
    pub fn new(
        topic: String,
        interface: &Interface,
        offered_qos: Vec<QoS>,
        record_qos: QoS,
        action: Option<ActionObservation>,
    ) -> BagResult<Self> {
        let mut channel = Self {
            id: String::new(),
            topic,
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
            interface: interface.definition().clone(),
            offered_qos,
            record_qos,
            log_time_domain: "recording_anchored_steady".into(),
            publish_time_source: "unavailable".into(),
            sequence_epoch: 0,
            action,
            role: ChannelRole::Business,
            companion_for: None,
        };
        channel
            .offered_qos
            .sort_by_cached_key(|q| serde_json::to_string(q).unwrap());
        channel.offered_qos.dedup();
        channel.id = channel.content_id()?;
        channel.validate()?;
        Ok(channel)
    }
    pub(crate) fn content_id(&self) -> BagResult<String> {
        let mut without_id = self.clone();
        without_id.id.clear();
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&without_id)?)
        ))
    }
    pub(crate) fn with_epoch(&self, sequence: u64) -> BagResult<Self> {
        let mut channel = self.clone();
        channel.sequence_epoch = (sequence >> 32) as u32;
        channel.id = channel.content_id()?;
        Ok(channel)
    }
    pub fn validate(&self) -> BagResult<Interface> {
        aos_core::names::validate_absolute_name(&self.topic)?;
        self.record_qos.validate()?;
        for qos in &self.offered_qos {
            qos.validate()?;
        }
        let interface = Interface::from_json(self.interface.clone())?;
        if interface.kind() != InterfaceKind::Message
            || interface.type_name() != self.type_name
            || interface.type_hash() != self.type_hash
            || self.id != self.content_id()?
            || self.log_time_domain != "recording_anchored_steady"
            || self.publish_time_source != "unavailable"
        {
            return Err(BagError::corrupt(
                "channel identity, type/hash, or time domain mismatch",
            ));
        }
        match self.role {
            ChannelRole::Business if self.companion_for.is_some() => {
                return Err(BagError::corrupt("business channel has companion_for"));
            }
            ChannelRole::SampleMetadata
                if self.companion_for.is_none()
                    || self.topic != METADATA_TOPIC
                    || self.action.is_some()
                    || self.type_hash != sample_metadata_interface().type_hash() =>
            {
                return Err(BagError::corrupt("invalid sample metadata channel"));
            }
            _ => {}
        }
        if let Some(action) = &self.action {
            aos_core::names::validate_absolute_name(&action.base)?;
            if action.base.split('/').any(|p| p == "_action")
                || self.topic != format!("{}{}", action.base, action.kind.suffix())
            {
                return Err(BagError::corrupt("invalid Action observation base/suffix"));
            }
            let parent = Interface::from_json(action.interface.clone())?;
            let generated = ActionInterfaces::new(&parent)?;
            let expected = match action.kind {
                ActionObservationKind::Feedback => generated.feedback,
                ActionObservationKind::Status => generated.status,
            };
            if expected.type_hash() != self.type_hash {
                return Err(BagError::corrupt(
                    "Action parent and derived observation disagree",
                ));
            }
        } else if self.role == ChannelRole::Business
            && self.topic.split('/').any(|p| p == "_action")
        {
            return Err(BagError::corrupt(
                "Action internal topic requires its parent Interface",
            ));
        }
        Ok(interface)
    }
    pub(crate) fn companion(&self) -> BagResult<Self> {
        let interface = sample_metadata_interface();
        let mut companion = Self {
            id: String::new(),
            topic: METADATA_TOPIC.into(),
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
            interface: interface.definition().clone(),
            offered_qos: Vec::new(),
            record_qos: self.record_qos.clone(),
            log_time_domain: self.log_time_domain.clone(),
            publish_time_source: self.publish_time_source.clone(),
            sequence_epoch: self.sequence_epoch,
            action: None,
            role: ChannelRole::SampleMetadata,
            companion_for: Some(self.id.clone()),
        };
        companion.id = companion.content_id()?;
        Ok(companion)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedReceive {
    /// Signed decimal nanoseconds relative to this recording's steady anchor.
    pub steady_offset_ns: String,
    /// Signed decimal nanoseconds since the Unix epoch, preserving clock jumps.
    pub system_unix_ns: String,
    pub scope: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedSourceTimestamp {
    pub kind: String,
    pub value: String,
    pub clock_id: String,
    pub comparable: bool,
    pub origin: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedSource {
    pub timestamp: Option<RecordedSourceTimestamp>,
    pub publisher_id: Option<String>,
    pub sequence: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleMetadata {
    pub recording_sequence: u64,
    pub channel_id: String,
    pub recorded_payload_size_bytes: u64,
    pub adapter_payload_size_bytes: u64,
    pub received: Option<RecordedReceive>,
    pub source: Option<RecordedSource>,
}
impl SampleMetadata {
    pub(crate) fn capture(
        sample: &Sample<Vec<u8>>,
        sequence: u64,
        channel_id: String,
        anchor: Instant,
    ) -> Self {
        Self {
            recording_sequence: sequence,
            channel_id,
            recorded_payload_size_bytes: sample.payload.len() as u64,
            adapter_payload_size_bytes: sample.payload_size_bytes as u64,
            received: sample.received.map(|r| RecordedReceive {
                steady_offset_ns: if r.steady >= anchor {
                    r.steady.duration_since(anchor).as_nanos().to_string()
                } else {
                    format!("-{}", anchor.duration_since(r.steady).as_nanos())
                },
                system_unix_ns: system_ns(r.system),
                scope: "adapter_delivery".into(),
            }),
            source: sample.source.as_ref().map(|s| RecordedSource {
                timestamp: s.timestamp.as_ref().map(|t| RecordedSourceTimestamp {
                    kind: t.kind.clone(),
                    value: t.value.clone(),
                    clock_id: t.clock_id.clone(),
                    comparable: t.comparable,
                    origin: t.origin.clone(),
                }),
                publisher_id: s.publisher_id.clone(),
                sequence: s.sequence,
            }),
        }
    }
}
pub(crate) fn sample_metadata_interface() -> Interface {
    static INTERFACE: OnceLock<Interface> = OnceLock::new();
    INTERFACE.get_or_init(|| Interface::from_json(json!({
        "name":"aos_bag/msg/SampleMetadata", "kind":"msg",
        "message":{
            "type":"object",
            "properties":{
                "recording_sequence":{"type":"integer","minimum":0},
                "channel_id":{"type":"string","pattern":"^[0-9a-f]{64}$"},
                "recorded_payload_size_bytes":{"type":"integer","minimum":0},
                "adapter_payload_size_bytes":{"type":"integer","minimum":0},
                "received":{"type":["object","null"]},
                "source":{"type":["object","null"]}
            },
            "required":["recording_sequence","channel_id","recorded_payload_size_bytes","adapter_payload_size_bytes","received","source"],
            "additionalProperties":false
        }
    })).expect("built-in metadata Interface is valid")).clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStopReason {
    Requested,
    Interrupted,
    CapacityLimit,
    DiskFull,
    IoError,
    ContextClosed,
    ResourceLimit,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecordCounters {
    pub received_total: u64,
    pub received_bytes: u64,
    pub written_total: u64,
    pub written_bytes: u64,
    pub adapter_dropped_total: u64,
    /// Rejected by the adapter before copying or enqueueing; not received_total.
    pub adapter_oversized_total: u64,
    pub adapter_oversized_bytes: u64,
    /// New deliveries rejected because in-progress copies exhausted the budget.
    pub adapter_capacity_rejected_total: u64,
    pub adapter_stats_unavailable: u64,
    /// Adapter queue length observed immediately before a subscription closes.
    /// Concurrent delivery can make this a lower bound.
    pub adapter_queued_at_close_total: u64,
    pub writer_queue_dropped_total: u64,
    pub writer_queue_dropped_bytes: u64,
    pub oversized_total: u64,
    pub validation_failed_total: u64,
    /// Dequeued messages rejected by the Interface validation resource budget.
    /// Kept separate from JSON/schema mismatch; ingress continues afterward.
    pub resource_rejected_total: u64,
    /// Failed endpoint admission attempts, including persistence deadlines.
    pub endpoint_rejected_total: u64,
    /// Failed node/type description attempts; retries use endpoint backoff.
    pub description_failed_total: u64,
    /// Local descriptor persistence deadlines that returned Timeout.
    pub descriptor_registration_timeouts_total: u64,
    /// Successful registrations completed after their caller timed out.
    /// Retrying an identical channel id does not consume another descriptor.
    pub descriptor_registration_late_commits_total: u64,
    pub discarded_on_close_total: u64,
    /// Successfully handed to the MCAP writer but not confirmed by a completed
    /// chunk flush/close when a file error occurred. Recovery decides survival.
    pub unconfirmed_on_failure_total: u64,
    pub source_sequence_samples: u64,
    /// Recorder sequence and local queues do not prove end-to-end loss.
    pub end_to_end_lost_total: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardInfo {
    pub file: String,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub clean_close: bool,
    pub messages: u64,
    pub first_log_time: Option<u64>,
    pub last_log_time: Option<u64>,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySource {
    pub file: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub verified_unchanged: bool,
    pub error: Option<BagError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub profile: String,
    pub format_version: u32,
    pub recording_id: String,
    pub system_anchor_ns: u64,
    pub writer_version: String,
    pub state: String,
    pub incomplete: bool,
    pub clean_close: bool,
    pub stop_reason: Option<RecordStopReason>,
    pub error: Option<BagError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_error: Option<BagError>,
    pub counters: RecordCounters,
    pub shards: Vec<ShardInfo>,
    pub recovery_sources: Vec<RecoverySource>,
    pub recovered_orphan_business: u64,
    pub recovered_orphan_metadata: u64,
    pub configuration: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BagInfo {
    pub manifest: Manifest,
    /// Business channels only; internal companion channels never replay.
    pub channels: Vec<ChannelInfo>,
    pub messages: u64,
    pub verified_payload_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReport {
    pub destination: PathBuf,
    pub info: BagInfo,
    pub sources: Vec<RecoverySource>,
    pub discarded_orphan_business: u64,
    pub discarded_orphan_metadata: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedEndpoint {
    pub topic: String,
    pub type_hash: String,
    pub reason: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    Recording,
    Stopping,
    Finished,
    Failed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordStatus {
    pub state: RecordState,
    pub directory: PathBuf,
    pub recording_id: String,
    pub counters: RecordCounters,
    pub active_channels: Vec<ChannelInfo>,
    pub unresolved: Vec<UnresolvedEndpoint>,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub shards: usize,
    pub stop_reason: Option<RecordStopReason>,
    pub error: Option<BagError>,
    /// True only when workers, subscriptions and the recorder Node have
    /// finished closing without a cleanup error. Separate from file validity.
    #[serde(default)]
    pub resources_closed: bool,
    #[serde(default)]
    pub cleanup_error: Option<BagError>,
}
/// Small polling snapshot. Full Interface documents are available from status,
/// but never cloned while the recorder's per-sample mutex is held.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordProgress {
    pub state: RecordState,
    pub directory: PathBuf,
    pub recording_id: String,
    pub counters: RecordCounters,
    pub active_channel_count: usize,
    pub unresolved_count: usize,
    /// At most eight bounded reasons; this is not the complete endpoint list.
    pub unresolved_sample: Vec<UnresolvedEndpoint>,
    pub queue_messages: usize,
    pub queue_bytes: usize,
    pub shards: usize,
    pub stop_reason: Option<RecordStopReason>,
    pub error: Option<BagError>,
    pub resources_closed: bool,
    pub cleanup_error: Option<BagError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordReport {
    pub status: RecordStatus,
    pub manifest: Manifest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlaybackClockOptions {
    /// A source-style absolute name. The replay namespace and core remaps apply.
    pub topic: String,
    pub period_ms: u64,
}
impl Default for PlaybackClockOptions {
    fn default() -> Self {
        Self {
            topic: "/clock".into(),
            period_ms: 20,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PlaybackOptions {
    pub node: NodeConfigRequest,
    pub namespace: Option<String>,
    pub original_names: bool,
    pub topics: Vec<String>,
    pub channel_ids: Vec<String>,
    pub exclude: Vec<String>,
    pub topic_qos: BTreeMap<String, QoS>,
    pub channel_qos: BTreeMap<String, QoS>,
    pub rate: f64,
    pub start_paused: bool,
    pub allow_incomplete: bool,
    pub clock: Option<PlaybackClockOptions>,
    pub control_timeout_ms: u64,
    pub subscriber_wait_ms: u64,
    pub limits: ReaderLimits,
}
impl Default for PlaybackOptions {
    fn default() -> Self {
        Self {
            node: NodeConfigRequest::default(),
            namespace: None,
            original_names: false,
            topics: Vec::new(),
            channel_ids: Vec::new(),
            exclude: Vec::new(),
            topic_qos: BTreeMap::new(),
            channel_qos: BTreeMap::new(),
            rate: 1.0,
            start_paused: false,
            allow_incomplete: false,
            clock: None,
            control_timeout_ms: 2000,
            subscriber_wait_ms: 0,
            limits: ReaderLimits::default(),
        }
    }
}
impl PlaybackOptions {
    pub fn validate(&self) -> BagResult<()> {
        self.limits.validate()?;
        validate_rate(self.rate)?;
        if self.original_names == self.namespace.is_some() || self.control_timeout_ms == 0 {
            return Err(BagError::invalid(
                "choose an explicit replay namespace or original_names, and a positive control timeout",
            ));
        }
        if let Some(namespace) = &self.namespace {
            let normalized = aos_core::names::normalize_namespace(namespace)?;
            if normalized == "/" || &normalized != namespace {
                return Err(BagError::invalid(
                    "replay namespace must be a normalized non-root namespace",
                ));
            }
        }
        checked_duration(self.control_timeout_ms)?;
        checked_duration(self.subscriber_wait_ms)?;
        validate_names(&self.topics)?;
        validate_names(&self.exclude)?;
        for (name, qos) in &self.topic_qos {
            aos_core::names::validate_absolute_name(name)?;
            qos.validate()?;
        }
        for qos in self.channel_qos.values() {
            qos.validate()?;
        }
        if let Some(clock) = &self.clock {
            aos_core::names::validate_absolute_name(&clock.topic)?;
            if clock.period_ms == 0 {
                return Err(BagError::invalid("clock period must be positive"));
            }
            checked_duration(clock.period_ms)?;
        }
        Ok(())
    }
    pub(crate) fn selected(&self, channel: &ChannelInfo) -> bool {
        (self.topics.is_empty() || self.topics.contains(&channel.topic))
            && (self.channel_ids.is_empty() || self.channel_ids.contains(&channel.id))
            && !self.exclude.contains(&channel.topic)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackState {
    Ready,
    Playing,
    Paused,
    Stopped,
    Finished,
    Failed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackEndpoint {
    pub channel_id: String,
    pub source: String,
    pub target: String,
    pub type_hash: String,
    pub qos: QoS,
    pub qos_source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackStatus {
    pub state: PlaybackState,
    pub position_ns: u64,
    pub duration_ns: u64,
    pub rate: f64,
    pub epoch: u64,
    pub published_total: u64,
    pub published_bytes: u64,
    pub endpoints: Vec<PlaybackEndpoint>,
    pub original_names: bool,
    pub incomplete_source: bool,
    pub clock_topic: Option<String>,
    pub error: Option<BagError>,
    #[serde(default)]
    pub resources_closed: bool,
    #[serde(default)]
    pub cleanup_error: Option<BagError>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackReport {
    pub status: PlaybackStatus,
}

/// Bounded polling snapshot. Full endpoint descriptions are available through
/// `Player::status()` and are never copied while the dispatch lock is held.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlaybackProgress {
    pub state: PlaybackState,
    pub position_ns: u64,
    pub duration_ns: u64,
    pub rate: f64,
    pub epoch: u64,
    pub published_total: u64,
    pub published_bytes: u64,
    pub endpoint_count: usize,
    pub error: Option<BagError>,
    pub resources_closed: bool,
    pub cleanup_error: Option<BagError>,
}

pub(crate) fn validate_rate(rate: f64) -> BagResult<()> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err(BagError::invalid(
            "playback rate must be finite and greater than zero",
        ));
    }
    Ok(())
}
pub(crate) fn checked_duration(ms: u64) -> BagResult<Duration> {
    let duration = Duration::from_millis(ms);
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| BagError::invalid("steady deadline overflow"))?;
    Ok(duration)
}
pub(crate) fn validate_names(names: &[String]) -> BagResult<()> {
    for name in names {
        aos_core::names::validate_absolute_name(name)?;
    }
    Ok(())
}
pub(crate) fn system_ns(time: SystemTime) -> String {
    match time.duration_since(UNIX_EPOCH) {
        Ok(time) => time.as_nanos().to_string(),
        Err(error) => format!("-{}", error.duration().as_nanos()),
    }
}
pub(crate) fn now_ns() -> BagResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BagError::invalid("recording system clock is before Unix epoch"))?
        .as_nanos()
        .try_into()
        .map_err(|_| BagError::invalid("system clock exceeds MCAP u64 nanoseconds"))
}
