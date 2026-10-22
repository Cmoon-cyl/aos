use crate::{
    format::{channel_metadata, RecordingHeader},
    types::RECORDING_METADATA,
    BagError, BagErrorKind, BagResult, ChannelInfo, Manifest, RecordCounters, RecordStopReason,
    SampleMetadata, ShardInfo, StorageOptions, FORMAT_VERSION, PROFILE,
};
use mcap::{
    records::{MessageHeader, Metadata},
    WriteOptions, Writer,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(crate) struct RecordedMessage {
    pub channel: Arc<ChannelInfo>,
    pub sequence: u64,
    pub log_time: u64,
    pub publish_time: u64,
    pub payload: Vec<u8>,
    pub metadata: SampleMetadata,
}

pub(crate) enum AppendOutcome {
    Written,
    Limit(RecordStopReason),
}
fn admission_outcome(error: BagError) -> BagResult<AppendOutcome> {
    match error.kind {
        BagErrorKind::CapacityLimit => Ok(AppendOutcome::Limit(RecordStopReason::CapacityLimit)),
        BagErrorKind::ResourceLimit => Ok(AppendOutcome::Limit(RecordStopReason::ResourceLimit)),
        _ => Err(error),
    }
}

type ChannelDescriptor = (String, String, Vec<u8>, BTreeMap<String, String>);
fn channel_descriptors(channel: &ChannelInfo) -> BagResult<(Vec<ChannelDescriptor>, usize)> {
    let companion = channel.companion()?;
    let mut descriptors = Vec::with_capacity(2);
    let mut estimate = 0usize;
    for item in [channel, &companion] {
        let schema = serde_json::to_vec(&item.interface["message"])?;
        let metadata = channel_metadata(item)?;
        estimate = estimate
            .checked_add(schema.len())
            .and_then(|n| n.checked_add(serde_json::to_vec(&metadata).ok()?.len().checked_mul(2)?))
            .and_then(|n| n.checked_add(item.topic.len() + item.type_name.len() + 256))
            .ok_or_else(|| BagError::resource("descriptor size overflow"))?;
        descriptors.push((item.type_name.clone(), item.topic.clone(), schema, metadata));
    }
    Ok((descriptors, estimate))
}

/// Shared by discovery and the writer. This includes the companion schema and
/// both channel descriptors before an endpoint receives an adapter queue.
pub(crate) fn precheck_channel(channel: &ChannelInfo, options: &StorageOptions) -> BagResult<()> {
    channel.validate()?;
    let (descriptors, estimate) = channel_descriptors(channel)?;
    if options.limits.channels < 2
        || descriptors
            .iter()
            .any(|d| d.2.len() > options.limits.schema_bytes)
        || estimate > options.limits.descriptor_bytes
        || estimate > options.chunk_bytes
    {
        return Err(BagError::resource(
            "channel and companion exceed descriptor, schema, or chunk limits",
        ));
    }
    if (estimate as u64)
        .saturating_mul(3)
        .saturating_add(33 * 1024)
        > options.shard_bytes
    {
        return Err(BagError::resource(
            "shard is too small for channel and companion descriptors",
        ));
    }
    Ok(())
}
#[derive(Debug)]
pub(crate) struct CapacityExceeded;
impl std::fmt::Display for CapacityExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AOS bag file capacity limit")
    }
}
impl std::error::Error for CapacityExceeded {}

/// Counts the file's high-water mark, not bytes overwritten by MCAP seeking.
/// The hard quota also applies to indexes/footer. A failed close stays partial.
struct QuotaFile {
    file: File,
    high_water: Arc<AtomicU64>,
    maximum: u64,
    position: u64,
    #[cfg(test)]
    fault: Arc<std::sync::Mutex<Option<io::ErrorKind>>>,
}
impl Write for QuotaFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(kind) = *self.fault.lock().unwrap() {
            return Err(io::Error::new(kind, "injected bag write failure"));
        }
        let end = self
            .position
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::StorageFull, CapacityExceeded))?;
        if end > self.maximum {
            return Err(io::Error::new(io::ErrorKind::StorageFull, CapacityExceeded));
        }
        let written = self.file.write(bytes)?;
        self.position += written as u64;
        self.high_water.fetch_max(self.position, Ordering::Relaxed);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
impl Seek for QuotaFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.position = self.file.seek(position)?;
        Ok(self.position)
    }
}

struct ShardWriter {
    writer: Option<Writer<QuotaFile>>,
    high_water: Arc<AtomicU64>,
    maximum: u64,
    info: ShardInfo,
    channels: BTreeMap<String, (u16, u16)>,
    descriptor_bytes: usize,
    chunk_bytes: usize,
    chunk_messages: usize,
    chunk_channels: BTreeSet<u16>,
    chunk_count: usize,
    index_reserve: u64,
    failed: bool,
    started: Instant,
    flushed: Instant,
}
impl Drop for ShardWriter {
    fn drop(&mut self) {
        // Upstream Writer::drop tries finish; an implicit finish would turn an
        // abandoned/failed file into an apparently closed one without a report.
        if let Some(writer) = self.writer.take() {
            drop(writer.into_inner());
        }
    }
}
impl ShardWriter {
    fn write<T>(
        &mut self,
        operation: impl FnOnce(&mut Writer<QuotaFile>) -> mcap::McapResult<T>,
    ) -> BagResult<T> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| BagError::new(BagErrorKind::Closed, "shard writer is closed"))?;
        let result = operation(writer);
        if result.is_err() {
            self.failed = true;
        }
        result.map_err(Into::into)
    }
    fn size(&self) -> u64 {
        self.high_water.load(Ordering::Relaxed)
    }
    fn close_reserve(&self) -> u64 {
        // Repeat schemas/channels, final chunk and summary indexes. This is a
        // conservative admission reserve; QuotaFile is the independent limit.
        32 * 1024 + 2 * self.descriptor_bytes as u64 + self.index_reserve
    }
    fn fits(&self, additional: usize) -> bool {
        self.size()
            .checked_add(self.chunk_bytes as u64)
            .and_then(|v| v.checked_add(self.close_reserve()))
            .and_then(|v| v.checked_add(additional as u64))
            .is_some_and(|v| v <= self.maximum)
    }
    fn flush(&mut self) -> BagResult<()> {
        if self.chunk_bytes != 0 {
            self.write(|writer| writer.flush())?;
            self.chunk_count += 1;
            self.index_reserve = self
                .index_reserve
                .checked_add(256 + self.chunk_channels.len() as u64 * 32)
                .ok_or_else(|| BagError::resource("chunk index reserve overflow"))?;
            self.chunk_bytes = 0;
            self.chunk_messages = 0;
            self.chunk_channels.clear();
        }
        self.flushed = Instant::now();
        Ok(())
    }
}

pub(crate) struct StorageWriter {
    directory: PathBuf,
    options: StorageOptions,
    pub(crate) manifest: Manifest,
    current: Option<ShardWriter>,
    closed_bytes: u64,
    descriptors: BTreeMap<String, Arc<ChannelInfo>>,
    descriptor_bytes: usize,
    #[cfg(test)]
    fault: Arc<std::sync::Mutex<Option<io::ErrorKind>>>,
}
impl StorageWriter {
    pub(crate) fn create(
        directory: &Path,
        options: StorageOptions,
        recording_id: String,
        anchor_ns: u64,
        configuration: Value,
    ) -> BagResult<Self> {
        options.validate()?;
        #[cfg(unix)]
        let mut builder = fs::DirBuilder::new();
        #[cfg(not(unix))]
        let builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(directory).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                BagError::invalid("recording/recovery destination must not exist")
            } else {
                error.into()
            }
        })?;
        let manifest = Manifest {
            profile: PROFILE.into(),
            format_version: FORMAT_VERSION,
            recording_id,
            system_anchor_ns: anchor_ns,
            writer_version: format!("aos-bag/{}/mcap-0.26.0", env!("CARGO_PKG_VERSION")),
            state: "recording".into(),
            incomplete: true,
            clean_close: false,
            stop_reason: None,
            error: None,
            cleanup_error: None,
            counters: RecordCounters::default(),
            shards: Vec::new(),
            recovery_sources: Vec::new(),
            recovered_orphan_business: 0,
            recovered_orphan_metadata: 0,
            configuration,
        };
        let mut this = Self {
            directory: directory.into(),
            options,
            manifest,
            current: None,
            closed_bytes: 0,
            descriptors: BTreeMap::new(),
            descriptor_bytes: 0,
            #[cfg(test)]
            fault: Arc::new(std::sync::Mutex::new(None)),
        };
        this.save_manifest()?;
        this.open_shard()?;
        Ok(this)
    }
    fn header(&self) -> RecordingHeader {
        RecordingHeader {
            profile: self.manifest.profile.clone(),
            format_version: self.manifest.format_version,
            recording_id: self.manifest.recording_id.clone(),
            system_anchor_ns: self.manifest.system_anchor_ns,
            writer_version: self.manifest.writer_version.clone(),
            configuration: self.manifest.configuration.clone(),
        }
    }
    fn open_shard(&mut self) -> BagResult<()> {
        if self.manifest.shards.len() >= self.options.limits.shards {
            return Err(BagError::resource("maximum shard count reached"));
        }
        // Reserve both current manifest and its atomic replacement.
        let maximum = self
            .options
            .total_bytes
            .saturating_sub(self.closed_bytes)
            .saturating_sub(2 * self.options.limits.manifest_bytes as u64)
            .min(self.options.shard_bytes);
        if maximum < 32 * 1024 + self.options.chunk_bytes as u64 {
            return Err(BagError::new(
                BagErrorKind::CapacityLimit,
                "remaining recording capacity cannot safely open another shard",
            ));
        }
        let name = format!("{:06}.mcap.partial", self.manifest.shards.len());
        let path = self.directory.join(&name);
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        let high_water = Arc::new(AtomicU64::new(0));
        let mut writer = WriteOptions::new()
            .profile(PROFILE)
            .library(&self.manifest.writer_version)
            .compression(None)
            .chunk_size(None)
            .use_chunks(true)
            .calculate_chunk_crcs(true)
            .calculate_data_section_crc(true)
            .calculate_summary_section_crc(true)
            .create(QuotaFile {
                file,
                high_water: high_water.clone(),
                maximum,
                position: 0,
                #[cfg(test)]
                fault: self.fault.clone(),
            })?;
        let metadata = Metadata {
            name: RECORDING_METADATA.into(),
            metadata: BTreeMap::from([("header".into(), serde_json::to_string(&self.header())?)]),
        };
        if let Err(error) = writer.write_metadata(&metadata) {
            drop(writer.into_inner());
            return Err(error.into());
        }
        let info = ShardInfo {
            file: name,
            size_bytes: high_water.load(Ordering::Relaxed),
            sha256: None,
            clean_close: false,
            messages: 0,
            first_log_time: None,
            last_log_time: None,
            first_sequence: None,
            last_sequence: None,
        };
        self.manifest.shards.push(info.clone());
        self.current = Some(ShardWriter {
            writer: Some(writer),
            high_water,
            maximum,
            info,
            channels: BTreeMap::new(),
            descriptor_bytes: 0,
            chunk_bytes: 0,
            chunk_messages: 0,
            chunk_channels: BTreeSet::new(),
            chunk_count: 0,
            index_reserve: 0,
            failed: false,
            started: Instant::now(),
            flushed: Instant::now(),
        });
        self.save_manifest()
    }
    pub(crate) fn register(&mut self, channel: Arc<ChannelInfo>) -> BagResult<()> {
        self.register_admitted(channel)?
    }
    pub(crate) fn register_admitted(
        &mut self,
        channel: Arc<ChannelInfo>,
    ) -> BagResult<BagResult<()>> {
        if self.descriptors.contains_key(&channel.id) {
            return Ok(Ok(()));
        }
        if let Err(error) = precheck_channel(&channel, &self.options) {
            return Ok(Err(error));
        }
        let (_, bytes) = channel_descriptors(&channel)?;
        if self.descriptors.len().saturating_add(1).saturating_mul(2) > self.options.limits.channels
            || self.descriptor_bytes.saturating_add(bytes) > self.options.limits.descriptor_bytes
        {
            return Ok(Err(BagError::resource(
                "recording channel/schema resource limit reached",
            )));
        }
        if let Err(error) = self.ensure_channel_admitted(&channel)? {
            return Ok(Err(error));
        }
        // Descriptor chunk is durable to an ordinary flush before a receiver
        // is allowed to enqueue samples. No business message is reserialized.
        self.flush()?;
        self.descriptor_bytes += bytes;
        self.descriptors.insert(channel.id.clone(), channel);
        Ok(Ok(()))
    }
    #[cfg(test)]
    pub(crate) fn fail_next_write(&mut self, kind: io::ErrorKind) {
        *self.fault.lock().unwrap() = Some(kind);
    }
    fn rotation_admission(&self) -> BagResult<()> {
        if self.manifest.shards.len() >= self.options.limits.shards {
            return Err(BagError::resource("maximum shard count reached"));
        }
        let current_reserve = self.current.as_ref().map_or(0, |shard| {
            shard
                .size()
                .saturating_add(shard.chunk_bytes as u64)
                .saturating_add(shard.close_reserve())
        });
        let remaining = self
            .options
            .total_bytes
            .saturating_sub(self.closed_bytes)
            .saturating_sub(current_reserve)
            .saturating_sub(2 * self.options.limits.manifest_bytes as u64)
            .min(self.options.shard_bytes);
        if remaining < 32 * 1024 + self.options.chunk_bytes as u64 {
            return Err(BagError::new(
                BagErrorKind::CapacityLimit,
                "remaining recording capacity cannot safely open another shard",
            ));
        }
        Ok(())
    }
    fn ensure_channel_admitted(&mut self, channel: &ChannelInfo) -> BagResult<BagResult<()>> {
        if self.current.is_none() {
            if let Err(error) = self.rotation_admission() {
                return Ok(Err(error));
            }
            self.open_shard()?;
        }
        if self
            .current
            .as_ref()
            .unwrap()
            .channels
            .contains_key(&channel.id)
        {
            return Ok(Ok(()));
        }
        let (descriptors, estimate) = channel_descriptors(channel)?;
        if estimate > self.options.chunk_bytes {
            return Ok(Err(BagError::resource(
                "a channel and companion descriptor exceed the chunk limit",
            )));
        }
        let shard = self.current.as_ref().unwrap();
        if shard.channels.len().saturating_add(1).saturating_mul(2) > self.options.limits.channels
            || shard.descriptor_bytes.saturating_add(estimate)
                > self.options.limits.descriptor_bytes
        {
            return Ok(Err(BagError::resource(
                "shard descriptor resource limit reached",
            )));
        }
        if !shard.fits(estimate.saturating_mul(3).saturating_add(1024))
            || shard.chunk_count >= self.options.chunks_per_shard
        {
            if shard.info.messages == 0 {
                return Ok(Err(BagError::resource(
                    "shard is too small for its channel descriptors",
                )));
            }
            if let Err(error) = self.rotation_admission() {
                return Ok(Err(error));
            }
            self.close_shard()?;
            self.open_shard()?;
            return self.ensure_channel_admitted(channel);
        }
        let shard = self.current.as_mut().unwrap();
        shard.flush()?;
        let mut ids = Vec::new();
        for (name, topic, schema, metadata) in descriptors {
            let id = shard.write(|writer| writer.add_schema(&name, "jsonschema", &schema))?;
            let id = shard.write(|writer| writer.add_channel(id, &topic, "json", &metadata))?;
            ids.push(id);
        }
        shard.descriptor_bytes += estimate;
        shard.chunk_bytes = estimate;
        shard.channels.insert(channel.id.clone(), (ids[0], ids[1]));
        shard.flush()?;
        Ok(Ok(()))
    }
    pub(crate) fn append(&mut self, message: &RecordedMessage) -> BagResult<()> {
        match self.append_admitted(message)? {
            AppendOutcome::Written => Ok(()),
            AppendOutcome::Limit(reason) => Err(BagError::new(
                if reason == RecordStopReason::CapacityLimit {
                    BagErrorKind::CapacityLimit
                } else {
                    BagErrorKind::ResourceLimit
                },
                "recording admission limit reached",
            )),
        }
    }
    pub(crate) fn append_admitted(
        &mut self,
        message: &RecordedMessage,
    ) -> BagResult<AppendOutcome> {
        if message.metadata.recording_sequence != message.sequence
            || message.metadata.channel_id != message.channel.id
            || message.channel.sequence_epoch != (message.sequence >> 32) as u32
            || message.metadata.recorded_payload_size_bytes != message.payload.len() as u64
            || message.publish_time != message.log_time
            || message.log_time < self.manifest.system_anchor_ns
        {
            return Err(BagError::corrupt(
                "sample pair identity, size, or timestamp mismatch",
            ));
        }
        if self
            .manifest
            .shards
            .iter()
            .rev()
            .find_map(|s| s.last_sequence)
            .is_some_and(|s| s >= message.sequence)
        {
            return Err(BagError::corrupt("recording sequence did not increase"));
        }
        if let Some(current) = &self.current {
            if current
                .info
                .last_sequence
                .is_some_and(|s| s >= message.sequence)
                || current
                    .info
                    .last_log_time
                    .is_some_and(|t| t > message.log_time)
            {
                return Err(BagError::corrupt("sample order did not increase"));
            }
        }
        if let Err(error) = self.register_admitted(message.channel.clone())? {
            return admission_outcome(error);
        }
        let metadata = serde_json::to_vec(&message.metadata)?;
        let pair_bytes = message
            .payload
            .len()
            .checked_add(metadata.len())
            .and_then(|n| n.checked_add(62))
            .ok_or_else(|| BagError::resource("sample pair size overflow"))?;
        if pair_bytes > self.options.chunk_bytes {
            return Err(BagError::resource("sample pair exceeds chunk limit"));
        }
        if let Some(current) = &self.current {
            if current.info.messages > 0
                && (!current.fits(pair_bytes.saturating_add(512))
                    || current.info.messages >= self.options.messages_per_shard
                    || current.chunk_count >= self.options.chunks_per_shard
                    || current.started.elapsed()
                        >= Duration::from_millis(self.options.shard_interval_ms))
            {
                if let Err(error) = self.rotation_admission() {
                    return admission_outcome(error);
                }
                self.close_shard()?;
                self.open_shard()?;
            }
        }
        if let Err(error) = self.ensure_channel_admitted(&message.channel)? {
            return admission_outcome(error);
        }
        let shard = self.current.as_mut().unwrap();
        if !shard.fits(pair_bytes.saturating_add(512)) {
            return Ok(AppendOutcome::Limit(RecordStopReason::CapacityLimit));
        }
        if shard.chunk_bytes.saturating_add(pair_bytes) > self.options.chunk_bytes {
            shard.flush()?;
        }
        let (business, companion) = shard.channels[&message.channel.id];
        let header = MessageHeader {
            channel_id: business,
            sequence: message.sequence as u32,
            log_time: message.log_time,
            publish_time: message.publish_time,
        };
        shard.write(|writer| writer.write_to_known_channel(&header, &message.payload))?;
        shard.write(|writer| {
            writer.write_to_known_channel(
                &MessageHeader {
                    channel_id: companion,
                    ..header
                },
                &metadata,
            )
        })?;
        // The upper writer has chunk_size=None: it cannot split this pair.
        shard.chunk_bytes += pair_bytes;
        shard.chunk_messages += 1;
        shard.chunk_channels.insert(business);
        shard.chunk_channels.insert(companion);
        shard.index_reserve = shard
            .index_reserve
            .checked_add(64)
            .ok_or_else(|| BagError::resource("message index reserve overflow"))?;
        shard.info.messages += 1;
        shard.info.first_log_time.get_or_insert(message.log_time);
        shard.info.last_log_time = Some(message.log_time);
        shard.info.first_sequence.get_or_insert(message.sequence);
        shard.info.last_sequence = Some(message.sequence);
        self.manifest.counters.written_total += 1;
        self.manifest.counters.written_bytes += message.payload.len() as u64;
        Ok(AppendOutcome::Written)
    }
    pub(crate) fn flush_due(&mut self) -> BagResult<()> {
        if self.current.as_ref().is_some_and(|s| {
            s.flushed.elapsed() >= Duration::from_millis(self.options.chunk_interval_ms)
        }) {
            self.flush()?;
        }
        Ok(())
    }
    pub(crate) fn flush(&mut self) -> BagResult<()> {
        if let Some(current) = &mut self.current {
            current.flush()?;
        }
        self.save_manifest()
    }
    fn update_current_info(&mut self) {
        if let Some(current) = &self.current {
            let mut info = current.info.clone();
            info.size_bytes = current.size();
            *self.manifest.shards.last_mut().unwrap() = info;
        }
    }
    fn close_shard(&mut self) -> BagResult<()> {
        self.update_current_info();
        let Some(mut shard) = self.current.take() else {
            return Ok(());
        };
        let mut writer = shard.writer.take().unwrap();
        let finish = writer.finish();
        let mut file = writer.into_inner();
        if let Err(error) = finish {
            self.manifest.shards.last_mut().unwrap().size_bytes = shard.size();
            return Err(error.into());
        }
        file.flush()?;
        drop(file);
        let old = self.directory.join(&shard.info.file);
        let new_name = shard.info.file.trim_end_matches(".partial").to_owned();
        let size = fs::metadata(&old)?.len();
        let digest = file_sha256(&old)?;
        fs::rename(&old, self.directory.join(&new_name))?;
        shard.info.file = new_name;
        shard.info.size_bytes = size;
        shard.info.sha256 = Some(digest);
        shard.info.clean_close = true;
        self.closed_bytes = self
            .closed_bytes
            .checked_add(size)
            .ok_or_else(|| BagError::resource("recording size overflow"))?;
        *self.manifest.shards.last_mut().unwrap() = shard.info.clone();
        self.save_manifest()
    }
    pub(crate) fn save_manifest(&mut self) -> BagResult<()> {
        self.update_current_info();
        let mut bytes = serde_json::to_vec_pretty(&self.manifest)?;
        bytes.push(b'\n');
        if bytes.len() > self.options.limits.manifest_bytes {
            return Err(BagError::resource("manifest exceeds configured limit"));
        }
        aos_core::write_private_atomic(self.directory.join("manifest.json"), &bytes)?;
        Ok(())
    }
    pub(crate) fn finish(
        &mut self,
        reason: RecordStopReason,
        mut counters: RecordCounters,
        error: Option<BagError>,
    ) -> BagResult<Manifest> {
        // Writer-confirmed totals are authoritative even if manager counters
        // raced the last write; queue reception never stands in for a write.
        counters.written_total = self.manifest.counters.written_total;
        counters.written_bytes = self.manifest.counters.written_bytes;
        let unconfirmed = self
            .current
            .as_ref()
            .map_or(0, |shard| shard.chunk_messages as u64);
        let mut error = error;
        if self.current.as_ref().is_some_and(|shard| shard.failed) {
            counters.unconfirmed_on_failure_total += unconfirmed;
            self.update_current_info();
            self.current.take();
        } else if let Err(close_error) = self.close_shard() {
            counters.unconfirmed_on_failure_total += unconfirmed;
            self.manifest.cleanup_error = Some(close_error.clone());
            error.get_or_insert(close_error);
        }
        self.manifest.counters = counters;
        self.manifest.state = if error.is_some() {
            "failed"
        } else {
            "finished"
        }
        .into();
        self.manifest.clean_close = self.manifest.shards.iter().all(|s| s.clean_close);
        self.manifest.incomplete = !self.manifest.clean_close
            || error.is_some()
            || !self.manifest.recovery_sources.is_empty();
        self.manifest.stop_reason = Some(match error.as_ref().map(|e| e.kind) {
            Some(BagErrorKind::DiskFull) => RecordStopReason::DiskFull,
            Some(BagErrorKind::CapacityLimit) => RecordStopReason::CapacityLimit,
            Some(BagErrorKind::ResourceLimit) => RecordStopReason::ResourceLimit,
            Some(_) => RecordStopReason::IoError,
            None => reason,
        });
        self.manifest.error = error;
        if let Err(save_error) = self.save_manifest() {
            if let Some(cleanup_error) = &mut self.manifest.cleanup_error {
                // Keep the first cleanup kind/offset and expose the later
                // persistence failure without replacing the operational error.
                cleanup_error
                    .message
                    .push_str(&format!("; final manifest save failed: {save_error}"));
            } else {
                self.manifest.cleanup_error = Some(save_error.clone());
            }
            // Preserve the operational error even if the final report cannot
            // reach disk. The in-memory manifest retains the cleanup failure.
            return Err(self.manifest.error.clone().unwrap_or(save_error));
        }
        Ok(self.manifest.clone())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct FileStamp {
    size: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
    #[cfg(windows)]
    identity: (u64, u64),
}
impl FileStamp {
    fn read(metadata: &fs::Metadata) -> BagResult<Self> {
        if !metadata.file_type().is_file() {
            return Err(BagError::invalid("bag source must be a regular file"));
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if metadata.file_attributes() & 0x400 != 0 {
                return Err(BagError::invalid("bag source cannot be a reparse point"));
            }
        }
        Ok(Self {
            size: metadata.len(),
            modified: metadata.modified().ok(),
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            },
            #[cfg(windows)]
            identity: {
                use std::os::windows::fs::MetadataExt;
                // Timestamps are change indicators, not a stable file ID.
                // Equal-metadata replacements between cursors can be missed.
                (metadata.creation_time(), metadata.last_write_time())
            },
        })
    }
}

/// Remembers validated source metadata without keeping a descriptor open for
/// every shard. Each active cursor reopens with no-follow and verifies its fd
/// against this stamp before use and after a validation pass/chunk. This is a
/// source stability check, not a snapshot against a privileged writer. Unix
/// stamps include device/inode identity and ctime; Windows timestamps cannot
/// detect equal-metadata replacements between active cursors.
pub(crate) struct PinnedFile {
    path: PathBuf,
    stamp: FileStamp,
    #[cfg(test)]
    bytes_read: AtomicU64,
}
impl PinnedFile {
    pub(crate) fn open(path: &Path) -> BagResult<Arc<Self>> {
        let file = regular_file(path)?;
        let stamp = FileStamp::read(&file.metadata()?)?;
        let this = Arc::new(Self {
            path: path.into(),
            stamp,
            #[cfg(test)]
            bytes_read: AtomicU64::new(0),
        });
        this.check_unchanged()?;
        Ok(this)
    }
    pub(crate) fn len(&self) -> u64 {
        self.stamp.size
    }
    #[cfg(test)]
    pub(crate) fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
    pub(crate) fn check_unchanged(&self) -> BagResult<()> {
        let named = FileStamp::read(&fs::symlink_metadata(&self.path)?)
            .map_err(|_| BagError::corrupt("bag source pathname no longer names a regular file"))?;
        if named != self.stamp {
            return Err(BagError::corrupt(
                "bag source changed after its identity was pinned",
            ));
        }
        Ok(())
    }
    pub(crate) fn cursor(self: &Arc<Self>) -> BagResult<PinnedCursor> {
        let file = regular_file(&self.path)?;
        if FileStamp::read(&file.metadata()?)? != self.stamp {
            return Err(BagError::corrupt("bag source changed before reopening"));
        }
        self.check_unchanged()?;
        Ok(PinnedCursor {
            source: self.clone(),
            file,
        })
    }
    pub(crate) fn sha256(self: &Arc<Self>) -> BagResult<String> {
        let mut source = self.cursor()?;
        let mut hash = Sha256::new();
        let mut bytes = [0u8; 64 * 1024];
        loop {
            let n = source.read(&mut bytes)?;
            if n == 0 {
                break;
            }
            hash.update(&bytes[..n]);
        }
        source.check_unchanged()?;
        Ok(format!("{:x}", hash.finalize()))
    }
}

pub(crate) struct PinnedCursor {
    source: Arc<PinnedFile>,
    file: File,
}
impl PinnedCursor {
    pub(crate) fn check_unchanged(&self) -> BagResult<()> {
        if FileStamp::read(&self.file.metadata()?)? != self.source.stamp {
            return Err(BagError::corrupt("opened bag source changed while reading"));
        }
        self.source.check_unchanged()
    }
}
impl Read for PinnedCursor {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(bytes)?;
        #[cfg(test)]
        self.source
            .bytes_read
            .fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}
impl Seek for PinnedCursor {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        self.file.seek(offset)
    }
}

pub(crate) fn file_sha256(path: &Path) -> BagResult<String> {
    PinnedFile::open(path)?.sha256()
}

pub(crate) fn regular_file(path: &Path) -> BagResult<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT
        options.custom_flags(0x00200000);
        // Exclude incompatible write/delete access while this handle is open.
        // PinnedFile retains no handle between cursors, so this does not protect
        // those intervals against replacements with matching metadata.
        options.share_mode(0x00000001); // FILE_SHARE_READ
    }
    let file = options.open(path).map_err(|error| {
        #[cfg(unix)]
        if matches!(error.raw_os_error(), Some(libc::EMFILE | libc::ENFILE)) {
            return BagError::resource("not enough file descriptors to pin bag source shards");
        }
        BagError::from(error)
    })?;
    let opened = FileStamp::read(&file.metadata()?)?;
    let named = FileStamp::read(&fs::symlink_metadata(path)?)?;
    if opened != named {
        return Err(BagError::corrupt(
            "bag source changed while it was being opened",
        ));
    }
    Ok(file)
}

pub(crate) fn shard_path(directory: &Path, file: &str) -> BagResult<PathBuf> {
    let path = Path::new(file);
    if path.components().count() != 1
        || path.file_name().and_then(|s| s.to_str()) != Some(file)
        || !(file.ends_with(".mcap") || file.ends_with(".mcap.partial"))
    {
        return Err(BagError::corrupt(
            "manifest contains an invalid shard filename",
        ));
    }
    Ok(directory.join(path))
}

#[cfg(test)]
mod admission_and_pinning_tests {
    use super::*;
    use crate::storage_tests::{interface, message, storage_options, Temp};
    use aos_core::QoS;
    use serde_json::json;

    fn channel() -> Arc<ChannelInfo> {
        Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &interface(),
                vec![QoS::default()],
                QoS::default(),
                None,
            )
            .unwrap(),
        )
    }
    fn writer(path: &Path, options: StorageOptions) -> StorageWriter {
        StorageWriter::create(
            path,
            options,
            uuid::Uuid::new_v4().to_string(),
            10,
            json!({}),
        )
        .unwrap()
    }

    #[test]
    fn shard_limit_is_a_typed_clean_stop_before_closing_the_current_shard() {
        let temp = Temp::new();
        let directory = temp.0.join("shard-limit");
        let mut options = storage_options();
        options.messages_per_shard = 1;
        options.limits.shards = 1;
        let mut writer = writer(&directory, options.clone());
        let channel = channel();
        assert!(matches!(
            writer.append_admitted(&message(&channel, 1, 10)).unwrap(),
            AppendOutcome::Written
        ));
        assert!(matches!(
            writer.append_admitted(&message(&channel, 2, 11)).unwrap(),
            AppendOutcome::Limit(RecordStopReason::ResourceLimit)
        ));
        assert!(writer.current.is_some());
        assert_eq!(writer.manifest.shards.len(), 1);
        let manifest = writer
            .finish(
                RecordStopReason::ResourceLimit,
                RecordCounters::default(),
                None,
            )
            .unwrap();
        assert!(manifest.clean_close && !manifest.incomplete);
        assert!(manifest.error.is_none());
        assert_eq!(manifest.stop_reason, Some(RecordStopReason::ResourceLimit));
        assert_eq!(
            crate::inspect(
                &directory,
                crate::InspectOptions {
                    limits: options.limits,
                    ..Default::default()
                }
            )
            .unwrap()
            .messages,
            1
        );
    }

    #[test]
    fn rejected_channel_is_not_a_storage_failure_and_valid_data_still_closes() {
        let temp = Temp::new();
        let directory = temp.0.join("rejected-channel");
        let options = storage_options();
        let mut writer = writer(&directory, options.clone());
        let good = channel();
        writer.append(&message(&good, 1, 10)).unwrap();
        let mut definition = interface().definition().clone();
        definition["name"] = json!("test/msg/Large");
        definition["message"]["description"] = json!("x".repeat(options.limits.schema_bytes));
        let large = aos_core::Interface::from_json(definition).unwrap();
        let rejected = Arc::new(
            ChannelInfo::new(
                "/large".into(),
                &large,
                vec![QoS::default()],
                QoS::default(),
                None,
            )
            .unwrap(),
        );
        assert_eq!(
            precheck_channel(&rejected, &options).unwrap_err().kind,
            BagErrorKind::ResourceLimit
        );
        assert_eq!(
            writer
                .register_admitted(rejected)
                .unwrap()
                .unwrap_err()
                .kind,
            BagErrorKind::ResourceLimit
        );
        assert!(!writer.current.as_ref().unwrap().failed);
        writer.append(&message(&good, 2, 11)).unwrap();
        let manifest = writer
            .finish(RecordStopReason::Requested, RecordCounters::default(), None)
            .unwrap();
        assert!(manifest.clean_close && !manifest.incomplete && manifest.error.is_none());
        assert_eq!(manifest.counters.written_total, 2);
    }

    #[test]
    fn actual_append_failure_is_an_outer_error_not_a_clean_limit() {
        let temp = Temp::new();
        let mut writer = writer(&temp.0.join("write-failure"), storage_options());
        let channel = channel();
        writer.register(channel.clone()).unwrap();
        writer.fail_next_write(io::ErrorKind::StorageFull);
        let error = writer
            .append_admitted(&message(&channel, 1, 10))
            .err()
            .unwrap();
        assert_eq!(error.kind, BagErrorKind::DiskFull);
        assert!(writer.current.as_ref().unwrap().failed);
    }

    #[test]
    fn finish_preserves_primary_error_kind_and_records_cleanup_failure_separately() {
        let temp = Temp::new();
        let directory = temp.0.join("finish-double-failure");
        let mut writer = writer(&directory, storage_options());
        writer.append(&message(&channel(), 1, 10)).unwrap();
        writer.fail_next_write(io::ErrorKind::StorageFull);
        let primary = BagError::resource("recovery destination descriptor limit");
        let manifest = writer
            .finish(
                RecordStopReason::ResourceLimit,
                RecordCounters::default(),
                Some(primary.clone()),
            )
            .unwrap();
        assert_eq!(
            manifest.error.as_ref().unwrap().kind,
            BagErrorKind::ResourceLimit
        );
        assert_eq!(manifest.error.as_ref().unwrap().message, primary.message);
        assert_eq!(
            manifest.cleanup_error.as_ref().unwrap().kind,
            BagErrorKind::DiskFull
        );
        assert_eq!(manifest.stop_reason, Some(RecordStopReason::ResourceLimit));
        assert_eq!(manifest.state, "failed");
        assert!(!manifest.clean_close);
        let reread = crate::offline::read_manifest(&directory, &storage_options().limits).unwrap();
        assert_eq!(reread.error.unwrap().kind, BagErrorKind::ResourceLimit);
        assert_eq!(reread.cleanup_error.unwrap().kind, BagErrorKind::DiskFull);
    }

    #[test]
    fn storage_admission_covers_the_default_playback_index_budget() {
        let mut options = crate::StorageOptions::default();
        assert!(options.playback_index_upper_bound() <= crate::ReaderLimits::default().index_bytes);
        options.validate().unwrap();
        options.limits.index_bytes = options.playback_index_upper_bound() - 1;
        assert!(options
            .validate()
            .unwrap_err()
            .message
            .contains("index_bytes"));
    }

    #[test]
    fn final_manifest_write_failure_also_cannot_replace_the_primary_error() {
        let temp = Temp::new();
        let directory = temp.0.join("manifest-write-failure");
        let mut writer = writer(&directory, storage_options());
        writer.append(&message(&channel(), 1, 10)).unwrap();
        fs::remove_file(directory.join("manifest.json")).unwrap();
        fs::create_dir(directory.join("manifest.json")).unwrap();
        let primary = BagError::resource("original destination limit");
        let result = writer.finish(
            RecordStopReason::ResourceLimit,
            RecordCounters::default(),
            Some(primary.clone()),
        );
        assert_eq!(result.unwrap_err().kind, BagErrorKind::ResourceLimit);
        assert_eq!(
            writer.manifest.error.as_ref().unwrap().message,
            primary.message
        );
        assert!(writer.manifest.cleanup_error.is_some());
        assert!(writer.current.is_none());
    }

    fn assert_close_and_manifest_failures_are_both_retained(primary: Option<BagError>) {
        let temp = Temp::new();
        let directory = temp.0.join("close-and-manifest-failure");
        let mut writer = writer(&directory, storage_options());
        writer.append(&message(&channel(), 1, 10)).unwrap();
        writer.fail_next_write(io::ErrorKind::StorageFull);
        let manifest_path = directory.join("manifest.json");
        fs::remove_file(&manifest_path).unwrap();
        fs::create_dir(&manifest_path).unwrap();
        let expected = primary
            .clone()
            .unwrap_or_else(|| BagError::new(BagErrorKind::DiskFull, "injected bag write failure"));
        let result = writer.finish(
            RecordStopReason::Requested,
            RecordCounters::default(),
            primary,
        );
        assert_eq!(result.unwrap_err(), expected);
        assert_eq!(writer.manifest.error.as_ref(), Some(&expected));
        let cleanup = writer.manifest.cleanup_error.as_ref().unwrap();
        assert_eq!(cleanup.kind, BagErrorKind::DiskFull);
        assert_eq!(cleanup.file_offset, None);
        assert!(cleanup.message.starts_with("injected bag write failure"));
        assert!(
            cleanup
                .message
                .contains("; final manifest save failed: Io:"),
            "later cleanup failure was lost: {}",
            cleanup.message
        );
        assert_eq!(writer.manifest.state, "failed");
        assert_eq!(
            writer.manifest.stop_reason,
            Some(if expected.kind == BagErrorKind::DiskFull {
                RecordStopReason::DiskFull
            } else {
                RecordStopReason::ResourceLimit
            })
        );
        assert!(!writer.manifest.clean_close && writer.manifest.incomplete);
        assert!(writer.current.is_none());
        assert!(manifest_path.is_dir());
        let serialized = serde_json::to_vec(&writer.manifest).unwrap();
        let roundtrip: Manifest = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(roundtrip.error.as_ref(), Some(&expected));
        assert_eq!(roundtrip.cleanup_error.as_ref(), Some(cleanup));
    }

    #[test]
    fn close_and_manifest_failures_preserve_an_existing_primary_error() {
        assert_close_and_manifest_failures_are_both_retained(Some(
            BagError::resource("original destination limit").at(29),
        ));
    }

    #[test]
    fn close_and_manifest_failures_retain_the_first_cleanup_as_primary() {
        assert_close_and_manifest_failures_are_both_retained(None);
    }

    #[cfg(unix)]
    #[test]
    fn pinned_source_never_follows_a_replacement_or_symlink() {
        let temp = Temp::new();
        let path = temp.0.join("source");
        fs::write(&path, b"original").unwrap();
        let source = PinnedFile::open(&path).unwrap();
        source.sha256().unwrap();
        let mut opened = source.cursor().unwrap();
        fs::rename(&path, temp.0.join("original")).unwrap();
        fs::write(&path, b"replaced").unwrap();
        let mut original = Vec::new();
        opened.read_to_end(&mut original).unwrap();
        assert_eq!(original, b"original");
        assert!(opened.check_unchanged().is_err());
        assert!(source.cursor().is_err());
        assert_eq!(
            source.check_unchanged().unwrap_err().kind,
            BagErrorKind::Corrupt
        );
        fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(temp.0.join("original"), &path).unwrap();
        assert!(regular_file(&path).is_err());
        assert_eq!(
            source.check_unchanged().unwrap_err().kind,
            BagErrorKind::Corrupt
        );
    }

    #[cfg(unix)]
    #[test]
    fn pinned_source_detects_same_size_in_place_modification() {
        let temp = Temp::new();
        let path = temp.0.join("source");
        fs::write(&path, b"original").unwrap();
        let source = PinnedFile::open(&path).unwrap();
        source.sha256().unwrap();
        std::thread::sleep(Duration::from_millis(2));
        fs::write(&path, b"modified").unwrap();
        assert_eq!(source.sha256().unwrap_err().kind, BagErrorKind::Corrupt);
    }
}
