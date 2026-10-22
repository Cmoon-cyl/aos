use crate::{
    format::{decode_channel, RecordingHeader},
    storage::{PinnedCursor, PinnedFile, RecordedMessage},
    types::RECORDING_METADATA,
    BagError, BagResult, ChannelInfo, ChannelRole, ReaderLimits, SampleMetadata, PROFILE,
};
use aos_core::Interface;
use mcap::{
    records::{Channel, Record, SchemaHeader},
    sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions},
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::path::Path;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

struct RawStream {
    file: PinnedCursor,
    source: Arc<PinnedFile>,
    reader: LinearReader,
    offset: u64,
    ended: bool,
    limit: usize,
}
impl RawStream {
    fn new(source: Arc<PinnedFile>, limits: &ReaderLimits) -> BagResult<Self> {
        source.check_unchanged()?;
        Ok(Self {
            file: source.cursor()?,
            source,
            reader: LinearReader::new_with_options(
                LinearReaderOptions::default()
                    .with_emit_chunks(true)
                    .with_validate_data_section_crc(true)
                    .with_validate_summary_section_crc(true)
                    .with_check_finishes_after_end_magic(true)
                    .with_record_length_limit(limits.record_bytes),
            ),
            // The upstream reader consumes leading magic independently.
            offset: mcap::MAGIC.len() as u64,
            ended: false,
            limit: limits.record_bytes,
        })
    }
    fn next(&mut self) -> BagResult<Option<(u64, Record<'static>)>> {
        if self.ended {
            return Ok(None);
        }
        while let Some(event) = self.reader.next_event() {
            match event.map_err(|error| BagError::from(error).at(self.offset))? {
                LinearReadEvent::ReadRequest(need) => {
                    if need > self.limit.saturating_add(4096) {
                        return Err(
                            BagError::resource("MCAP read request exceeds limit").at(self.offset)
                        );
                    }
                    let read = loop {
                        match self.file.read(self.reader.insert(need)) {
                            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                            other => break other?,
                        }
                    };
                    self.reader.notify_read(read);
                }
                LinearReadEvent::Record { opcode, data } => {
                    let offset = self.offset;
                    self.offset = self
                        .offset
                        .checked_add(9 + data.len() as u64)
                        .ok_or_else(|| BagError::resource("MCAP file offset overflow"))?;
                    let record = mcap::parse_record(opcode, data)
                        .map_err(|error| BagError::from(error).at(offset))?
                        .into_owned();
                    return Ok(Some((offset, record)));
                }
            }
        }
        self.ended = true;
        self.file.check_unchanged()?;
        Ok(None)
    }
}

#[derive(Clone)]
struct StoredSchema {
    // MCAP ids are local to a shard, so canonical descriptors use id zero.
    header: SchemaHeader,
    data: Arc<Vec<u8>>,
}
impl StoredSchema {
    fn matches(&self, header: &SchemaHeader, data: &[u8]) -> bool {
        self.header.name == header.name
            && self.header.encoding == header.encoding
            && self.data.as_slice() == data
    }
}
struct StoredChannel {
    schema_id: u16,
    shared: Arc<ChannelDescriptor>,
}
impl std::ops::Deref for StoredChannel {
    type Target = ChannelDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}
impl StoredChannel {
    fn matches(&self, channel: &Channel) -> bool {
        self.schema_id == channel.schema_id && self.shared.matches(channel)
    }
}
struct ChannelDescriptor {
    record: Channel,
    info: Arc<ChannelInfo>,
    interface: Interface,
    schema: Arc<StoredSchema>,
}
impl ChannelDescriptor {
    fn matches(&self, channel: &Channel) -> bool {
        self.record.topic == channel.topic
            && self.record.message_encoding == channel.message_encoding
            && self.record.metadata == channel.metadata
    }
}

/// A recording-wide pool. MCAP ids may differ in each shard; validated AOS
/// content ids and schema content identify the immutable shared allocations.
#[derive(Default)]
pub(crate) struct DescriptorCatalog {
    schemas: BTreeMap<[u8; 32], Arc<StoredSchema>>,
    channels: BTreeMap<String, Arc<ChannelDescriptor>>,
    header: Option<Arc<RecordingHeader>>,
    descriptor_bytes: usize,
    header_bytes: usize,
}
impl DescriptorCatalog {
    pub(crate) fn memory_bytes(&self) -> usize {
        self.descriptor_bytes
            .saturating_mul(4)
            .saturating_add(self.channels.len().saturating_mul(512))
            .saturating_add(self.header_bytes.saturating_mul(2))
    }
    fn schema(&mut self, mut header: SchemaHeader, data: Vec<u8>) -> BagResult<Arc<StoredSchema>> {
        let mut hash = Sha256::new();
        hash.update((header.name.len() as u64).to_le_bytes());
        hash.update(header.name.as_bytes());
        hash.update((header.encoding.len() as u64).to_le_bytes());
        hash.update(header.encoding.as_bytes());
        hash.update(&data);
        let key = hash.finalize().into();
        if let Some(known) = self.schemas.get(&key) {
            if !known.matches(&header, &data) {
                return Err(BagError::corrupt("schema content hash collision"));
            }
            return Ok(known.clone());
        }
        self.descriptor_bytes = self
            .descriptor_bytes
            .saturating_add(data.len() + header.name.len() + 64);
        header.id = 0;
        let schema = Arc::new(StoredSchema {
            header,
            data: Arc::new(data),
        });
        self.schemas.insert(key, schema.clone());
        Ok(schema)
    }
    fn channel(
        &mut self,
        mut record: Channel,
        schema: Arc<StoredSchema>,
    ) -> BagResult<Arc<ChannelDescriptor>> {
        if let Some(known) = record
            .metadata
            .get("aos.channel_id")
            .and_then(|id| self.channels.get(id))
        {
            if !known.matches(&record) || !Arc::ptr_eq(&schema, &known.schema) {
                return Err(BagError::corrupt("channel id changes across shards"));
            }
            return Ok(known.clone());
        }
        let (info, interface) = decode_channel(
            &record,
            &schema.header.name,
            &schema.header.encoding,
            &schema.data,
        )?;
        self.descriptor_bytes = self
            .descriptor_bytes
            .saturating_add(serde_json::to_vec(&info)?.len());
        record.id = 0;
        record.schema_id = 0;
        let shared = Arc::new(ChannelDescriptor {
            record,
            info: Arc::new(info),
            interface,
            schema,
        });
        self.channels.insert(shared.info.id.clone(), shared.clone());
        Ok(shared)
    }
}

#[derive(Clone)]
struct ChunkLocation {
    offset: u64,
    length: u64,
    end: u64,
    business_channels: Box<[u16]>,
}

/// Built only by a full, hash-checked offline pass. Each shard retains one
/// descriptor map, shared by its playback cursors; entries contain no payload.
pub(crate) struct ShardIndex {
    source: Arc<PinnedFile>,
    limits: ReaderLimits,
    header: Arc<RecordingHeader>,
    schemas: BTreeMap<u16, Arc<StoredSchema>>,
    channels: BTreeMap<u16, Arc<StoredChannel>>,
    descriptor_bytes: usize,
    chunks: Box<[ChunkLocation]>,
}
impl ShardIndex {
    pub(crate) fn memory_bytes(&self) -> usize {
        // Shared descriptor allocations are counted by DescriptorCatalog once.
        (self.schemas.len() + self.channels.len())
            .saturating_mul(64)
            .saturating_add(self.chunks.len().saturating_mul(64))
            .saturating_add(
                self.chunks
                    .iter()
                    .map(|c| c.business_channels.len().saturating_mul(2))
                    .sum::<usize>(),
            )
            .saturating_add(512)
    }
    #[cfg(test)]
    pub(crate) fn cursor(self: &Arc<Self>, target: u64) -> BagResult<IndexedReader> {
        self.cursor_selected(target, None)
    }
    pub(crate) fn cursor_selected(
        self: &Arc<Self>,
        target: u64,
        selected: Option<&BTreeSet<String>>,
    ) -> BagResult<IndexedReader> {
        self.source.check_unchanged()?;
        let mut decoder = BagReader::from_file(self.source.clone(), self.limits.clone(), false)?;
        decoder.header_seen = true;
        decoder.header = Some(self.header.as_ref().clone());
        decoder.schemas = self.schemas.clone();
        decoder.channels = self.channels.clone();
        decoder.descriptor_bytes = self.descriptor_bytes;
        Ok(IndexedReader {
            index: self.clone(),
            next_chunk: self.chunks.partition_point(|chunk| chunk.end < target),
            decoder,
            selected: selected.map(|ids| {
                self.channels
                    .iter()
                    .filter_map(|(id, channel)| ids.contains(&channel.info.id).then_some(*id))
                    .collect()
            }),
        })
    }
}

pub(crate) struct IndexedReader {
    index: Arc<ShardIndex>,
    next_chunk: usize,
    decoder: BagReader,
    selected: Option<BTreeSet<u16>>,
}
// One stack result is live at a time; boxing would allocate for every replayed
// message merely to make the two empty scheduling variants smaller.
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexedRead {
    Message(RecordedMessage),
    Progress,
    End,
}
impl IndexedReader {
    #[cfg(test)]
    pub(crate) fn next_message(&mut self) -> BagResult<Option<RecordedMessage>> {
        loop {
            match self.next_step()? {
                IndexedRead::Message(message) => return Ok(Some(message)),
                IndexedRead::Progress => {}
                IndexedRead::End => return Ok(None),
            }
        }
    }
    /// At most one chunk is handled per step, including irrelevant chunks.
    /// The dispatch loop can service controls at every such boundary.
    pub(crate) fn next_step(&mut self) -> BagResult<IndexedRead> {
        if let Some(message) = self.decoder.ready.pop_front() {
            return Ok(IndexedRead::Message(message));
        }
        let Some(chunk) = self.index.chunks.get(self.next_chunk) else {
            self.index.source.check_unchanged()?;
            return Ok(IndexedRead::End);
        };
        if self.selected.as_ref().is_some_and(|selected| {
            !chunk
                .business_channels
                .iter()
                .any(|id| selected.contains(id))
        }) {
            self.next_chunk += 1;
            return Ok(IndexedRead::Progress);
        }
        self.index.source.check_unchanged()?;
        let mut file = self.index.source.cursor()?;
        file.seek(SeekFrom::Start(chunk.offset))?;
        let mut prefix = [0u8; 9];
        file.read_exact(&mut prefix)?;
        let length = u64::from_le_bytes(prefix[1..].try_into().unwrap());
        if length > self.index.limits.record_bytes as u64 || length + 9 != chunk.length {
            return Err(BagError::corrupt("indexed chunk length changed").at(chunk.offset));
        }
        let mut bytes = vec![0u8; length as usize];
        file.read_exact(&mut bytes)?;
        let record = mcap::parse_record(prefix[0], &bytes)?.into_owned();
        if !matches!(record, Record::Chunk { .. }) {
            return Err(BagError::corrupt("indexed record is not a chunk").at(chunk.offset));
        }
        self.decoder.accept_outer(chunk.offset, record)?;
        file.check_unchanged()?;
        self.index.source.check_unchanged()?;
        self.next_chunk += 1;
        Ok(IndexedRead::Progress)
    }
}

/// Only a fully CRC-checked chunk can populate ready. The same transaction
/// commits its schema/channel definitions and complete business/metadata pairs.
pub(crate) struct BagReader {
    raw: RawStream,
    limits: ReaderLimits,
    recovering: bool,
    header_seen: bool,
    data_end: bool,
    footer: bool,
    summary_start: Option<u64>,
    summary_offsets_start: Option<u64>,
    statistics_seen: bool,
    schemas: BTreeMap<u16, Arc<StoredSchema>>,
    channels: BTreeMap<u16, Arc<StoredChannel>>,
    descriptor_bytes: usize,
    ready: VecDeque<RecordedMessage>,
    previous: Option<(u64, u64)>,
    message_counts: BTreeMap<u16, u64>,
    message_count: u64,
    pub(crate) header: Option<RecordingHeader>,
    pub(crate) orphan_business: u64,
    pub(crate) orphan_metadata: u64,
    index: Option<Vec<ChunkLocation>>,
    catalog: Option<Arc<Mutex<DescriptorCatalog>>>,
}
impl BagReader {
    #[cfg(test)]
    pub(crate) fn open(path: &Path, limits: ReaderLimits, recovering: bool) -> BagResult<Self> {
        Self::from_file(PinnedFile::open(path)?, limits, recovering)
    }
    pub(crate) fn from_file(
        source: Arc<PinnedFile>,
        limits: ReaderLimits,
        recovering: bool,
    ) -> BagResult<Self> {
        limits.validate()?;
        Ok(Self {
            raw: RawStream::new(source, &limits)?,
            limits,
            recovering,
            header_seen: false,
            data_end: false,
            footer: false,
            summary_start: None,
            summary_offsets_start: None,
            statistics_seen: false,
            schemas: BTreeMap::new(),
            channels: BTreeMap::new(),
            descriptor_bytes: 0,
            ready: VecDeque::new(),
            previous: None,
            message_counts: BTreeMap::new(),
            message_count: 0,
            header: None,
            orphan_business: 0,
            orphan_metadata: 0,
            index: None,
            catalog: None,
        })
    }
    pub(crate) fn enable_index(&mut self, catalog: Arc<Mutex<DescriptorCatalog>>) {
        self.index = Some(Vec::new());
        self.catalog = Some(catalog);
    }
    pub(crate) fn into_index(self) -> BagResult<Arc<ShardIndex>> {
        if !self.raw.ended || !self.footer {
            return Err(BagError::corrupt(
                "index requires complete shard verification",
            ));
        }
        self.raw.source.check_unchanged()?;
        let header = self
            .header
            .ok_or_else(|| BagError::corrupt("index lacks recording header"))?;
        let mut catalog = self.catalog.as_ref().unwrap().lock().unwrap();
        let header = if let Some(shared) = &catalog.header {
            if shared.as_ref() != &header {
                return Err(BagError::corrupt("recording header changes across shards"));
            }
            shared.clone()
        } else {
            catalog.header_bytes = serde_json::to_vec(&header)?.len();
            let header = Arc::new(header);
            catalog.header = Some(header.clone());
            header
        };
        Ok(Arc::new(ShardIndex {
            source: self.raw.source,
            limits: self.limits,
            header,
            schemas: self.schemas,
            channels: self.channels,
            descriptor_bytes: self.descriptor_bytes,
            chunks: self
                .index
                .ok_or_else(|| BagError::invalid("index collection was not enabled"))?
                .into_boxed_slice(),
        }))
    }
    pub(crate) fn read_header(&mut self) -> BagResult<RecordingHeader> {
        while self.header.is_none() {
            self.advance()?;
            if self.raw.ended {
                return Err(BagError::corrupt("missing AOS recording metadata"));
            }
        }
        Ok(self.header.clone().unwrap())
    }
    pub(crate) fn channels(&self) -> impl Iterator<Item = &ChannelInfo> {
        self.channels
            .values()
            .filter(|c| c.info.role == ChannelRole::Business)
            .map(|c| c.info.as_ref())
    }
    pub(crate) fn next_message(&mut self) -> BagResult<Option<RecordedMessage>> {
        loop {
            if let Some(message) = self.ready.pop_front() {
                return Ok(Some(message));
            }
            if self.raw.ended {
                return Ok(None);
            }
            self.advance()?;
        }
    }
    fn advance(&mut self) -> BagResult<()> {
        let Some((offset, record)) = self.raw.next()? else {
            if !self.header_seen || self.header.is_none() || !self.data_end || !self.footer {
                return Err(BagError::corrupt(
                    "missing MCAP header, AOS metadata, data end, or footer",
                )
                .at(self.raw.offset));
            }
            return Ok(());
        };
        self.accept_outer(offset, record).map_err(|error| {
            if error.file_offset.is_some() {
                error
            } else {
                error.at(offset)
            }
        })
    }
    fn accept_outer(&mut self, offset: u64, record: Record<'static>) -> BagResult<()> {
        if !self.header_seen {
            match record {
                Record::Header(header) if header.profile == PROFILE => {
                    self.header_seen = true;
                    return Ok(());
                }
                _ => {
                    return Err(BagError::corrupt(
                        "first record must use the aos-json-v1 profile",
                    ))
                }
            }
        }
        if self.footer {
            return Err(BagError::corrupt("record after MCAP footer"));
        }
        if self.data_end && !matches!(record, Record::Footer(_)) {
            self.summary_start.get_or_insert(offset);
        }
        match record {
            Record::Chunk { header, data } => {
                if self.data_end || self.header.is_none() {
                    return Err(BagError::corrupt("chunk outside initialized data section"));
                }
                if !header.compression.is_empty()
                    || header.uncompressed_crc == 0
                    || header.uncompressed_size != data.len() as u64
                    || header.compressed_size != data.len() as u64
                    || data.len() > self.limits.record_bytes
                {
                    return Err(BagError::corrupt(
                        "chunk must be bounded, uncompressed, and have a nonzero CRC",
                    ));
                }
                let expected_bounds = (header.message_start_time, header.message_end_time);
                let records = mcap::read::ChunkReader::new(header, &data)?;
                let previous_count = self.message_count;
                let business_channels = self.accept_chunk(records, expected_bounds)?;
                if self.message_count != previous_count {
                    if let Some(index) = &mut self.index {
                        let max_entries = self.limits.index_bytes / 64;
                        if index.len() >= max_entries {
                            return Err(BagError::resource("playback chunk index limit exceeded"));
                        }
                        if index.len() == index.capacity() {
                            index
                                .try_reserve_exact((max_entries - index.len()).min(1024))
                                .map_err(|_| {
                                    BagError::resource("playback index allocation failed")
                                })?;
                        }
                        index.push(ChunkLocation {
                            offset,
                            length: self.raw.offset - offset,
                            end: expected_bounds.1,
                            business_channels: business_channels.into_boxed_slice(),
                        });
                    }
                }
                Ok(())
            }
            Record::Metadata(metadata) if !self.data_end && metadata.name == RECORDING_METADATA => {
                let text = metadata
                    .metadata
                    .get("header")
                    .ok_or_else(|| BagError::corrupt("missing AOS metadata header"))?;
                if text.len() > self.limits.manifest_bytes || self.header.is_some() {
                    return Err(BagError::corrupt(
                        "duplicate/oversized AOS recording metadata",
                    ));
                }
                let header: RecordingHeader = serde_json::from_str(text)?;
                header.validate()?;
                self.header = Some(header);
                Ok(())
            }
            Record::DataEnd(end) => {
                if self.data_end || end.data_section_crc == 0 {
                    return Err(BagError::corrupt("missing data CRC or duplicate DataEnd"));
                }
                self.data_end = true;
                Ok(())
            }
            Record::Footer(footer) => {
                if !self.data_end
                    || footer.summary_start != self.summary_start.unwrap_or(0)
                    || footer.summary_offset_start != self.summary_offsets_start.unwrap_or(0)
                    || (self.summary_start.is_some() && footer.summary_crc == 0)
                    || !self.statistics_seen
                {
                    return Err(BagError::corrupt(
                        "invalid footer/summary offsets, CRC, or statistics",
                    ));
                }
                self.footer = true;
                Ok(())
            }
            Record::Schema { header, data } if self.data_end => {
                let known = self
                    .schemas
                    .get(&header.id)
                    .ok_or_else(|| BagError::corrupt("summary introduces an unknown schema"))?;
                if !known.matches(&header, data.as_ref()) {
                    return Err(BagError::corrupt(
                        "summary schema differs from data section",
                    ));
                }
                Ok(())
            }
            Record::Channel(channel) if self.data_end => {
                let known = self
                    .channels
                    .get(&channel.id)
                    .ok_or_else(|| BagError::corrupt("summary introduces an unknown channel"))?;
                if !known.matches(&channel) {
                    return Err(BagError::corrupt(
                        "summary channel differs from data section",
                    ));
                }
                Ok(())
            }
            Record::Statistics(statistics) if self.data_end => {
                if self.statistics_seen
                    || statistics.message_count != self.message_count
                    || statistics.channel_count != self.channels.len() as u32
                    || statistics.schema_count != self.schemas.len() as u16
                    || statistics.channel_message_counts != self.message_counts
                {
                    return Err(BagError::corrupt(
                        "summary statistics differ from verified data",
                    ));
                }
                self.statistics_seen = true;
                Ok(())
            }
            Record::SummaryOffset(_) if self.data_end => {
                self.summary_offsets_start.get_or_insert(offset);
                Ok(())
            }
            Record::MessageIndex(_) if !self.data_end => Ok(()),
            Record::ChunkIndex(_) | Record::MetadataIndex(_) if self.data_end => Ok(()),
            _ => Err(BagError::corrupt(
                "unsupported or misplaced record in aos-json-v1",
            )),
        }
    }
    fn accept_chunk<'a>(
        &mut self,
        records: impl Iterator<Item = mcap::McapResult<Record<'a>>>,
        expected_bounds: (u64, u64),
    ) -> BagResult<Vec<u16>> {
        let mut schemas = self.schemas.clone();
        let mut channels = self.channels.clone();
        let mut descriptor_bytes = self.descriptor_bytes;
        let mut business = BTreeMap::<u64, RecordedMessage>::new();
        let mut metadata = BTreeMap::<
            u64,
            (
                Arc<ChannelInfo>,
                mcap::records::MessageHeader,
                SampleMetadata,
            ),
        >::new();
        let mut order = Vec::new();
        let mut counts = BTreeMap::<u16, u64>::new();
        let mut bounds: Option<(u64, u64)> = None;
        for (index, record) in records.enumerate() {
            if index >= self.limits.chunk_records {
                return Err(BagError::resource("too many records in a chunk"));
            }
            // Completing this iterator verifies the chunk CRC. Nothing below
            // is committed to self until the entire iteration succeeds.
            match record? {
                Record::Schema { header, data } => {
                    if header.id == 0 {
                        return Err(BagError::corrupt("schema id zero is invalid"));
                    }
                    if data.len() > self.limits.schema_bytes {
                        return Err(BagError::resource("schema size exceeded"));
                    }
                    if let Some(known) = schemas.get(&header.id) {
                        if !known.matches(&header, data.as_ref()) {
                            return Err(BagError::corrupt("schema id was redefined"));
                        }
                    } else {
                        descriptor_bytes = descriptor_bytes
                            .checked_add(data.len() + header.name.len() + 64)
                            .ok_or_else(|| BagError::resource("descriptor size overflow"))?;
                        let id = header.id;
                        let schema = match &self.catalog {
                            Some(catalog) => {
                                catalog.lock().unwrap().schema(header, data.into_owned())?
                            }
                            None => Arc::new(StoredSchema {
                                header,
                                data: Arc::new(data.into_owned()),
                            }),
                        };
                        schemas.insert(id, schema);
                    }
                }
                Record::Channel(channel) => {
                    if let Some(known) = channels.get(&channel.id) {
                        if !known.matches(&channel) {
                            return Err(BagError::corrupt("channel id was redefined"));
                        }
                    } else {
                        if channels.len() >= self.limits.channels {
                            return Err(BagError::resource("channel count exceeded"));
                        }
                        let schema = schemas.get(&channel.schema_id).ok_or_else(|| {
                            BagError::corrupt("channel references unknown schema")
                        })?;
                        let id = channel.id;
                        let schema_id = channel.schema_id;
                        let shared = match &self.catalog {
                            Some(catalog) => {
                                catalog.lock().unwrap().channel(channel, schema.clone())?
                            }
                            None => {
                                let (info, interface) = decode_channel(
                                    &channel,
                                    &schema.header.name,
                                    &schema.header.encoding,
                                    &schema.data,
                                )?;
                                Arc::new(ChannelDescriptor {
                                    record: channel,
                                    info: Arc::new(info),
                                    interface,
                                    schema: schema.clone(),
                                })
                            }
                        };
                        descriptor_bytes = descriptor_bytes
                            .checked_add(serde_json::to_vec(shared.info.as_ref())?.len())
                            .ok_or_else(|| BagError::resource("descriptor size overflow"))?;
                        channels.insert(id, Arc::new(StoredChannel { schema_id, shared }));
                    }
                }
                Record::Message { header, data } => {
                    let known = channels
                        .get(&header.channel_id)
                        .ok_or_else(|| BagError::corrupt("message references unknown channel"))?;
                    if header.publish_time != header.log_time
                        || header.log_time < self.header.as_ref().unwrap().system_anchor_ns
                    {
                        return Err(BagError::corrupt(
                            "unavailable publish time must equal anchored log time",
                        ));
                    }
                    let payload_value = serde_json::from_slice(&data)?;
                    known
                        .interface
                        .validate("message", &payload_value)
                        .map_err(|error| match error {
                            aos_core::Error::ResourceLimit(_) => BagError::from(error),
                            _ => BagError::corrupt(error.to_string()),
                        })?;
                    let sequence =
                        (u64::from(known.info.sequence_epoch) << 32) | u64::from(header.sequence);
                    bounds = Some(match bounds {
                        None => (header.log_time, header.log_time),
                        Some((lo, hi)) => (lo.min(header.log_time), hi.max(header.log_time)),
                    });
                    *counts.entry(header.channel_id).or_default() += 1;
                    match known.info.role {
                        ChannelRole::Business => {
                            if business.contains_key(&sequence) {
                                return Err(BagError::corrupt(
                                    "duplicate business sequence in one chunk",
                                ));
                            }
                            order.push(sequence);
                            business.insert(
                                sequence,
                                RecordedMessage {
                                    channel: known.info.clone(),
                                    sequence,
                                    log_time: header.log_time,
                                    publish_time: header.publish_time,
                                    payload: data.into_owned(),
                                    // Replaced only after its companion validates.
                                    metadata: SampleMetadata {
                                        recording_sequence: sequence,
                                        channel_id: known.info.id.clone(),
                                        recorded_payload_size_bytes: 0,
                                        adapter_payload_size_bytes: 0,
                                        received: None,
                                        source: None,
                                    },
                                },
                            );
                        }
                        ChannelRole::SampleMetadata => {
                            let evidence: SampleMetadata = serde_json::from_value(payload_value)?;
                            if evidence.recording_sequence != sequence
                                || metadata.contains_key(&sequence)
                            {
                                return Err(BagError::corrupt(
                                    "duplicate/mismatched companion sequence",
                                ));
                            }
                            if let Some(received) = &evidence.received {
                                if received.scope != "adapter_delivery"
                                    || received.steady_offset_ns.parse::<i128>().is_err()
                                    || received.system_unix_ns.parse::<i128>().is_err()
                                {
                                    return Err(BagError::corrupt(
                                        "invalid preserved adapter receive evidence",
                                    ));
                                }
                            }
                            metadata.insert(sequence, (known.info.clone(), header, evidence));
                        }
                    }
                }
                _ => {
                    return Err(BagError::corrupt(
                        "only schema, channel and message records are allowed inside a chunk",
                    ))
                }
            }
            if descriptor_bytes > self.limits.descriptor_bytes {
                return Err(BagError::resource("descriptor memory limit exceeded"));
            }
        }
        if bounds.unwrap_or((0, 0)) != expected_bounds {
            return Err(BagError::corrupt(
                "chunk message bounds disagree with its contents",
            ));
        }
        let mut ready = VecDeque::new();
        let mut previous = self.previous;
        let mut orphan_business = 0;
        for sequence in order {
            let mut message = business.remove(&sequence).unwrap();
            let Some((companion, header, evidence)) = metadata.remove(&sequence) else {
                orphan_business += 1;
                continue;
            };
            if companion.companion_for.as_ref() != Some(&message.channel.id)
                || evidence.channel_id != message.channel.id
                || evidence.recorded_payload_size_bytes != message.payload.len() as u64
                || header.log_time != message.log_time
                || header.publish_time != message.publish_time
            {
                return Err(BagError::corrupt(
                    "business/metadata pair identity, bytes, or time mismatch",
                ));
            }
            if previous.is_some_and(|(seq, time)| sequence <= seq || message.log_time < time) {
                return Err(BagError::corrupt(
                    "messages are not in recording sequence/log-time order",
                ));
            }
            previous = Some((sequence, message.log_time));
            message.metadata = evidence;
            ready.push_back(message);
        }
        let orphan_metadata = metadata.len() as u64;
        if !self.recovering && (orphan_business != 0 || orphan_metadata != 0) {
            return Err(BagError::corrupt(
                "orphaned business/metadata messages in a verified chunk",
            ));
        }
        let messages = counts
            .values()
            .try_fold(0u64, |sum, count| sum.checked_add(*count))
            .ok_or_else(|| BagError::resource("message count overflow"))?;
        if self
            .message_count
            .checked_add(messages)
            .is_none_or(|n| n > self.limits.messages_per_shard.saturating_mul(2))
        {
            return Err(BagError::resource("per-shard message count exceeded"));
        }
        self.schemas = schemas;
        self.channels = channels;
        self.descriptor_bytes = descriptor_bytes;
        self.previous = previous;
        self.orphan_business += orphan_business;
        self.orphan_metadata += orphan_metadata;
        self.message_count += messages;
        let business_channels = counts
            .keys()
            .filter(|id| self.channels[id].info.role == ChannelRole::Business)
            .copied()
            .collect();
        for (channel, count) in counts {
            *self.message_counts.entry(channel).or_default() += count;
        }
        self.ready = ready;
        Ok(business_channels)
    }
}

#[cfg(test)]
mod indexed_tests {
    use super::*;
    use crate::{
        offline::inspect_indexed,
        storage::StorageWriter,
        storage_tests::{interface, message, storage_options, Temp},
        InspectOptions, RecordCounters, RecordStopReason,
    };
    use aos_core::QoS;
    use serde_json::json;

    #[test]
    fn repeated_late_seek_reads_only_its_target_chunk_and_never_rehashes_prefix_shards() {
        let temp = Temp::new();
        let directory = temp.0.join("indexed-recording");
        let mut options = storage_options();
        options.messages_per_shard = 32;
        options.total_bytes = 16 * 1024 * 1024;
        let mut writer = StorageWriter::create(
            &directory,
            options.clone(),
            uuid::Uuid::new_v4().to_string(),
            10,
            json!({}),
        )
        .unwrap();
        let channel = Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &interface(),
                vec![QoS::default()],
                QoS::default(),
                None,
            )
            .unwrap(),
        );
        for sequence in 0..512 {
            let mut item = message(&channel, sequence, 10 + sequence * 10);
            item.payload =
                serde_json::to_vec(&json!({"value":sequence,"text":"x".repeat(2048)})).unwrap();
            item.metadata.recorded_payload_size_bytes = item.payload.len() as u64;
            item.metadata.adapter_payload_size_bytes = item.payload.len() as u64;
            writer.append(&item).unwrap();
            writer.flush().unwrap();
        }
        writer
            .finish(RecordStopReason::Requested, RecordCounters::default(), None)
            .unwrap();
        let (_, indexes) = inspect_indexed(
            &directory,
            InspectOptions {
                limits: options.limits,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(indexes.len() >= 16);
        let before: Vec<_> = indexes
            .iter()
            .map(|index| index.source.bytes_read())
            .collect();
        for _ in 0..20 {
            let index = indexes.last().unwrap();
            let mut cursor = index.cursor(10 + 511 * 10).unwrap();
            let message = cursor.next_message().unwrap().unwrap();
            assert_eq!(message.sequence, 511);
            assert!(cursor.next_message().unwrap().is_none());
        }
        for (index, before) in indexes.iter().zip(before.iter()).take(indexes.len() - 1) {
            assert_eq!(
                index.source.bytes_read(),
                *before,
                "seek read a prefix shard"
            );
        }
        let bytes = indexes.last().unwrap().source.bytes_read() - before.last().unwrap();
        assert!(
            bytes < 20 * 4096,
            "seek reread more than one small chunk: {bytes}"
        );
        assert!(
            indexes
                .iter()
                .map(|index| index.memory_bytes())
                .sum::<usize>()
                < ReaderLimits::default().index_bytes
        );
    }

    #[cfg(unix)]
    #[test]
    fn indexed_cursor_rejects_replacement_after_preflight() {
        let temp = Temp::new();
        let directory = temp.0.join("changed-recording");
        crate::runtime_tests::playback_fixture(&directory);
        let (info, indexes) = inspect_indexed(&directory, InspectOptions::default()).unwrap();
        let path = directory.join(&info.manifest.shards[0].file);
        let replacement = directory.join("replacement");
        std::fs::copy(&path, &replacement).unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert_eq!(
            indexes[0].cursor(0).err().unwrap().kind,
            crate::BagErrorKind::Corrupt
        );
    }
}

#[cfg(test)]
#[path = "reader_r2_tests.rs"]
pub(crate) mod r2_tests;
