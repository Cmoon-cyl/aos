use super::*;
use crate::{
    offline::inspect_indexed,
    storage::StorageWriter,
    storage_tests::{interface, message, storage_options, Temp},
    BagErrorKind, InspectOptions, RecordCounters, RecordStopReason, StorageOptions,
};
use aos_core::QoS;
use serde_json::json;

// These acceptance fixtures deliberately write tens of MiB / hundreds of
// fsync'd shards. Serialize that test-only disk pressure so unrelated short
// discovery deadlines are not testing fixture construction contention.
pub(crate) fn acceptance_fixture_guard() -> std::sync::MutexGuard<'static, ()> {
    static FIXTURES: Mutex<()> = Mutex::new(());
    FIXTURES.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn channel(topic: &str, interface: &Interface) -> Arc<ChannelInfo> {
    Arc::new(
        ChannelInfo::new(
            topic.into(),
            interface,
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    )
}

pub(crate) fn large_schema_fixture(directory: &Path) -> ReaderLimits {
    let mut definition = interface().definition().clone();
    definition["message"]["description"] = json!("");
    let empty_size = serde_json::to_vec(&definition["message"]).unwrap().len();
    definition["message"]["description"] = json!("x".repeat(1024 * 1024 - empty_size));
    assert_eq!(
        serde_json::to_vec(&definition["message"]).unwrap().len(),
        1024 * 1024
    );
    let interface = Interface::from_json(definition).unwrap();
    let channel = channel("/large", &interface);
    let options = StorageOptions {
        messages_per_shard: 1,
        ..Default::default()
    };
    let mut writer = StorageWriter::create(
        directory,
        options.clone(),
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    for sequence in 1..=17 {
        writer
            .append(&message(&channel, sequence, 10 + sequence))
            .unwrap();
    }
    let manifest = writer
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap();
    assert_eq!(manifest.shards.len(), 17);
    assert!(manifest.clean_close && manifest.error.is_none());
    options.limits
}

fn accounted_bytes(indexes: &[Arc<ShardIndex>]) -> (usize, usize, usize) {
    let mut schemas = BTreeMap::new();
    let mut channels = BTreeMap::new();
    for index in indexes {
        for schema in index.schemas.values() {
            schemas.insert(Arc::as_ptr(schema) as usize, schema);
        }
        for channel in index.channels.values() {
            channels.insert(channel.info.id.clone(), &channel.shared);
        }
    }
    let descriptor_bytes: usize = schemas
        .values()
        .map(|s| s.data.len() + s.header.name.len() + 64)
        .sum::<usize>()
        + channels
            .values()
            .map(|c| serde_json::to_vec(c.info.as_ref()).unwrap().len())
            .sum::<usize>();
    let bytes = indexes.iter().map(|i| i.memory_bytes()).sum::<usize>()
        + descriptor_bytes * 4
        + channels.len() * 512
        + serde_json::to_vec(indexes[0].header.as_ref())
            .unwrap()
            .len()
            * 2;
    (bytes, schemas.len(), channels.len())
}

#[test]
fn seventeen_one_mib_schema_shards_share_the_descriptor_and_fit_default_index() {
    let _fixture = acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("large-schema");
    let limits = large_schema_fixture(&directory);
    let (info, indexes) = inspect_indexed(
        &directory,
        InspectOptions {
            limits,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(info.messages, 17);
    let (bytes, schemas, channels) = accounted_bytes(&indexes);
    assert_eq!((schemas, channels), (2, 2));
    let first = indexes[0]
        .channels
        .values()
        .find(|c| c.info.role == ChannelRole::Business)
        .unwrap();
    for index in &indexes[1..] {
        let repeated = index
            .channels
            .values()
            .find(|c| c.info.id == first.info.id)
            .unwrap();
        assert!(Arc::ptr_eq(&first.shared, &repeated.shared));
        assert!(Arc::ptr_eq(&first.schema, &repeated.schema));
    }
    assert!(bytes < 16 * 1024 * 1024, "accounted bytes: {bytes}");
    println!("large schema: shards={} unique_channels={channels} unique_schemas={schemas} index_bytes={bytes}", indexes.len());
}

#[test]
fn many_channel_shards_charge_descriptors_once_and_enforce_the_exact_public_budget() {
    let _fixture = acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("many-channels");
    let mut options = storage_options();
    options.limits.channels = 256;
    options.messages_per_shard = 32;
    options.shard_bytes = 2 * 1024 * 1024;
    options.total_bytes = 16 * 1024 * 1024;
    let channels: Vec<_> = (0..32)
        .map(|i| channel(&format!("/topic_{i}"), &interface()))
        .collect();
    let mut writer = StorageWriter::create(
        &directory,
        options.clone(),
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    for sequence in 0..512 {
        writer
            .append(&message(
                &channels[(31 - sequence % 32) as usize],
                sequence,
                10 + sequence,
            ))
            .unwrap();
    }
    writer
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap();
    let (_, indexes) = inspect_indexed(
        &directory,
        InspectOptions {
            limits: options.limits.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(indexes.len(), 16);
    let (bytes, schemas, channel_count) = accounted_bytes(&indexes);
    assert_eq!((schemas, channel_count), (2, 64));
    for original in indexes[0].channels.values() {
        for index in &indexes[1..] {
            let repeated = index
                .channels
                .values()
                .find(|c| c.info.id == original.info.id)
                .unwrap();
            assert!(Arc::ptr_eq(&original.shared, &repeated.shared));
        }
    }
    let mut limits = options.limits;
    limits.index_bytes = bytes;
    inspect_indexed(
        &directory,
        InspectOptions {
            limits: limits.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    limits.index_bytes -= 1;
    let error = inspect_indexed(
        &directory,
        InspectOptions {
            limits,
            ..Default::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.kind, BagErrorKind::ResourceLimit);
    assert!(error.message.contains("index_bytes"));
    println!("multi-channel accounting: shards=16 unique_channels={channel_count} unique_schemas={schemas} index_bytes={bytes}");
}

pub(crate) fn sparse_fixture(directory: &Path, large_payload: bool) -> ReaderLimits {
    let rare = channel("/rare", &interface());
    let dense = channel("/dense", &interface());
    let options = StorageOptions {
        messages_per_shard: 16,
        chunks_per_shard: 16,
        total_bytes: 256 * 1024 * 1024,
        shard_bytes: 8 * 1024 * 1024,
        chunk_bytes: 512 * 1024,
        ..Default::default()
    };
    let mut writer = StorageWriter::create(
        directory,
        options.clone(),
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({}),
    )
    .unwrap();
    for sequence in 0..514 {
        let selected = sequence == 0 || sequence == 513;
        let mut sample = message(
            if selected { &rare } else { &dense },
            sequence,
            if sequence == 513 {
                3_600_000_000_010
            } else {
                10 + sequence
            },
        );
        if large_payload && !selected {
            sample.payload =
                serde_json::to_vec(&json!({"value": 7, "text": "x".repeat(128 * 1024)})).unwrap();
            sample.metadata.recorded_payload_size_bytes = sample.payload.len() as u64;
            sample.metadata.adapter_payload_size_bytes = sample.payload.len() as u64;
        }
        writer.append(&sample).unwrap();
        writer.flush().unwrap();
    }
    writer
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap();
    options.limits
}

#[test]
fn sparse_selection_skips_payload_reads_and_yields_after_each_chunk() {
    let _fixture = acceptance_fixture_guard();
    let temp = Temp::new();
    let directory = temp.0.join("sparse");
    let limits = sparse_fixture(&directory, false);
    let (info, indexes) = inspect_indexed(
        &directory,
        InspectOptions {
            limits,
            ..Default::default()
        },
    )
    .unwrap();
    let selected = BTreeSet::from([info
        .channels
        .iter()
        .find(|c| c.topic == "/rare")
        .unwrap()
        .id
        .clone()]);
    let initial: u64 = indexes.iter().map(|i| i.source.bytes_read()).sum();
    let mut sequences = Vec::new();
    let mut progress = 0;
    for index in &indexes {
        let mut cursor = index.cursor_selected(0, Some(&selected)).unwrap();
        let mut prior_chunk = 0;
        loop {
            match cursor.next_step().unwrap() {
                IndexedRead::Progress => {
                    assert_eq!(
                        cursor.next_chunk,
                        prior_chunk + 1,
                        "one step crossed more than one chunk"
                    );
                    prior_chunk = cursor.next_chunk;
                    progress += 1;
                }
                IndexedRead::Message(message) => sequences.push(message.sequence),
                IndexedRead::End => break,
            }
        }
    }
    assert_eq!(sequences, [0, 513]);
    assert_eq!(progress, 514);
    let read = indexes.iter().map(|i| i.source.bytes_read()).sum::<u64>() - initial;
    assert!(
        read < 32 * 1024,
        "irrelevant chunk payloads were read: {read}"
    );
    println!(
        "sparse scan: chunks={progress} selected_messages=2 bytes_read_after_preflight={read}"
    );
}
