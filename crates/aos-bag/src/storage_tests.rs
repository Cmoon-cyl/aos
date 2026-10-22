use super::*;
use crate::{
    format::{channel_metadata, RecordingHeader},
    reader::BagReader,
    storage::{file_sha256, RecordedMessage, StorageWriter},
    types::{now_ns, RECORDING_METADATA},
};
use aos_core::{Interface, QoS};
use mcap::records::{MessageHeader, Metadata};
use serde_json::json;
use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

pub(crate) struct Temp(pub(crate) PathBuf);
impl Temp {
    pub(crate) fn new() -> Self {
        let path = std::env::temp_dir().join(format!("aos-bag-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
pub(crate) fn storage_options() -> StorageOptions {
    StorageOptions {
        chunk_bytes: 64 * 1024,
        shard_bytes: 256 * 1024,
        total_bytes: 2 * 1024 * 1024,
        limits: ReaderLimits {
            record_bytes: 512 * 1024,
            schema_bytes: 64 * 1024,
            descriptor_bytes: 512 * 1024,
            channels: 64,
            manifest_bytes: 64 * 1024,
            ..Default::default()
        },
        ..Default::default()
    }
}
pub(crate) fn interface() -> Interface {
    Interface::from_json(json!({
        "name":"test/msg/Sample","kind":"msg",
        "message":{"type":"object","properties":{"value":{"type":"integer"},"text":{"type":"string"}},"required":["value","text"],"additionalProperties":false}
    })).unwrap()
}
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
pub(crate) fn message(channel: &Arc<ChannelInfo>, sequence: u64, time: u64) -> RecordedMessage {
    let channel = Arc::new(channel.with_epoch(sequence).unwrap());
    let payload = b" \n{ \"value\" : 7, \"text\" : \"\\u4f60\\u597d\" } \t".to_vec();
    RecordedMessage {
        metadata: SampleMetadata {
            recording_sequence: sequence,
            channel_id: channel.id.clone(),
            recorded_payload_size_bytes: payload.len() as u64,
            adapter_payload_size_bytes: payload.len() as u64,
            received: Some(RecordedReceive {
                steady_offset_ns: "-123".into(),
                system_unix_ns: "1760000000000000001".into(),
                scope: "adapter_delivery".into(),
            }),
            source: Some(RecordedSource {
                timestamp: Some(RecordedSourceTimestamp {
                    kind: "zenoh_hlc".into(),
                    value: "18446744073709551615".into(),
                    clock_id: "clock-id".into(),
                    comparable: false,
                    origin: "router_or_publisher".into(),
                }),
                publisher_id: Some("publisher-id".into()),
                sequence: Some(u64::MAX),
            }),
        },
        channel,
        sequence,
        log_time: time,
        publish_time: time,
        payload,
    }
}
fn create(writer_dir: &std::path::Path, options: StorageOptions) -> StorageWriter {
    StorageWriter::create(
        writer_dir,
        options,
        uuid::Uuid::new_v4().to_string(),
        10,
        json!({"test":true}),
    )
    .unwrap()
}
fn finish(writer: &mut StorageWriter) -> Manifest {
    writer
        .finish(RecordStopReason::Requested, RecordCounters::default(), None)
        .unwrap()
}

#[test]
fn exact_bytes_metadata_shards_and_sequence_epoch_round_trip_offline() {
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let mut options = storage_options();
    options.messages_per_shard = 2;
    let mut writer = create(&directory, options.clone());
    let channel = channel();
    let sequences = [u32::MAX as u64 - 1, u32::MAX as u64, 1u64 << 32];
    for (index, sequence) in sequences.iter().enumerate() {
        writer
            .append(&message(&channel, *sequence, 10 + index as u64))
            .unwrap();
    }
    let manifest = finish(&mut writer);
    assert!(manifest.clean_close && !manifest.incomplete);
    assert_eq!(manifest.shards.len(), 2);
    let info = inspect(
        &directory,
        InspectOptions {
            limits: options.limits.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(info.messages, 3);
    assert_eq!(info.channels.len(), 2);
    assert_eq!(
        info.channels
            .iter()
            .map(|c| c.sequence_epoch)
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 1].into_iter().collect()
    );
    let mut found = Vec::new();
    let mut upstream_found = Vec::new();
    for shard in &manifest.shards {
        assert!(shard.file.ends_with(".mcap"));
        let bytes = fs::read(directory.join(&shard.file)).unwrap();
        for item in mcap::MessageStream::new(&bytes).unwrap() {
            let item = item.unwrap();
            if item.channel.metadata["aos.channel_role"] == "business" {
                let epoch: u32 = item.channel.metadata["aos.sequence_epoch"].parse().unwrap();
                let sequence = (u64::from(epoch) << 32) | u64::from(item.sequence);
                assert_eq!(
                    item.data.as_ref(),
                    message(&channel, sequence, item.log_time).payload
                );
                assert_eq!(item.channel.message_encoding, "json");
                assert_eq!(item.channel.schema.as_ref().unwrap().encoding, "jsonschema");
                upstream_found.push(sequence);
            }
        }
        let mut reader =
            BagReader::open(&directory.join(&shard.file), options.limits.clone(), false).unwrap();
        while let Some(item) = reader.next_message().unwrap() {
            let expected = message(&channel, item.sequence, item.log_time);
            assert_eq!(item.payload, expected.payload);
            assert_eq!(item.metadata, expected.metadata);
            found.push(item.sequence);
        }
    }
    assert_eq!(found, sequences);
    assert_eq!(upstream_found, sequences);
    assert!(info.manifest.counters.end_to_end_lost_total.is_none());
}

#[test]
fn recovery_continues_after_validation_resource_limit_but_stops_after_corruption() {
    let heavy = Interface::from_json(json!({
        "name":"test/msg/RecoveryWorkLimit","kind":"msg",
        "message":{"type":"array","items":{"allOf":(0..1022)
            .map(|index| json!({"type":"integer","minimum":-index}))
            .collect::<Vec<_>>()}}
    }))
    .unwrap();
    let heavy = Arc::new(
        ChannelInfo::new(
            "/heavy".into(),
            &heavy,
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    );
    for (case, payload, expected_kind, recovered_messages) in [
        (
            "resource_limit",
            json!(vec![0; 8192]),
            BagErrorKind::ResourceLimit,
            1,
        ),
        ("schema_mismatch", json!(["bad"]), BagErrorKind::Corrupt, 0),
    ] {
        let temp = Temp::new();
        let source = temp.0.join(case);
        let destination = temp.0.join("recovered");
        let options = StorageOptions {
            messages_per_shard: 1,
            ..Default::default()
        };
        let mut writer = create(&source, options.clone());
        let mut rejected = message(&heavy, 0, 10);
        rejected.payload = serde_json::to_vec(&payload).unwrap();
        rejected.metadata.recorded_payload_size_bytes = rejected.payload.len() as u64;
        rejected.metadata.adapter_payload_size_bytes = rejected.payload.len() as u64;
        // Raw append intentionally bypasses ingress validation, as an older
        // producer or another MCAP writer can persist this valid JSON payload.
        writer.append(&rejected).unwrap();
        let healthy = message(&channel(), 1, 11);
        writer.append(&healthy).unwrap();
        let manifest = finish(&mut writer);
        assert_eq!(manifest.shards.len(), 2);
        let original_hashes: Vec<_> = manifest
            .shards
            .iter()
            .map(|shard| file_sha256(&source.join(&shard.file)).unwrap())
            .collect();
        let error = inspect(
            &source,
            InspectOptions {
                limits: options.limits.clone(),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.kind, expected_kind, "{case}: {error}");
        let report = recover(
            &source,
            &destination,
            RecoveryOptions {
                storage: options.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            report.sources[0].error.as_ref().unwrap().kind,
            expected_kind
        );
        assert_eq!(report.info.messages, recovered_messages);
        assert!(report.info.manifest.incomplete);
        assert!(report
            .sources
            .iter()
            .all(|source| source.verified_unchanged));
        if expected_kind == BagErrorKind::ResourceLimit {
            assert!(report.sources[1].error.is_none());
            let mut reader = BagReader::open(
                &destination.join(&report.info.manifest.shards[0].file),
                options.limits.clone(),
                false,
            )
            .unwrap();
            let recovered = reader.next_message().unwrap().unwrap();
            assert_eq!(recovered.sequence, healthy.sequence);
            assert_eq!(recovered.payload, healthy.payload);
            assert!(reader.next_message().unwrap().is_none());
        } else {
            assert_eq!(
                report.sources[1].error.as_ref().unwrap().kind,
                BagErrorKind::Incomplete
            );
        }
        for (shard, hash) in manifest.shards.iter().zip(original_hashes) {
            assert_eq!(file_sha256(&source.join(&shard.file)).unwrap(), hash);
        }
        eprintln!(
            "{case}: inspect={:?}; source_errors={:?}; recovered_messages={}; unchanged={}",
            error.kind,
            report
                .sources
                .iter()
                .map(|source| source.error.as_ref().map(|error| error.kind))
                .collect::<Vec<_>>(),
            report.info.messages,
            report
                .sources
                .iter()
                .all(|source| source.verified_unchanged),
        );
    }
}

#[test]
fn reader_preserves_descriptor_admission_resource_limit() {
    let temp = Temp::new();
    let source = temp.0.join("descriptor-over-budget.mcap");
    let mut writer = mcap::WriteOptions::new()
        .profile(PROFILE)
        .compression(None)
        .create(fs::File::create(&source).unwrap())
        .unwrap();
    let header = RecordingHeader {
        profile: PROFILE.into(),
        format_version: FORMAT_VERSION,
        recording_id: uuid::Uuid::new_v4().to_string(),
        system_anchor_ns: 10,
        writer_version: "fixture".into(),
        configuration: json!({}),
    };
    writer
        .write_metadata(&Metadata {
            name: RECORDING_METADATA.into(),
            metadata: BTreeMap::from([("header".into(), serde_json::to_string(&header).unwrap())]),
        })
        .unwrap();
    let mut descriptor = channel().as_ref().clone();
    descriptor.interface["message"] = json!({"allOf":vec![json!({"type":"integer"}); 1024]});
    let schema = writer
        .add_schema(
            &descriptor.type_name,
            "jsonschema",
            &serde_json::to_vec(&descriptor.interface["message"]).unwrap(),
        )
        .unwrap();
    writer
        .add_channel(
            schema,
            &descriptor.topic,
            "json",
            &channel_metadata(&descriptor).unwrap(),
        )
        .unwrap();
    writer.finish().unwrap();
    drop(writer.into_inner());
    let mut reader = BagReader::open(&source, storage_options().limits, false).unwrap();
    let error = reader.next_message().err().unwrap();
    assert_eq!(error.kind, BagErrorKind::ResourceLimit, "{error}");
    eprintln!("descriptor admission: {error}");
}

fn descriptor_zero_id_fixture(schema_id: u16, channel_id: u16, corrupt: bool) {
    let temp = Temp::new();
    let source = temp.0.join("recording");
    let destination = temp.0.join("recovered");
    let options = StorageOptions {
        messages_per_shard: 1,
        ..Default::default()
    };
    let business = channel();
    let mut writer = create(&source, options.clone());
    writer.append(&message(&business, 0, 10)).unwrap();
    writer.append(&message(&business, 1, 11)).unwrap();
    let mut manifest = finish(&mut writer);
    assert_eq!(manifest.shards.len(), 2);
    let first = source.join(&manifest.shards[0].file);
    let header = BagReader::open(&first, options.limits.clone(), false)
        .unwrap()
        .read_header()
        .unwrap();

    // Use the fixed upstream writer, including its CRCs and indexes. Its
    // low-level Message path preserves supplied IDs, including a malformed
    // Schema ID. Channel ID zero is explicitly permitted by MCAP 0.x.
    let mut raw = mcap::WriteOptions::new()
        .profile(PROFILE)
        .compression(None)
        .chunk_size(None)
        .calculate_chunk_crcs(true)
        .calculate_data_section_crc(true)
        .calculate_summary_section_crc(true)
        .create(fs::File::create(&first).unwrap())
        .unwrap();
    raw.write_metadata(&Metadata {
        name: RECORDING_METADATA.into(),
        metadata: BTreeMap::from([("header".into(), serde_json::to_string(&header).unwrap())]),
    })
    .unwrap();
    let item = message(&business, 0, 10);
    let companion = item.channel.companion().unwrap();
    for (descriptor, schema_id, channel_id, data) in [
        (
            item.channel.as_ref(),
            schema_id,
            channel_id,
            item.payload.clone(),
        ),
        (
            &companion,
            2,
            2,
            serde_json::to_vec(&item.metadata).unwrap(),
        ),
    ] {
        raw.write(&mcap::Message {
            channel: Arc::new(mcap::Channel {
                id: channel_id,
                topic: descriptor.topic.clone(),
                message_encoding: "json".into(),
                metadata: channel_metadata(descriptor).unwrap(),
                schema: Some(Arc::new(mcap::Schema {
                    id: schema_id,
                    name: descriptor.type_name.clone(),
                    encoding: "jsonschema".into(),
                    data: serde_json::to_vec(&descriptor.interface["message"])
                        .unwrap()
                        .into(),
                })),
            }),
            sequence: 0,
            log_time: 10,
            publish_time: 10,
            data: data.into(),
        })
        .unwrap();
    }
    raw.finish().unwrap();
    drop(raw.into_inner());
    manifest.shards[0].size_bytes = fs::metadata(&first).unwrap().len();
    manifest.shards[0].sha256 = Some(file_sha256(&first).unwrap());
    fs::write(
        source.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let original_hashes: Vec<_> = manifest
        .shards
        .iter()
        .map(|shard| file_sha256(&source.join(&shard.file)).unwrap())
        .collect();
    let inspected = inspect(&source, InspectOptions::default());
    if corrupt {
        let error = inspected.unwrap_err();
        assert_eq!(error.kind, BagErrorKind::Corrupt);
        assert!(error.message.contains("schema id zero"), "{error}");
    } else {
        assert_eq!(inspected.unwrap().messages, 2);
        let bytes = fs::read(&first).unwrap();
        let upstream: Vec<_> = mcap::MessageStream::new(&bytes)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(upstream[0].channel.id, 0);
        assert_eq!(upstream[0].data.as_ref(), item.payload);
    }
    let recovered = recover(&source, &destination, RecoveryOptions { storage: options }).unwrap();
    if corrupt {
        assert_eq!(recovered.info.messages, 0);
        let error = recovered.sources[0].error.as_ref().unwrap();
        assert_eq!(error.kind, BagErrorKind::Corrupt);
        assert!(error.message.contains("schema id zero"), "{error}");
        assert_eq!(
            recovered.sources[1].error.as_ref().unwrap().kind,
            BagErrorKind::Incomplete
        );
    } else {
        assert_eq!(recovered.info.messages, 2);
        assert!(recovered
            .sources
            .iter()
            .all(|source| source.error.is_none()));
    }
    assert!(recovered
        .sources
        .iter()
        .all(|source| source.verified_unchanged));
    for (shard, hash) in manifest.shards.iter().zip(original_hashes) {
        assert_eq!(file_sha256(&source.join(&shard.file)).unwrap(), hash);
    }
}

#[test]
fn zero_schema_id_is_corruption_and_stops_recovery_of_later_shards() {
    descriptor_zero_id_fixture(0, 1, true);
}

#[test]
fn zero_channel_id_is_valid_mcap_and_recovers_with_later_shards() {
    descriptor_zero_id_fixture(1, 0, false);
}

#[test]
fn resource_rejection_counter_reads_old_records_and_serializes_separately() {
    let old: RecordCounters = serde_json::from_value(json!({"validation_failed_total":3})).unwrap();
    assert_eq!(old.resource_rejected_total, 0);
    let current = RecordCounters {
        resource_rejected_total: 5,
        ..old
    };
    let json = serde_json::to_value(&current).unwrap();
    assert_eq!(json["validation_failed_total"], 3);
    assert_eq!(json["resource_rejected_total"], 5);
    assert_eq!(
        serde_json::from_value::<RecordCounters>(json).unwrap(),
        current
    );
}

#[test]
fn image_payload_round_trips_through_raw_writer_inspect_and_reader() {
    // Same eleven-node schema and 640x480 mono16 fixture as the Core typed
    // publish/subscribe regression. No pattern, enum or deep comparison.
    let interface = Interface::from_json(json!({"name":"test/msg/Image","kind":"msg","message":{
        "type":"object","properties":{
            "header":{"type":"object","properties":{
                "stamp":{"type":"object","properties":{"sec":{"type":"integer"},"nanosec":{"type":"integer"}}},
                "frame_id":{"type":"string"}}},
            "height":{"type":"integer"},"width":{"type":"integer"},"encoding":{"type":"string"},
            "step":{"type":"integer"},"data":{"type":"string"}
        }
    }})).unwrap();
    let payload = json!({"header":{"stamp":{"sec":1,"nanosec":0},"frame_id":"camera"},
        "height":480,"width":640,"encoding":"mono16","step":1280,"data":"A".repeat(819_200)});
    let channel = Arc::new(
        ChannelInfo::new(
            "/image".into(),
            &interface,
            vec![QoS::default()],
            QoS::default(),
            None,
        )
        .unwrap(),
    );
    let temp = Temp::new();
    let directory = temp.0.join("image");
    let options = StorageOptions::default();
    let mut writer = create(&directory, options.clone());
    let mut original = message(&channel, 3, 15);
    original.payload = serde_json::to_vec(&payload).unwrap();
    original.metadata.recorded_payload_size_bytes = original.payload.len() as u64;
    original.metadata.adapter_payload_size_bytes = original.payload.len() as u64;
    writer.append(&original).unwrap();
    let manifest = finish(&mut writer);
    let info = inspect(&directory, InspectOptions::default()).unwrap();
    assert_eq!(info.messages, 1);
    assert_eq!(info.verified_payload_bytes, original.payload.len() as u64);
    let mut reader = BagReader::open(
        &directory.join(&manifest.shards[0].file),
        options.limits,
        false,
    )
    .unwrap();
    let received = reader.next_message().unwrap().unwrap();
    assert_eq!(received.sequence, original.sequence);
    assert_eq!(received.payload, original.payload);
    assert_eq!(received.metadata, original.metadata);
    assert!(reader.next_message().unwrap().is_none());
    eprintln!(
        "Image round trip: data_chars=819200; serialized_bytes={}; messages={}; exact_payload=true",
        original.payload.len(),
        info.messages
    );
}

// Test fixture locator only. Production parsing is exclusively upstream MCAP.
fn chunks(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut cursor = mcap::MAGIC.len();
    let mut found = Vec::new();
    while cursor + 9 <= bytes.len() {
        let len = u64::from_le_bytes(bytes[cursor + 1..cursor + 9].try_into().unwrap()) as usize;
        let end = cursor + 9 + len;
        if end > bytes.len() {
            break;
        }
        if bytes[cursor] == mcap::records::op::CHUNK {
            found.push((cursor, end));
        }
        if bytes[cursor] == mcap::records::op::FOOTER {
            break;
        }
        cursor = end;
    }
    found
}

#[test]
fn recovery_commits_only_whole_crc_chunks_and_never_changes_sources() {
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let options = storage_options();
    let mut writer = create(&directory, options.clone());
    let channel = channel();
    for sequence in 0..2 {
        writer
            .append(&message(&channel, sequence, 10 + sequence))
            .unwrap();
        writer.flush().unwrap();
    }
    let manifest = finish(&mut writer);
    let original = fs::read(directory.join(&manifest.shards[0].file)).unwrap();
    let boundaries = chunks(&original);
    assert_eq!(boundaries.len(), 3); // descriptors, first pair, second pair
    for (label, source_bytes, expected) in [
        ("inside_chunk", original[..boundaries[2].0 + 50].to_vec(), 1),
        ("after_chunk", original[..boundaries[2].1].to_vec(), 2),
        (
            "bad_crc",
            {
                let mut data = original.clone();
                let (start, end) = boundaries[2];
                let value = data[start..end]
                    .windows(3)
                    .position(|window| window == b": 7")
                    .unwrap();
                // Preserve valid JSON/schema and pair size. Only the CRC can
                // reveal this mutation before committing the whole chunk.
                data[start + value + 2] = b'6';
                data
            },
            1,
        ),
    ] {
        let source = temp.0.join(format!("{label}.mcap.partial"));
        fs::write(&source, &source_bytes).unwrap();
        let before = file_sha256(&source).unwrap();
        let destination = temp.0.join(format!("{label}-recovered"));
        let report = recover(
            &source,
            &destination,
            RecoveryOptions {
                storage: options.clone(),
            },
        )
        .unwrap();
        assert_eq!(report.info.messages, expected, "{label}");
        assert!(report.info.manifest.incomplete);
        assert!(report.info.manifest.clean_close);
        assert!(report.sources[0].error.is_some());
        if label == "bad_crc" {
            assert!(
                report.sources[0]
                    .error
                    .as_ref()
                    .unwrap()
                    .message
                    .to_lowercase()
                    .contains("crc"),
                "{report:?}"
            );
        }
        assert!(report.sources[0].verified_unchanged);
        assert_eq!(file_sha256(&source).unwrap(), before);
        assert_eq!(fs::read(&source).unwrap(), source_bytes);
        assert!(matches!(
            inspect(&destination, InspectOptions::default()),
            Err(BagError {
                kind: BagErrorKind::Incomplete,
                ..
            })
        ));
    }
}

#[test]
fn orphaned_business_and_metadata_including_split_pairs_are_never_replayed() {
    let temp = Temp::new();
    let source = temp.0.join("orphans.mcap");
    let options = storage_options();
    let mut writer = mcap::WriteOptions::new()
        .profile(PROFILE)
        .compression(None)
        .chunk_size(None)
        .calculate_chunk_crcs(true)
        .calculate_data_section_crc(true)
        .calculate_summary_section_crc(true)
        .create(fs::File::create(&source).unwrap())
        .unwrap();
    let header = RecordingHeader {
        profile: PROFILE.into(),
        format_version: FORMAT_VERSION,
        recording_id: uuid::Uuid::new_v4().to_string(),
        system_anchor_ns: 10,
        writer_version: "fixture".into(),
        configuration: json!({}),
    };
    writer
        .write_metadata(&Metadata {
            name: RECORDING_METADATA.into(),
            metadata: BTreeMap::from([("header".into(), serde_json::to_string(&header).unwrap())]),
        })
        .unwrap();
    let channel = channel();
    let companion = channel.companion().unwrap();
    let mut ids = Vec::new();
    for item in [channel.as_ref(), &companion] {
        let schema = writer
            .add_schema(
                &item.type_name,
                "jsonschema",
                &serde_json::to_vec(&item.interface["message"]).unwrap(),
            )
            .unwrap();
        ids.push(
            writer
                .add_channel(
                    schema,
                    &item.topic,
                    "json",
                    &channel_metadata(item).unwrap(),
                )
                .unwrap(),
        );
    }
    for sequence in 0..3 {
        let item = message(&channel, sequence, 10 + sequence);
        let header = MessageHeader {
            channel_id: ids[0],
            sequence: sequence as u32,
            log_time: item.log_time,
            publish_time: item.log_time,
        };
        if sequence != 1 {
            writer
                .write_to_known_channel(&header, &item.payload)
                .unwrap();
        }
        if sequence == 0 {
            // The matching sequence exists in the next valid CRC chunk.
            // Recovery must not join a business/metadata pair across chunks.
            writer.flush().unwrap();
        }
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id: ids[1],
                    ..header
                },
                &serde_json::to_vec(&item.metadata).unwrap(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    drop(writer.into_inner());
    let mut strict = BagReader::open(&source, options.limits.clone(), false).unwrap();
    assert!(strict.next_message().is_err());
    let destination = temp.0.join("recovered");
    let report = recover(&source, &destination, RecoveryOptions { storage: options }).unwrap();
    assert_eq!(report.info.messages, 1);
    assert_eq!(report.discarded_orphan_business, 1);
    assert_eq!(report.discarded_orphan_metadata, 2);
    assert_eq!(report.info.manifest.shards[0].first_sequence, Some(2));
    assert!(report.info.manifest.incomplete);
}

#[test]
fn manifests_reject_sha_type_changes_and_path_escape() {
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let options = storage_options();
    let mut writer = create(&directory, options.clone());
    writer.append(&message(&channel(), 0, 10)).unwrap();
    let mut manifest = finish(&mut writer);
    manifest.shards[0].sha256 = Some("0".repeat(64));
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(inspect(&directory, InspectOptions::default()).is_err());
    manifest.shards[0].file = "../outside.mcap".into();
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(inspect(&directory, InspectOptions::default()).is_err());
    let mut channel = channel().as_ref().clone();
    channel.interface["message"]["properties"]["value"]["type"] = json!("boolean");
    assert!(channel.validate().is_err());
}

#[test]
fn resource_limits_reject_bad_sizes_and_unbounded_records_before_copying() {
    let mut record = RecordOptions::default();
    assert!(record.validate().is_err());
    record.directory = PathBuf::from("not-created");
    record.all_topics = true;
    assert!(record.validate().is_ok());
    record.topics.push("/also_explicit".into());
    assert!(record.validate().is_err());
    record.topics.clear();
    record.message_bytes = usize::MAX;
    assert!(record.validate().is_err());
    let temp = Temp::new();
    let source = temp.0.join("oversized.mcap");
    let mut bytes = mcap::MAGIC.to_vec();
    bytes.push(mcap::records::op::HEADER);
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    fs::write(&source, bytes).unwrap();
    let mut reader = BagReader::open(&source, storage_options().limits, true).unwrap();
    assert!(reader.next_message().is_err());
    assert!(now_ns().is_ok());
}

#[test]
fn disk_full_and_other_write_failures_leave_partial_files_and_unconfirmed_counts() {
    let temp = Temp::new();
    let options = storage_options();
    for (label, injected, expected) in [
        (
            "disk_full",
            std::io::ErrorKind::StorageFull,
            BagErrorKind::DiskFull,
        ),
        (
            "write_error",
            std::io::ErrorKind::PermissionDenied,
            BagErrorKind::Io,
        ),
    ] {
        let directory = temp.0.join(label);
        let mut writer = create(&directory, options.clone());
        let channel = channel();
        writer.append(&message(&channel, 0, 10)).unwrap();
        writer.flush().unwrap();
        writer.append(&message(&channel, 1, 11)).unwrap();
        writer.fail_next_write(injected);
        let error = writer.append(&message(&channel, 2, 12)).unwrap_err();
        assert_eq!(error.kind, expected);
        let manifest = writer
            .finish(
                RecordStopReason::Requested,
                RecordCounters::default(),
                Some(error),
            )
            .unwrap();
        assert!(manifest.incomplete && !manifest.clean_close);
        assert_eq!(manifest.error.as_ref().unwrap().kind, expected);
        assert_eq!(manifest.counters.unconfirmed_on_failure_total, 1);
        assert!(manifest.shards[0].file.ends_with(".partial"));
        let destination = temp.0.join(format!("{label}-recovered"));
        let report = recover(
            &directory,
            &destination,
            RecoveryOptions {
                storage: options.clone(),
            },
        )
        .unwrap();
        assert_eq!(report.info.messages, 1);
        assert!(report.sources[0].verified_unchanged);
    }
}

#[test]
fn total_capacity_stops_without_removing_or_overwriting_older_shards() {
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let mut options = storage_options();
    options.shard_bytes = 128 * 1024;
    options.total_bytes = 512 * 1024;
    let mut writer = create(&directory, options.clone());
    let channel = channel();
    let mut previous_files = BTreeMap::new();
    let mut accepted = 0;
    loop {
        let mut item = message(&channel, accepted, 10 + accepted);
        item.payload =
            serde_json::to_vec(&json!({"value":accepted,"text":"x".repeat(8192)})).unwrap();
        item.metadata.recorded_payload_size_bytes = item.payload.len() as u64;
        item.metadata.adapter_payload_size_bytes = item.payload.len() as u64;
        match writer.append(&item) {
            Ok(()) => accepted += 1,
            Err(error) => {
                assert_eq!(error.kind, BagErrorKind::CapacityLimit);
                break;
            }
        }
        for shard in &writer.manifest.shards {
            if shard.clean_close {
                previous_files
                    .entry(shard.file.clone())
                    .or_insert_with(|| shard.sha256.clone().unwrap());
            }
        }
        assert!(accepted < 10_000, "bounded writer must eventually stop");
    }
    let manifest = writer
        .finish(
            RecordStopReason::CapacityLimit,
            RecordCounters::default(),
            None,
        )
        .unwrap();
    assert!(manifest.clean_close && !manifest.incomplete);
    assert_eq!(manifest.stop_reason, Some(RecordStopReason::CapacityLimit));
    assert!(manifest.shards.len() > 1);
    for (file, digest) in previous_files {
        assert_eq!(file_sha256(&directory.join(file)).unwrap(), digest);
    }
    let size: u64 = fs::read_dir(&directory)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    assert!(size <= options.total_bytes);
    let info = inspect(
        &directory,
        InspectOptions {
            limits: options.limits,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(info.messages, accepted);
}

#[test]
fn recovery_destination_resource_exhaustion_is_not_reported_as_source_damage() {
    let temp = Temp::new();
    let source = temp.0.join("source");
    let options = storage_options();
    let mut writer = create(&source, options.clone());
    let channel = channel();
    for sequence in 0..3 {
        writer
            .append(&message(&channel, sequence, 10 + sequence))
            .unwrap();
    }
    let manifest = finish(&mut writer);
    assert_eq!(manifest.shards.len(), 1);
    let source_hash = file_sha256(&source.join(&manifest.shards[0].file)).unwrap();
    let destination = temp.0.join("too-many-output-shards");
    let mut recovery_storage = options;
    recovery_storage.messages_per_shard = 1;
    recovery_storage.limits.shards = 1;
    let result = recover(
        &source,
        &destination,
        RecoveryOptions {
            storage: recovery_storage,
        },
    );
    assert_eq!(result.unwrap_err().kind, BagErrorKind::ResourceLimit);
    let failed: Manifest =
        serde_json::from_slice(&fs::read(destination.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(failed.state, "failed");
    assert!(failed.incomplete);
    assert_eq!(failed.stop_reason, Some(RecordStopReason::ResourceLimit));
    assert_eq!(failed.error.unwrap().kind, BagErrorKind::ResourceLimit);
    assert_eq!(failed.recovery_sources.len(), 1);
    assert!(failed
        .recovery_sources
        .iter()
        .all(|source| source.error.is_none()));
    assert!(!failed.recovery_sources[0].verified_unchanged);
    assert_eq!(
        file_sha256(&source.join(&manifest.shards[0].file)).unwrap(),
        source_hash
    );
}

#[test]
fn reader_shard_budget_exhaustion_is_not_reported_as_source_corruption() {
    let temp = Temp::new();
    let directory = temp.0.join("recording");
    let mut options = storage_options();
    options.messages_per_shard = 1;
    let mut writer = create(&directory, options);
    let channel = channel();
    for sequence in 0..2 {
        writer
            .append(&message(&channel, sequence, 10 + sequence))
            .unwrap();
    }
    assert_eq!(finish(&mut writer).shards.len(), 2);
    assert_eq!(
        inspect(&directory, InspectOptions::default())
            .unwrap()
            .messages,
        2
    );
    let error = inspect(
        &directory,
        InspectOptions {
            limits: ReaderLimits {
                shards: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind, BagErrorKind::ResourceLimit);
}
