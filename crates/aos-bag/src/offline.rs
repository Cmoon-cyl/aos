use crate::{
    reader::{BagReader, DescriptorCatalog, ShardIndex},
    storage::{regular_file, shard_path, PinnedFile, StorageWriter},
    BagError, BagErrorKind, BagInfo, BagResult, ChannelInfo, InspectOptions, Manifest,
    ReaderLimits, RecordCounters, RecordStopReason, RecoveryOptions, RecoveryReport,
    RecoverySource, FORMAT_VERSION, PROFILE,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub(crate) fn read_manifest(directory: &Path, limits: &ReaderLimits) -> BagResult<Manifest> {
    limits.validate()?;
    let source = regular_file(&directory.join("manifest.json"))?;
    let mut bytes = Vec::new();
    source
        .take(limits.manifest_bytes as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limits.manifest_bytes {
        return Err(BagError::resource("manifest byte limit exceeded"));
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    if manifest.shards.len() > limits.shards {
        return Err(BagError::resource("manifest shard limit exceeded"));
    }
    if manifest.profile != PROFILE
        || manifest.format_version != FORMAT_VERSION
        || uuid::Uuid::parse_str(&manifest.recording_id).is_err()
        || manifest.shards.is_empty()
        || !matches!(manifest.state.as_str(), "recording" | "finished" | "failed")
        || (!manifest.incomplete
            && (!manifest.clean_close
                || manifest.state != "finished"
                || manifest.error.is_some()
                || manifest.cleanup_error.is_some()
                || !manifest.recovery_sources.is_empty()
                || manifest.recovered_orphan_business != 0
                || manifest.recovered_orphan_metadata != 0))
        || (manifest.clean_close && manifest.shards.iter().any(|shard| !shard.clean_close))
        || (manifest.cleanup_error.is_some() && manifest.error.is_none())
        || (manifest.state == "finished"
            && (manifest.stop_reason.is_none() || manifest.error.is_some()))
        || (manifest.state == "failed"
            && (manifest.stop_reason.is_none() || manifest.error.is_none()))
        || (manifest.state == "recording"
            && (manifest.clean_close || manifest.stop_reason.is_some() || manifest.error.is_some()))
    {
        return Err(BagError::corrupt("invalid recording manifest"));
    }
    let mut names = BTreeSet::new();
    for shard in &manifest.shards {
        shard_path(directory, &shard.file)?;
        if !names.insert(&shard.file) {
            return Err(BagError::corrupt("duplicate shard filename"));
        }
    }
    Ok(manifest)
}

/// Offline, bounded verification. This never creates a Context or calls an
/// online provider. A partial/corrupt container must be recovered separately.
pub fn inspect(directory: &Path, options: InspectOptions) -> BagResult<BagInfo> {
    inspect_impl(directory, options, false).map(|(info, _)| info)
}

pub(crate) fn inspect_indexed(
    directory: &Path,
    options: InspectOptions,
) -> BagResult<(BagInfo, Vec<Arc<ShardIndex>>)> {
    inspect_impl(directory, options, true)
}

fn inspect_impl(
    directory: &Path,
    options: InspectOptions,
    indexed: bool,
) -> BagResult<(BagInfo, Vec<Arc<ShardIndex>>)> {
    let manifest = read_manifest(directory, &options.limits)?;
    if manifest.incomplete && !options.allow_incomplete {
        return Err(BagError::new(
            BagErrorKind::Incomplete,
            "recording is incomplete; inspect with allow_incomplete after recovery",
        ));
    }
    let mut channels = BTreeMap::<String, ChannelInfo>::new();
    let mut descriptor_bytes = 0usize;
    let mut messages = 0u64;
    let mut bytes = 0u64;
    let mut previous: Option<(u64, u64)> = None;
    let mut indexes = Vec::new();
    let mut index_bytes = 0usize;
    let catalog = Arc::new(Mutex::new(DescriptorCatalog::default()));
    for shard in &manifest.shards {
        if !shard.clean_close || shard.sha256.is_none() {
            return Err(BagError::new(
                BagErrorKind::Incomplete,
                format!(
                    "{} is partial; recover into a new directory first",
                    shard.file
                ),
            ));
        }
        let path = shard_path(directory, &shard.file)?;
        let file = PinnedFile::open(&path)?;
        if file.len() != shard.size_bytes || Some(file.sha256()?) != shard.sha256 {
            return Err(BagError::corrupt(format!(
                "shard size or SHA256 mismatch: {}",
                shard.file
            )));
        }
        let mut reader = BagReader::from_file(file.clone(), options.limits.clone(), false)?;
        if indexed {
            reader.enable_index(catalog.clone());
        }
        let header = reader.read_header()?;
        if header.recording_id != manifest.recording_id
            || header.system_anchor_ns != manifest.system_anchor_ns
            || header.configuration != manifest.configuration
            || header.writer_version != manifest.writer_version
        {
            return Err(BagError::corrupt(
                "shard recording header differs from manifest",
            ));
        }
        let mut count = 0u64;
        let mut first = None;
        let mut last = None;
        while let Some(message) = reader.next_message()? {
            if previous
                .is_some_and(|(seq, time)| message.sequence <= seq || message.log_time < time)
            {
                return Err(BagError::corrupt(
                    "cross-shard recording order is not monotonic",
                ));
            }
            previous = Some((message.sequence, message.log_time));
            first.get_or_insert((message.sequence, message.log_time));
            last = Some((message.sequence, message.log_time));
            count += 1;
            messages = messages
                .checked_add(1)
                .ok_or_else(|| BagError::resource("recording message count overflow"))?;
            bytes = bytes
                .checked_add(message.payload.len() as u64)
                .ok_or_else(|| BagError::resource("recording byte count overflow"))?;
        }
        if count != shard.messages
            || first.map(|(s, _)| s) != shard.first_sequence
            || first.map(|(_, t)| t) != shard.first_log_time
            || last.map(|(s, _)| s) != shard.last_sequence
            || last.map(|(_, t)| t) != shard.last_log_time
        {
            return Err(BagError::corrupt(
                "shard message count or ranges differ from manifest",
            ));
        }
        for channel in reader.channels() {
            if let Some(existing) = channels.get(&channel.id) {
                if existing != channel {
                    return Err(BagError::corrupt("channel id changes across shards"));
                }
            } else {
                descriptor_bytes = descriptor_bytes
                    .checked_add(serde_json::to_vec(channel)?.len())
                    .ok_or_else(|| BagError::resource("descriptor size overflow"))?;
                if descriptor_bytes > options.limits.descriptor_bytes
                    || channels.len().saturating_add(1).saturating_mul(2) > options.limits.channels
                {
                    return Err(BagError::resource("recording descriptor limit exceeded"));
                }
                channels.insert(channel.id.clone(), channel.clone());
            }
        }
        file.check_unchanged()?;
        if indexed {
            let index = reader.into_index()?;
            index_bytes = index_bytes
                .checked_add(index.memory_bytes())
                .ok_or_else(|| BagError::resource("playback index size overflow"))?;
            if index_bytes.saturating_add(catalog.lock().unwrap().memory_bytes())
                > options.limits.index_bytes
            {
                return Err(BagError::resource(
                    "recording playback index exceeds index_bytes",
                ));
            }
            indexes.push(index);
        }
    }
    if manifest.counters.written_total != messages || manifest.counters.written_bytes != bytes {
        return Err(BagError::corrupt(
            "manifest written totals differ from verified messages",
        ));
    }
    Ok((
        BagInfo {
            manifest,
            channels: channels.into_values().collect(),
            messages,
            verified_payload_bytes: bytes,
        },
        indexes,
    ))
}

/// Rewrites only complete, CRC-validated chunks to a new directory. Business
/// and companion records are accepted as same-chunk pairs. Source bytes are
/// never modified; original hashes and any discarded suffix are reported.
pub fn recover(
    source: &Path,
    destination: &Path,
    options: RecoveryOptions,
) -> BagResult<RecoveryReport> {
    recover_impl(source, destination, options, |_| {})
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecoveryStage {
    BeforeSource(usize),
    BeforeFinalHash(usize),
}

fn recover_impl(
    source: &Path,
    destination: &Path,
    options: RecoveryOptions,
    mut checkpoint: impl FnMut(RecoveryStage),
) -> BagResult<RecoveryReport> {
    options.storage.validate()?;
    let limits = options.storage.limits.clone();
    let source_metadata = fs::symlink_metadata(source)?;
    if source_metadata.file_type().is_symlink() {
        return Err(BagError::invalid("recovery source must not be a symlink"));
    }
    let (paths, original_manifest): (Vec<PathBuf>, Option<Manifest>) = if source_metadata.is_dir() {
        let manifest = read_manifest(source, &limits)?;
        let source_absolute = fs::canonicalize(source)?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if fs::canonicalize(parent)?.starts_with(source_absolute) {
            return Err(BagError::invalid(
                "recovery destination must be outside the source recording",
            ));
        }
        let paths = manifest
            .shards
            .iter()
            .map(|shard| recovery_shard_path(source, shard))
            .collect::<BagResult<Vec<_>>>()?;
        (paths, Some(manifest))
    } else if source_metadata.file_type().is_file() {
        (vec![source.into()], None)
    } else {
        return Err(BagError::invalid(
            "recovery source must be a regular file or recording directory",
        ));
    };
    let files = paths
        .iter()
        .map(|path| PinnedFile::open(path))
        .collect::<BagResult<Vec<_>>>()?;
    let mut sources = paths
        .iter()
        .zip(&files)
        .map(|(path, file)| {
            Ok(RecoverySource {
                file: path.to_string_lossy().into_owned(),
                size_bytes: file.len(),
                sha256: file.sha256()?,
                verified_unchanged: false,
                error: None,
            })
        })
        .collect::<BagResult<Vec<_>>>()?;
    let mut first_reader = BagReader::from_file(files[0].clone(), limits.clone(), true)?;
    let header = first_reader.read_header()?;
    if original_manifest.as_ref().is_some_and(|m| {
        m.recording_id != header.recording_id || m.system_anchor_ns != header.system_anchor_ns
    }) {
        return Err(BagError::corrupt(
            "source manifest disagrees with shard recording header",
        ));
    }
    let mut writer = StorageWriter::create(
        destination,
        options.storage,
        header.recording_id.clone(),
        header.system_anchor_ns,
        serde_json::json!({"recovery":true, "source_configuration":header.configuration}),
    )?;
    let mut first_reader = Some(first_reader);
    let mut stopped = false;
    let mut previous: Option<(u64, u64)> = None;
    let mut orphan_business = 0u64;
    let mut orphan_metadata = 0u64;
    for (index, file) in files.iter().enumerate() {
        if stopped {
            sources[index].error = Some(
                BagError::new(
                    BagErrorKind::Incomplete,
                    "not read after the first damaged source shard",
                )
                .at(0),
            );
            continue;
        }
        checkpoint(RecoveryStage::BeforeSource(index));
        let mut reader = match first_reader.take() {
            Some(reader) => reader,
            None => match BagReader::from_file(file.clone(), limits.clone(), true) {
                Ok(reader) => reader,
                Err(error) => {
                    stopped = error.kind != BagErrorKind::ResourceLimit;
                    sources[index].error = Some(error);
                    continue;
                }
            },
        };
        let mut destination_error = None;
        let result = (|| -> BagResult<()> {
            let next_header = reader.read_header()?;
            if next_header != header {
                return Err(BagError::corrupt(
                    "source shards have inconsistent recording headers",
                ));
            }
            while let Some(message) = reader.next_message()? {
                if previous
                    .is_some_and(|(seq, time)| message.sequence <= seq || message.log_time < time)
                {
                    return Err(BagError::corrupt(
                        "source shard sequence/time moved backwards",
                    ));
                }
                if let Err(error) = writer.append(&message) {
                    destination_error = Some(error);
                    break;
                }
                previous = Some((message.sequence, message.log_time));
            }
            Ok(())
        })();
        orphan_business += reader.orphan_business;
        orphan_metadata += reader.orphan_metadata;
        // Keep verified metadata even if a topic had no complete sample pairs.
        for channel in reader.channels() {
            if destination_error.is_some() {
                break;
            }
            if let Err(error) = writer.register(Arc::new(channel.clone())) {
                destination_error = Some(error);
                break;
            }
        }
        if let Some(error) = destination_error {
            // Target quota/resource/write failures are not source damage.
            // Preserve their failure state even when completed output shards
            // can still be cleanly closed. Original hashes are pre-read facts;
            // verified_unchanged stays false because final verification was
            // not reached.
            writer.manifest.recovery_sources = sources.clone();
            writer.manifest.recovered_orphan_business = orphan_business;
            writer.manifest.recovered_orphan_metadata = orphan_metadata;
            finish_recovery(
                &mut writer,
                RecordStopReason::IoError,
                RecordCounters::default(),
                Some(error.clone()),
            )?;
            return Err(error);
        }
        if let Err(error) = result {
            // A local admission budget does not prove source corruption.
            // Keep this refusal in the report and try the next shard. A real
            // corrupt/incomplete source still stops the ordered recovery.
            stopped = error.kind != BagErrorKind::ResourceLimit;
            sources[index].error = Some(error);
        }
    }
    for (index, (file, report)) in files.iter().zip(&mut sources).enumerate() {
        let identity_matches = file.check_unchanged().is_ok();
        checkpoint(RecoveryStage::BeforeFinalHash(index));
        report.verified_unchanged =
            identity_matches && file.sha256().is_ok_and(|hash| hash == report.sha256);
        if !report.verified_unchanged {
            let changed =
                "source changed during recovery; its initial hash is not a stable snapshot";
            report.error = Some(match report.error.take() {
                Some(original) => BagError {
                    message: format!("{}; {changed}", original.message),
                    ..original
                },
                None => BagError::corrupt(changed),
            });
        }
    }
    writer.manifest.recovery_sources = sources.clone();
    writer.manifest.recovered_orphan_business = orphan_business;
    writer.manifest.recovered_orphan_metadata = orphan_metadata;
    finish_recovery(
        &mut writer,
        RecordStopReason::Requested,
        RecordCounters::default(),
        None,
    )?;
    let info = inspect(
        destination,
        InspectOptions {
            allow_incomplete: true,
            limits,
        },
    )?;
    Ok(RecoveryReport {
        destination: destination.into(),
        info,
        sources,
        discarded_orphan_business: orphan_business,
        discarded_orphan_metadata: orphan_metadata,
    })
}

// A crash can occur after the finalized shard is renamed but before the
// manifest is atomically replaced. Only this exact unfinished shard may use
// the finalized sibling; normal inspection still requires a complete manifest.
fn recovery_shard_path(directory: &Path, shard: &crate::ShardInfo) -> BagResult<PathBuf> {
    let path = shard_path(directory, &shard.file)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => Ok(path),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                && !shard.clean_close
                && shard.sha256.is_none() =>
        {
            let Some(final_name) = shard.file.strip_suffix(".partial") else {
                return Err(error.into());
            };
            let final_path = shard_path(directory, final_name)?;
            regular_file(&final_path)?;
            Ok(final_path)
        }
        Err(error) => Err(error.into()),
    }
}

fn finish_recovery(
    writer: &mut StorageWriter,
    reason: RecordStopReason,
    counters: RecordCounters,
    error: Option<BagError>,
) -> BagResult<Manifest> {
    let manifest = writer.finish(reason, counters, error)?;
    if let Some(error) = &manifest.error {
        return Err(error.clone());
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_tests::{interface, message, storage_options, Temp};

    fn writer(directory: &Path) -> StorageWriter {
        StorageWriter::create(
            directory,
            storage_options(),
            uuid::Uuid::new_v4().to_string(),
            10,
            serde_json::json!({}),
        )
        .unwrap()
    }

    fn add_message(writer: &mut StorageWriter) {
        let channel = Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &interface(),
                vec![aos_core::QoS::default()],
                aos_core::QoS::default(),
                None,
            )
            .unwrap(),
        );
        writer.append(&message(&channel, 1, 20)).unwrap();
    }

    #[test]
    fn recovery_replaced_second_source_finishes_verified_prefix_and_keeps_reports() {
        let temp = Temp::new();
        let source = temp.0.join("source");
        let destination = temp.0.join("recovered");
        let mut options = storage_options();
        options.messages_per_shard = 1;
        let mut writer = StorageWriter::create(
            &source,
            options.clone(),
            uuid::Uuid::new_v4().to_string(),
            10,
            serde_json::json!({}),
        )
        .unwrap();
        let channel = Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &interface(),
                vec![aos_core::QoS::default()],
                aos_core::QoS::default(),
                None,
            )
            .unwrap(),
        );
        for sequence in 1..=3 {
            writer
                .append(&message(&channel, sequence, 10 + sequence))
                .unwrap();
        }
        let manifest = writer
            .finish(RecordStopReason::Requested, RecordCounters::default(), None)
            .unwrap();
        let replaced = source.join(&manifest.shards[1].file);
        let report = recover_impl(
            &source,
            &destination,
            RecoveryOptions {
                storage: options.clone(),
            },
            |stage| {
                if stage == RecoveryStage::BeforeSource(1) {
                    let backup = source.join("replacement");
                    fs::copy(&replaced, &backup).unwrap();
                    fs::rename(&backup, &replaced).unwrap();
                }
            },
        )
        .unwrap();
        assert_eq!(report.info.messages, 1);
        assert!(report.info.manifest.clean_close && report.info.manifest.incomplete);
        assert_eq!(report.info.manifest.state, "finished");
        assert_eq!(report.sources.len(), 3);
        assert!(report.sources[0].verified_unchanged);
        assert!(!report.sources[1].verified_unchanged);
        assert_eq!(
            report.sources[1].error.as_ref().unwrap().kind,
            BagErrorKind::Corrupt
        );
        assert_eq!(
            report.sources[2].error.as_ref().unwrap().kind,
            BagErrorKind::Incomplete
        );
        let saved = read_manifest(&destination, &options.limits).unwrap();
        assert_eq!(saved.recovery_sources.len(), 3);
        assert_ne!(saved.state, "recording");
    }

    #[test]
    fn recovery_final_source_deletion_finishes_output_and_keeps_initial_hash() {
        let temp = Temp::new();
        let source = temp.0.join("source");
        let destination = temp.0.join("recovered");
        let mut writer = writer(&source);
        add_message(&mut writer);
        let manifest = writer
            .finish(RecordStopReason::Requested, RecordCounters::default(), None)
            .unwrap();
        let original_hash = manifest.shards[0].sha256.as_ref().unwrap();
        let file = source.join(&manifest.shards[0].file);
        let report = recover_impl(
            &source,
            &destination,
            RecoveryOptions {
                storage: storage_options(),
            },
            |stage| {
                if stage == RecoveryStage::BeforeFinalHash(0) {
                    fs::remove_file(&file).unwrap();
                }
            },
        )
        .unwrap();
        assert_eq!(report.info.messages, 1);
        assert_eq!(report.info.manifest.state, "finished");
        assert!(report.info.manifest.clean_close);
        assert_eq!(&report.sources[0].sha256, original_hash);
        assert!(!report.sources[0].verified_unchanged);
        assert!(report.sources[0].error.is_some());
        assert_eq!(
            read_manifest(&destination, &storage_options().limits)
                .unwrap()
                .recovery_sources
                .len(),
            1
        );
    }

    #[test]
    fn recovery_final_write_failure_keeps_destination_error_kind() {
        let temp = Temp::new();
        let directory = temp.0.join("destination");
        let mut writer = writer(&directory);
        add_message(&mut writer);
        writer.fail_next_write(std::io::ErrorKind::StorageFull);
        let error = finish_recovery(
            &mut writer,
            RecordStopReason::Requested,
            RecordCounters::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.kind, BagErrorKind::DiskFull);
        let manifest = read_manifest(&directory, &storage_options().limits).unwrap();
        assert!(manifest.incomplete);
        assert_eq!(manifest.state, "failed");
        assert_eq!(manifest.error.unwrap().kind, BagErrorKind::DiskFull);
        assert!(manifest.recovery_sources.is_empty());
    }

    #[test]
    fn manifest_cannot_hide_recovery_or_error_as_complete() {
        let temp = Temp::new();
        let directory = temp.0.join("source");
        let mut writer = writer(&directory);
        add_message(&mut writer);
        let complete = finish_recovery(
            &mut writer,
            RecordStopReason::Requested,
            RecordCounters::default(),
            None,
        )
        .unwrap();
        for index in 0..5 {
            let mut manifest = complete.clone();
            match index {
                0 => manifest.recovery_sources.push(RecoverySource {
                    file: "source.mcap".into(),
                    size_bytes: 0,
                    sha256: "0".repeat(64),
                    verified_unchanged: true,
                    error: None,
                }),
                1 => manifest.error = Some(BagError::corrupt("source was incomplete")),
                2 => manifest.state = "recording".into(),
                3 => manifest.recovered_orphan_business = 1,
                _ => manifest.state = "unknown".into(),
            }
            fs::write(
                directory.join("manifest.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            for allow_incomplete in [false, true] {
                assert_eq!(
                    inspect(
                        &directory,
                        InspectOptions {
                            allow_incomplete,
                            limits: storage_options().limits,
                        },
                    )
                    .unwrap_err()
                    .kind,
                    BagErrorKind::Corrupt,
                    "mutation {index}"
                );
            }
        }
    }

    #[test]
    fn recovery_accepts_finalized_shard_after_rename_before_manifest_save() {
        let temp = Temp::new();
        let source = temp.0.join("source");
        let mut writer = writer(&source);
        add_message(&mut writer);
        let unfinished = fs::read(source.join("manifest.json")).unwrap();
        finish_recovery(
            &mut writer,
            RecordStopReason::Requested,
            RecordCounters::default(),
            None,
        )
        .unwrap();
        fs::write(source.join("manifest.json"), &unfinished).unwrap();
        let report = recover(
            &source,
            &temp.0.join("recovered"),
            RecoveryOptions {
                storage: storage_options(),
            },
        )
        .unwrap();
        assert_eq!(report.info.messages, 1);
        assert!(report.info.manifest.incomplete);
        assert!(report.sources[0].verified_unchanged);
        assert!(report.sources[0].file.ends_with(".mcap"));
        assert_eq!(fs::read(source.join("manifest.json")).unwrap(), unfinished);
    }
}
