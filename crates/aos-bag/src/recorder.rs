use crate::{
    storage::{precheck_channel, AppendOutcome, RecordedMessage, StorageWriter},
    types::{now_ns, SampleMetadata},
    ActionObservation, BagError, BagErrorKind, BagResult, ChannelInfo, RecordOptions,
    RecordProgress, RecordReport, RecordState, RecordStatus, RecordStopReason, UnresolvedEndpoint,
};
use aos_core::{
    ActionInterfaces, ActionObservationKind, BoundedSerializedSample, Context, EndpointOptions,
    EntityKind, GraphEntity, Interface, Node, NodeOptions, QueueLimits, TypeDescriptionLimits,
    TypedSubscription, MAX_INTERFACE_SCHEMA_BYTES,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

type EndpointKey = (String, String, String);
struct Registration {
    channel: Arc<ChannelInfo>,
    reply: mpsc::SyncSender<BagResult<()>>,
    completion: Arc<Mutex<RegistrationCompletion>>,
}
#[derive(Default)]
struct RegistrationCompletion {
    cancelled: bool,
    timed_out: bool,
    result: Option<BagResult<()>>,
}
impl Registration {
    fn execute(
        &self,
        shared: &Shared,
        admit: impl FnOnce(Arc<ChannelInfo>) -> BagResult<BagResult<()>>,
    ) -> BagResult<()> {
        if self.completion.lock().unwrap().cancelled {
            return Ok(());
        }
        // Once disk work begins it cannot be rolled back safely. Publish its
        // outcome under the same lock as timeout, so a completed result wins
        // and a genuinely late commit remains visible in the counters.
        let result = admit(self.channel.clone());
        let admission = result.clone().and_then(|admission| admission);
        let mut completion = self.completion.lock().unwrap();
        if admission.is_ok() && completion.timed_out {
            shared
                .state
                .lock()
                .unwrap()
                .status
                .counters
                .descriptor_registration_late_commits_total += 1;
        }
        completion.result = Some(admission.clone());
        let _ = self.reply.send(admission);
        result.map(|_| ())
    }
}
struct State {
    // status holds only small scalar facts. Interface documents and unresolved
    // details live in immutable snapshots cloned outside the per-sample lock.
    status: RecordStatus,
    subscriptions: Arc<[Arc<Subscription>]>,
    channels: Arc<[Arc<ChannelInfo>]>,
    unresolved: Arc<[UnresolvedEndpoint]>,
    queue: VecDeque<(RecordedMessage, usize)>,
    next_sequence: u64,
    sequence_exhausted: bool,
    registration: Option<Registration>,
    manager_done: bool,
    discovery_done: bool,
    writer_done: bool,
    report: Option<RecordReport>,
}
struct Shared {
    state: Mutex<State>,
    stopping: AtomicBool,
    changed: Condvar,
    stop_changed: Condvar,
    options: RecordOptions,
    anchor: Instant,
    system_anchor_ns: u64,
}
impl Shared {
    fn status(&self) -> RecordStatus {
        let (mut status, channels, unresolved) = {
            let state = self.state.lock().unwrap();
            (
                state.status.clone(),
                state.channels.clone(),
                state.unresolved.clone(),
            )
        };
        status.active_channels = channels
            .iter()
            .map(|channel| channel.as_ref().clone())
            .collect();
        status.unresolved = unresolved.to_vec();
        status
    }
    fn progress(&self) -> RecordProgress {
        let (mut progress, unresolved) = {
            let state = self.state.lock().unwrap();
            let status = &state.status;
            (
                RecordProgress {
                    state: status.state,
                    directory: status.directory.clone(),
                    recording_id: status.recording_id.clone(),
                    counters: status.counters.clone(),
                    active_channel_count: state.channels.len(),
                    unresolved_count: state.unresolved.len(),
                    unresolved_sample: Vec::new(),
                    queue_messages: status.queue_messages,
                    queue_bytes: status.queue_bytes,
                    shards: status.shards,
                    stop_reason: status.stop_reason,
                    error: status.error.clone(),
                    resources_closed: status.resources_closed,
                    cleanup_error: status.cleanup_error.clone(),
                },
                state.unresolved.clone(),
            )
        };
        progress.unresolved_sample = unresolved
            .iter()
            .take(8)
            .map(|endpoint| UnresolvedEndpoint {
                topic: endpoint.topic.chars().take(256).collect(),
                type_hash: endpoint.type_hash.chars().take(128).collect(),
                reason: endpoint.reason.chars().take(512).collect(),
            })
            .collect();
        progress
    }
    fn cleanup_failed(&self, error: BagError) {
        self.state
            .lock()
            .unwrap()
            .status
            .cleanup_error
            .get_or_insert(error);
        self.changed.notify_all();
    }
    fn cleanup_result(&self, result: aos_core::Result<()>) {
        if let Err(error) = result {
            // Context shutdown or a previous close may already have released
            // the entity; Closed is the backend's idempotent-close outcome.
            if error != aos_core::Error::Closed {
                self.cleanup_failed(error.into());
            }
        }
    }
    fn publish_discovery(
        &self,
        subscriptions: &BTreeMap<EndpointKey, Arc<Subscription>>,
        unresolved: Vec<UnresolvedEndpoint>,
    ) {
        let active: Arc<[_]> = subscriptions.values().cloned().collect();
        let channels: Arc<[_]> = subscriptions.values().map(|s| s.channel.clone()).collect();
        let unresolved: Arc<[_]> = unresolved.into();
        let retired = {
            let mut state = self.state.lock().unwrap();
            (
                std::mem::replace(&mut state.subscriptions, active),
                std::mem::replace(&mut state.channels, channels),
                std::mem::replace(&mut state.unresolved, unresolved),
            )
        };
        // A subscription's final release may call the transport close path.
        drop(retired);
        self.changed.notify_all();
    }
    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }
    fn request_stop(&self, reason: RecordStopReason, error: Option<BagError>) {
        let mut state = self.state.lock().unwrap();
        if state.status.stop_reason.is_none() {
            state.status.stop_reason = Some(reason);
            state.status.state = RecordState::Stopping;
        }
        if let Some(error) = error {
            state.status.error.get_or_insert(error);
        }
        self.stopping.store(true, Ordering::Release);
        self.changed.notify_all();
        self.stop_changed.notify_all();
    }
    fn register(&self, channel: Arc<ChannelInfo>) -> BagResult<()> {
        let (reply, receive) = mpsc::sync_channel(1);
        let completion = Arc::new(Mutex::new(RegistrationCompletion::default()));
        {
            let mut state = self.state.lock().unwrap();
            if state.status.stop_reason.is_some() {
                return Err(BagError::new(BagErrorKind::Closed, "recorder is stopping"));
            }
            if state.registration.is_some() {
                return Err(BagError::resource(
                    "a descriptor registration is already pending",
                ));
            }
            state.registration = Some(Registration {
                channel,
                reply,
                completion: completion.clone(),
            });
            self.changed.notify_all();
        }
        let deadline = Instant::now() + Duration::from_millis(self.options.control_timeout_ms);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let stopping = self.stopping();
            if left.is_zero() || stopping {
                let mut outcome = completion.lock().unwrap();
                if let Some(result) = &outcome.result {
                    return result.clone();
                }
                outcome.cancelled = true;
                outcome.timed_out = !stopping;
                let retired = {
                    let mut state = self.state.lock().unwrap();
                    if outcome.timed_out {
                        state.status.counters.descriptor_registration_timeouts_total += 1;
                    }
                    if state
                        .registration
                        .as_ref()
                        .is_some_and(|pending| Arc::ptr_eq(&pending.completion, &completion))
                    {
                        state.registration.take()
                    } else {
                        None
                    }
                };
                drop(outcome);
                drop(retired);
                return Err(BagError::new(
                    if stopping {
                        BagErrorKind::Closed
                    } else {
                        BagErrorKind::Timeout
                    },
                    "descriptor persistence did not finish before the control deadline",
                ));
            }
            match receive.recv_timeout(left.min(Duration::from_millis(10))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(BagError::new(
                        BagErrorKind::Closed,
                        "recording writer closed during descriptor registration",
                    ))
                }
            }
        }
    }
    fn enqueue(
        &self,
        channel: &mut Arc<ChannelInfo>,
        sample: aos_core::Sample<Vec<u8>>,
    ) -> BagResult<()> {
        // The manager is the single ingress order owner. Adapter delivery
        // instants stay separate and are never replaced with this timestamp.
        let elapsed: u64 = self
            .anchor
            .elapsed()
            .as_nanos()
            .try_into()
            .map_err(|_| BagError::resource("recording steady duration overflow"))?;
        let log_time = self
            .system_anchor_ns
            .checked_add(elapsed)
            .ok_or_else(|| BagError::resource("recording log timestamp overflow"))?;
        let mut state = self.state.lock().unwrap();
        state.status.counters.received_total += 1;
        state.status.counters.received_bytes += sample.payload.len() as u64;
        if state.status.stop_reason.is_some() {
            state.status.counters.discarded_on_close_total += 1;
            return Ok(());
        }
        if sample.source.as_ref().and_then(|s| s.sequence).is_some() {
            state.status.counters.source_sequence_samples += 1;
        }
        if state.sequence_exhausted {
            return Err(BagError::resource("recording sequence exhausted u64"));
        }
        let sequence = state.next_sequence;
        match sequence.checked_add(1) {
            Some(next) => state.next_sequence = next,
            None => state.sequence_exhausted = true,
        }
        let source_bytes = sample.source.as_ref().map_or(0usize, |source| {
            source
                .publisher_id
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(source.timestamp.as_ref().map_or(0, |timestamp| {
                    timestamp
                        .kind
                        .len()
                        .saturating_add(timestamp.value.len())
                        .saturating_add(timestamp.clock_id.len())
                        .saturating_add(timestamp.origin.len())
                }))
        });
        if sample.payload.len() > self.options.message_bytes
            || source_bytes.saturating_add(512) > self.options.metadata_bytes
        {
            state.status.counters.oversized_total += 1;
            return Ok(());
        }
        if channel.sequence_epoch != (sequence >> 32) as u32 {
            *channel = Arc::new(channel.with_epoch(sequence)?);
        }
        let channel = channel.clone();
        let metadata = SampleMetadata::capture(&sample, sequence, channel.id.clone(), self.anchor);
        let metadata_size = serde_json::to_vec(&metadata)?.len();
        if metadata_size > self.options.metadata_bytes {
            state.status.counters.oversized_total += 1;
            return Ok(());
        }
        let message = RecordedMessage {
            channel,
            sequence,
            log_time,
            publish_time: log_time,
            payload: sample.payload,
            metadata,
        };
        let bytes = message
            .payload
            .len()
            .checked_add(metadata_size)
            .ok_or_else(|| BagError::resource("queue byte size overflow"))?;
        if state.queue.len() >= self.options.queue_messages
            || state
                .status
                .queue_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > self.options.queue_bytes)
        {
            state.status.counters.writer_queue_dropped_total += 1;
            state.status.counters.writer_queue_dropped_bytes += message.payload.len() as u64;
            return Ok(());
        }
        state.queue.push_back((message, bytes));
        state.status.queue_messages = state.queue.len();
        state.status.queue_bytes += bytes;
        self.changed.notify_all();
        Ok(())
    }
}

/// A recorder owns its ordinary Node and separate discovery, ingress and writer
/// workers, never the supplied Context. wait confirms their shutdown and keeps
/// resource cleanup errors separate from recorded-file validity.
pub struct Recorder {
    shared: Arc<Shared>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}
impl Recorder {
    pub fn start(context: Context, options: RecordOptions) -> BagResult<Self> {
        options.validate()?;
        if context.is_shutdown() {
            return Err(BagError::new(BagErrorKind::Closed, "Context is closed"));
        }
        match std::fs::symlink_metadata(&options.directory) {
            Ok(_) => return Err(BagError::invalid("recording destination already exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let request = match context.process_bootstrap() {
            Some(bootstrap) => bootstrap.node_request(options.node.clone())?,
            None => options.node.clone(),
        };
        let config = aos_core::resolve_node_config(
            "aos_bag_recorder",
            "/",
            NodeOptions {
                summary: "Record validated Topic bytes and receive evidence to MCAP".into(),
                tags: vec!["aos.bag.recorder".into()],
                ..Default::default()
            },
            &request,
        )?;
        let node = Node::new_resolved(context.clone(), config)?;
        let anchor = Instant::now();
        let system_anchor_ns = now_ns()?;
        let recording_id = uuid::Uuid::new_v4().to_string();
        let store = StorageWriter::create(
            &options.directory,
            options.storage.clone(),
            recording_id.clone(),
            system_anchor_ns,
            serde_json::json!({
                "node_fqn":node.fully_qualified_name(), "topics":options.topics,
                "all_topics":options.all_topics, "exclude":options.exclude,
                "qos":options.qos, "topic_qos":options.topic_qos,
                "discovery_interval_ms":options.discovery_interval_ms,
                "description_timeout_ms":options.description_timeout_ms,
                "queue_messages":options.queue_messages, "queue_bytes":options.queue_bytes,
                "adapter_queue_bytes":options.adapter_queue_bytes,
                "message_bytes":options.message_bytes, "metadata_bytes":options.metadata_bytes,
                "storage":options.storage
            }),
        )?;
        let shared = Arc::new(Shared {
            stopping: AtomicBool::new(false),
            state: Mutex::new(State {
                status: RecordStatus {
                    state: RecordState::Recording,
                    directory: options.directory.clone(),
                    recording_id,
                    counters: Default::default(),
                    active_channels: Vec::new(),
                    unresolved: Vec::new(),
                    queue_messages: 0,
                    queue_bytes: 0,
                    shards: 1,
                    stop_reason: None,
                    error: None,
                    resources_closed: false,
                    cleanup_error: None,
                },
                subscriptions: Arc::from([]),
                channels: Arc::from([]),
                unresolved: Arc::from([]),
                queue: VecDeque::new(),
                next_sequence: 0,
                sequence_exhausted: false,
                registration: None,
                manager_done: false,
                discovery_done: false,
                writer_done: false,
                report: None,
            }),
            changed: Condvar::new(),
            stop_changed: Condvar::new(),
            options,
            anchor,
            system_anchor_ns,
        });
        let writer_state = shared.clone();
        let writer = thread::Builder::new()
            .name("aos-bag-writer".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| writer_loop(&writer_state, store)));
                if result.is_err() {
                    writer_state.request_stop(
                        RecordStopReason::IoError,
                        Some(BagError::new(BagErrorKind::Io, "recording writer panicked")),
                    );
                    let mut state = writer_state.state.lock().unwrap();
                    state.writer_done = true;
                    state.status.state = RecordState::Failed;
                    writer_state.changed.notify_all();
                }
            })?;
        let discovery_state = shared.clone();
        let discovery_context = context.clone();
        let discovery_node = node.clone();
        let discovery = match thread::Builder::new()
            .name("aos-bag-discovery".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    discovery_loop(&discovery_state, &discovery_context, &discovery_node)
                }));
                if result.is_err() {
                    let error =
                        BagError::new(BagErrorKind::Io, "recording discovery worker panicked");
                    discovery_state.cleanup_failed(error.clone());
                    discovery_state.request_stop(RecordStopReason::IoError, Some(error));
                }
                discovery_state.state.lock().unwrap().discovery_done = true;
                discovery_state.changed.notify_all();
            }) {
            Ok(worker) => worker,
            Err(error) => {
                shared.request_stop(RecordStopReason::IoError, Some(error.into()));
                shared.cleanup_result(node.destroy_node());
                {
                    let mut state = shared.state.lock().unwrap();
                    state.discovery_done = true;
                    state.manager_done = true;
                }
                shared.changed.notify_all();
                let _ = writer.join();
                return Err(BagError::new(
                    BagErrorKind::Io,
                    "cannot start recording discovery worker",
                ));
            }
        };
        let manager_state = shared.clone();
        let failure_cleanup_node = node.clone();
        let manager = match thread::Builder::new()
            .name("aos-bag-ingress".into())
            .spawn(move || {
                let result =
                    catch_unwind(AssertUnwindSafe(|| manager_loop(&manager_state, &context)));
                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error))
                        if error.kind == BagErrorKind::Closed && context.is_shutdown() =>
                    {
                        manager_state.request_stop(RecordStopReason::ContextClosed, None);
                        None
                    }
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some(BagError::new(
                        BagErrorKind::Io,
                        "recording ingress worker panicked",
                    )),
                };
                if let Some(error) = error {
                    manager_state.request_stop(reason_for(&error), Some(error));
                }
                {
                    let mut state = manager_state.state.lock().unwrap();
                    while !state.discovery_done {
                        state = manager_state.changed.wait(state).unwrap();
                    }
                }
                manager_state.cleanup_result(node.destroy_node());
                let mut state = manager_state.state.lock().unwrap();
                state.manager_done = true;
                state.status.active_channels.clear();
                manager_state.changed.notify_all();
            }) {
            Ok(worker) => worker,
            Err(error) => {
                shared.request_stop(RecordStopReason::IoError, Some(error.into()));
                let _ = discovery.join();
                shared.cleanup_result(failure_cleanup_node.destroy_node());
                shared.state.lock().unwrap().manager_done = true;
                shared.changed.notify_all();
                let _ = writer.join();
                return Err(BagError::new(
                    BagErrorKind::Io,
                    "cannot start recording ingress worker",
                ));
            }
        };
        Ok(Self {
            shared,
            workers: Mutex::new(vec![writer, discovery, manager]),
        })
    }
    pub fn status(&self) -> RecordStatus {
        self.shared.status()
    }
    /// Frequent polling should use this bounded snapshot instead of cloning
    /// complete Interface documents from status().
    pub fn progress(&self) -> RecordProgress {
        self.shared.progress()
    }
    pub fn stop(&self, reason: RecordStopReason) -> BagResult<()> {
        self.shared.request_stop(reason, None);
        Ok(())
    }
    pub fn wait(&self, timeout: Duration) -> BagResult<RecordReport> {
        self.wait_with_check(timeout, || Ok(()))
    }
    /// Wait under one deadline, running interruption checks without holding
    /// recorder state. An interrupted wait does not change the stop request.
    pub fn wait_with_check(
        &self,
        timeout: Duration,
        mut check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<RecordReport> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| BagError::invalid("recording wait deadline overflow"))?;
        let mut state = self.shared.state.lock().unwrap();
        loop {
            drop(state);
            check()?;
            state = self.shared.state.lock().unwrap();
            if state.manager_done && state.writer_done {
                break;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(BagError::new(
                    BagErrorKind::Timeout,
                    "recording is not fully closed",
                ));
            }
            state = self
                .shared
                .changed
                .wait_timeout(state, left.min(Duration::from_millis(50)))
                .unwrap()
                .0;
        }
        let report = state.report.clone().ok_or_else(|| {
            state.status.error.clone().unwrap_or_else(|| {
                BagError::new(
                    BagErrorKind::Io,
                    "recording finished without a final manifest",
                )
            })
        })?;
        drop(state);
        for worker in self.workers.lock().unwrap().drain(..) {
            let _ = worker.join();
        }
        Ok(report)
    }
}
impl Drop for Recorder {
    fn drop(&mut self) {
        self.shared.request_stop(RecordStopReason::Requested, None);
    }
}

fn writer_loop(shared: &Shared, mut store: StorageWriter) {
    let mut failure = None;
    let mut clean_limit = false;
    loop {
        let (registration, message, done) = {
            let mut state = shared.state.lock().unwrap();
            while state.registration.is_none() && state.queue.is_empty() && !state.manager_done {
                let (next, timed) = shared
                    .changed
                    .wait_timeout(state, Duration::from_millis(20))
                    .unwrap();
                state = next;
                if timed.timed_out() {
                    break;
                }
            }
            let registration = state.registration.take();
            let message = if registration.is_none() {
                state.queue.pop_front()
            } else {
                None
            }
            .map(|(message, bytes)| {
                state.status.queue_bytes -= bytes;
                state.status.queue_messages = state.queue.len();
                message
            });
            let done = state.manager_done
                && state.queue.is_empty()
                && registration.is_none()
                && message.is_none();
            (registration, message, done)
        };
        if let Some(registration) = registration {
            if let Err(error) =
                registration.execute(shared, |channel| store.register_admitted(channel))
            {
                failure = Some(error);
                break;
            }
        }
        if let Some(message) = message {
            match store.append_admitted(&message) {
                Ok(AppendOutcome::Written) => {}
                Ok(AppendOutcome::Limit(reason)) => {
                    shared
                        .state
                        .lock()
                        .unwrap()
                        .status
                        .counters
                        .discarded_on_close_total += 1;
                    shared.request_stop(reason, None);
                    clean_limit = true;
                    break;
                }
                Err(error) => {
                    shared
                        .state
                        .lock()
                        .unwrap()
                        .status
                        .counters
                        .discarded_on_close_total += 1;
                    failure = Some(error);
                    break;
                }
            }
            let mut state = shared.state.lock().unwrap();
            state.status.counters.written_total = store.manifest.counters.written_total;
            state.status.counters.written_bytes = store.manifest.counters.written_bytes;
            state.status.shards = store.manifest.shards.len();
        }
        if let Err(error) = store.flush_due() {
            failure = Some(error);
            break;
        }
        if done {
            break;
        }
    }
    if failure.is_some() || clean_limit {
        if let Some(error) = &failure {
            shared.request_stop(reason_for(error), Some(error.clone()));
        }
        let mut state = shared.state.lock().unwrap();
        state.status.counters.discarded_on_close_total += state.queue.len() as u64;
        state.queue.clear();
        state.status.queue_bytes = 0;
        state.status.queue_messages = 0;
        if let Some(registration) = state.registration.take() {
            let error = failure
                .clone()
                .unwrap_or_else(|| BagError::resource("recording reached its clean storage limit"));
            let _ = registration.reply.send(Err(error));
        }
        while !state.manager_done {
            state = shared.changed.wait(state).unwrap();
        }
    }
    let (reason, counters, error) = {
        let state = shared.state.lock().unwrap();
        (
            state
                .status
                .stop_reason
                .unwrap_or(RecordStopReason::Requested),
            state.status.counters.clone(),
            failure.or_else(|| state.status.error.clone()),
        )
    };
    let result = store.finish(reason, counters, error);
    if let Some(cleanup_error) = store.manifest.cleanup_error.clone() {
        shared
            .state
            .lock()
            .unwrap()
            .status
            .cleanup_error
            .get_or_insert(cleanup_error);
    }
    match result {
        Ok(manifest) => {
            {
                let mut state = shared.state.lock().unwrap();
                state.status.counters = manifest.counters.clone();
                state.status.shards = manifest.shards.len();
                state.status.stop_reason = manifest.stop_reason;
                state.status.error = manifest
                    .error
                    .clone()
                    .or_else(|| state.status.cleanup_error.clone());
                state.status.state = if state.status.error.is_some() {
                    RecordState::Failed
                } else {
                    RecordState::Finished
                };
                state.status.resources_closed =
                    state.manager_done && state.status.cleanup_error.is_none();
            }
            let status = shared.status();
            let mut state = shared.state.lock().unwrap();
            state.report = Some(RecordReport { status, manifest });
            state.writer_done = true;
        }
        Err(error) => {
            let mut state = shared.state.lock().unwrap();
            state.status.error = Some(error);
            state.status.state = RecordState::Failed;
            state.status.resources_closed =
                state.manager_done && state.status.cleanup_error.is_none();
            state.writer_done = true;
        }
    }
    shared.changed.notify_all();
}

struct Subscription {
    channel: Arc<ChannelInfo>,
    epoch_channel: Mutex<Arc<ChannelInfo>>,
    subscription: TypedSubscription,
    dropped: AtomicU64,
    oversized: AtomicU64,
    oversized_bytes: AtomicU64,
    capacity_rejected: AtomicU64,
    active: AtomicBool,
}
fn observe_queue(shared: &Shared, subscription: &Subscription, closing: bool) {
    if let Some(stats) = subscription.subscription.queue_stats() {
        let previous = subscription
            .dropped
            .fetch_max(stats.dropped_total, Ordering::AcqRel);
        let dropped = stats.dropped_total.saturating_sub(previous);
        let (oversized, oversized_bytes, capacity_rejected) = if let Some(bytes) = stats.bytes {
            (
                bytes.oversized_messages_total.saturating_sub(
                    subscription
                        .oversized
                        .fetch_max(bytes.oversized_messages_total, Ordering::AcqRel),
                ),
                bytes.oversized_bytes_total.saturating_sub(
                    subscription
                        .oversized_bytes
                        .fetch_max(bytes.oversized_bytes_total, Ordering::AcqRel),
                ),
                bytes.capacity_rejected_total.saturating_sub(
                    subscription
                        .capacity_rejected
                        .fetch_max(bytes.capacity_rejected_total, Ordering::AcqRel),
                ),
            )
        } else {
            (0, 0, 0)
        };
        if dropped == 0
            && oversized == 0
            && oversized_bytes == 0
            && capacity_rejected == 0
            && (!closing || stats.queued == 0)
        {
            return;
        }
        let mut state = shared.state.lock().unwrap();
        let counters = &mut state.status.counters;
        counters.adapter_dropped_total += dropped;
        counters.adapter_oversized_total += oversized;
        counters.adapter_oversized_bytes += oversized_bytes;
        counters.adapter_capacity_rejected_total += capacity_rejected;
        if closing {
            counters.adapter_queued_at_close_total += stats.queued as u64;
        }
    }
}
fn close_subscription(shared: &Shared, subscription: &Subscription) {
    if subscription.active.swap(false, Ordering::AcqRel) {
        // Finish accounting for a sample already taken by ingress before
        // closing its backend. Otherwise TypedSubscription's post-receive
        // closed check could hide that sample from the discard counters.
        let _receiving = subscription.epoch_channel.lock().unwrap();
        observe_queue(shared, subscription, true);
        shared.cleanup_result(subscription.subscription.close());
        // Capture admission/drop counters once more after the queue closes:
        // callbacks may have delivered between the pre-close snapshot and close.
        // The queued-at-close lower bound must only be added by the first read.
        observe_queue(shared, subscription, false);
    }
}
fn manager_loop(shared: &Shared, context: &Context) -> BagResult<()> {
    while !shared.stopping() {
        if context.is_shutdown() {
            shared.request_stop(RecordStopReason::ContextClosed, None);
            break;
        }
        let subscriptions = shared.state.lock().unwrap().subscriptions.clone();
        let mut received_any = false;
        for subscription in subscriptions.iter() {
            if shared.stopping() {
                break;
            }
            let mut epoch_channel = subscription.epoch_channel.lock().unwrap();
            if !subscription.active.load(Ordering::Acquire) {
                continue;
            }
            match subscription
                .subscription
                .recv_serialized_bounded_timeout(Duration::ZERO, shared.options.message_bytes)
            {
                Ok(BoundedSerializedSample::Sample(sample)) => {
                    received_any = true;
                    shared.enqueue(&mut epoch_channel, sample)?;
                }
                Ok(BoundedSerializedSample::Oversized { payload_size_bytes }) => {
                    received_any = true;
                    let mut state = shared.state.lock().unwrap();
                    state.status.counters.received_total += 1;
                    state.status.counters.received_bytes += payload_size_bytes as u64;
                    if state.status.stop_reason.is_some() {
                        state.status.counters.discarded_on_close_total += 1;
                    } else {
                        state.status.counters.oversized_total += 1;
                    }
                }
                Err(aos_core::Error::Timeout) => {}
                Err(
                    error @ (aos_core::Error::Validation(_) | aos_core::Error::ResourceLimit(_)),
                ) => {
                    received_any = true;
                    let mut state = shared.state.lock().unwrap();
                    state.status.counters.received_total += 1;
                    if state.status.stop_reason.is_some() {
                        state.status.counters.discarded_on_close_total += 1;
                    } else if matches!(error, aos_core::Error::ResourceLimit(_)) {
                        state.status.counters.resource_rejected_total += 1;
                    } else {
                        state.status.counters.validation_failed_total += 1;
                    }
                }
                Err(aos_core::Error::Closed) if context.is_shutdown() => {
                    shared.request_stop(RecordStopReason::ContextClosed, None);
                    break;
                }
                Err(aos_core::Error::Closed) if !subscription.active.load(Ordering::Acquire) => {}
                Err(error) => return Err(error.into()),
            }
            observe_queue(shared, subscription, false);
        }
        if !received_any {
            let state = shared.state.lock().unwrap();
            if state.status.stop_reason.is_none() {
                drop(
                    shared
                        .changed
                        .wait_timeout(state, Duration::from_millis(2))
                        .unwrap(),
                );
            }
        }
    }
    Ok(())
}

struct Retry {
    failures: u32,
    after: Instant,
    reason: String,
}
impl Retry {
    fn failed(previous: Option<&Self>, interval_ms: u64, reason: String) -> Self {
        let failures = previous.map_or(1, |previous| previous.failures.saturating_add(1));
        let delay_ms = interval_ms
            .max(100)
            .saturating_mul(1 << failures.saturating_sub(1).min(8))
            .min(30_000);
        Self {
            failures,
            after: Instant::now() + Duration::from_millis(delay_ms),
            reason: reason.chars().take(2048).collect(),
        }
    }
    fn waiting(&self) -> bool {
        Instant::now() < self.after
    }
}
#[derive(Default)]
struct DiscoveryState {
    excluded_nodes: BTreeMap<String, bool>,
    failed_nodes: BTreeMap<String, Retry>,
    failed_endpoints: BTreeMap<EndpointKey, Retry>,
    next_provider: BTreeMap<EndpointKey, usize>,
    pending_action: Option<PendingActionDescription>,
    cursor: usize,
}
struct PendingActionDescription {
    key: EndpointKey,
    provider_id: String,
    interface: Interface,
}
fn unresolved(key: &EndpointKey, reason: impl Into<String>) -> UnresolvedEndpoint {
    UnresolvedEndpoint {
        topic: key.0.clone(),
        type_hash: key.2.clone(),
        reason: reason.into().chars().take(2048).collect(),
    }
}
fn discovery_loop(shared: &Shared, context: &Context, node: &Node) {
    let watch = context.watch_graph().ok().flatten();
    let mut subscriptions = BTreeMap::<EndpointKey, Arc<Subscription>>::new();
    let mut discovery = DiscoveryState::default();
    while !shared.stopping() {
        if context.is_shutdown() {
            shared.request_stop(RecordStopReason::ContextClosed, None);
            break;
        }
        // One total RPC budget per round, independent of endpoint count. An
        // admitted subscription continues receiving on the ingress worker.
        let deadline =
            Instant::now() + Duration::from_millis(shared.options.description_timeout_ms);
        let graph = match &watch {
            Some(watch) => watch.snapshot(),
            None => context.graph(deadline.saturating_duration_since(Instant::now())),
        };
        match graph {
            Ok(graph) => discover(
                shared,
                context,
                node,
                graph,
                &mut subscriptions,
                &mut discovery,
                deadline,
            ),
            Err(error) => {
                shared
                    .state
                    .lock()
                    .unwrap()
                    .status
                    .counters
                    .description_failed_total += 1;
                shared.publish_discovery(
                    &subscriptions,
                    vec![UnresolvedEndpoint {
                        topic: String::new(),
                        type_hash: String::new(),
                        reason: format!("graph discovery unavailable: {error}")
                            .chars()
                            .take(2048)
                            .collect(),
                    }],
                );
            }
        }
        // Ingress and writer notifications must not defeat discovery's
        // interval/backoff. Only stop or the absolute interval ends this wait.
        let next_round =
            Instant::now() + Duration::from_millis(shared.options.discovery_interval_ms);
        let mut state = shared.state.lock().unwrap();
        while state.status.stop_reason.is_none() {
            let left = next_round.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            state = shared.stop_changed.wait_timeout(state, left).unwrap().0;
        }
    }
    for subscription in subscriptions.values() {
        close_subscription(shared, subscription);
    }
    let unresolved = shared.state.lock().unwrap().unresolved.clone();
    shared.publish_discovery(&BTreeMap::new(), unresolved.to_vec());
}
fn discover(
    shared: &Shared,
    context: &Context,
    node: &Node,
    graph: Vec<GraphEntity>,
    subscriptions: &mut BTreeMap<EndpointKey, Arc<Subscription>>,
    discovery: &mut DiscoveryState,
    deadline: Instant,
) {
    // Do not blame a healthy peer for a request given only the last few
    // microseconds of the round. An unstarted RPC is deferred; an attempted,
    // failed RPC must retain its failure and let other endpoints advance.
    let minimum_rpc_budget = Duration::from_millis(shared.options.description_timeout_ms) / 4;
    let maximum = shared.options.storage.limits.channels;
    let graph_limit = maximum.saturating_mul(32);
    let truncated = graph.len() > graph_limit;
    let graph: Vec<_> = graph.into_iter().take(graph_limit).collect();
    let live_nodes: BTreeSet<_> = graph.iter().map(|entity| entity.node_id.as_str()).collect();
    if !truncated {
        discovery
            .excluded_nodes
            .retain(|id, _| live_nodes.contains(id.as_str()));
        discovery
            .failed_nodes
            .retain(|id, _| live_nodes.contains(id.as_str()));
    }
    let mut groups = BTreeMap::<EndpointKey, Vec<&GraphEntity>>::new();
    for entity in &graph {
        let Some(endpoint) = &entity.endpoint else {
            continue;
        };
        if entity.kind == EntityKind::Publisher
            && entity.node_id != node.id()
            && shared.options.selected(&endpoint.name)
        {
            groups
                .entry((
                    endpoint.name.clone(),
                    endpoint.type_name.clone(),
                    endpoint.type_hash.clone(),
                ))
                .or_default()
                .push(entity);
        }
    }
    if !truncated {
        discovery
            .failed_endpoints
            .retain(|key, _| groups.contains_key(key));
        discovery
            .next_provider
            .retain(|key, _| groups.contains_key(key));
        if discovery.pending_action.as_ref().is_some_and(|pending| {
            !groups.get(&pending.key).is_some_and(|providers| {
                providers
                    .iter()
                    .any(|provider| provider.node_id == pending.provider_id)
            })
        }) {
            discovery.pending_action = None;
        }
        let removed: Vec<_> = subscriptions
            .keys()
            .filter(|key| !groups.contains_key(*key))
            .cloned()
            .collect();
        for key in removed {
            close_subscription(shared, &subscriptions.remove(&key).unwrap());
        }
    }
    let groups: Vec<_> = groups.into_iter().collect();
    // At most one validated observation Interface is retained across rounds.
    // Its parent RPC gets the next complete budget instead of restarting the
    // first RPC forever. A parent failure clears this continuation and backs
    // off the endpoint normally.
    let start = discovery
        .pending_action
        .as_ref()
        .and_then(|pending| groups.iter().position(|(key, _)| key == &pending.key))
        .unwrap_or_else(|| {
            if groups.is_empty() {
                0
            } else {
                discovery.cursor % groups.len()
            }
        });
    let mut resume_at = None;
    let mut unresolved_endpoints = BTreeMap::<EndpointKey, UnresolvedEndpoint>::new();
    if truncated {
        shared
            .state
            .lock()
            .unwrap()
            .status
            .counters
            .endpoint_rejected_total += 1;
        let key = (String::new(), String::new(), String::new());
        unresolved_endpoints.insert(
            key.clone(),
            unresolved(
                &key,
                "graph discovery exceeds admission limit; existing subscriptions retained",
            ),
        );
    }
    for offset in 0..groups.len() {
        if shared.stopping() {
            break;
        }
        let index = (start + offset) % groups.len();
        let (key, advertised) = &groups[index];
        let mut providers = Vec::new();
        let mut pending = false;
        for provider in advertised {
            let exclude = if let Some(exclude) = discovery.excluded_nodes.get(&provider.node_id) {
                *exclude
            } else if let Some(retry) = discovery
                .failed_nodes
                .get(&provider.node_id)
                .filter(|retry| retry.waiting())
            {
                unresolved_endpoints.insert(key.clone(), unresolved(key, retry.reason.clone()));
                pending = true;
                continue;
            } else {
                let Some(left) = rpc_time_left(deadline, minimum_rpc_budget) else {
                    resume_at.get_or_insert(index);
                    unresolved_endpoints.insert(
                        key.clone(),
                        unresolved(key, "node description deferred by discovery round budget"),
                    );
                    pending = true;
                    continue;
                };
                let card = match context.get_node_card_bounded(
                    &node_fqn(provider),
                    left,
                    Context::MAX_NODE_CARD_RESPONSE_BYTES,
                ) {
                    Ok(card) => card,
                    Err(error) => {
                        shared
                            .state
                            .lock()
                            .unwrap()
                            .status
                            .counters
                            .description_failed_total += 1;
                        let reason = format!("node description unavailable: {error}");
                        let retry = Retry::failed(
                            discovery.failed_nodes.get(&provider.node_id),
                            shared.options.discovery_interval_ms,
                            reason.clone(),
                        );
                        if discovery.failed_nodes.len() < graph_limit
                            || discovery.failed_nodes.contains_key(&provider.node_id)
                        {
                            discovery
                                .failed_nodes
                                .insert(provider.node_id.clone(), retry);
                        }
                        unresolved_endpoints.insert(key.clone(), unresolved(key, reason));
                        pending = true;
                        continue;
                    }
                };
                discovery.failed_nodes.remove(&provider.node_id);
                let exclude = card["tags"].as_array().is_some_and(|tags| {
                    tags.iter().any(|tag| {
                        tag.as_str()
                            .is_some_and(|tag| matches!(tag, "aos.bag.recorder" | "aos.bag.replay"))
                    })
                });
                if discovery.excluded_nodes.len() < graph_limit {
                    discovery
                        .excluded_nodes
                        .insert(provider.node_id.clone(), exclude);
                }
                exclude
            };
            if !exclude {
                providers.push(*provider);
            }
        }
        if providers.is_empty() {
            if !pending {
                if let Some(previous) = subscriptions.remove(key) {
                    close_subscription(shared, &previous);
                }
            }
            continue;
        }
        let mut offered = providers
            .iter()
            .filter_map(|provider| provider.qos.clone())
            .collect::<Vec<_>>();
        offered.sort_by_cached_key(|qos| serde_json::to_string(qos).unwrap());
        offered.dedup();
        let current = subscriptions.get(key).cloned();
        if current
            .as_ref()
            .is_some_and(|current| current.channel.offered_qos == offered)
        {
            continue;
        }
        if let Some(retry) = discovery
            .failed_endpoints
            .get(key)
            .filter(|retry| retry.waiting())
        {
            unresolved_endpoints.insert(key.clone(), unresolved(key, retry.reason.clone()));
            continue;
        }
        if current.is_none() && rpc_time_left(deadline, minimum_rpc_budget).is_none() {
            resume_at.get_or_insert(index);
            unresolved_endpoints.insert(
                key.clone(),
                unresolved(key, "type description deferred by discovery round budget"),
            );
            continue;
        }
        let description = match &current {
            Some(current) => Interface::from_json_bounded(
                current.channel.interface.clone(),
                shared
                    .options
                    .storage
                    .limits
                    .schema_bytes
                    .min(MAX_INTERFACE_SCHEMA_BYTES),
            )
            .map(|interface| Some((interface, current.channel.action.clone())))
            .map_err(BagError::from),
            None => describe(
                context,
                &providers,
                deadline,
                minimum_rpc_budget,
                key,
                discovery,
                &shared.options,
            ),
        };
        let result = match description {
            Ok(Some((interface, action))) => {
                prepare_subscription(shared, node, key, &interface, offered, action)
            }
            Ok(None) => {
                resume_at.get_or_insert(index);
                unresolved_endpoints.insert(
                    key.clone(),
                    unresolved(key, "type description deferred by discovery round budget"),
                );
                continue;
            }
            Err(error) => {
                shared
                    .state
                    .lock()
                    .unwrap()
                    .status
                    .counters
                    .description_failed_total += 1;
                Err(error)
            }
        };
        match result {
            Ok(subscription) => {
                discovery.failed_endpoints.remove(key);
                if let Some(previous) = subscriptions.insert(key.clone(), Arc::new(subscription)) {
                    close_subscription(shared, &previous);
                }
            }
            Err(error) => {
                if !shared.stopping() {
                    shared
                        .state
                        .lock()
                        .unwrap()
                        .status
                        .counters
                        .endpoint_rejected_total += 1;
                    let retry = Retry::failed(
                        discovery.failed_endpoints.get(key),
                        shared.options.discovery_interval_ms,
                        error.to_string(),
                    );
                    unresolved_endpoints.insert(key.clone(), unresolved(key, retry.reason.clone()));
                    if discovery.failed_endpoints.len() < maximum
                        || discovery.failed_endpoints.contains_key(key)
                    {
                        discovery.failed_endpoints.insert(key.clone(), retry);
                    }
                }
            }
        }
    }
    discovery.cursor = resume_at.unwrap_or(start.saturating_add(1));
    shared.publish_discovery(
        subscriptions,
        unresolved_endpoints.into_values().take(maximum).collect(),
    );
}
fn prepare_subscription(
    shared: &Shared,
    node: &Node,
    key: &EndpointKey,
    interface: &Interface,
    offered: Vec<aos_core::QoS>,
    action: Option<ActionObservation>,
) -> BagResult<Subscription> {
    let qos = shared
        .options
        .topic_qos
        .get(&key.0)
        .cloned()
        .unwrap_or_else(|| {
            if action
                .as_ref()
                .is_some_and(|action| action.kind == ActionObservationKind::Status)
            {
                aos_core::action::status_qos()
            } else {
                shared.options.qos.clone()
            }
        });
    shared.options.validate_qos_budget(&qos)?;
    let channel = Arc::new(ChannelInfo::new(
        key.0.clone(),
        interface,
        offered,
        qos.clone(),
        action,
    )?);
    precheck_channel(&channel, &shared.options.storage)?;
    let resolved = if let Some(action) = &channel.action {
        format!(
            "{}{}",
            node.resolve_name_details(&action.base, aos_core::NameKind::Action)?
                .resolved_name,
            action.kind.suffix()
        )
    } else {
        node.resolve_name_details(&channel.topic, aos_core::NameKind::Topic)?
            .resolved_name
    };
    if resolved != channel.topic {
        return Err(BagError::invalid(
            "recorder remap changes a selected source endpoint",
        ));
    }
    shared.register(channel.clone())?;
    if shared.stopping() {
        return Err(BagError::new(
            BagErrorKind::Closed,
            "recorder stopped during descriptor admission",
        ));
    }
    let endpoint_options = EndpointOptions {
        summary: "Bag recorder observation".into(),
        ..Default::default()
    };
    let limits = QueueLimits {
        message_bytes: shared.options.message_bytes,
        queue_bytes: shared.options.adapter_queue_bytes,
    };
    let subscription = if let Some(action) = &channel.action {
        node.create_action_observation_subscription_with_limits(
            &Interface::from_json(action.interface.clone())?,
            &action.base,
            action.kind,
            qos,
            limits,
            endpoint_options,
        )
    } else {
        node.create_subscription_with_limits(
            interface,
            &channel.topic,
            qos,
            limits,
            endpoint_options,
        )
    }?;
    if subscription.endpoint().name != channel.topic {
        shared.cleanup_result(subscription.close());
        return Err(BagError::invalid(
            "recorder remap changes a selected source endpoint",
        ));
    }
    if subscription.queue_stats().is_none() {
        shared
            .state
            .lock()
            .unwrap()
            .status
            .counters
            .adapter_stats_unavailable += 1;
    }
    let subscription = Subscription {
        epoch_channel: Mutex::new(channel.clone()),
        channel,
        subscription,
        dropped: AtomicU64::new(0),
        oversized: AtomicU64::new(0),
        oversized_bytes: AtomicU64::new(0),
        capacity_rejected: AtomicU64::new(0),
        active: AtomicBool::new(true),
    };
    if shared.stopping() {
        close_subscription(shared, &subscription);
        return Err(BagError::new(
            BagErrorKind::Closed,
            "recorder stopped during subscription creation",
        ));
    }
    Ok(subscription)
}
fn describe(
    context: &Context,
    providers: &[&GraphEntity],
    deadline: Instant,
    minimum_rpc_budget: Duration,
    key: &EndpointKey,
    discovery: &mut DiscoveryState,
    options: &RecordOptions,
) -> BagResult<Option<(Interface, Option<ActionObservation>)>> {
    describe_with_budget(context, providers, key, discovery, options, || {
        rpc_time_left(deadline, minimum_rpc_budget)
    })
}

// A deterministic budget source keeps the cross-provider deferral boundary
// testable without racing two wall-clock reads a few microseconds apart.
fn describe_with_budget(
    context: &Context,
    providers: &[&GraphEntity],
    key: &EndpointKey,
    discovery: &mut DiscoveryState,
    options: &RecordOptions,
    mut rpc_budget: impl FnMut() -> Option<Duration>,
) -> BagResult<Option<(Interface, Option<ActionObservation>)>> {
    let progress_limit = options.storage.limits.channels.saturating_mul(32);
    let limits = TypeDescriptionLimits {
        schema_bytes: options
            .storage
            .limits
            .schema_bytes
            .min(MAX_INTERFACE_SCHEMA_BYTES),
        ..TypeDescriptionLimits::default()
    };
    let endpoint = providers[0].endpoint.as_ref().unwrap();
    let continuation = if discovery
        .pending_action
        .as_ref()
        .is_some_and(|pending| &pending.key == key)
    {
        discovery.pending_action.take()
    } else {
        None
    };
    let mut continuation = continuation.filter(|pending| {
        providers
            .iter()
            .any(|provider| provider.node_id == pending.provider_id)
    });
    let start = continuation
        .as_ref()
        .and_then(|pending| {
            providers
                .iter()
                .position(|provider| provider.node_id == pending.provider_id)
        })
        .unwrap_or_else(|| {
            discovery.next_provider.get(key).copied().unwrap_or(0) % providers.len()
        });
    let mut last = None;
    for offset in 0..providers.len() {
        let index = (start + offset) % providers.len();
        let provider = providers[index];
        let initial_budget = if continuation.is_none() {
            let Some(left) = rpc_budget() else {
                // Preserve a previous provider's failure without treating
                // this unstarted provider as failed or advancing its cursor.
                return match last {
                    Some(error) => Err(error),
                    None => Ok(None),
                };
            };
            Some(left)
        } else {
            None
        };
        let attempt = (|| -> BagResult<_> {
            let interface = if let Some(pending) = continuation.take() {
                pending.interface
            } else {
                context.get_type_description_bounded(
                    &node_fqn(provider),
                    &endpoint.type_name,
                    Some(&endpoint.type_hash),
                    initial_budget.expect("a new description has an RPC budget"),
                    limits,
                )?
            };
            if !endpoint.name.split('/').any(|p| p == "_action") {
                return Ok(Some((interface, None)));
            }
            let kind = if endpoint
                .name
                .ends_with(ActionObservationKind::Feedback.suffix())
            {
                ActionObservationKind::Feedback
            } else if endpoint
                .name
                .ends_with(ActionObservationKind::Status.suffix())
            {
                ActionObservationKind::Status
            } else {
                return Err(BagError::invalid(
                    "only Action feedback/status Topics may be recorded",
                ));
            };
            let base = endpoint
                .name
                .strip_suffix(kind.suffix())
                .ok_or_else(|| BagError::invalid("Action observation suffix mismatch"))?;
            if base.is_empty() || base == "/" || base.split('/').any(|part| part == "_action") {
                return Err(BagError::invalid(
                    "Action observation requires a non-internal Action base",
                ));
            }
            aos_core::names::validate_absolute_name(base)?;
            let parent_name = interface.definition()["action_type"]
                .as_str()
                .ok_or_else(|| BagError::corrupt("Action observation lacks parent type name"))?;
            let parent_hash = interface.definition()["action_hash"]
                .as_str()
                .ok_or_else(|| BagError::corrupt("Action observation lacks parent type hash"))?;
            let Some(left) = rpc_budget() else {
                if discovery.pending_action.is_some() {
                    // Retain the existing continuation and retry this new
                    // endpoint later, without recording a provider failure.
                    return Ok(None);
                }
                discovery.pending_action = Some(PendingActionDescription {
                    key: key.clone(),
                    provider_id: provider.node_id.clone(),
                    interface,
                });
                return Ok(None);
            };
            // The MCAP schema limit belongs to the observation message. The
            // Action parent is still bounded by the common Interface profile.
            let parent = context.get_type_description_bounded(
                &node_fqn(provider),
                parent_name,
                Some(parent_hash),
                left,
                TypeDescriptionLimits::default(),
            )?;
            let types = ActionInterfaces::new(&parent)?;
            let expected = if kind == ActionObservationKind::Feedback {
                types.feedback
            } else {
                types.status
            };
            if expected.type_hash() != interface.type_hash() {
                return Err(BagError::corrupt(
                    "provider Action parent does not match observation type",
                ));
            }
            let action = ActionObservation {
                base: base.into(),
                kind,
                interface: parent.definition().clone(),
            };
            Ok(Some((interface, Some(action))))
        })();
        match attempt {
            Ok(value) => {
                if value.is_some() {
                    discovery.next_provider.remove(key);
                }
                return Ok(value);
            }
            Err(error) => {
                if discovery.next_provider.len() < progress_limit
                    || discovery.next_provider.contains_key(key)
                {
                    discovery
                        .next_provider
                        .insert(key.clone(), (index + 1) % providers.len());
                }
                last = Some(error);
                // There is no time for another provider. Preserve this
                // provider's next index instead of advancing over providers
                // whose RPCs have never been attempted.
                if rpc_budget().is_none() {
                    break;
                }
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        BagError::new(
            BagErrorKind::Transport,
            "no provider returned its complete Interface",
        )
    }))
}
fn rpc_time_left(deadline: Instant, minimum: Duration) -> Option<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    (!left.is_zero() && left >= minimum).then_some(left)
}
fn node_fqn(entity: &GraphEntity) -> String {
    format!(
        "{}/{}",
        entity.namespace.trim_end_matches('/'),
        entity.node_name
    )
}
fn reason_for(error: &BagError) -> RecordStopReason {
    match error.kind {
        BagErrorKind::CapacityLimit => RecordStopReason::CapacityLimit,
        BagErrorKind::ResourceLimit => RecordStopReason::ResourceLimit,
        BagErrorKind::DiskFull => RecordStopReason::DiskFull,
        BagErrorKind::Closed => RecordStopReason::ContextClosed,
        _ => RecordStopReason::IoError,
    }
}

#[cfg(test)]
#[path = "recorder_tests.rs"]
mod regression_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use aos_core::{QoS, ReceiveMetadata, ReceiveScope, Sample, SourceMetadata, SourceTimestamp};

    pub(super) fn shared() -> Shared {
        let mut options = RecordOptions {
            directory: "unused-test-directory".into(),
            all_topics: true,
            message_bytes: 128,
            metadata_bytes: 1024,
            ..Default::default()
        };
        options.queue_messages = 10;
        Shared {
            stopping: AtomicBool::new(false),
            state: Mutex::new(State {
                status: RecordStatus {
                    state: RecordState::Recording,
                    directory: options.directory.clone(),
                    recording_id: "test".into(),
                    counters: Default::default(),
                    active_channels: Vec::new(),
                    unresolved: Vec::new(),
                    queue_messages: 0,
                    queue_bytes: 0,
                    shards: 1,
                    stop_reason: None,
                    error: None,
                    resources_closed: false,
                    cleanup_error: None,
                },
                subscriptions: Arc::from([]),
                channels: Arc::from([]),
                unresolved: Arc::from([]),
                queue: VecDeque::new(),
                next_sequence: 0,
                sequence_exhausted: false,
                registration: None,
                manager_done: false,
                discovery_done: false,
                writer_done: false,
                report: None,
            }),
            changed: Condvar::new(),
            stop_changed: Condvar::new(),
            options,
            anchor: Instant::now(),
            system_anchor_ns: 10,
        }
    }

    #[test]
    fn ingress_count_and_byte_bounds_drop_newest_without_replacing_valid_samples() {
        let mut channel = Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &crate::storage_tests::interface(),
                vec![QoS::default()],
                QoS::default(),
                None,
            )
            .unwrap(),
        );
        for byte_limit in [false, true] {
            let mut shared = shared();
            let sample =
                || Sample::without_receive_metadata(b"{\"value\":7,\"text\":\"ok\"}".to_vec());
            shared.enqueue(&mut channel, sample()).unwrap();
            if byte_limit {
                shared.options.queue_bytes = shared.state.lock().unwrap().status.queue_bytes;
            } else {
                shared.options.queue_messages = 1;
            }
            shared.enqueue(&mut channel, sample()).unwrap();
            shared
                .enqueue(
                    &mut channel,
                    Sample::without_receive_metadata(vec![b'x'; 129]),
                )
                .unwrap();
            {
                let mut state = shared.state.lock().unwrap();
                assert_eq!(state.queue.len(), 1);
                assert_eq!(state.queue.front().unwrap().0.sequence, 0);
                assert_eq!(state.status.counters.writer_queue_dropped_total, 1);
                assert_eq!(state.status.counters.oversized_total, 1);
                assert_eq!(state.status.counters.received_total, 3);
                assert!(state.status.counters.end_to_end_lost_total.is_none());
                assert!(state.queue.front().unwrap().0.metadata.received.is_none());
                state.queue.clear();
                state.status.queue_bytes = 0;
                state.status.queue_messages = 0;
            }
            shared.enqueue(&mut channel, sample()).unwrap();
            assert_eq!(
                shared
                    .state
                    .lock()
                    .unwrap()
                    .queue
                    .front()
                    .unwrap()
                    .0
                    .sequence,
                3
            );
            shared.request_stop(RecordStopReason::Requested, None);
            shared.enqueue(&mut channel, sample()).unwrap();
            assert_eq!(
                shared.state.lock().unwrap().status.counters.received_total,
                5
            );
            assert_eq!(
                shared
                    .state
                    .lock()
                    .unwrap()
                    .status
                    .counters
                    .discarded_on_close_total,
                1
            );
        }
    }

    #[test]
    fn delayed_adapter_and_backward_source_times_remain_evidence_not_ingress_order() {
        let shared = shared();
        let mut channel = Arc::new(
            ChannelInfo::new(
                "/data".into(),
                &crate::storage_tests::interface(),
                vec![QoS::default()],
                QoS::default(),
                None,
            )
            .unwrap(),
        );
        let cases = [
            (
                shared.anchor + Duration::from_millis(20),
                std::time::UNIX_EPOCH + Duration::from_nanos(100),
                "18446744073709551615",
                500,
            ),
            (
                shared.anchor - Duration::from_millis(10),
                std::time::UNIX_EPOCH - Duration::from_nanos(100),
                "1",
                2,
            ),
        ];
        for (index, (steady, system, source_time, source_sequence)) in cases.into_iter().enumerate()
        {
            let payload = format!(" {{ \"value\":{index}, \"text\":\"raw\" }}\n").into_bytes();
            shared
                .enqueue(
                    &mut channel,
                    Sample {
                        payload_size_bytes: payload.len(),
                        payload,
                        received: Some(ReceiveMetadata {
                            steady,
                            system,
                            scope: ReceiveScope::AdapterDelivery,
                        }),
                        source: Some(SourceMetadata {
                            timestamp: Some(SourceTimestamp {
                                kind: "zenoh_hlc".into(),
                                value: source_time.into(),
                                clock_id: "unverified-remote-clock".into(),
                                comparable: false,
                                origin: "router_or_publisher".into(),
                            }),
                            publisher_id: Some("publisher".into()),
                            sequence: Some(source_sequence),
                        }),
                    },
                )
                .unwrap();
        }
        let state = shared.state.lock().unwrap();
        assert_eq!(state.queue.len(), 2);
        let first = &state.queue[0].0;
        let second = &state.queue[1].0;
        assert_eq!((first.sequence, second.sequence), (0, 1));
        assert!(first.log_time <= second.log_time);
        for message in [first, second] {
            assert_eq!(message.publish_time, message.log_time);
            assert_eq!(message.metadata.recording_sequence, message.sequence);
        }
        for (message, steady, system, source_time, source_sequence) in [
            (first, "20000000", "100", "18446744073709551615", 500),
            (second, "-10000000", "-100", "1", 2),
        ] {
            let received = message.metadata.received.as_ref().unwrap();
            assert_eq!(received.steady_offset_ns, steady);
            assert_eq!(received.system_unix_ns, system);
            let source = message.metadata.source.as_ref().unwrap();
            assert_eq!(source.sequence, Some(source_sequence));
            let timestamp = source.timestamp.as_ref().unwrap();
            assert_eq!(timestamp.value, source_time);
            assert!(!timestamp.comparable);
        }
        assert_eq!(state.status.counters.source_sequence_samples, 2);
        assert!(state.status.counters.end_to_end_lost_total.is_none());
    }
}
