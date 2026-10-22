use crate::{
    offline::inspect_indexed,
    reader::{IndexedRead, IndexedReader, ShardIndex},
    storage::RecordedMessage,
    types::validate_rate,
    BagError, BagErrorKind, BagInfo, BagResult, ChannelInfo, InspectOptions, PlaybackEndpoint,
    PlaybackOptions, PlaybackProgress, PlaybackReport, PlaybackState, PlaybackStatus,
};
use aos_core::{
    Clock, ClockType, Context, EndpointOptions, Interface, NameKind, Node, NodeOptions,
    ResolvedNodeConfig, TypedPublisher,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone)]
struct Timeline {
    base: u64,
    end: u64,
    anchor: Instant,
    rate: f64,
    running: bool,
}
impl Timeline {
    fn position_at(&self, now: Instant) -> u64 {
        if !self.running || self.base >= self.end {
            return self.base;
        }
        let remaining = self.end - self.base;
        let elapsed = now.saturating_duration_since(self.anchor).as_secs_f64() * 1e9;
        let advance = elapsed * self.rate;
        // Bound before converting or adding. Very fast rates finish rather
        // than overflowing a Duration; time never wraps to an earlier sample.
        if !advance.is_finite() || advance >= remaining as f64 {
            self.end
        } else {
            self.base + (advance.floor() as u64).min(remaining)
        }
    }
    fn position(&self) -> u64 {
        self.position_at(Instant::now())
    }
    fn reanchor(&mut self, position: u64, running: bool) {
        self.base = position;
        self.anchor = Instant::now();
        self.running = running;
    }
    fn wait_for(&self, position: u64) -> BagResult<Duration> {
        scaled_wait(position.saturating_sub(self.position()), self.rate)
    }
}
fn scaled_wait(nanoseconds: u64, rate: f64) -> BagResult<Duration> {
    validate_rate(rate)?;
    let seconds = (nanoseconds as f64 / rate) / 1e9;
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| BagError::invalid("playback deadline exceeds Duration range"))?;
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| BagError::invalid("playback deadline exceeds steady clock range"))?;
    Ok(duration)
}
struct SharedState {
    status: PlaybackStatus,
    timeline: Timeline,
    done: bool,
}
struct Shared {
    state: Mutex<SharedState>,
    endpoints: Arc<[PlaybackEndpoint]>,
    changed: Condvar,
    stop: AtomicBool,
}
enum Control {
    Start,
    Pause,
    Resume,
    Seek(u64),
    Rate(f64),
    Wake,
}
struct Command {
    operation: Control,
    deadline: Instant,
    outcome: Arc<Mutex<CommandOutcome>>,
    reply: mpsc::SyncSender<BagResult<PlaybackStatus>>,
}
enum CommandOutcome {
    Pending,
    Cancelled,
    Completed(BagResult<PlaybackStatus>),
}
fn control_timeout() -> BagError {
    BagError::new(
        BagErrorKind::Timeout,
        "playback control expired before committing; no transition was applied",
    )
}
fn cancel_or_result(outcome: &Mutex<CommandOutcome>) -> BagResult<PlaybackStatus> {
    cancel_or_result_with_error(outcome, control_timeout())
}
fn cancel_or_result_with_error(
    outcome: &Mutex<CommandOutcome>,
    error: BagError,
) -> BagResult<PlaybackStatus> {
    let mut outcome = outcome.lock().unwrap();
    match &*outcome {
        CommandOutcome::Completed(result) => result.clone(),
        CommandOutcome::Pending | CommandOutcome::Cancelled => {
            *outcome = CommandOutcome::Cancelled;
            Err(error)
        }
    }
}
impl Command {
    fn check(&self) -> BagResult<()> {
        if !matches!(*self.outcome.lock().unwrap(), CommandOutcome::Pending)
            || Instant::now() >= self.deadline
        {
            Err(control_timeout())
        } else {
            Ok(())
        }
    }
    /// Cancellation and all local state changes share this one commit point.
    /// The closure cannot perform disk or transport I/O. A caller losing the
    /// cancellation race returns this exact snapshot, even if its timer fired.
    fn commit(
        &self,
        apply: impl FnOnce() -> BagResult<PlaybackStatus>,
    ) -> BagResult<PlaybackStatus> {
        let mut outcome = self.outcome.lock().unwrap();
        if !matches!(*outcome, CommandOutcome::Pending) || Instant::now() >= self.deadline {
            *outcome = CommandOutcome::Cancelled;
            return Err(control_timeout());
        }
        let result = apply();
        *outcome = CommandOutcome::Completed(result.clone());
        result
    }
}

/// One dispatch worker owns playback order and all state transitions. Control
/// replies are barriers: no old local message can publish after a successful
/// pause or seek. Transport/consumer queues already handed a sample cannot be
/// recalled. stop requests closure; wait returns the worker report, whose
/// resources_closed and cleanup_error fields confirm actual resource cleanup.
pub struct Player {
    shared: Arc<Shared>,
    commands: mpsc::SyncSender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
    control_timeout: Duration,
    subscriber_wait: Duration,
}
impl Player {
    pub fn open(context: Context, source: &Path, options: PlaybackOptions) -> BagResult<Self> {
        options.validate()?;
        let control_timeout = Duration::from_millis(options.control_timeout_ms);
        let subscriber_wait = Duration::from_millis(options.subscriber_wait_ms);
        let start_budget = control_timeout
            .checked_add(subscriber_wait)
            .ok_or_else(|| BagError::invalid("start readiness/control budget overflow"))?;
        Instant::now()
            .checked_add(start_budget)
            .ok_or_else(|| BagError::invalid("start readiness/control deadline overflow"))?;
        if context.is_shutdown() {
            return Err(BagError::new(BagErrorKind::Closed, "Context is closed"));
        }
        // Complete offline validation precedes any replay network declaration.
        let (info, indexes) = inspect_indexed(
            source,
            InspectOptions {
                allow_incomplete: options.allow_incomplete,
                limits: options.limits.clone(),
            },
        )?;
        let request = match context.process_bootstrap() {
            Some(bootstrap) => bootstrap.node_request(options.node.clone())?,
            None => options.node.clone(),
        };
        let config = aos_core::resolve_node_config(
            "aos_bag_player", "/",
            NodeOptions {
                summary: "Replay observation Topics from a verified AOS bag; no goal or parameter control".into(),
                tags: vec!["aos.bag.replay".into()],
                ..Default::default()
            },
            &request,
        )?;
        let prepared = prepare_endpoints(&info, &options, &config)?;
        let clock_target = options.clock.as_ref().map(|clock| {
            let candidate = candidate(&clock.topic, &options);
            let target = config.resolve_name(&candidate, NameKind::Topic)?.resolved_name;
            check_target(&target, &options)?;
            if prepared.iter().any(|p| p.status.target == target) {
                return Err(BagError::invalid("generated clock collides with a selected recorded channel; select one time source"));
            }
            Ok((candidate, target))
        }).transpose()?;
        let duration = info
            .manifest
            .shards
            .iter()
            .filter_map(|s| s.last_log_time)
            .max()
            .unwrap_or(info.manifest.system_anchor_ns)
            .checked_sub(info.manifest.system_anchor_ns)
            .ok_or_else(|| BagError::corrupt("recording ends before its anchor"))?;
        scaled_wait(duration, options.rate)?;
        if clock_target.is_some() {
            clock_time(info.manifest.system_anchor_ns, duration)?;
        }
        let node = Node::new_resolved(context.clone(), config)?;
        let mut publishers = BTreeMap::new();
        let endpoint_options = EndpointOptions {
            summary: "Replay publisher; publishes recorded observations only".into(),
            side_effects: Some("external_effect".into()),
            contract_version: Some("aos-json-v1".into()),
        };
        for endpoint in &prepared {
            let publisher = if let Some(action) = &endpoint.channel.action {
                node.create_action_observation_publisher(
                    &Interface::from_json(action.interface.clone())?,
                    &endpoint.candidate,
                    action.kind,
                    endpoint.status.qos.clone(),
                    endpoint_options.clone(),
                )?
            } else {
                node.create_publisher(
                    &endpoint.interface,
                    &endpoint.candidate,
                    endpoint.status.qos.clone(),
                    endpoint_options.clone(),
                )?
            };
            if publisher.endpoint().name != endpoint.status.target {
                return Err(BagError::invalid(
                    "replay endpoint changed after preflight name resolution",
                ));
            }
            publishers.insert(endpoint.channel.id.clone(), publisher);
        }
        let replay_clock = match (&options.clock, &clock_target) {
            (Some(clock_options), Some((candidate, target))) => {
                let publisher = node.create_publisher(
                    &aos_core::clock_interface(),
                    candidate,
                    aos_core::qos_preset("clock")?,
                    endpoint_options,
                )?;
                if publisher.endpoint().name != *target {
                    return Err(BagError::invalid("clock target changed after preflight"));
                }
                let clock = context.create_clock(ClockType::Ros)?;
                clock.set_use_sim_time(true)?;
                Some(ReplayClock {
                    clock,
                    publisher,
                    period: Duration::from_millis(clock_options.period_ms),
                    last: None,
                })
            }
            _ => None,
        };
        let timeline = Timeline {
            base: 0,
            end: duration,
            anchor: Instant::now(),
            rate: options.rate,
            running: false,
        };
        let status = PlaybackStatus {
            state: PlaybackState::Ready,
            position_ns: 0,
            duration_ns: duration,
            rate: options.rate,
            epoch: 0,
            published_total: 0,
            published_bytes: 0,
            endpoints: Vec::new(),
            original_names: options.original_names,
            incomplete_source: info.manifest.incomplete,
            clock_topic: clock_target.map(|(_, target)| target),
            error: None,
            resources_closed: false,
            cleanup_error: None,
        };
        let shared = Arc::new(Shared {
            endpoints: prepared.into_iter().map(|p| p.status).collect(),
            state: Mutex::new(SharedState {
                status: status.clone(),
                timeline: timeline.clone(),
                done: false,
            }),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let (send, receive) = mpsc::sync_channel(32);
        let worker_shared = shared.clone();
        let cursor = Cursor::new(indexes, &info, &options);
        let worker = thread::Builder::new()
            .name("aos-bag-playback".into())
            .spawn(move || {
                let mut runtime = Runtime {
                    shared: worker_shared.clone(),
                    context,
                    node,
                    publishers,
                    clock: replay_clock,
                    info,
                    options,
                    cursor,
                    pending: None,
                    timeline,
                    status,
                };
                let outcome = catch_unwind(AssertUnwindSafe(|| runtime.run(receive)));
                match outcome {
                    Ok(Ok(())) => {}
                    Ok(Err(error))
                        if error.kind == BagErrorKind::Closed
                            && worker_shared.stop.load(Ordering::Acquire) =>
                    {
                        runtime
                            .timeline
                            .reanchor(runtime.timeline.position(), false);
                        runtime.status.state = PlaybackState::Stopped;
                    }
                    Ok(Err(error)) => runtime.fail(error),
                    Err(_) => {
                        runtime.fail(BagError::new(BagErrorKind::Io, "playback worker panicked"))
                    }
                }
                runtime.cleanup();
                runtime.sync();
                worker_shared.state.lock().unwrap().done = true;
                worker_shared.changed.notify_all();
            })?;
        Ok(Self {
            shared,
            commands: send,
            worker: Mutex::new(Some(worker)),
            control_timeout,
            subscriber_wait,
        })
    }
    fn control(&self, operation: Control) -> BagResult<PlaybackStatus> {
        self.control_with_check(operation, || Ok(()))
    }
    fn control_with_check(
        &self,
        operation: Control,
        mut check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        check()?;
        if self.shared.state.lock().unwrap().done {
            return Err(BagError::new(
                BagErrorKind::Closed,
                "playback is closed; open a new Player to replay again",
            ));
        }
        let extra_wait = match operation {
            Control::Start => self.subscriber_wait,
            _ => Duration::ZERO,
        };
        let budget = self
            .control_timeout
            .checked_add(extra_wait)
            .ok_or_else(|| BagError::invalid("control budget overflow"))?;
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or_else(|| BagError::invalid("control deadline overflow"))?;
        let outcome = Arc::new(Mutex::new(CommandOutcome::Pending));
        let (reply, receive) = mpsc::sync_channel(1);
        self.commands
            .try_send(Command {
                operation,
                deadline,
                outcome: outcome.clone(),
                reply,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => BagError::resource("playback control queue is full"),
                mpsc::TrySendError::Disconnected(_) => {
                    BagError::new(BagErrorKind::Closed, "playback dispatch worker closed")
                }
            })?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match receive.recv_timeout(remaining.min(Duration::from_millis(50))) {
                Ok(result) => return result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // No runtime/commit lock is held while checking caller
                    // cancellation. This still waits for the one request with
                    // its original deadline; it never resubmits a command.
                    if let Err(error) = check() {
                        return cancel_or_result_with_error(&outcome, error);
                    }
                    if Instant::now() >= deadline {
                        return cancel_or_result(&outcome);
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(BagError::new(
                        BagErrorKind::Closed,
                        "playback closed before replying",
                    ));
                }
            }
        }
    }
    pub fn start(&self) -> BagResult<PlaybackStatus> {
        self.control(Control::Start)
    }
    pub fn start_with_check(
        &self,
        check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        self.control_with_check(Control::Start, check)
    }
    pub fn pause(&self) -> BagResult<PlaybackStatus> {
        self.control(Control::Pause)
    }
    pub fn pause_with_check(
        &self,
        check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        self.control_with_check(Control::Pause, check)
    }
    pub fn resume(&self) -> BagResult<PlaybackStatus> {
        self.control(Control::Resume)
    }
    pub fn resume_with_check(
        &self,
        check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        self.control_with_check(Control::Resume, check)
    }
    pub fn seek(&self, offset_ns: u64) -> BagResult<PlaybackStatus> {
        self.control(Control::Seek(offset_ns))
    }
    pub fn seek_with_check(
        &self,
        offset_ns: u64,
        check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        self.control_with_check(Control::Seek(offset_ns), check)
    }
    pub fn set_rate(&self, rate: f64) -> BagResult<PlaybackStatus> {
        validate_rate(rate)?;
        self.control(Control::Rate(rate))
    }
    pub fn set_rate_with_check(
        &self,
        rate: f64,
        check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackStatus> {
        validate_rate(rate)?;
        self.control_with_check(Control::Rate(rate), check)
    }
    pub fn stop(&self) -> BagResult<()> {
        self.shared.stop.store(true, Ordering::Release);
        let (reply, _) = mpsc::sync_channel(1);
        let _ = self.commands.try_send(Command {
            operation: Control::Wake,
            deadline: Instant::now(),
            outcome: Arc::new(Mutex::new(CommandOutcome::Pending)),
            reply,
        });
        Ok(())
    }
    pub fn status(&self) -> PlaybackStatus {
        let mut status = self.scalar_status();
        status.endpoints = self.shared.endpoints.to_vec();
        status
    }
    fn scalar_status(&self) -> PlaybackStatus {
        let state = self.shared.state.lock().unwrap();
        let mut status = state.status.clone();
        status.position_ns = state.timeline.position();
        status
    }
    pub fn progress(&self) -> PlaybackProgress {
        let status = self.scalar_status();
        PlaybackProgress {
            state: status.state,
            position_ns: status.position_ns,
            duration_ns: status.duration_ns,
            rate: status.rate,
            epoch: status.epoch,
            published_total: status.published_total,
            published_bytes: status.published_bytes,
            endpoint_count: self.shared.endpoints.len(),
            error: status.error,
            resources_closed: status.resources_closed,
            cleanup_error: status.cleanup_error,
        }
    }
    pub fn wait(&self, timeout: Duration) -> BagResult<PlaybackReport> {
        self.wait_with_check(timeout, || Ok(()))
    }
    pub fn wait_with_check(
        &self,
        timeout: Duration,
        mut check: impl FnMut() -> BagResult<()>,
    ) -> BagResult<PlaybackReport> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| BagError::invalid("playback wait deadline overflow"))?;
        let mut report = loop {
            check()?;
            let state = self.shared.state.lock().unwrap();
            if state.done {
                break PlaybackReport {
                    status: state.status.clone(),
                };
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(BagError::new(
                    BagErrorKind::Timeout,
                    "playback is not fully closed",
                ));
            }
            drop(
                self.shared
                    .changed
                    .wait_timeout(state, left.min(Duration::from_millis(50)))
                    .unwrap(),
            );
        };
        report.status.endpoints = self.shared.endpoints.to_vec();
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
        Ok(report)
    }
}
impl Drop for Player {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct PreparedEndpoint {
    channel: ChannelInfo,
    interface: Interface,
    candidate: String,
    status: PlaybackEndpoint,
}
fn candidate(source: &str, options: &PlaybackOptions) -> String {
    options
        .namespace
        .as_ref()
        .map_or_else(|| source.into(), |ns| format!("{ns}{source}"))
}
fn check_target(target: &str, options: &PlaybackOptions) -> BagResult<()> {
    aos_core::names::validate_absolute_name(target)?;
    if let Some(namespace) = &options.namespace {
        if target != namespace && !target.starts_with(&format!("{namespace}/")) {
            return Err(BagError::invalid("core remap escapes the replay namespace"));
        }
    }
    Ok(())
}
fn prepare_endpoints(
    info: &BagInfo,
    options: &PlaybackOptions,
    config: &ResolvedNodeConfig,
) -> BagResult<Vec<PreparedEndpoint>> {
    let selected: Vec<_> = info
        .channels
        .iter()
        .filter(|c| options.selected(c))
        .collect();
    for topic in &options.topics {
        if !info.channels.iter().any(|c| &c.topic == topic) {
            return Err(BagError::invalid(format!("unknown recorded topic {topic}")));
        }
    }
    for id in &options.channel_ids {
        if !info.channels.iter().any(|c| &c.id == id) {
            return Err(BagError::invalid(format!("unknown business channel {id}")));
        }
    }
    for topic in options.topic_qos.keys() {
        if !selected.iter().any(|c| &c.topic == topic) {
            return Err(BagError::invalid(
                "QoS override does not name a selected topic",
            ));
        }
    }
    for id in options.channel_qos.keys() {
        if !selected.iter().any(|c| &c.id == id) {
            return Err(BagError::invalid(
                "QoS override does not name a selected channel",
            ));
        }
    }
    let mut targets = BTreeMap::<String, String>::new();
    let mut result = Vec::new();
    for channel in selected {
        let (candidate, target) = if let Some(action) = &channel.action {
            let candidate = candidate(&action.base, options);
            let base = config
                .resolve_name(&candidate, NameKind::Action)?
                .resolved_name;
            (candidate, format!("{base}{}", action.kind.suffix()))
        } else {
            let candidate = candidate(&channel.topic, options);
            let target = config
                .resolve_name(&candidate, NameKind::Topic)?
                .resolved_name;
            (candidate, target)
        };
        check_target(&target, options)?;
        if let Some(source) = targets.insert(target.clone(), channel.topic.clone()) {
            if source != channel.topic {
                return Err(BagError::invalid(
                    "two different recorded sources map to one replay target",
                ));
            }
        }
        let (qos, source) = if let Some(qos) = options.channel_qos.get(&channel.id) {
            (qos.clone(), "channel_override")
        } else if let Some(qos) = options.topic_qos.get(&channel.topic) {
            (qos.clone(), "topic_override")
        } else {
            if channel.offered_qos.len() > 1 && !options.channel_ids.contains(&channel.id) {
                return Err(BagError::invalid(
                    "conflicting offered QoS requires explicit channel selection or an override",
                ));
            }
            (channel.record_qos.clone(), "recorded")
        };
        result.push(PreparedEndpoint {
            interface: channel.validate()?,
            channel: channel.clone(),
            candidate,
            status: PlaybackEndpoint {
                channel_id: channel.id.clone(),
                source: channel.topic.clone(),
                target,
                type_hash: channel.type_hash.clone(),
                qos,
                qos_source: source.into(),
            },
        });
    }
    Ok(result)
}

struct Cursor {
    indexes: Vec<Arc<ShardIndex>>,
    shards: Vec<crate::ShardInfo>,
    selected: BTreeSet<String>,
    shard_index: usize,
    reader: Option<IndexedReader>,
    anchor_ns: u64,
    target: u64,
    previous: Option<(u64, u64)>,
}
impl Cursor {
    fn new(indexes: Vec<Arc<ShardIndex>>, info: &BagInfo, options: &PlaybackOptions) -> Self {
        Self {
            indexes,
            shards: info.manifest.shards.clone(),
            selected: info
                .channels
                .iter()
                .filter(|c| options.selected(c))
                .map(|c| c.id.clone())
                .collect(),
            shard_index: 0,
            reader: None,
            anchor_ns: info.manifest.system_anchor_ns,
            target: info.manifest.system_anchor_ns,
            previous: None,
        }
    }
    fn next_step(
        &mut self,
        command: Option<&Command>,
        stop: &AtomicBool,
    ) -> BagResult<IndexedRead> {
        if stop.load(Ordering::Acquire) {
            return Err(BagError::new(
                BagErrorKind::Closed,
                "playback stop requested",
            ));
        }
        if let Some(command) = command {
            command.check()?
        }
        if self.reader.is_none() {
            let Some(index) = self.indexes.get(self.shard_index) else {
                return Ok(IndexedRead::End);
            };
            self.reader = Some(index.cursor_selected(self.target, Some(&self.selected))?);
        }
        match self.reader.as_mut().unwrap().next_step()? {
            IndexedRead::Message(message) => {
                if self
                    .previous
                    .is_some_and(|(seq, time)| message.sequence <= seq || message.log_time < time)
                {
                    return Err(BagError::corrupt("playback source order changed"));
                }
                self.previous = Some((message.sequence, message.log_time));
                if message.log_time >= self.target && self.selected.contains(&message.channel.id) {
                    return Ok(IndexedRead::Message(message));
                }
            }
            IndexedRead::End => {
                self.reader = None;
                self.shard_index += 1;
            }
            IndexedRead::Progress => {}
        }
        Ok(IndexedRead::Progress)
    }
    fn seek(
        &mut self,
        offset: u64,
        command: &Command,
        stop: &AtomicBool,
    ) -> BagResult<Option<RecordedMessage>> {
        let target = self
            .anchor_ns
            .checked_add(offset)
            .ok_or_else(|| BagError::invalid("seek target timestamp overflow"))?;
        self.target = target;
        self.shard_index = self
            .shards
            .iter()
            .position(|shard| shard.last_log_time.is_some_and(|time| time >= target))
            .unwrap_or(self.shards.len());
        self.reader = None;
        self.previous = None;
        loop {
            match self.next_step(Some(command), stop)? {
                IndexedRead::Message(message) => return Ok(Some(message)),
                IndexedRead::Progress => {}
                IndexedRead::End => return Ok(None),
            }
        }
    }
}

struct ReplayClock {
    clock: Clock,
    publisher: TypedPublisher,
    period: Duration,
    last: Option<Instant>,
}
impl ReplayClock {
    fn publish(&mut self, anchor_ns: u64, position: u64) -> BagResult<()> {
        let time = clock_time(anchor_ns, position)?;
        self.clock.update_ros_time(time)?;
        self.publisher
            .publish(&serde_json::json!({
                "clock":{"sec":time / 1_000_000_000, "nanosec":time % 1_000_000_000}
            }))
            .map_err(BagError::from)?;
        self.last = Some(Instant::now());
        Ok(())
    }
    fn due_in(&self) -> Duration {
        self.last.map_or(Duration::ZERO, |last| {
            self.period.saturating_sub(last.elapsed())
        })
    }
}
impl Drop for ReplayClock {
    fn drop(&mut self) {
        let _ = self.clock.close();
    }
}
fn clock_time(anchor: u64, position: u64) -> BagResult<i64> {
    anchor
        .checked_add(position)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| BagError::invalid("replay clock exceeds nonnegative i64 nanoseconds"))
}

struct Runtime {
    shared: Arc<Shared>,
    context: Context,
    node: Node,
    publishers: BTreeMap<String, TypedPublisher>,
    clock: Option<ReplayClock>,
    info: BagInfo,
    options: PlaybackOptions,
    cursor: Cursor,
    pending: Option<RecordedMessage>,
    timeline: Timeline,
    status: PlaybackStatus,
}
impl Runtime {
    fn cleanup(&mut self) {
        let mut error = None;
        let mut remember = |result: aos_core::Result<()>| {
            if let Err(failure) = result {
                if !matches!(failure, aos_core::Error::Closed) {
                    error.get_or_insert_with(|| BagError::from(failure));
                }
            }
        };
        for publisher in self.publishers.values() {
            remember(publisher.close());
        }
        if let Some(clock) = &self.clock {
            remember(clock.publisher.close());
            remember(clock.clock.close());
        }
        remember(self.node.destroy_node());
        // Indexes retain identities only. Close the one active read cursor
        // before publishing the resource-cleanup outcome.
        self.cursor.reader.take();
        self.pending.take();
        self.status.resources_closed = error.is_none();
        if self.status.error.is_none() && error.is_some() {
            self.status.error = error.clone();
            self.status.state = PlaybackState::Failed;
        }
        self.status.cleanup_error = error;
        self.publishers.clear();
        self.clock.take();
    }
    fn sync(&mut self) {
        self.status.position_ns = self.timeline.position();
        let mut state = self.shared.state.lock().unwrap();
        state.status = self.status.clone();
        state.timeline = self.timeline.clone();
        self.shared.changed.notify_all();
    }
    fn fail(&mut self, error: BagError) {
        self.timeline.reanchor(self.timeline.position(), false);
        self.status.error = Some(error);
        self.status.state = PlaybackState::Failed;
        self.sync();
    }
    fn publish_clock(&mut self) -> BagResult<()> {
        if let Some(clock) = &mut self.clock {
            clock.publish(
                self.info.manifest.system_anchor_ns,
                self.timeline.position(),
            )?;
        }
        Ok(())
    }
    fn apply_control(
        &mut self,
        command: &Command,
        apply: impl FnOnce(&mut Self),
    ) -> BagResult<PlaybackStatus> {
        command.commit(|| {
            apply(self);
            self.sync();
            let mut status = self.status.clone();
            status.endpoints = self.shared.endpoints.to_vec();
            Ok(status)
        })
    }
    fn handle(&mut self, command: &Command) -> BagResult<PlaybackStatus> {
        command.check()?;
        match command.operation {
            Control::Start => {
                if self.status.state != PlaybackState::Ready {
                    return Err(BagError::invalid("start requires a ready Player"));
                }
                if self.options.subscriber_wait_ms > 0 {
                    let deadline = Instant::now()
                        .checked_add(Duration::from_millis(self.options.subscriber_wait_ms))
                        .ok_or_else(|| {
                            BagError::invalid("subscriber readiness deadline overflow")
                        })?;
                    for publisher in self
                        .publishers
                        .values()
                        .chain(self.clock.iter().map(|clock| &clock.publisher))
                    {
                        loop {
                            command.check()?;
                            if self.shared.stop.load(Ordering::Acquire) {
                                // Stop is processed by the outer dispatch loop.
                                // It must not turn an uncommitted Start into a
                                // fatal transport/closed error.
                                return Err(BagError::new(
                                    BagErrorKind::Timeout,
                                    "playback start cancelled by stop; no transition was applied",
                                ));
                            }
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            let result = publisher
                                .wait_for_subscribers(remaining.min(Duration::from_millis(50)));
                            match result {
                                Ok(()) => break,
                                Err(aos_core::Error::Timeout) if Instant::now() < deadline => {
                                    // Only the read-only matching wait repeats.
                                    // Keep this Start and its original deadline;
                                    // no control or publication is reissued.
                                }
                                Err(error) => {
                                    let error = BagError::from(error);
                                    return Err(BagError {
                                        message: format!(
                                            "subscriber readiness for {}: {}",
                                            publisher.endpoint().name,
                                            error.message
                                        ),
                                        ..error
                                    });
                                }
                            }
                        }
                    }
                }
                command.check()?;
                if self.shared.stop.load(Ordering::Acquire) {
                    return Err(BagError::new(
                        BagErrorKind::Timeout,
                        "playback start cancelled by stop; no transition was applied",
                    ));
                }
                self.apply_control(command, |this| {
                    this.status.state = if this.options.start_paused {
                        PlaybackState::Paused
                    } else {
                        PlaybackState::Playing
                    };
                    this.timeline
                        .reanchor(this.timeline.base, !this.options.start_paused);
                })
            }
            Control::Pause => {
                if !matches!(
                    self.status.state,
                    PlaybackState::Playing | PlaybackState::Paused
                ) {
                    return Err(BagError::invalid("pause requires playing or paused state"));
                }
                self.apply_control(command, |this| {
                    this.timeline.reanchor(this.timeline.position(), false);
                    this.status.state = PlaybackState::Paused;
                })
            }
            Control::Resume => {
                if self.status.state != PlaybackState::Paused {
                    return Err(BagError::invalid("resume requires paused state"));
                }
                self.apply_control(command, |this| {
                    this.timeline.reanchor(this.timeline.base, true);
                    this.status.state = PlaybackState::Playing;
                })
            }
            Control::Rate(rate) => {
                if !matches!(
                    self.status.state,
                    PlaybackState::Ready | PlaybackState::Playing | PlaybackState::Paused
                ) {
                    return Err(BagError::invalid(
                        "cannot change the rate of a closed Player",
                    ));
                }
                let position = self.timeline.position();
                scaled_wait(self.timeline.end.saturating_sub(position), rate)?;
                self.apply_control(command, |this| {
                    this.timeline.reanchor(position, this.timeline.running);
                    this.timeline.rate = rate;
                    this.status.rate = rate;
                })
            }
            Control::Seek(offset) => {
                if !matches!(
                    self.status.state,
                    PlaybackState::Ready | PlaybackState::Playing | PlaybackState::Paused
                ) {
                    return Err(BagError::invalid("cannot seek a closed Player"));
                }
                if self.clock.is_some() {
                    clock_time(self.info.manifest.system_anchor_ns, offset)?;
                }
                let mut cursor =
                    Cursor::new(self.cursor.indexes.clone(), &self.info, &self.options);
                let pending = cursor.seek(offset, command, &self.shared.stop)?;
                let epoch = self
                    .status
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| BagError::resource("playback epoch exhausted"))?;
                self.apply_control(command, |this| {
                    this.cursor = cursor;
                    this.pending = pending;
                    this.status.epoch = epoch;
                    this.timeline
                        .reanchor(offset, this.timeline.running && this.pending.is_some());
                    if this.pending.is_none() && this.status.state != PlaybackState::Ready {
                        this.status.state = PlaybackState::Finished;
                    }
                })
            }
            Control::Wake => self.apply_control(command, |_| {}),
        }
    }
    fn process_command(&mut self, command: Command) -> BagResult<()> {
        let mut result = self.handle(&command);
        let pending = matches!(*command.outcome.lock().unwrap(), CommandOutcome::Pending);
        if pending {
            // Validation/readiness failures also have one reply/cancel outcome.
            result = command.commit(|| result);
        }
        let fatal = result
            .as_ref()
            .err()
            .filter(|error| {
                matches!(
                    error.kind,
                    BagErrorKind::Io
                        | BagErrorKind::DiskFull
                        | BagErrorKind::Corrupt
                        | BagErrorKind::Transport
                        | BagErrorKind::Closed
                )
            })
            .cloned();
        let applied = result.is_ok();
        let _ = command.reply.send(result);
        if let Some(error) = fatal {
            return Err(error);
        }
        // The acknowledgement covers local dispatch state. Clock publication
        // follows the commit; a transport failure is visible via status/wait.
        if applied && self.status.state != PlaybackState::Ready {
            self.publish_clock()?;
        }
        Ok(())
    }
    fn run(&mut self, receive: mpsc::Receiver<Command>) -> BagResult<()> {
        loop {
            if self.shared.stop.load(Ordering::Acquire) {
                self.timeline.reanchor(self.timeline.position(), false);
                self.status.state = PlaybackState::Stopped;
                return Ok(());
            }
            if self.context.is_shutdown() {
                return Err(BagError::new(
                    BagErrorKind::Closed,
                    "Context closed during playback",
                ));
            }
            if self.status.state == PlaybackState::Finished {
                return Ok(());
            }
            match receive.try_recv() {
                Ok(command) => {
                    self.process_command(command)?;
                    continue;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.timeline.reanchor(self.timeline.position(), false);
                    self.status.state = PlaybackState::Stopped;
                    return Ok(());
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            if matches!(
                self.status.state,
                PlaybackState::Playing | PlaybackState::Paused
            ) && self
                .clock
                .as_ref()
                .is_some_and(|clock| clock.due_in().is_zero())
            {
                self.publish_clock()?;
            }
            if self.status.state == PlaybackState::Playing {
                if self.pending.is_none() {
                    match self.cursor.next_step(None, &self.shared.stop)? {
                        IndexedRead::Message(message) => self.pending = Some(message),
                        IndexedRead::Progress => continue,
                        IndexedRead::End => {
                            self.timeline.reanchor(self.timeline.position(), false);
                            self.status.state = PlaybackState::Finished;
                            self.publish_clock()?;
                            return Ok(());
                        }
                    }
                    // Let a queued pause/seek take the barrier before dispatch.
                    continue;
                }
                let message = self.pending.as_ref().unwrap();
                let due = message
                    .log_time
                    .checked_sub(self.info.manifest.system_anchor_ns)
                    .ok_or_else(|| BagError::corrupt("message precedes recording anchor"))?;
                if due <= self.timeline.position() {
                    let message = self.pending.take().unwrap();
                    self.publishers
                        .get(&message.channel.id)
                        .ok_or_else(|| {
                            BagError::corrupt("no replay publisher for selected channel")
                        })?
                        .publish_serialized(&message.payload)?;
                    self.status.published_total += 1;
                    self.status.published_bytes += message.payload.len() as u64;
                    self.sync();
                    continue;
                }
            }
            let mut wait = Duration::from_millis(50);
            if self.status.state == PlaybackState::Playing {
                if let Some(message) = &self.pending {
                    wait = wait.min(
                        self.timeline
                            .wait_for(message.log_time - self.info.manifest.system_anchor_ns)?,
                    );
                }
            }
            if matches!(
                self.status.state,
                PlaybackState::Playing | PlaybackState::Paused
            ) {
                if let Some(clock) = &self.clock {
                    wait = wait.min(clock.due_in());
                }
            }
            match receive.recv_timeout(wait) {
                Ok(command) => {
                    self.process_command(command)?;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.timeline.reanchor(self.timeline.position(), false);
                    self.status.state = PlaybackState::Stopped;
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_descriptions_never_enter_the_dispatch_mutex_snapshot() {
        let temp = crate::storage_tests::Temp::new();
        let directory = temp.0.join("source");
        crate::runtime_tests::playback_fixture(&directory);
        let network = crate::runtime_tests::Network::new();
        let context = network.context();
        let player = Player::open(
            context.clone(),
            &directory,
            PlaybackOptions {
                namespace: Some("/replay".into()),
                start_paused: true,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(player.shared.endpoints.len(), 1);
        assert!(player
            .shared
            .state
            .lock()
            .unwrap()
            .status
            .endpoints
            .is_empty());
        assert_eq!(player.start().unwrap().endpoints.len(), 1);
        assert!(player
            .shared
            .state
            .lock()
            .unwrap()
            .status
            .endpoints
            .is_empty());
        assert_eq!(player.seek(100_000_000).unwrap().endpoints.len(), 1);
        assert_eq!(player.status().endpoints.len(), 1);
        assert_eq!(player.progress().endpoint_count, 1);
        assert!(player
            .shared
            .state
            .lock()
            .unwrap()
            .status
            .endpoints
            .is_empty());
        player.stop().unwrap();
        assert_eq!(
            player
                .wait(Duration::from_secs(5))
                .unwrap()
                .status
                .endpoints
                .len(),
            1
        );
        assert!(player
            .shared
            .state
            .lock()
            .unwrap()
            .status
            .endpoints
            .is_empty());
        context.shutdown().unwrap();
    }

    #[test]
    fn progress_size_stays_bounded_with_four_thousand_long_endpoint_descriptions() {
        let (mut player, _receive) = pending_player();
        let endpoint = PlaybackEndpoint {
            channel_id: "id".repeat(64),
            source: "/source".repeat(256),
            target: "/target".repeat(256),
            type_hash: "hash".repeat(64),
            qos: aos_core::QoS::default(),
            qos_source: "recorded".into(),
        };
        Arc::get_mut(&mut player.shared).unwrap().endpoints = vec![endpoint; 4096].into();
        assert_eq!(player.status().endpoints.len(), 4096);
        let started = Instant::now();
        for _ in 0..1000 {
            let progress = player.progress();
            assert_eq!(progress.endpoint_count, 4096);
            assert!(serde_json::to_vec(&progress).unwrap().len() < 1024);
        }
        println!(
            "4096 endpoints: 1000 compact polls took {} us",
            started.elapsed().as_micros()
        );
        assert!(player
            .shared
            .state
            .lock()
            .unwrap()
            .status
            .endpoints
            .is_empty());
    }
    fn status_fixture() -> PlaybackStatus {
        PlaybackStatus {
            state: PlaybackState::Paused,
            position_ns: 123,
            duration_ns: 1000,
            rate: 1.0,
            epoch: 7,
            published_total: 0,
            published_bytes: 0,
            endpoints: Vec::new(),
            original_names: false,
            incomplete_source: false,
            clock_topic: None,
            error: None,
            resources_closed: false,
            cleanup_error: None,
        }
    }
    fn command_fixture(deadline: Instant) -> Command {
        Command {
            operation: Control::Pause,
            deadline,
            outcome: Arc::new(Mutex::new(CommandOutcome::Pending)),
            reply: mpsc::sync_channel(1).0,
        }
    }

    fn pending_player() -> (Player, mpsc::Receiver<Command>) {
        let (commands, receive) = mpsc::sync_channel(32);
        (
            Player {
                shared: Arc::new(Shared {
                    endpoints: Arc::from([]),
                    state: Mutex::new(SharedState {
                        status: status_fixture(),
                        timeline: Timeline {
                            base: 123,
                            end: 1000,
                            anchor: Instant::now(),
                            rate: 1.0,
                            running: false,
                        },
                        done: false,
                    }),
                    changed: Condvar::new(),
                    stop: AtomicBool::new(false),
                }),
                commands,
                worker: Mutex::new(None),
                control_timeout: Duration::from_secs(30),
                subscriber_wait: Duration::from_secs(60),
            },
            receive,
        )
    }

    #[test]
    fn caller_check_cancels_one_pending_control_without_holding_runtime_locks() {
        let (player, receive) = pending_player();
        let started = Instant::now();
        let mut checks = 0;
        let error = player
            .start_with_check(|| {
                assert!(player.shared.state.try_lock().is_ok());
                checks += 1;
                if checks == 1 {
                    Ok(())
                } else {
                    Err(BagError::new(BagErrorKind::Closed, "caller interrupted"))
                }
            })
            .unwrap_err();
        assert_eq!(error.kind, BagErrorKind::Closed);
        assert!(started.elapsed() < Duration::from_secs(1));
        let command = receive.try_recv().unwrap();
        assert!(
            receive.try_recv().is_err(),
            "the operation was not reissued"
        );
        let mut applied = false;
        assert!(command
            .commit(|| {
                applied = true;
                Ok(status_fixture())
            })
            .is_err());
        assert!(!applied);
    }

    #[test]
    fn caller_can_interrupt_report_wait_and_reenter_status_without_deadlock() {
        let (player, _) = pending_player();
        let mut checks = 0;
        let started = Instant::now();
        let error = player
            .wait_with_check(Duration::from_secs(30), || {
                assert_eq!(player.status().epoch, 7);
                checks += 1;
                if checks == 1 {
                    Ok(())
                } else {
                    Err(BagError::new(BagErrorKind::Closed, "caller interrupted"))
                }
            })
            .unwrap_err();
        assert_eq!(error.kind, BagErrorKind::Closed);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(!player.shared.state.lock().unwrap().done);
    }

    #[test]
    fn cancellation_after_preflight_prevents_every_local_state_change() {
        let command = command_fixture(Instant::now() + Duration::from_secs(1));
        command.check().unwrap();
        assert_eq!(
            cancel_or_result(&command.outcome).unwrap_err().kind,
            BagErrorKind::Timeout
        );
        let mut applied = false;
        let error = command
            .commit(|| {
                applied = true;
                Ok(status_fixture())
            })
            .unwrap_err();
        assert_eq!(error.kind, BagErrorKind::Timeout);
        assert!(!applied);
    }

    #[test]
    fn expiry_after_preflight_prevents_commit_and_does_not_change_epoch() {
        let mut command = command_fixture(Instant::now() + Duration::from_secs(1));
        command.check().unwrap();
        command.deadline = Instant::now() - Duration::from_nanos(1);
        let mut epoch = 6;
        assert_eq!(
            command
                .commit(|| {
                    epoch += 1;
                    Ok(status_fixture())
                })
                .unwrap_err()
                .kind,
            BagErrorKind::Timeout
        );
        assert_eq!(epoch, 6);
    }

    #[test]
    fn timeout_losing_the_commit_race_returns_the_exact_committed_snapshot() {
        let command = command_fixture(Instant::now() + Duration::from_secs(1));
        let expected = command.commit(|| Ok(status_fixture())).unwrap();
        let actual = cancel_or_result(&command.outcome).unwrap();
        assert_eq!(actual.epoch, expected.epoch);
        assert_eq!(actual.position_ns, expected.position_ns);
        assert_eq!(actual.state, PlaybackState::Paused);
        assert!(matches!(
            *command.outcome.lock().unwrap(),
            CommandOutcome::Completed(Ok(_))
        ));
    }
    #[test]
    fn timeline_pauses_reanchors_and_checks_extreme_rates() {
        let now = Instant::now();
        let mut timeline = Timeline {
            base: 10,
            end: 10_000_000_000,
            anchor: now,
            rate: 2.0,
            running: true,
        };
        assert_eq!(
            timeline.position_at(now + Duration::from_millis(500)),
            1_000_000_010
        );
        timeline.base = timeline.position_at(now + Duration::from_millis(500));
        timeline.running = false;
        assert_eq!(
            timeline.position_at(now + Duration::from_secs(10)),
            1_000_000_010
        );
        assert!(scaled_wait(1, 0.0).is_err());
        assert!(scaled_wait(1, f64::NAN).is_err());
        assert!(scaled_wait(u64::MAX, f64::MIN_POSITIVE).is_err());
        assert!(scaled_wait(u64::MAX, f64::MAX).is_ok());
    }

    #[test]
    fn qos_ambiguity_requires_selection_and_distinct_sources_cannot_merge() {
        let temp = crate::storage_tests::Temp::new();
        let directory = temp.0.join("recording");
        crate::runtime_tests::playback_fixture(&directory);
        let mut info = crate::inspect(&directory, InspectOptions::default()).unwrap();
        let mut options = PlaybackOptions {
            namespace: Some("/replay".into()),
            ..Default::default()
        };
        let resolve = |options: &PlaybackOptions| {
            aos_core::resolve_node_config("test", "/", NodeOptions::default(), &options.node)
                .unwrap()
        };
        let channel = &mut info.channels[0];
        channel
            .offered_qos
            .push(aos_core::qos_preset("sensor_data").unwrap());
        channel.id = channel.content_id().unwrap();
        assert!(prepare_endpoints(&info, &options, &resolve(&options)).is_err());
        options.channel_ids.push(info.channels[0].id.clone());
        assert_eq!(
            prepare_endpoints(&info, &options, &resolve(&options))
                .unwrap()
                .len(),
            1
        );
        options
            .topic_qos
            .insert("/data".into(), aos_core::qos_preset("sensor_data").unwrap());
        options
            .channel_qos
            .insert(info.channels[0].id.clone(), aos_core::QoS::default());
        let endpoints = prepare_endpoints(&info, &options, &resolve(&options)).unwrap();
        assert_eq!(endpoints[0].status.qos_source, "channel_override");
        assert_eq!(endpoints[0].status.qos, aos_core::QoS::default());
        options.channel_ids.clear();
        options.channel_qos.clear();
        let second = ChannelInfo::new(
            "/other".into(),
            &crate::storage_tests::interface(),
            vec![aos_core::QoS::default()],
            aos_core::QoS::default(),
            None,
        )
        .unwrap();
        info.channels.push(second);
        options.node.program = serde_json::json!({"remaps":[
            {"kind":"topic","from":"/replay/data","to":"/replay/merged"},
            {"kind":"topic","from":"/replay/other","to":"/replay/merged"}
        ]});
        assert!(prepare_endpoints(&info, &options, &resolve(&options)).is_err());
    }
}
