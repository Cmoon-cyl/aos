use std::{
    collections::VecDeque,
    ffi::OsString,
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::{
    process::{OwnedChild, RunDirectory},
    EventSink, LaunchError, LaunchErrorCode, LaunchEvent, LaunchEventKind, LaunchPlan,
    LaunchResult, OutputStream, PlannedProcess, ProbeFactory, ProcessExit, ProcessLog,
    ProcessSummary, ReadinessPhase, ReadinessProbe, RestartPolicy, RunOutcome, RunProgress,
    RunSummary, StopReason, SCHEMA_VERSION,
};

const CLEANUP_GRACE: Duration = Duration::from_secs(2);
const SHUTDOWN_OUTPUT_GRACE: Duration = Duration::from_secs(30);
const POLL_QUANTUM: Duration = Duration::from_millis(10);
const OUTPUT_BUDGET: usize = 1024 * 1024;
const PER_PROCESS_OUTPUT: usize = 64 * 1024;
// A valid 128-node DAG can emit over 4096 dependency events in one poll.
const MAX_EVENTS: usize = 128 * 128 + 1024;

// Durations are limited to one day during prepare. If the OS steady clock ever
// cannot represent a bounded cleanup deadline, expire it now instead of panicking.
fn cleanup_deadline(now: Instant, duration: Duration) -> Instant {
    now.checked_add(duration).unwrap_or(now)
}

struct Slot {
    plan: PlannedProcess,
    dependencies: Vec<usize>,
    child: Option<OwnedChild>,
    generation: u32,
    restarts: u32,
    ready: bool,
    phase: ReadinessPhase,
    node_id: Option<String>,
    readiness_deadline: Option<Instant>,
    backoff: Option<Instant>,
    cleanup_deadline: Option<Instant>,
    output_deadline: Option<Instant>,
    next_cleanup_signal: Option<Instant>,
    last_exit: Option<ProcessExit>,
    exit_notified: bool,
    done: bool,
}

struct Shutdown {
    layers: Vec<Vec<usize>>,
    layer: usize,
    stage: u8,
    stage_deadline: Instant,
    layer_deadline: Instant,
    deadline: Instant,
    cleanup_deadline: Instant,
    interrupt_ratio: f64,
}

/// An owned process group for every generation. No process-global signal
/// handler or network session is installed by this crate.
pub struct Supervisor {
    launch_id: String,
    started: Instant,
    directory: RunDirectory,
    prefix_path: OsString,
    slots: Vec<Slot>,
    layers: Vec<Vec<usize>>,
    probe: Option<Box<dyn ReadinessProbe>>,
    shutdown_time: Duration,
    interrupt_ratio: f64,
    shutdown: Option<Shutdown>,
    stop_output_deadline: Option<Instant>,
    stop_reason: Option<StopReason>,
    cleanup_errors: Vec<LaunchError>,
    output_error: Option<LaunchError>,
    events: VecDeque<LaunchEvent>,
    output_cursor: usize,
    output_backpressured: bool,
    finished: bool,
}

impl Supervisor {
    /// Performs setup only; children begin in `poll`. With no readiness
    /// requirements, the factory is never called and may be None.
    pub fn start(plan: LaunchPlan, probes: Option<&mut dyn ProbeFactory>) -> LaunchResult<Self> {
        if !cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            return Err(LaunchError::new(
                LaunchErrorCode::Unsupported,
                "start",
                "launch supports Linux and macOS",
            ));
        }
        let probe = if let Some(context) = &plan.context {
            Some(
                probes
                    .ok_or_else(|| {
                        LaunchError::invalid("start", "readiness requires a probe factory")
                    })?
                    .create(context)?,
            )
        } else {
            None
        };
        // Probe creation may block on connection setup. Create no run files
        // until it succeeds, so an OS signal during setup leaves no directory.
        let directory = RunDirectory::create()?;
        let names: Vec<_> = plan
            .processes
            .iter()
            .map(|process| process.spec.id.clone())
            .collect();
        let total = plan.shutdown.sigint_timeout + plan.shutdown.sigterm_timeout;
        let interrupt_ratio = if total == 0.0 {
            0.0
        } else {
            plan.shutdown.sigint_timeout / total
        };
        let slots = plan
            .processes
            .into_iter()
            .map(|process| {
                let dependencies = process
                    .spec
                    .depends_on
                    .iter()
                    .map(|name| {
                        names
                            .iter()
                            .position(|candidate| candidate == name)
                            .expect("validated dependency")
                    })
                    .collect();
                Slot {
                    plan: process,
                    dependencies,
                    child: None,
                    generation: 0,
                    restarts: 0,
                    ready: false,
                    phase: ReadinessPhase::Waiting,
                    node_id: None,
                    readiness_deadline: None,
                    backoff: None,
                    cleanup_deadline: None,
                    output_deadline: None,
                    next_cleanup_signal: None,
                    last_exit: None,
                    exit_notified: false,
                    done: false,
                }
            })
            .collect();
        Ok(Self {
            launch_id: uuid::Uuid::new_v4().to_string(),
            started: Instant::now(),
            directory,
            prefix_path: plan.prefix_path,
            slots,
            layers: plan.layers,
            probe,
            shutdown_time: Duration::from_secs_f64(total),
            interrupt_ratio,
            shutdown: None,
            stop_output_deadline: None,
            stop_reason: None,
            cleanup_errors: Vec::new(),
            output_error: None,
            events: VecDeque::new(),
            output_cursor: 0,
            output_backpressured: false,
            finished: false,
        })
    }

    /// Runs bounded control and output work. The caller's sink and readiness
    /// callbacks must themselves be nonblocking. An output failure is returned
    /// only after owned children have completed the bounded cleanup.
    pub fn poll(
        &mut self,
        max_wait: Duration,
        sink: &mut dyn EventSink,
    ) -> LaunchResult<RunProgress> {
        self.expire_shutdown_output();
        if self.finished {
            self.flush_events(sink);
            return self.result();
        }
        self.observe_children();
        if self.stop_reason.is_none() {
            self.check_readiness();
            self.spawn_eligible();
        }
        self.advance_shutdown();
        self.output_backpressured = false;
        self.flush_events(sink);
        self.drain_output(sink);
        self.finish_cleanups();
        self.advance_shutdown();
        if self.slots.iter().all(|slot| slot.done) {
            self.finish();
        }
        self.flush_events(sink);
        if !self.finished && !max_wait.is_zero() {
            std::thread::sleep(max_wait.min(POLL_QUANTUM));
        }
        self.result()
    }

    pub fn request_stop(&mut self, reason: StopReason) {
        if self.finished {
            // Process cleanup can finish before a backpressured terminal
            // event is accepted. A late stop must still bound that output;
            // it does not rewrite the already-completed process result.
            if !self.events.is_empty() {
                self.stop_output_deadline
                    .get_or_insert_with(|| cleanup_deadline(Instant::now(), SHUTDOWN_OUTPUT_GRACE));
            }
            return;
        }
        if self.stop_reason.is_some() {
            return;
        }
        if let StopReason::Failure(error) = &reason {
            self.event(None, LaunchEventKind::LaunchFailed, json!({"error": error}));
        }
        self.stop_reason = Some(reason);
        for slot in &mut self.slots {
            slot.backoff = None;
            if slot.child.is_none() {
                slot.done = true;
            }
        }
        let now = Instant::now();
        let deadline = cleanup_deadline(now, self.shutdown_time);
        self.stop_output_deadline = Some(cleanup_deadline(
            deadline,
            CLEANUP_GRACE + SHUTDOWN_OUTPUT_GRACE,
        ));
        self.shutdown = Some(Shutdown {
            layers: self.layers.iter().rev().cloned().collect(),
            layer: 0,
            stage: 0,
            stage_deadline: now,
            layer_deadline: now,
            deadline,
            cleanup_deadline: cleanup_deadline(deadline, CLEANUP_GRACE),
            interrupt_ratio: self.interrupt_ratio,
        });
    }

    pub fn summary(&self) -> RunSummary {
        let outcome = if !self.finished {
            RunOutcome::Running
        } else if !self.cleanup_errors.is_empty()
            || self.output_error.is_some()
            || matches!(self.stop_reason, Some(StopReason::Failure(_)))
        {
            RunOutcome::Failed
        } else if self.stop_reason.is_some() {
            RunOutcome::Stopped
        } else {
            RunOutcome::Completed
        };
        RunSummary {
            launch_id: self.launch_id.clone(),
            outcome,
            stop_reason: self.stop_reason.clone(),
            processes: self
                .slots
                .iter()
                .map(|slot| ProcessSummary {
                    process_id: slot.plan.spec.id.clone(),
                    generations: slot.generation,
                    restarts: slot.restarts,
                    ready: slot.ready,
                    exit: slot.last_exit.clone(),
                })
                .collect(),
            cleanup_errors: self.cleanup_errors.clone(),
        }
    }

    fn result(&self) -> LaunchResult<RunProgress> {
        if self.finished && self.events.is_empty() {
            match &self.output_error {
                Some(error) => Err(error.clone()),
                None => Ok(RunProgress::Finished),
            }
        } else if self.stop_reason.is_some() {
            Ok(RunProgress::Stopping)
        } else {
            Ok(RunProgress::Running)
        }
    }

    fn fail(&mut self, error: LaunchError) {
        self.request_stop(StopReason::Failure(error));
    }

    fn cleanup_failed(&mut self, index: usize, error: LaunchError) {
        let mut error = error.for_process(&self.slots[index].plan.spec.id);
        error.code = LaunchErrorCode::CleanupFailed;
        self.cleanup_errors.push(error.clone());
        self.fail(error);
    }

    fn observe_children(&mut self) {
        for index in 0..self.slots.len() {
            if self.slots[index].exit_notified || self.slots[index].child.is_none() {
                continue;
            }
            let exit = match self.slots[index].child.as_mut().unwrap().observe_exit() {
                Ok(Some(exit)) => exit,
                Ok(None) => continue,
                Err(error) => {
                    self.cleanup_failed(index, error);
                    let slot = &mut self.slots[index];
                    if let Some(probe) = &mut self.probe {
                        probe.forget(&slot.child.as_ref().unwrap().identity);
                    }
                    slot.child.as_mut().unwrap().abandon_cleanup();
                    slot.child = None;
                    slot.ready = false;
                    slot.done = true;
                    continue;
                }
            };
            let was_ready = self.slots[index].ready;
            let identity = self.slots[index].child.as_ref().unwrap().identity.clone();
            if let Some(probe) = &mut self.probe {
                probe.forget(&identity);
            }
            let slot = &mut self.slots[index];
            slot.exit_notified = true;
            slot.ready = false;
            slot.last_exit = Some(exit.clone());
            slot.readiness_deadline = None;
            slot.cleanup_deadline = Some(cleanup_deadline(Instant::now(), CLEANUP_GRACE));
            self.event(
                Some(index),
                LaunchEventKind::ProcessExited,
                json!({"exit": exit}),
            );
            if was_ready {
                self.dependency_events(index, LaunchEventKind::DependencyUnavailable);
            }
            self.signal(index, 9, "leader_exit_cleanup");
        }
    }

    fn check_readiness(&mut self) {
        for index in 0..self.slots.len() {
            let slot = &self.slots[index];
            if slot.ready
                || slot.exit_notified
                || slot.child.is_none()
                || slot.plan.readiness.is_none()
            {
                continue;
            }
            let now = Instant::now();
            let deadline = slot.readiness_deadline.expect("readiness child deadline");
            if now >= deadline {
                self.fail(
                    LaunchError::new(
                        LaunchErrorCode::ReadinessTimeout,
                        "readiness",
                        "owned process did not become ready before its steady deadline",
                    )
                    .for_process(&slot.plan.spec.id),
                );
                break;
            }
            let progress = self.probe.as_mut().expect("validated probe").poll(
                &slot.child.as_ref().unwrap().identity,
                slot.plan.readiness.as_ref().unwrap(),
                deadline.saturating_duration_since(now),
            );
            let progress = match progress {
                Ok(progress) => progress,
                Err(error) => {
                    self.fail(error.for_process(&self.slots[index].plan.spec.id));
                    break;
                }
            };
            if progress.phase != ReadinessPhase::Waiting
                && progress.node_id.as_ref().is_none_or(String::is_empty)
            {
                self.fail(
                    LaunchError::new(
                        LaunchErrorCode::ReadinessFailed,
                        "readiness",
                        "discovery progress must identify the owned NodeCard",
                    )
                    .for_process(&self.slots[index].plan.spec.id),
                );
                break;
            }
            if let (Some(previous), Some(current)) = (&self.slots[index].node_id, &progress.node_id)
            {
                if previous != current {
                    self.fail(
                        LaunchError::new(
                            LaunchErrorCode::ReadinessFailed,
                            "readiness",
                            "node identity changed inside one process generation",
                        )
                        .for_process(&self.slots[index].plan.spec.id),
                    );
                    break;
                }
            }
            let previous = self.slots[index].phase;
            if previous == ReadinessPhase::Waiting && progress.phase != ReadinessPhase::Waiting {
                self.event(
                    Some(index),
                    LaunchEventKind::Discovered,
                    json!({"node_id": progress.node_id}),
                );
            }
            if previous != progress.phase
                && matches!(
                    progress.phase,
                    ReadinessPhase::Configuring | ReadinessPhase::Activating
                )
            {
                self.event(
                    Some(index),
                    LaunchEventKind::LifecycleTransition,
                    json!({"phase": progress.phase, "node_id": progress.node_id}),
                );
            }
            let slot = &mut self.slots[index];
            slot.phase = progress.phase;
            if progress.node_id.is_some() {
                slot.node_id = progress.node_id;
            }
            if progress.phase == ReadinessPhase::Ready {
                slot.ready = true;
                slot.readiness_deadline = None;
                let node_id = slot.node_id.clone();
                self.event(
                    Some(index),
                    LaunchEventKind::Ready,
                    json!({"node_id": node_id}),
                );
                self.dependency_events(index, LaunchEventKind::DependencyReady);
            }
        }
    }

    fn spawn_eligible(&mut self) {
        for index in 0..self.slots.len() {
            if self.stop_reason.is_some() {
                break;
            }
            let slot = &self.slots[index];
            if slot.done
                || slot.child.is_some()
                || slot
                    .backoff
                    .is_some_and(|deadline| Instant::now() < deadline)
            {
                continue;
            }
            if slot
                .dependencies
                .iter()
                .any(|dependency| !self.slots[*dependency].ready)
            {
                if slot
                    .dependencies
                    .iter()
                    .any(|dependency| self.slots[*dependency].done)
                {
                    self.fail(
                        LaunchError::new(
                            LaunchErrorCode::ReadinessFailed,
                            "dependency",
                            "dependency finished before dependent could start",
                        )
                        .for_process(&slot.plan.spec.id),
                    );
                }
                continue;
            }
            let generation = slot.generation + 1;
            let readiness_deadline = match slot
                .plan
                .readiness
                .as_ref()
                .map(|ready| {
                    Instant::now().checked_add(ready.timeout).ok_or_else(|| {
                        LaunchError::invalid("readiness", "readiness exceeds steady clock range")
                    })
                })
                .transpose()
            {
                Ok(deadline) => deadline,
                Err(error) => {
                    self.fail(error.for_process(&self.slots[index].plan.spec.id));
                    break;
                }
            };
            let mut bootstrap = slot.plan.bootstrap.clone();
            bootstrap.launch_id = self.launch_id.clone();
            bootstrap.generation = generation;
            let spawned = self.directory.write(&bootstrap).and_then(|path| {
                OwnedChild::spawn(
                    &slot.plan,
                    path,
                    &self.launch_id,
                    generation,
                    &self.prefix_path,
                )
            });
            let child = match spawned {
                Ok(child) => child,
                Err(error) => {
                    self.fail(error.for_process(&self.slots[index].plan.spec.id));
                    break;
                }
            };
            #[cfg(windows)]
            if let Some(probe) = &mut self.probe {
                probe.register_owned_job(&child.identity, child.owned_job());
            }
            let slot = &mut self.slots[index];
            slot.child = Some(child);
            slot.generation = generation;
            slot.restarts = generation - 1;
            slot.backoff = None;
            slot.ready = false;
            slot.phase = ReadinessPhase::Waiting;
            slot.node_id = None;
            slot.exit_notified = false;
            slot.cleanup_deadline = None;
            slot.next_cleanup_signal = None;
            slot.readiness_deadline = readiness_deadline;
            self.event(Some(index), LaunchEventKind::ProcessStarted, json!({}));
        }
    }

    fn signal(&mut self, index: usize, number: i32, reason: &str) {
        let Some(child) = self.slots[index].child.as_mut() else {
            return;
        };
        if child.reaped || child.final_signal_sent {
            return;
        }
        match child.signal(number) {
            Ok(()) => {
                if number == 9 {
                    self.slots[index].next_cleanup_signal =
                        Some(cleanup_deadline(Instant::now(), Duration::from_millis(50)));
                }
                self.event(
                    Some(index),
                    LaunchEventKind::ProcessSignal,
                    json!({"signal": number, "reason": reason}),
                );
            }
            Err(error) => self.cleanup_failed(index, error),
        }
    }

    fn advance_shutdown(&mut self) {
        let Some(mut shutdown) = self.shutdown.take() else {
            return;
        };
        let now = Instant::now();
        if now >= shutdown.deadline {
            for index in 0..self.slots.len() {
                self.signal(index, 9, "shutdown_deadline");
            }
            if now >= shutdown.cleanup_deadline {
                for index in 0..self.slots.len() {
                    if self.slots[index]
                        .child
                        .as_ref()
                        .is_some_and(|child| !child.reaped)
                    {
                        self.expire_cleanup(index);
                    }
                }
            }
            self.shutdown = Some(shutdown);
            return;
        }
        while shutdown.layer < shutdown.layers.len() {
            let layer = &shutdown.layers[shutdown.layer];
            if layer.iter().all(|index| {
                self.slots[*index]
                    .child
                    .as_ref()
                    .is_none_or(|child| child.reaped)
            }) {
                shutdown.layer += 1;
                shutdown.stage = 0;
                continue;
            }
            if shutdown.stage == 0 {
                let remaining = shutdown.deadline.saturating_duration_since(now);
                let layer_budget = remaining / (shutdown.layers.len() - shutdown.layer) as u32;
                shutdown.layer_deadline = cleanup_deadline(now, layer_budget);
                shutdown.stage_deadline =
                    cleanup_deadline(now, layer_budget.mul_f64(shutdown.interrupt_ratio));
                shutdown.stage = 1;
                for index in layer {
                    self.event(
                        Some(*index),
                        LaunchEventKind::ProcessStopping,
                        json!({"reason": self.stop_reason}),
                    );
                    self.signal(*index, 2, "shutdown");
                }
            }
            if shutdown.stage == 1 && now >= shutdown.stage_deadline {
                shutdown.stage = 2;
                for index in layer {
                    self.signal(*index, 15, "shutdown");
                }
            }
            if shutdown.stage == 2 && now >= shutdown.layer_deadline {
                shutdown.stage = 3;
                for index in layer {
                    self.signal(*index, 9, "shutdown");
                }
            }
            break;
        }
        self.shutdown = Some(shutdown);
    }

    fn expire_shutdown_output(&mut self) {
        if self
            .stop_output_deadline
            .is_none_or(|deadline| Instant::now() < deadline)
        {
            return;
        }
        let pipe_pending = self.slots.iter().any(|slot| {
            slot.child
                .as_ref()
                .is_some_and(|child| child.reaped && !child.output_closed())
        });
        if !pipe_pending && self.events.is_empty() {
            return;
        }
        // This is an absolute stop/output bound, independent of sink progress.
        // Never report a healthy but slow sink as a process-group failure.
        // Post-reap pipes have separate drain-time and raw-byte bounds. Apply
        // this absolute deadline even after an earlier output failure; that
        // error remains authoritative. Closing reaped pipes needs no signal.
        self.output_failed(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "absolute shutdown output-drain deadline exceeded; pending output was discarded",
        ));
        for slot in &mut self.slots {
            if slot.child.as_ref().is_some_and(|child| child.reaped) {
                slot.child = None;
                slot.done = true;
                slot.ready = false;
            }
        }
    }

    fn finish_cleanups(&mut self) {
        for index in 0..self.slots.len() {
            if !self.slots[index].exit_notified {
                continue;
            }
            let Some(child) = self.slots[index].child.as_mut() else {
                continue;
            };
            let now = Instant::now();
            if !child.reaped {
                match child.other_group_members() {
                    Ok(false) => {
                        if let Err(error) = child.reap() {
                            self.cleanup_failed(index, error);
                            self.expire_cleanup(index);
                            continue;
                        }
                        self.slots[index].output_deadline =
                            Some(cleanup_deadline(now, CLEANUP_GRACE));
                    }
                    Ok(true) => {
                        if self.slots[index]
                            .next_cleanup_signal
                            .is_some_and(|deadline| now >= deadline)
                        {
                            let result = self.slots[index].child.as_mut().unwrap().signal(9);
                            self.slots[index].next_cleanup_signal =
                                Some(cleanup_deadline(now, Duration::from_millis(50)));
                            if let Err(error) = result {
                                self.cleanup_failed(index, error);
                            }
                        }
                    }
                    Err(error) => {
                        self.cleanup_failed(index, error);
                        self.expire_cleanup(index);
                        continue;
                    }
                }
            }
            let child = self.slots[index].child.as_ref().unwrap();
            if child.reaped && child.output_closed() {
                self.slots[index].child = None;
                self.after_exit(index);
            } else if child.reaped {
                // Group ownership is already settled. A slow event sink must
                // not convert pending pipe bytes into a group-cleanup failure.
                // Parsing the fixed reap-time pipe snapshot can take more than
                // two seconds because each visit has byte/record quotas. New
                // writes cannot replenish it or extend the tail-close window.
                // Raw reads also have a separate limit. Explicit stop has an
                // absolute output deadline even while bounded residue remains.
                if self.output_backpressured
                    || !self.events.is_empty()
                    || child.has_reaped_output_residue()
                {
                    self.slots[index].output_deadline = Some(cleanup_deadline(now, CLEANUP_GRACE));
                } else if self.slots[index]
                    .output_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    self.expire_cleanup(index);
                }
            } else if self.slots[index]
                .cleanup_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                self.expire_cleanup(index);
            }
        }
    }

    fn expire_cleanup(&mut self, index: usize) {
        self.expire_cleanup_with_error(
            index,
            LaunchError::new(
                LaunchErrorCode::CleanupFailed,
                "cleanup",
                "owned group or output pipes did not close before cleanup deadline",
            ),
        );
    }

    fn expire_cleanup_with_error(&mut self, index: usize, error: LaunchError) {
        self.cleanup_failed(index, error);
        if let Some(child) = &mut self.slots[index].child {
            if !child.reaped {
                let _ = child.signal(9);
                if child.observe_exit().ok().flatten().is_some() {
                    let _ = child.reap();
                }
            }
            child.abandon_cleanup();
        }
        self.slots[index].child = None;
        self.slots[index].done = true;
        self.slots[index].ready = false;
    }

    fn after_exit(&mut self, index: usize) {
        if self.stop_reason.is_some() {
            self.slots[index].done = true;
            return;
        }
        let slot = &self.slots[index];
        let exit = slot.last_exit.as_ref().expect("observed exit");
        let restart = match slot.plan.spec.restart.policy {
            RestartPolicy::Never => false,
            RestartPolicy::OnFailure => !exit.success(),
            RestartPolicy::Always => true,
        };
        if restart {
            if slot.restarts >= slot.plan.spec.restart.max_restarts {
                self.event(
                    Some(index),
                    LaunchEventKind::RestartExhausted,
                    json!({"restarts": slot.restarts}),
                );
                self.slots[index].done = true;
                self.fail(
                    LaunchError::new(
                        LaunchErrorCode::RestartExhausted,
                        "restart",
                        "finite restart budget exhausted",
                    )
                    .for_process(&self.slots[index].plan.spec.id),
                );
            } else {
                let delay = Duration::from_secs_f64(slot.plan.spec.restart.delay);
                let Some(deadline) = Instant::now().checked_add(delay) else {
                    self.fail(
                        LaunchError::invalid("restart", "backoff exceeds steady clock range")
                            .for_process(&self.slots[index].plan.spec.id),
                    );
                    return;
                };
                self.slots[index].backoff = Some(deadline);
                let delay_ms = delay.as_millis().min(u64::MAX as u128) as u64;
                self.event(Some(index), LaunchEventKind::RestartScheduled, json!({"next_generation": self.slots[index].generation + 1, "delay_ms": delay_ms}));
            }
        } else {
            let success = exit.success();
            self.slots[index].done = true;
            if !success {
                self.fail(
                    LaunchError::new(
                        LaunchErrorCode::ProcessFailed,
                        "process",
                        "process exited unsuccessfully",
                    )
                    .for_process(&self.slots[index].plan.spec.id),
                );
            }
        }
    }

    fn dependency_events(&mut self, provider: usize, kind: LaunchEventKind) {
        for index in 0..self.slots.len() {
            if self.slots[index].dependencies.contains(&provider) {
                self.event(
                    Some(index),
                    kind,
                    json!({
                        "dependency": self.slots[provider].plan.spec.id,
                        "dependency_generation": self.slots[provider].generation,
                    }),
                );
            }
        }
    }

    fn drain_output(&mut self, sink: &mut dyn EventSink) {
        // Preserve control/data events when the caller's bounded writer is full.
        // Child pipes provide bounded backpressure while process control keeps
        // advancing in poll. The sink owns the eventual stall deadline.
        if !self.events.is_empty() {
            return;
        }
        let count = self.slots.len();
        let mut remaining = OUTPUT_BUDGET;
        for offset in 0..count {
            let index = (self.output_cursor + offset) % count;
            let Some(child) = &mut self.slots[index].child else {
                continue;
            };
            let identity = child.identity.clone();
            let budget = remaining.min(PER_PROCESS_OUTPUT);
            let batch = child.output(budget);
            remaining -= budget;
            match batch {
                Ok(batch) => {
                    for record in batch.records {
                        if record.truncated_bytes > 0 {
                            self.event(
                                Some(index),
                                LaunchEventKind::OutputTruncated,
                                json!({"stream": record.stream, "bytes": record.truncated_bytes}),
                            );
                        } else {
                            let details = (record.stream == OutputStream::Stdout)
                                .then(|| crate::output::stdout_details(&record.bytes))
                                .flatten();
                            if let Some(details) = details {
                                self.event_with_details(
                                    Some(index),
                                    LaunchEventKind::ProcessStdout,
                                    details,
                                );
                            } else if self.output_error.is_none() {
                                if let Err(error) = sink.log(&ProcessLog {
                                    identity: identity.clone(),
                                    stream: record.stream,
                                    text: String::from_utf8_lossy(&record.bytes).into_owned(),
                                }) {
                                    self.event(
                                        Some(index),
                                        LaunchEventKind::OutputDropped,
                                        json!({"stream":record.stream,"lines":1,
                                            "reason":format!("{:?}",error.kind())}),
                                    );
                                }
                            }
                        }
                    }
                    self.flush_events(sink);
                    if !self.events.is_empty() {
                        self.output_cursor = (index + 1) % count;
                        break;
                    }
                }
                Err(error) if error.code == LaunchErrorCode::CleanupFailed => {
                    self.expire_cleanup_with_error(index, error);
                }
                Err(error) => self.fail(error.for_process(&self.slots[index].plan.spec.id)),
            }
            if remaining == 0 {
                self.output_cursor = (index + 1) % count;
                break;
            }
        }
    }

    fn output_failed(&mut self, error: std::io::Error) {
        if self.output_error.is_none() {
            let error =
                LaunchError::new(LaunchErrorCode::OutputIoError, "output", error.to_string());
            self.output_error = Some(error.clone());
            self.fail(error);
        }
    }

    fn flush_events(&mut self, sink: &mut dyn EventSink) {
        while let Some(event) = self.events.front() {
            if self.output_error.is_none() {
                if let Err(error) = sink.emit(event) {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        self.output_backpressured = true;
                        break;
                    }
                    self.output_failed(error);
                }
            }
            self.events.pop_front();
        }
    }

    fn event(&mut self, index: Option<usize>, event: LaunchEventKind, details: Value) {
        self.event_with_details(
            index,
            event,
            serde_json::value::to_raw_value(&details).expect("structured event is valid JSON"),
        );
    }

    fn event_with_details(
        &mut self,
        index: Option<usize>,
        event: LaunchEventKind,
        details: Box<serde_json::value::RawValue>,
    ) {
        if self.output_error.is_some() {
            return;
        }
        if self.events.len() >= MAX_EVENTS {
            self.output_failed(std::io::Error::other("bounded event queue exhausted"));
            return;
        }
        let slot = index.map(|index| &self.slots[index]);
        let child = slot.and_then(|slot| slot.child.as_ref());
        self.events.push_back(LaunchEvent {
            schema_version: SCHEMA_VERSION,
            launch_id: self.launch_id.clone(),
            process_id: slot.map(|slot| slot.plan.spec.id.clone()),
            generation: slot.map(|slot| slot.generation),
            event,
            elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            pid: child.map(|child| child.identity.pid),
            pgid: child.map(|child| child.pgid),
            details,
        });
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(mut probe) = self.probe.take() {
            probe.close();
        }
        self.event(
            None,
            LaunchEventKind::LaunchFinished,
            json!({"summary": self.summary()}),
        );
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if let Some(probe) = &mut self.probe {
            probe.close();
        }
        // Kill all groups first, then reap under one shared deadline.
        for slot in &mut self.slots {
            if let Some(child) = &mut slot.child {
                if !child.reaped {
                    let _ = child.signal(9);
                }
            }
        }
        let deadline = cleanup_deadline(Instant::now(), CLEANUP_GRACE);
        while Instant::now() < deadline {
            let mut pending = false;
            for slot in &mut self.slots {
                if let Some(child) = &mut slot.child {
                    if child.reaped {
                        continue;
                    }
                    if child.observe_exit().ok().flatten().is_some()
                        && matches!(child.other_group_members(), Ok(false))
                    {
                        let _ = child.reap();
                    }
                    pending |= !child.reaped;
                }
            }
            if !pending {
                break;
            }
            std::thread::sleep(POLL_QUANTUM);
        }
        for slot in &mut self.slots {
            if let Some(child) = &mut slot.child {
                if !child.reaped {
                    let _ = child.signal(9);
                    if child.observe_exit().ok().flatten().is_some() {
                        let _ = child.reap();
                    }
                }
                child.abandon_cleanup();
            }
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn absolute_output_deadline_closes_reaped_pipes_after_an_earlier_output_error() {
        let cwd = std::env::temp_dir();
        let planned = PlannedProcess {
            spec: serde_json::from_value(json!({
                "id":"writer", "package":"fixture", "entry":"echo", "args":["pending"]
            }))
            .unwrap(),
            entry: crate::ResolvedEntry {
                interpreter: None,
                interpreter_args: Vec::new(),
                package: "fixture".into(),
                version: "1".into(),
                executable: "/bin/echo".into(),
                share_dir: cwd.clone(),
                runtime_abi: crate::RUNTIME_ABI.into(),
            },
            cwd,
            bootstrap: crate::ProcessBootstrap {
                schema_version: 1,
                launch_id: String::new(),
                process_id: "writer".into(),
                generation: 0,
                context: crate::DeploymentLayers::default(),
                node: crate::DeploymentLayers::default(),
            },
            readiness: None,
        };
        let mut supervisor = Supervisor::start(
            LaunchPlan {
                processes: vec![planned],
                layers: vec![vec![0]],
                context: None,
                shutdown: crate::ShutdownSpec::default(),
                prefix_path: OsString::new(),
            },
            None,
        )
        .unwrap();
        supervisor.spawn_eligible();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            supervisor.observe_children();
            supervisor.events.clear();
            supervisor.finish_cleanups();
            if supervisor.slots[0]
                .child
                .as_ref()
                .is_some_and(|child| child.reaped)
            {
                break;
            }
            assert!(Instant::now() < deadline, "test leader did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!supervisor.slots[0].child.as_ref().unwrap().output_closed());
        supervisor.output_failed(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "the original output failure",
        ));
        let original = supervisor.output_error.clone();
        // Inject only the absolute clock boundary, retaining the real reaped
        // child and its unread output pipes. The prior error must not bypass it.
        supervisor.stop_output_deadline = Some(Instant::now());
        supervisor.expire_shutdown_output();
        assert!(supervisor.slots[0].child.is_none());
        assert!(supervisor.slots[0].done);
        assert_eq!(supervisor.output_error, original);
        assert!(supervisor.cleanup_errors.is_empty());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn finished_stop_still_applies_its_absolute_output_deadline() {
        struct Blocked;
        impl EventSink for Blocked {
            fn emit(&mut self, _: &LaunchEvent) -> std::io::Result<()> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            fn log(&mut self, _: &ProcessLog) -> std::io::Result<()> {
                Ok(())
            }
        }
        for late_stop in [false, true] {
            let mut supervisor = Supervisor::start(
                LaunchPlan {
                    processes: vec![],
                    layers: vec![],
                    context: None,
                    shutdown: crate::ShutdownSpec::default(),
                    prefix_path: OsString::new(),
                },
                None,
            )
            .unwrap();
            if !late_stop {
                supervisor.request_stop(StopReason::Requested);
            }
            assert_ne!(
                supervisor.poll(Duration::ZERO, &mut Blocked).unwrap(),
                RunProgress::Finished,
            );
            assert!(supervisor.finished);
            assert!(!supervisor.events.is_empty());
            if late_stop {
                supervisor.request_stop(StopReason::Requested);
                assert!(
                    supervisor.stop_output_deadline.is_some(),
                    "late stop must bound the remaining output even after processes finish"
                );
            }
            // Inject only the clock boundary; the same finished/pending-events
            // state and poll path are used by a caller whose sink never drains.
            supervisor.stop_output_deadline = Some(Instant::now());
            let error = supervisor.poll(Duration::ZERO, &mut Blocked).unwrap_err();
            assert_eq!(error.code, LaunchErrorCode::OutputIoError);
            assert!(supervisor.events.is_empty());
            assert!(supervisor.summary().cleanup_errors.is_empty());
        }
    }
}
