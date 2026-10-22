//! CLI adaptation only. Recording, replay state and dispatch barriers live in aos-bag.
use std::path::Path;
#[cfg(any(unix, windows))]
use std::time::Duration;

use aos_bag::{BagError, BagErrorKind, InspectOptions, RecoveryOptions};
#[cfg(any(unix, windows))]
use aos_bag::{PlaybackOptions, RecordOptions};
use serde::{de::DeserializeOwned, Serialize};
#[cfg(any(unix, windows))]
use serde_json::json;
use serde_json::Value;

use super::{CliError, Result};

fn error(error: BagError) -> CliError {
    let (code, exit) = match error.kind {
        BagErrorKind::InvalidArgument => ("INVALID_ARGUMENT", 2),
        BagErrorKind::Corrupt => ("BAG_CORRUPT", 2),
        BagErrorKind::Incomplete => ("BAG_INCOMPLETE", 2),
        BagErrorKind::Timeout => ("TIMEOUT", 4),
        BagErrorKind::Closed => ("BAG_CLOSED", 3),
        BagErrorKind::Transport => ("COMMUNICATION_ERROR", 3),
        BagErrorKind::DiskFull => ("BAG_DISK_FULL", 1),
        BagErrorKind::CapacityLimit => ("BAG_CAPACITY_LIMIT", 1),
        BagErrorKind::ResourceLimit => ("BAG_RESOURCE_LIMIT", 1),
        BagErrorKind::Io => ("BAG_IO_ERROR", 1),
    };
    CliError {
        code,
        message: error.to_string(),
        exit,
    }
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|error| CliError::input(format!("bag options: {error}")))
}

fn value(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| CliError::input(error.to_string()))
}

fn native_options(request: &Value, live: bool) -> Result<Value> {
    let mut options = request.clone();
    let map = options
        .as_object_mut()
        .ok_or_else(|| CliError::input("expected object"))?;
    for field in [
        "directory",
        "destination",
        "context_config",
        "security_profile",
        "duration",
        "follow",
        "status_interval",
        "shutdown_timeout",
        "control_input",
        "start_offset_ns",
    ] {
        map.remove(field);
    }
    if live {
        if map.get("qos").is_some_and(Value::is_null) {
            map.remove("qos");
        }
        if let Some(qos) = map.get_mut("qos") {
            *qos = value(aos_core::resolve_qos(qos, "default", false)?.profile)?;
        }
        for name in ["topic_qos", "channel_qos"] {
            if let Some(profiles) = map.get_mut(name) {
                for qos in profiles
                    .as_object_mut()
                    .ok_or_else(|| CliError::input(format!("{name} must be an object")))?
                    .values_mut()
                {
                    *qos = value(aos_core::resolve_qos(qos, "default", false)?.profile)?;
                }
            }
        }
    }
    Ok(options)
}

pub fn execute(verb: &str, request: &Value) -> Result<Option<Value>> {
    let directory = Path::new(super::text(request, "directory"));
    match verb {
        "info" => {
            let options: InspectOptions = decode(native_options(request, false)?)?;
            Ok(Some(value(
                aos_bag::inspect(directory, options).map_err(error)?,
            )?))
        }
        "recover" => {
            let options: RecoveryOptions = decode(native_options(request, false)?)?;
            let destination = Path::new(super::text(request, "destination"));
            Ok(Some(value(
                aos_bag::recover(directory, destination, options).map_err(error)?,
            )?))
        }
        "record" | "play" => {
            #[cfg(any(unix, windows))]
            {
                live::execute(verb, request)?;
                Ok(None)
            }
            #[cfg(not(any(unix, windows)))]
            {
                Err(CliError::input(
                    "bag record/play CLI currently requires Unix; use the native SDK",
                ))
            }
        }
        _ => Err(CliError::input("unknown bag verb")),
    }
}

#[cfg(any(unix, windows))]
mod live {
    use super::*;
    use std::io::{BufRead, Read};
    use std::sync::mpsc::{self, Receiver, TryRecvError, TrySendError};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use aos_bag::{PlaybackState, Player, RecordState, RecordStopReason, Recorder};
    use serde::Deserialize;

    const TICK: Duration = Duration::from_millis(20);
    const CONTROL_LINE_BYTES: usize = 64 * 1024;

    struct Timing {
        duration: Option<Duration>,
        status_interval: Duration,
        shutdown: Duration,
    }

    impl Timing {
        fn parse(verb: &str, request: &Value) -> Result<Self> {
            if request["follow"] == true && request.get("duration").is_some() {
                return Err(CliError::input("follow conflicts with duration"));
            }
            let duration = if request.get("duration").is_some() {
                Some(super::super::publication::seconds(
                    request, "duration", 30.0, false,
                )?)
            } else if verb == "record" && request["follow"] != true {
                Some(Duration::from_secs(30))
            } else {
                None
            };
            Ok(Self {
                duration,
                status_interval: super::super::publication::seconds(
                    request,
                    "status_interval",
                    1.0,
                    false,
                )?,
                shutdown: super::super::publication::seconds(
                    request,
                    "shutdown_timeout",
                    5.0,
                    false,
                )?,
            })
        }
    }

    // At most one unsent control/header event and one coalescible snapshot live
    // outside the shared bounded writer. Stop and worker checks never wait for it.
    struct Output {
        pending: Option<Value>,
        progress: Option<Value>,
        coalesced: u64,
    }

    impl Output {
        fn new() -> Self {
            Self {
                pending: None,
                progress: None,
                coalesced: 0,
            }
        }
        fn flush(&mut self) -> Result<()> {
            for slot in [&mut self.pending, &mut self.progress] {
                if let Some(event) = slot {
                    if super::super::process_commands::try_stream_event(event)? {
                        *slot = None;
                    } else {
                        break;
                    }
                }
            }
            Ok(())
        }
        fn event(&mut self, name: &str, data: Value) -> Result<()> {
            if self.pending.is_some() {
                return Err(CliError::input(
                    "previous control event is still awaiting output",
                ));
            }
            // A held status predates this semantic event. Never emit an old
            // position/epoch after the acknowledgement of a newer barrier.
            if self.progress.take().is_some() {
                self.coalesced += 1;
            }
            self.pending = Some(json!({"schema_version":1,"event":name,"data":data}));
            self.flush()
        }
        fn progress(&mut self, name: &str, mut data: Value) -> Result<()> {
            if self.progress.is_some() {
                self.coalesced += 1;
            }
            data["coalesced_status_snapshots"] = json!(self.coalesced);
            self.progress = Some(json!({"schema_version":1,"event":name,"data":data}));
            self.flush()
        }
        fn summary(&mut self, mut data: Value) -> Result<()> {
            data["coalesced_status_snapshots"] = json!(self.coalesced);
            // Preserve unsent semantic events inside the terminal report. A
            // slow sink may still prevent the final report, yielding OUTPUT_ERROR.
            data["pending_event"] = self.pending.take().unwrap_or(Value::Null);
            self.progress.take();
            super::super::process_commands::set_stream_summary(
                &json!({"schema_version":1,"event":"summary","data":data}),
            )
        }
    }

    fn compact_record(status: &aos_bag::RecordStatus) -> Value {
        json!({"state":status.state,"directory":status.directory,"recording_id":status.recording_id,
            "counters":status.counters,"active_channel_count":status.active_channels.len(),
            "unresolved_count":status.unresolved.len(),
            "unresolved_sample":status.unresolved.iter().take(8).collect::<Vec<_>>(),
            "queue_messages":status.queue_messages,"queue_bytes":status.queue_bytes,
            "shards":status.shards,"stop_reason":status.stop_reason,"error":status.error,
            "resources_closed":status.resources_closed,"cleanup_error":status.cleanup_error})
    }

    fn compact_player(status: &aos_bag::PlaybackStatus) -> Value {
        json!({"state":status.state,"position_ns":status.position_ns,"duration_ns":status.duration_ns,
            "rate":status.rate,"epoch":status.epoch,"published_total":status.published_total,
            "published_bytes":status.published_bytes,"endpoint_count":status.endpoints.len(),
            "original_names":status.original_names,"incomplete_source":status.incomplete_source,
            "clock_topic":status.clock_topic,"error":status.error,
            "resources_closed":status.resources_closed,"cleanup_error":status.cleanup_error})
    }

    fn compact_player_progress(
        progress: &aos_bag::PlaybackProgress,
        initial: &aos_bag::PlaybackStatus,
    ) -> Value {
        json!({"state":progress.state,"position_ns":progress.position_ns,
            "duration_ns":progress.duration_ns,"rate":progress.rate,"epoch":progress.epoch,
            "published_total":progress.published_total,"published_bytes":progress.published_bytes,
            "endpoint_count":progress.endpoint_count,"error":progress.error,
            "resources_closed":progress.resources_closed,"cleanup_error":progress.cleanup_error,
            "original_names":initial.original_names,"incomplete_source":initial.incomplete_source,
            "clock_topic":initial.clock_topic})
    }

    fn interrupted(signal: i32) -> CliError {
        CliError {
            code: "INTERRUPTED",
            message: format!("bag stopped by signal {signal}"),
            exit: 128 + signal,
        }
    }

    pub(super) fn execute(verb: &str, request: &Value) -> Result<()> {
        let timing = Timing::parse(verb, request)?;
        let options = native_options(request, true)?;
        // Deserialize and validate every runtime option before network setup.
        let recorder = if verb == "record" {
            let mut options: RecordOptions = decode(options.clone())?;
            options.directory = super::super::text(request, "directory").into();
            options.validate().map_err(error)?;
            Some(options)
        } else {
            None
        };
        let player = if verb == "play" {
            let options: PlaybackOptions = decode(options)?;
            options.validate().map_err(error)?;
            if options.start_paused && request["control_input"] != "stdin" {
                return Err(CliError::input("start_paused requires control_input=stdin"));
            }
            Some(options)
        } else {
            None
        };
        let offset = super::super::optional_u64(request, "start_offset_ns")?.unwrap_or(0);
        let configuration =
            super::super::configuration::InvocationConfig::parse(request, false, false)?;
        super::super::phase("session_open");
        let (context, _) = super::super::open_context(&configuration)?;
        super::super::phase("bag_runtime");
        let result = match (recorder, player) {
            (Some(options), None) => record(&context, options, &timing),
            (None, Some(options)) => play(&context, request, options, &timing, offset),
            _ => unreachable!(),
        };
        // Shutdown may enter a blocking adapter. Keep it off this control
        // thread; only this CLI process owns the context and is exiting.
        let (send, receive) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = send.send(context.shutdown());
        });
        let closed = receive
            .recv_timeout(timing.shutdown)
            .map_err(|_| CliError {
                code: "TIMEOUT",
                message: "CLI Context shutdown is not confirmed".into(),
                exit: 4,
            })?
            .map_err(CliError::from);
        result.and(closed)
    }

    fn record(context: &aos_core::Context, options: RecordOptions, timing: &Timing) -> Result<()> {
        let signals = super::super::process_commands::Signals::install()?;
        let recorder = Recorder::start(context.clone(), options).map_err(error)?;
        let mut output = Output::new();
        let started = Instant::now();
        let mut next_status = started;
        let run = (|| -> Result<i32> {
            output.event("recording_started", json!(recorder.progress()))?;
            loop {
                let status = recorder.progress();
                let signal = signals.received();
                if signal != 0 {
                    recorder
                        .stop(RecordStopReason::Interrupted)
                        .map_err(error)?;
                    return Ok(signal);
                }
                if matches!(
                    status.state,
                    RecordState::Finished | RecordState::Failed | RecordState::Stopping
                ) {
                    return Ok(0);
                }
                if timing
                    .duration
                    .is_some_and(|limit| started.elapsed() >= limit)
                {
                    recorder.stop(RecordStopReason::Requested).map_err(error)?;
                    return Ok(0);
                }
                output.flush()?;
                if Instant::now() >= next_status {
                    output.progress("recording_status", json!(status))?;
                    next_status = Instant::now() + timing.status_interval;
                }
                std::thread::sleep(TICK);
            }
        })();
        if run.is_err() {
            let _ = recorder.stop(RecordStopReason::Requested);
        }
        let closed = recorder.wait(timing.shutdown);
        let mut report_error;
        let summary = match closed {
            Ok(report) => {
                report_error = report
                    .status
                    .error
                    .clone()
                    .or_else(|| report.status.cleanup_error.clone())
                    .map(error);
                if (report.status.state == RecordState::Failed || !report.status.resources_closed)
                    && report_error.is_none()
                {
                    report_error = Some(CliError {
                        code: "BAG_IO_ERROR",
                        message: "recording or resource cleanup failed without a detailed error"
                            .into(),
                        exit: 1,
                    });
                }
                json!({"mode":"record","resources_closed":report.status.resources_closed,
                    "cleanup_error":report.status.cleanup_error,"status":compact_record(&report.status),
                    "manifest":report.manifest})
            }
            Err(failure) => {
                let summary = json!({"mode":"record","resources_closed":false,
                    "status":compact_record(&recorder.status()),"close_error":failure});
                report_error = Some(error(failure));
                summary
            }
        };
        output.summary(summary)?;
        let signal = run?;
        if let Some(error) = report_error {
            return Err(error);
        }
        if signal != 0 {
            return Err(interrupted(signal));
        }
        Ok(())
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Control {
        id: String,
        command: String,
        offset_ns: Option<u64>,
        rate: Option<f64>,
    }

    impl Control {
        fn parse(line: &[u8]) -> Result<Self> {
            let text =
                std::str::from_utf8(line).map_err(|_| CliError::input("control must be UTF-8"))?;
            let control: Self = decode(aos_core::parse_config_json(text)?)?;
            if control.id.is_empty() || control.id.len() > 256 {
                return Err(CliError::input(
                    "control id must contain 1..256 UTF-8 bytes",
                ));
            }
            match control.command.as_str() {
                "seek" if control.offset_ns.is_some() && control.rate.is_none() => {}
                "rate"
                    if control.offset_ns.is_none()
                        && control
                            .rate
                            .is_some_and(|rate| rate.is_finite() && rate > 0.0) => {}
                "pause" | "resume" | "stop" | "status"
                    if control.offset_ns.is_none() && control.rate.is_none() => {}
                _ => {
                    return Err(CliError::input(
                        "invalid control command or command-specific fields",
                    ))
                }
            }
            Ok(control)
        }
        fn apply(&self, player: &Player, check: impl FnMut() -> aos_bag::BagResult<()>) -> Value {
            let result = match self.command.as_str() {
                "pause" => player.pause_with_check(check),
                "resume" => player.resume_with_check(check),
                "seek" => player.seek_with_check(self.offset_ns.unwrap(), check),
                "rate" => player.set_rate_with_check(self.rate.unwrap(), check),
                "stop" => player.stop().map(|_| player.status()),
                "status" => Ok(player.status()),
                _ => unreachable!(),
            };
            match result {
                Ok(status) => json!({"id":self.id,"command":self.command,"ok":true,
                    "status":compact_player(&status)}),
                Err(error) => json!({"id":self.id,"command":self.command,"ok":false,
                    "error":error,"status":compact_player(&player.status())}),
            }
        }
    }

    enum Input {
        Control(Control),
        Invalid(String),
    }

    enum InputTermination {
        Stop(Control),
        Eof,
        Failed(String),
    }

    struct Controls {
        receive: Receiver<Input>,
        terminal: Arc<Mutex<Option<InputTermination>>>,
    }

    impl Controls {
        fn has_terminal(&self) -> bool {
            self.terminal.lock().unwrap().is_some()
        }

        fn take_terminal(&self) -> Option<InputTermination> {
            self.terminal.lock().unwrap().take()
        }
    }

    fn controls() -> Controls {
        let (send, receive) = mpsc::sync_channel(32);
        let terminal = Arc::new(Mutex::new(None));
        let thread_terminal = terminal.clone();
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            let mut reader = stdin.lock();
            read_controls(&mut reader, send, thread_terminal);
            // This worker only reads this invocation's stdin. It may still be
            // in a blocking OS read at CLI exit; never join it on the stop path.
        });
        Controls { receive, terminal }
    }

    fn read_controls(
        reader: &mut impl BufRead,
        send: mpsc::SyncSender<Input>,
        thread_terminal: Arc<Mutex<Option<InputTermination>>>,
    ) {
        loop {
            let mut line = Vec::new();
            let read = reader
                .by_ref()
                .take((CONTROL_LINE_BYTES + 1) as u64)
                .read_until(b'\n', &mut line);
            let input = match read {
                Ok(0) => {
                    *thread_terminal.lock().unwrap() = Some(InputTermination::Eof);
                    break;
                }
                Ok(_) if line.len() > CONTROL_LINE_BYTES => {
                    *thread_terminal.lock().unwrap() = Some(InputTermination::Failed(
                        "control line exceeds 64 KiB".into(),
                    ));
                    break;
                }
                Ok(_) => match Control::parse(&line) {
                    Ok(control) if control.command == "stop" => {
                        *thread_terminal.lock().unwrap() = Some(InputTermination::Stop(control));
                        break;
                    }
                    Ok(control) => Input::Control(control),
                    Err(error) => Input::Invalid(error.message),
                },
                Err(error) => {
                    *thread_terminal.lock().unwrap() =
                        Some(InputTermination::Failed(format!("control input: {error}")));
                    break;
                }
            };
            match send.try_send(input) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    *thread_terminal.lock().unwrap() = Some(InputTermination::Failed(
                        "control input queue exceeds 32 pending commands".into(),
                    ));
                    break;
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
    }

    fn play(
        context: &aos_core::Context,
        request: &Value,
        options: PlaybackOptions,
        timing: &Timing,
        offset: u64,
    ) -> Result<()> {
        let signals = super::super::process_commands::Signals::install()?;
        let player = Player::open(
            context.clone(),
            Path::new(super::super::text(request, "directory")),
            options,
        )
        .map_err(error)?;
        let input = (request["control_input"] == "stdin").then(controls);
        let mut terminal_control_result = Value::Null;
        let mut input_stop_reason = Value::Null;
        let mut output = Output::new();
        let started = Instant::now();
        let mut next_status = started;
        let mut check_running = || {
            if signals.received() != 0 || input.as_ref().is_some_and(Controls::has_terminal) {
                player.stop()?;
                Err(BagError::new(
                    BagErrorKind::Closed,
                    "playback stop requested",
                ))
            } else {
                Ok(())
            }
        };
        let run = (|| -> Result<i32> {
            let initial = player.status();
            output.event(
                "playback_ready",
                json!({"status":compact_player(&initial),"endpoints":initial.endpoints}),
            )?;
            let prepare = (|| {
                if offset != 0 {
                    player.seek_with_check(offset, &mut check_running)?;
                }
                player.start_with_check(&mut check_running)?;
                Ok::<_, BagError>(())
            })();
            if let Err(failure) = prepare {
                // Preserve the signal/terminal-input exit contract below
                // rather than replacing it with the cancellation sentinel.
                if signals.received() == 0 && !input.as_ref().is_some_and(Controls::has_terminal) {
                    return Err(error(failure));
                }
            }
            loop {
                let status = player.progress();
                let signal = signals.received();
                if signal != 0 {
                    player.stop().map_err(error)?;
                    return Ok(signal);
                }
                if let Some(terminal) = input.as_ref().and_then(Controls::take_terminal) {
                    player.stop().map_err(error)?;
                    match terminal {
                        InputTermination::Stop(control) => {
                            input_stop_reason = json!("stop");
                            terminal_control_result = control.apply(&player, || Ok(()));
                            if output.pending.is_none() {
                                output.event("control_result", terminal_control_result.clone())?;
                            }
                            return Ok(0);
                        }
                        InputTermination::Eof => {
                            input_stop_reason = json!("eof");
                            return Ok(0);
                        }
                        InputTermination::Failed(message) => {
                            input_stop_reason = json!("input_error");
                            return Err(CliError {
                                code: "CONTROL_INPUT_ERROR",
                                message,
                                exit: 2,
                            });
                        }
                    }
                }
                if matches!(
                    status.state,
                    PlaybackState::Finished | PlaybackState::Failed | PlaybackState::Stopped
                ) {
                    return Ok(0);
                }
                if timing
                    .duration
                    .is_some_and(|limit| started.elapsed() >= limit)
                {
                    player.stop().map_err(error)?;
                    return Ok(0);
                }
                output.flush()?;
                if output.pending.is_none() {
                    if let Some(input) = &input {
                        match input.receive.try_recv() {
                            Ok(Input::Control(control)) => output.event(
                                "control_result",
                                control.apply(&player, &mut check_running),
                            )?,
                            Ok(Input::Invalid(message)) => {
                                output.event(
                                    "control_result",
                                    json!({"id":null,"ok":false,
                                    "error":{"kind":"invalid_argument","message":message},
                                    "status":compact_player(&player.status())}),
                                )?;
                            }
                            // The reader publishes its terminal reason before
                            // disconnecting; the next loop observes that reason.
                            Err(TryRecvError::Disconnected | TryRecvError::Empty) => {}
                        }
                    }
                }
                if Instant::now() >= next_status {
                    output.progress(
                        "playback_status",
                        compact_player_progress(&player.progress(), &initial),
                    )?;
                    next_status = Instant::now() + timing.status_interval;
                }
                std::thread::sleep(TICK);
            }
        })();
        if run.is_err() {
            let _ = player.stop();
        }
        let closed = player.wait(timing.shutdown);
        let mut report_error;
        let mut summary = match closed {
            Ok(report) => {
                report_error = report
                    .status
                    .error
                    .clone()
                    .or_else(|| report.status.cleanup_error.clone())
                    .map(error);
                if (report.status.state == PlaybackState::Failed || !report.status.resources_closed)
                    && report_error.is_none()
                {
                    report_error = Some(CliError {
                        code: "BAG_IO_ERROR",
                        message: "playback or resource cleanup failed without a detailed error"
                            .into(),
                        exit: 1,
                    });
                }
                json!({"mode":"play","resources_closed":report.status.resources_closed,
                    "cleanup_error":report.status.cleanup_error,"status":compact_player(&report.status)})
            }
            Err(failure) => {
                let summary = json!({"mode":"play","resources_closed":false,
                    "status":compact_player(&player.status()),"close_error":failure});
                report_error = Some(error(failure));
                summary
            }
        };
        summary["terminal_control_result"] = terminal_control_result;
        summary["input_stop_reason"] = input_stop_reason;
        summary["unapplied_controls"] = json!(input
            .as_ref()
            .map(|input| input
                .receive
                .try_iter()
                .map(|item| match item {
                    Input::Control(control) => json!({"id":control.id,"command":control.command}),
                    Input::Invalid(_) => json!({"id":null,"command":null}),
                })
                .collect::<Vec<_>>())
            .unwrap_or_default());
        output.summary(summary)?;
        let signal = run?;
        if let Some(error) = report_error {
            return Err(error);
        }
        if signal != 0 {
            return Err(interrupted(signal));
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn stop_and_eof_bypass_a_full_control_queue() {
            for suffix in ["", "{\"id\":\"stop\",\"command\":\"stop\"}\n"] {
                let (send, receive) = mpsc::sync_channel(32);
                let terminal = Arc::new(Mutex::new(None));
                let thread_terminal = terminal.clone();
                let text = format!(
                    "{}{suffix}",
                    "{\"id\":\"queued\",\"command\":\"status\"}\n".repeat(32)
                );
                let (done, finished) = mpsc::sync_channel(1);
                let worker = std::thread::spawn(move || {
                    read_controls(&mut std::io::Cursor::new(text), send, thread_terminal);
                    done.send(()).unwrap();
                });
                finished.recv_timeout(Duration::from_secs(1)).unwrap();
                worker.join().unwrap();
                assert_eq!(receive.try_iter().count(), 32);
                let reason = terminal.lock().unwrap().take().unwrap();
                assert!(matches!(
                    (suffix.is_empty(), reason),
                    (true, InputTermination::Eof) | (false, InputTermination::Stop(_))
                ));
            }
        }

        #[test]
        fn controls_reject_duplicates_unknown_fields_and_lossy_numeric_forms() {
            for bad in [
                r#"{"id":"x","command":"seek","offset_ns":3.0}"#,
                r#"{"id":"x","command":"seek","offset_ns":true}"#,
                r#"{"id":"x","command":"seek","offset_ns":-1}"#,
                r#"{"id":"x","command":"pause","command":"resume"}"#,
                r#"{"id":"x","command":"pause","rate":2}"#,
                r#"{"id":"x","command":"rate","rate":0}"#,
                r#"{"id":"x","command":"stop","unexpected":1}"#,
            ] {
                assert!(Control::parse(bad.as_bytes()).is_err(), "{bad}");
            }
            assert_eq!(
                Control::parse(br#"{"id":"x","command":"seek","offset_ns":18446744073709551615}"#)
                    .unwrap()
                    .offset_ns,
                Some(u64::MAX)
            );
        }
    }
}
