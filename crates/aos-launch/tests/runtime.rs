#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use aos_launch::*;
use serde_json::{json, Value};

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("aos-launch-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(root.join("lib/demo")).unwrap();
        fs::create_dir_all(root.join("share/demo")).unwrap();
        fs::create_dir_all(root.join("share/aos/resource_index/packages")).unwrap();
        let executable = std::env::current_exe().unwrap();
        let entry = root.join("lib/demo/child");
        fs::hard_link(&executable, &entry)
            .or_else(|_| fs::copy(&executable, &entry).map(|_| ()))
            .unwrap();
        let fixture = Self { root };
        fixture.manifest(json!({
            "schema_version":1, "name":"demo", "version":"1.0", "runtime_abi":RUNTIME_ABI,
            "entries":{"child":{"executable":"lib/demo/child","kind":"aos_node"}}
        }));
        fixture
    }
    fn manifest_path(&self) -> PathBuf {
        self.root
            .join("share/aos/resource_index/packages/demo.json")
    }
    fn manifest(&self, value: Value) {
        fs::write(self.manifest_path(), serde_json::to_vec(&value).unwrap()).unwrap();
    }
    fn index(&self) -> PackageIndex {
        PackageIndex::discover(std::slice::from_ref(&self.root)).unwrap()
    }
    fn spec(&self, processes: Vec<Value>) -> LaunchSpec {
        parse_launch(
            &json!({"schema_version":1,"context":{"domain_id":711,"zenoh_config":null},
            "processes":processes,"shutdown":{"sigint_timeout":0.12,"sigterm_timeout":0.12}})
            .to_string(),
        )
        .unwrap()
    }
    fn plan(&self, processes: Vec<Value>) -> LaunchPlan {
        prepare(
            &self.index(),
            self.spec(processes),
            LaunchOverrides::default(),
        )
        .unwrap()
    }
    fn marker(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn process(id: &str, mode: &str) -> Value {
    json!({"id":id,"package":"demo","entry":"child",
        "args":["--exact","fixture_process","--ignored","--nocapture"],
        "env":{"AOS_TEST_MODE":mode}})
}
fn ready(mut process: Value) -> Value {
    process["readiness"] =
        json!({"node":format!("/{}",process["id"].as_str().unwrap()),"timeout":2.0});
    process
}

type ProcessKey = (String, String, u32, u32);
type BirthTime = (u64, u64);

#[derive(Default)]
struct Sink {
    events: Vec<LaunchEvent>,
    stderr: Vec<String>,
    stdout_lines: usize,
    fail: bool,
    births: BTreeMap<ProcessKey, Option<BirthTime>>,
}
impl Sink {
    fn remember_identity(&mut self, event: &LaunchEvent) {
        if event.event == LaunchEventKind::ProcessStarted {
            self.births
                .entry(identity_key(event))
                .or_insert_with(|| observe_owned_birth(event.pid.unwrap()));
        }
    }
}
impl EventSink for Sink {
    fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
        self.remember_identity(event);
        if self.fail {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed sink"));
        }
        if event.event == LaunchEventKind::ProcessStdout {
            self.stdout_lines += 1;
            if self.stdout_lines > 1000 {
                return Ok(());
            }
        }
        self.events.push(event.clone());
        Ok(())
    }
    fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
        if self.fail {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed sink"));
        }
        if self.stderr.len() < 1000 {
            self.stderr.push(record.text.clone());
        }
        Ok(())
    }
}
fn drive(supervisor: &mut Supervisor, sink: &mut Sink) -> LaunchResult<()> {
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        if supervisor.poll(Duration::from_millis(5), sink)? == RunProgress::Finished {
            return Ok(());
        }
    }
    panic!("supervisor deadline: {:?}", supervisor.summary())
}
fn until(supervisor: &mut Supervisor, sink: &mut Sink, predicate: impl Fn(&Sink) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline {
        let progress = supervisor.poll(Duration::from_millis(5), sink).unwrap();
        if predicate(sink) {
            return;
        }
        assert_ne!(
            progress,
            RunProgress::Finished,
            "{:?}; control events: {:?}; diagnostics: {:?}",
            supervisor.summary(),
            sink.events
                .iter()
                .filter(|event| event.event != LaunchEventKind::ProcessStdout)
                .collect::<Vec<_>>(),
            sink.stderr,
        );
    }
    panic!("condition deadline");
}
fn started(sink: &Sink, id: &str) -> bool {
    sink.events.iter().any(|event| {
        event.event == LaunchEventKind::ProcessStarted && event.process_id.as_deref() == Some(id)
    })
}
fn stdout_contains(sink: &Sink, text: &str) -> bool {
    sink.events.iter().any(|event| {
        event.event == LaunchEventKind::ProcessStdout
            && details(event)["value"]["fixture"]
                .as_str()
                .is_some_and(|line| line.contains(text))
    })
}
fn details(event: &LaunchEvent) -> Value {
    serde_json::from_str(event.details.get()).unwrap()
}
fn identity_key(event: &LaunchEvent) -> ProcessKey {
    (
        event.launch_id.clone(),
        event.process_id.clone().unwrap(),
        event.generation.unwrap(),
        event.pid.unwrap(),
    )
}
fn process_birth(pid: u32) -> Option<BirthTime> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let ticks = stat
            .rsplit_once(") ")?
            .1
            .split_whitespace()
            .nth(19)?
            .parse()
            .ok()?;
        Some((ticks, 0))
    }
    #[cfg(target_os = "macos")]
    {
        use nix::libc;
        // SAFETY: PIDTBSDINFO writes exactly this initialized struct and does
        // not retain the pointer. Birth time distinguishes a reused PID.
        let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
        let size = std::mem::size_of_val(&info);
        let result = unsafe {
            libc::proc_pidinfo(
                pid as i32,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size as i32,
            )
        };
        (result == size as i32).then_some((info.pbi_start_tvsec, info.pbi_start_tvusec))
    }
}
fn assert_reaped(sink: &Sink) {
    use nix::libc;
    for event in &sink.events {
        if event.event == LaunchEventKind::ProcessStarted {
            // Observe only: even if the PID was reused by another test's child,
            // this assertion must never reap that other supervisor's leader.
            let mut information = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    event.pid.unwrap(),
                    &mut information,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if result == -1 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::ECHILD)
                );
            } else {
                let birth = sink.births[&identity_key(event)];
                assert!(
                    birth.is_some() && process_birth(event.pid.unwrap()) != birth,
                    "the original child is still waitable, rather than a reused PID"
                );
            }
        }
    }
}

fn observe_owned_birth(pid: u32) -> Option<BirthTime> {
    let birth = process_birth(pid);
    if birth.is_none() {
        #[cfg(target_os = "macos")]
        {
            use nix::libc;
            // macOS PROC_PIDTBSDINFO omits zombies. A spawn failure can send
            // SIGINT before the queued ProcessStarted reaches this sink.
            // Observe that exact exited child without consuming its status.
            let mut information = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    pid,
                    &mut information,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            assert_eq!(result, 0, "{}", io::Error::last_os_error());
            assert_eq!(information.si_pid as u32, pid);
            // Without a birth time assert_reaped conservatively requires
            // ECHILD; it cannot excuse a still-waitable PID as a reuse.
        }
        #[cfg(target_os = "linux")]
        panic!("the first start event still owns an unreaped leader: {pid}");
    }
    birth
}

#[cfg(target_os = "macos")]
#[test]
fn exited_unreaped_leader_keeps_the_reap_assertion() {
    use nix::libc;
    let mut child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    child.kill().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut information = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                child.id(),
                &mut information,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        assert_eq!(result, 0);
        if information.si_pid != 0 {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let event = LaunchEvent {
        schema_version: 1,
        launch_id: "exited-leader".into(),
        process_id: Some("child".into()),
        generation: Some(1),
        event: LaunchEventKind::ProcessStarted,
        elapsed_ms: 0,
        pid: Some(child.id()),
        pgid: None,
        details: serde_json::value::RawValue::from_string("{}".into()).unwrap(),
    };
    let mut sink = Sink::default();
    sink.emit(&event).unwrap();
    assert_eq!(sink.births[&identity_key(&event)], None);
    assert!(std::panic::catch_unwind(|| assert_reaped(&sink)).is_err());
    assert!(!child.wait().unwrap().success());
    assert_reaped(&sink);
}

#[derive(Default)]
struct ProbeState {
    seen: Vec<ProcessIdentity>,
    forgotten: Vec<ProcessIdentity>,
    allow: bool,
    invalid_identity: bool,
    closed: usize,
}
struct Factory(Arc<Mutex<ProbeState>>);
struct Probe(Arc<Mutex<ProbeState>>);
impl ProbeFactory for Factory {
    fn create(&mut self, context: &ResolvedLaunchContext) -> LaunchResult<Box<dyn ReadinessProbe>> {
        assert_eq!(context.domain_id, 711);
        Ok(Box::new(Probe(self.0.clone())))
    }
}
impl ReadinessProbe for Probe {
    fn poll(
        &mut self,
        expected: &ProcessIdentity,
        _: &ResolvedReadiness,
        remaining: Duration,
    ) -> LaunchResult<ReadinessProgress> {
        assert!(!remaining.is_zero());
        let mut state = self.0.lock().unwrap();
        state.seen.push(expected.clone());
        Ok(ReadinessProgress {
            phase: if state.allow {
                ReadinessPhase::Ready
            } else {
                ReadinessPhase::Waiting
            },
            node_id: if state.allow && !state.invalid_identity {
                Some(format!(
                    "node-{}-{}",
                    expected.process_id, expected.generation
                ))
            } else {
                None
            },
        })
    }
    fn forget(&mut self, identity: &ProcessIdentity) {
        self.0.lock().unwrap().forgotten.push(identity.clone());
    }
    fn close(&mut self) {
        self.0.lock().unwrap().closed += 1;
    }
}

#[test]
fn index_rejects_ambiguity_version_abi_traversal_and_symlink_escape() {
    let first = Fixture::new();
    let second = Fixture::new();
    let index = PackageIndex::discover(&[first.root.clone(), first.root.clone()]).unwrap();
    assert_eq!(index.prefixes(), [first.root.canonicalize().unwrap()]);
    assert_eq!(index.packages()["demo"].len(), 1);
    assert_eq!(
        index.package("demo", Some("2.0")).unwrap_err().code,
        LaunchErrorCode::VersionMismatch
    );
    let index = PackageIndex::discover(&[first.root.clone(), second.root.clone()]).unwrap();
    assert_eq!(
        index.entry("demo", "child").unwrap_err().code,
        LaunchErrorCode::AmbiguousPackage
    );
    assert_eq!(
        index.package("missing", None).unwrap_err().code,
        LaunchErrorCode::PackageNotFound
    );
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(first.manifest_path()).unwrap()).unwrap();
    manifest["runtime_abi"] = json!("aos.process.v99");
    first.manifest(manifest.clone());
    assert_eq!(
        PackageIndex::discover(std::slice::from_ref(&first.root))
            .unwrap_err()
            .code,
        LaunchErrorCode::Unsupported
    );
    manifest["runtime_abi"] = json!(RUNTIME_ABI);
    manifest["entries"]["child"]["executable"] = json!("../outside");
    first.manifest(manifest.clone());
    assert!(PackageIndex::discover(std::slice::from_ref(&first.root)).is_err());
    std::os::unix::fs::symlink(
        second.root.join("lib/demo/child"),
        first.root.join("lib/demo/escape"),
    )
    .unwrap();
    manifest["entries"]["child"]["executable"] = json!("lib/demo/escape");
    first.manifest(manifest);
    assert!(PackageIndex::discover(std::slice::from_ref(&first.root)).is_err());
}

#[test]
fn strict_json_and_preflight_reject_before_any_spawn() {
    assert!(parse_launch(r#"{"schema_version":1,"schema_version":1,"processes":[]}"#).is_err());
    let fixture = Fixture::new();
    let mut a = ready(process("a", "echo"));
    let mut b = ready(process("b", "echo"));
    a["depends_on"] = json!(["b"]);
    b["depends_on"] = json!(["a"]);
    assert!(prepare(
        &fixture.index(),
        fixture.spec(vec![a, b]),
        LaunchOverrides::default()
    )
    .is_err());
    let mut bad = process("bad", "echo");
    bad["env"][PROCESS_CONFIG_ENV] = json!("/tmp/injected");
    assert!(prepare(
        &fixture.index(),
        fixture.spec(vec![bad]),
        LaunchOverrides::default()
    )
    .is_err());
    let mut bad_prefix = process("badprefix", "echo");
    bad_prefix["env"][PREFIX_PATH_ENV] = json!("/tmp/injected");
    assert!(prepare(
        &fixture.index(),
        fixture.spec(vec![bad_prefix]),
        LaunchOverrides::default()
    )
    .is_err());
    let marker = fixture.marker("never-created");
    let mut good = process("first", "marker");
    good["env"]["AOS_TEST_MARKER"] = json!(marker);
    let mut last = process("last", "echo");
    last["entry"] = json!("missing");
    assert!(prepare(
        &fixture.index(),
        fixture.spec(vec![good, last]),
        LaunchOverrides::default()
    )
    .is_err());
    assert!(!marker.exists());
    let config = fixture.marker("bad-config.json");
    fs::write(&config, r#"{"name":"one","name":"two"}"#).unwrap();
    let mut child = process("badconfig", "echo");
    child["config_files"] = json!([config]);
    assert!(prepare(
        &fixture.index(),
        fixture.spec(vec![child]),
        LaunchOverrides::default()
    )
    .is_err());
}

#[test]
fn argv_cwd_bootstrap_and_separate_output_without_network_factory() {
    let fixture = Fixture::new();
    let marker = fixture.marker("injected");
    let literal = format!("$(touch {}) ; echo wrong", marker.display());
    let mut child = process("one", "echo");
    child["args"]
        .as_array_mut()
        .unwrap()
        .extend([json!("--skip"), json!(literal)]);
    let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
    let mut sink = Sink::default();
    drive(&mut supervisor, &mut sink).unwrap();
    assert_eq!(supervisor.summary().outcome, RunOutcome::Completed);
    assert!(!marker.exists());
    let line = sink
        .events
        .iter()
        .filter_map(|event| details(event)["value"].get("fixture").cloned())
        .find(|value| value.is_object())
        .unwrap();
    let value = line;
    assert!(value["args"].as_array().unwrap().contains(&json!(literal)));
    assert_eq!(
        PathBuf::from(value["cwd"].as_str().unwrap()),
        fixture.root.join("share/demo").canonicalize().unwrap()
    );
    assert_eq!(value["bootstrap"]["process_id"], "one");
    assert_eq!(value["bootstrap"]["generation"], 1);
    assert_eq!(value["mode"], 0o600);
    assert_eq!(value["directory_mode"], 0o700);
    assert_eq!(
        value["prefixes"],
        json!([fixture.root.canonicalize().unwrap()])
    );
    assert!(sink.stderr.iter().any(|line| line == "fixture-stderr"));
    assert!(sink
        .stderr
        .iter()
        .any(|line| line == "plain stdout diagnostic"));
    assert!(!sink
        .events
        .iter()
        .any(|event| event.event == LaunchEventKind::Ready));
    assert_reaped(&sink);
    let path = PathBuf::from(value["path"].as_str().unwrap());
    drop(supervisor);
    assert!(!path.exists());
    assert!(!path.parent().unwrap().exists());
}

#[test]
fn dependencies_wait_for_owned_readiness_and_stop_in_reverse_layers() {
    let fixture = Fixture::new();
    let state = Arc::new(Mutex::new(ProbeState::default()));
    let mut factory = Factory(state.clone());
    let a = ready(process("provider", "stubborn"));
    let mut b = ready(process("consumer", "stubborn"));
    b["depends_on"] = json!(["provider"]);
    let mut supervisor = Supervisor::start(fixture.plan(vec![a, b]), Some(&mut factory)).unwrap();
    let mut sink = Sink::default();
    until(&mut supervisor, &mut sink, |sink| {
        stdout_contains(sink, "fixture-ready")
    });
    assert!(started(&sink, "provider"));
    assert!(!started(&sink, "consumer"));
    state.lock().unwrap().allow = true;
    until(&mut supervisor, &mut sink, |sink| {
        sink.events.iter().any(|event| {
            event.event == LaunchEventKind::Ready && event.process_id.as_deref() == Some("consumer")
        })
    });
    supervisor.request_stop(StopReason::Requested);
    drive(&mut supervisor, &mut sink).unwrap();
    let stopping: Vec<_> = sink
        .events
        .iter()
        .filter(|event| event.event == LaunchEventKind::ProcessStopping)
        .map(|event| event.process_id.as_deref().unwrap())
        .collect();
    assert_eq!(stopping, ["consumer", "provider"]);
    assert_eq!(supervisor.summary().outcome, RunOutcome::Stopped);
    let state = state.lock().unwrap();
    assert!(state
        .seen
        .iter()
        .all(|identity| identity.generation == 1 && identity.pid > 0));
    assert_eq!(state.forgotten.len(), 2);
    assert_reaped(&sink);
}

#[test]
fn readiness_timeout_and_invalid_probe_identity_cleanup_every_child() {
    for invalid_identity in [false, true] {
        let fixture = Fixture::new();
        let state = Arc::new(Mutex::new(ProbeState {
            allow: invalid_identity,
            invalid_identity,
            ..Default::default()
        }));
        let mut factory = Factory(state);
        let mut child = ready(process("provider", "stubborn"));
        child["readiness"]["timeout"] = json!(0.12);
        let mut dependent = process("dependent", "echo");
        dependent["depends_on"] = json!(["provider"]);
        let mut supervisor =
            Supervisor::start(fixture.plan(vec![child, dependent]), Some(&mut factory)).unwrap();
        let mut sink = Sink::default();
        drive(&mut supervisor, &mut sink).unwrap();
        assert_eq!(supervisor.summary().outcome, RunOutcome::Failed);
        assert!(!started(&sink, "dependent"));
        assert!(supervisor.summary().cleanup_errors.is_empty());
        assert_reaped(&sink);
    }
}

#[test]
fn restart_policies_count_extra_generations_and_honor_delay() {
    for (mode, policy, generations, outcome) in [
        ("fail", "on_failure", 3, RunOutcome::Failed),
        ("echo", "on_failure", 1, RunOutcome::Completed),
        ("echo", "always", 3, RunOutcome::Failed),
        ("signal", "on_failure", 3, RunOutcome::Failed),
    ] {
        let fixture = Fixture::new();
        let mut child = process("worker", mode);
        child["restart"] = json!({"policy":policy,"delay":0.05,"max_restarts":2});
        let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
        let mut sink = Sink::default();
        drive(&mut supervisor, &mut sink).unwrap();
        let summary = supervisor.summary();
        assert_eq!(summary.outcome, outcome);
        assert_eq!(summary.processes[0].generations, generations);
        assert_eq!(summary.processes[0].restarts, generations - 1);
        let starts: Vec<_> = sink
            .events
            .iter()
            .filter(|event| event.event == LaunchEventKind::ProcessStarted)
            .collect();
        assert!(starts
            .windows(2)
            .all(|pair| pair[1].elapsed_ms - pair[0].elapsed_ms >= 50));
        assert_reaped(&sink);
    }
}

#[test]
fn stopping_during_backoff_cancels_restart() {
    let fixture = Fixture::new();
    let mut child = process("worker", "fail");
    child["restart"] = json!({"policy":"always","delay":1.0,"max_restarts":2});
    let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
    let mut sink = Sink::default();
    until(&mut supervisor, &mut sink, |sink| {
        sink.events
            .iter()
            .any(|event| event.event == LaunchEventKind::RestartScheduled)
    });
    supervisor.request_stop(StopReason::Requested);
    drive(&mut supervisor, &mut sink).unwrap();
    assert_eq!(supervisor.summary().processes[0].generations, 1);
    assert_eq!(supervisor.summary().outcome, RunOutcome::Stopped);
}

#[test]
fn provider_restart_requires_fresh_generation_and_does_not_replay_consumers() {
    use nix::{
        sys::signal::{kill, Signal},
        unistd::Pid,
    };
    let fixture = Fixture::new();
    let state = Arc::new(Mutex::new(ProbeState {
        allow: true,
        ..Default::default()
    }));
    let mut factory = Factory(state.clone());
    let mut provider = ready(process("provider", "stubborn"));
    provider["restart"] = json!({"policy":"on_failure","delay":0.03,"max_restarts":1});
    let mut consumer = process("consumer", "stubborn");
    consumer["depends_on"] = json!(["provider"]);
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![provider, consumer]), Some(&mut factory)).unwrap();
    let mut sink = Sink::default();
    until(&mut supervisor, &mut sink, |sink| {
        sink.events
            .iter()
            .filter(|event| details(event)["value"]["fixture"] == "fixture-ready")
            .count()
            == 2
    });
    let first_pid = sink
        .events
        .iter()
        .find(|event| {
            event.event == LaunchEventKind::ProcessStarted
                && event.process_id.as_deref() == Some("provider")
        })
        .unwrap()
        .pid
        .unwrap();
    state.lock().unwrap().allow = false;
    kill(Pid::from_raw(first_pid as i32), Signal::SIGKILL).unwrap();
    until(&mut supervisor, &mut sink, |sink| {
        sink.events.iter().any(|event| {
            event.event == LaunchEventKind::ProcessStarted
                && event.process_id.as_deref() == Some("provider")
                && event.generation == Some(2)
        })
    });
    assert!(!sink
        .events
        .iter()
        .any(|event| event.event == LaunchEventKind::Ready && event.generation == Some(2)));
    assert_eq!(supervisor.summary().processes[1].generations, 1);
    state.lock().unwrap().allow = true;
    until(&mut supervisor, &mut sink, |sink| {
        sink.events.iter().any(|event| {
            event.event == LaunchEventKind::Ready
                && event.process_id.as_deref() == Some("provider")
                && event.generation == Some(2)
        })
    });
    let second_pid = sink
        .events
        .iter()
        .find(|event| {
            event.event == LaunchEventKind::ProcessStarted
                && event.process_id.as_deref() == Some("provider")
                && event.generation == Some(2)
        })
        .unwrap()
        .pid
        .unwrap();
    assert_ne!(first_pid, second_pid);
    assert!(sink
        .events
        .iter()
        .any(|event| event.event == LaunchEventKind::DependencyUnavailable));
    assert!(sink
        .events
        .iter()
        .any(|event| event.event == LaunchEventKind::DependencyReady
            && details(event)["dependency_generation"] == 2));
    kill(Pid::from_raw(second_pid as i32), Signal::SIGKILL).unwrap();
    drive(&mut supervisor, &mut sink).unwrap();
    assert_eq!(supervisor.summary().outcome, RunOutcome::Failed);
    assert_eq!(supervisor.summary().processes[0].generations, 2);
    assert_eq!(supervisor.summary().processes[1].generations, 1);
    assert_eq!(state.lock().unwrap().closed, 1);
    assert_reaped(&sink);
}

#[test]
fn partial_spawn_failure_cleans_earlier_child_without_retry() {
    let fixture = Fixture::new();
    let second_path = fixture.root.join("lib/demo/second");
    fs::hard_link(fixture.root.join("lib/demo/child"), &second_path).unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(fixture.manifest_path()).unwrap()).unwrap();
    manifest["entries"]["second"] = json!({"executable":"lib/demo/second","kind":"aos_node"});
    fixture.manifest(manifest);
    let first = process("first", "stubborn");
    let mut second = process("second", "echo");
    second["entry"] = json!("second");
    second["restart"] = json!({"policy":"always","delay":0.0,"max_restarts":3});
    let plan = fixture.plan(vec![first, second]);
    fs::remove_file(second_path).unwrap();
    let mut supervisor = Supervisor::start(plan, None).unwrap();
    let mut sink = Sink::default();
    drive(&mut supervisor, &mut sink).unwrap();
    let summary = supervisor.summary();
    assert_eq!(summary.outcome, RunOutcome::Failed);
    assert_eq!(summary.processes[0].generations, 1);
    assert_eq!(summary.processes[1].generations, 0);
    assert!(matches!(
        summary.stop_reason,
        Some(StopReason::Failure(LaunchError {
            code: LaunchErrorCode::SpawnFailed,
            ..
        }))
    ));
    assert_reaped(&sink);
}

#[test]
fn leader_natural_exit_cleans_stubborn_grandchild_before_reap() {
    let fixture = Fixture::new();
    let marker = fixture.marker("grandchild-pid");
    let mut child = process("worker", "grandchild");
    child["env"]["AOS_TEST_MARKER"] = json!(marker);
    let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
    let mut sink = Sink::default();
    drive(&mut supervisor, &mut sink).unwrap();
    assert_eq!(
        supervisor.summary().outcome,
        RunOutcome::Completed,
        "{:?}",
        supervisor.summary()
    );
    let pid: i32 = fs::read_to_string(marker).unwrap().parse().unwrap();
    assert_process_not_live(pid);
    assert_reaped(&sink);
}

#[test]
fn shared_shutdown_deadline_is_not_multiplied_by_layer_or_child_count() {
    let fixture = Fixture::new();
    let state = Arc::new(Mutex::new(ProbeState {
        allow: true,
        ..Default::default()
    }));
    let mut factory = Factory(state);
    let processes = (0..5)
        .map(|layer| {
            let mut child = ready(process(&format!("p{layer}"), "stubborn"));
            if layer > 0 {
                child["depends_on"] = json!([format!("p{}", layer - 1)]);
            }
            child
        })
        .collect();
    let mut supervisor = Supervisor::start(fixture.plan(processes), Some(&mut factory)).unwrap();
    let mut sink = Sink::default();
    until(&mut supervisor, &mut sink, |sink| {
        sink.events
            .iter()
            .filter(|event| {
                event.event == LaunchEventKind::ProcessStdout
                    && details(event)["value"]["fixture"].as_str() == Some("fixture-ready")
            })
            .count()
            == 5
    });
    let started_at = Instant::now();
    supervisor.request_stop(StopReason::Requested);
    drive(&mut supervisor, &mut sink).unwrap();
    assert!(started_at.elapsed() < Duration::from_millis(700));
    assert_eq!(
        supervisor.summary().outcome,
        RunOutcome::Stopped,
        "{:?}",
        supervisor.summary()
    );
    assert!(sink.events.iter().any(
        |event| event.event == LaunchEventKind::ProcessSignal && details(event)["signal"] == 9
    ));
    assert_reaped(&sink);
}

#[test]
fn output_flood_is_bounded_and_sink_failure_still_cleans() {
    for broken in [false, true] {
        let fixture = Fixture::new();
        let mut supervisor =
            Supervisor::start(fixture.plan(vec![process("writer", "flood")]), None).unwrap();
        let mut sink = Sink::default();
        until(&mut supervisor, &mut sink, |sink| {
            sink.events
                .iter()
                .any(|event| event.event == LaunchEventKind::OutputTruncated)
        });
        let starts = sink.events.clone();
        let births = sink.births.clone();
        if broken {
            sink.fail = true;
        } else {
            supervisor.request_stop(StopReason::Requested);
        }
        let start = Instant::now();
        let result = drive(&mut supervisor, &mut sink);
        assert!(start.elapsed() < Duration::from_secs(2));
        if broken {
            assert_eq!(result.unwrap_err().code, LaunchErrorCode::OutputIoError);
            assert_eq!(supervisor.summary().outcome, RunOutcome::Failed);
        } else {
            result.unwrap();
            assert_eq!(supervisor.summary().outcome, RunOutcome::Stopped);
        }
        assert_reaped(&Sink {
            events: starts,
            births,
            ..Default::default()
        });
    }
}

#[test]
fn block_buffered_short_json_lines_are_lossless_and_ordered() {
    #[derive(Default)]
    struct OrderedSink {
        sequences: Vec<u64>,
        dropped: u64,
        diagnostics: usize,
    }
    impl EventSink for OrderedSink {
        fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
            if event.event == LaunchEventKind::ProcessStdout {
                self.sequences
                    .push(details(event)["value"]["sequence"].as_u64().unwrap());
            }
            if event.event == LaunchEventKind::OutputDropped {
                self.dropped += details(event)["lines"].as_u64().unwrap();
            }
            Ok(())
        }
        fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
            if record.stream == OutputStream::Stderr && record.text.starts_with("diagnostic ") {
                self.diagnostics += 1;
            }
            Ok(())
        }
    }
    let fixture = Fixture::new();
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![process("writer", "short-lines")]), None).unwrap();
    let mut sink = OrderedSink::default();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if supervisor
            .poll(Duration::from_millis(1), &mut sink)
            .unwrap()
            == RunProgress::Finished
        {
            break;
        }
        assert!(Instant::now() < deadline, "{:?}", supervisor.summary());
    }
    assert_eq!(supervisor.summary().outcome, RunOutcome::Completed);
    assert_eq!(sink.sequences, (0..5000).collect::<Vec<_>>());
    assert_eq!(sink.diagnostics, 5000);
    assert_eq!(sink.dropped, 0);
}

fn check_full_pipe_short_lines(process_count: usize) {
    const LINES: usize = 65_536;
    #[derive(Default)]
    struct CountingSink {
        accepted: Sink,
        lines: BTreeMap<String, usize>,
        dropped: u64,
    }
    impl EventSink for CountingSink {
        fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
            if event.event == LaunchEventKind::OutputDropped {
                self.dropped += details(event)["lines"].as_u64().unwrap();
            }
            self.accepted.emit(event)
        }
        fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
            if record.text == "short-lines-begin" {
                assert!(self
                    .lines
                    .insert(record.identity.process_id.clone(), 0)
                    .is_none());
            } else if record.text.is_empty() {
                if let Some(lines) = self.lines.get_mut(&record.identity.process_id) {
                    *lines += 1;
                }
            }
            Ok(())
        }
    }
    let fixture = Fixture::new();
    let processes = (0..process_count)
        .map(|number| {
            let mut child = process(&format!("writer{number}"), "full-pipe-short-lines");
            child["env"]["AOS_TEST_SHORT_STREAM"] =
                json!(if number % 2 == 0 { "stdout" } else { "stderr" });
            child
        })
        .collect();
    let mut supervisor = Supervisor::start(fixture.plan(processes), None).unwrap();
    let mut sink = CountingSink::default();
    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs(45);
    loop {
        if supervisor
            .poll(Duration::from_millis(10), &mut sink)
            .unwrap()
            == RunProgress::Finished
        {
            break;
        }
        assert!(Instant::now() < deadline, "{:?}", supervisor.summary());
    }
    let summary = supervisor.summary();
    assert_eq!(summary.outcome, RunOutcome::Completed, "{summary:?}");
    assert!(summary.cleanup_errors.is_empty(), "{summary:?}");
    assert_eq!(sink.lines.len(), process_count);
    for (id, count) in &sink.lines {
        assert_eq!(*count, LINES, "{id} lost short output lines");
    }
    assert_eq!(sink.dropped, 0);
    assert_reaped(&sink.accepted);
    println!(
        "delivered {} short lines from {process_count} processes in {:?}",
        LINES * process_count,
        started_at.elapsed()
    );
}

#[test]
fn full_pipe_short_lines_survive_reap_at_cli_poll_interval() {
    check_full_pipe_short_lines(1);
}

#[test]
fn many_full_pipes_preserve_short_lines_across_output_rotation() {
    check_full_pipe_short_lines(64);
}

#[test]
fn escaped_writer_after_leader_exit_or_signal_has_bounded_cleanup() {
    for short_lines in [false, true] {
        for stream in ["stdout", "stderr"] {
            for stopping in [false, true] {
                let fixture = Fixture::new();
                let marker = fixture.marker("escaped-writer");
                let mut child = process("writer", "escaped-parent");
                child["env"]["AOS_TEST_MARKER"] = json!(marker);
                child["env"]["AOS_TEST_ESCAPED_STREAM"] = json!(stream);
                child["env"]["AOS_TEST_ESCAPED_SHORT"] = json!(if short_lines { "1" } else { "0" });
                child["env"]["AOS_TEST_ESCAPED_HOLD"] = json!(if stopping { "1" } else { "0" });
                child["restart"] = json!({"policy":"on_failure","delay":0,"max_restarts":2});
                let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
                let mut sink = Sink::default();
                until(&mut supervisor, &mut sink, |_| marker.exists());
                let escaped: i32 = fs::read_to_string(&marker).unwrap().parse().unwrap();
                let leader = sink
                    .events
                    .iter()
                    .find(|event| event.event == LaunchEventKind::ProcessStarted)
                    .unwrap();
                assert_ne!(escaped, leader.pgid.unwrap());
                assert_eq!(
                    nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(escaped)))
                        .unwrap()
                        .as_raw(),
                    escaped,
                    "fixture must really escape with setsid"
                );
                if stopping {
                    supervisor.request_stop(StopReason::Signal(nix::libc::SIGTERM));
                }
                let started_at = Instant::now();
                drive(&mut supervisor, &mut sink).unwrap();
                assert!(
                    started_at.elapsed() < Duration::from_secs(if short_lines { 6 } else { 4 })
                );
                let summary = supervisor.summary();
                assert_eq!(summary.outcome, RunOutcome::Failed, "{summary:?}");
                assert!(
                    summary.cleanup_errors.iter().any(|error| {
                        error.code == LaunchErrorCode::CleanupFailed
                            && (error.stage == "cleanup"
                                || (error.stage == "output_drain"
                                    && error.message.contains("new bytes after leader reap")))
                    }),
                    "{summary:?}"
                );
                assert_eq!(summary.processes[0].generations, 1);
                assert!(!sink
                    .events
                    .iter()
                    .any(|event| { event.event == LaunchEventKind::RestartScheduled }));
                // The supervisor has no signal authority over this escaped group.
                // The fixture exits on EPIPE after its pipe's read end is closed.
                let deadline = Instant::now() + Duration::from_secs(3);
                while !fixture.marker("escaped-writer-finished").exists() {
                    assert!(
                        Instant::now() < deadline,
                        "escaped fixture did not observe EPIPE"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                assert_reaped(&sink);
            }
        }
    }
}

#[test]
fn slow_escaped_writer_cannot_extend_reaped_pipe_cleanup() {
    for stream in ["stdout", "stderr"] {
        for failed_stop in [false, true] {
            let fixture = Fixture::new();
            let marker = fixture.marker("escaped-writer");
            let mut child = process("writer", "escaped-parent");
            child["env"]["AOS_TEST_MARKER"] = json!(marker);
            child["env"]["AOS_TEST_ESCAPED_STREAM"] = json!(stream);
            child["env"]["AOS_TEST_ESCAPED_SLOW"] = json!("1");
            child["env"]["AOS_TEST_ESCAPED_HOLD"] = json!(if failed_stop { "1" } else { "0" });
            child["restart"] = json!({"policy":"on_failure","delay":0,"max_restarts":2});
            let mut supervisor = Supervisor::start(fixture.plan(vec![child]), None).unwrap();
            let mut sink = Sink::default();
            until(&mut supervisor, &mut sink, |_| marker.exists());
            let escaped: i32 = fs::read_to_string(&marker).unwrap().parse().unwrap();
            let leader = sink
                .events
                .iter()
                .find(|event| event.event == LaunchEventKind::ProcessStarted)
                .unwrap();
            assert_ne!(escaped, leader.pgid.unwrap());
            assert_eq!(
                nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(escaped)))
                    .unwrap()
                    .as_raw(),
                escaped
            );
            if failed_stop {
                sink.fail = true;
                supervisor.request_stop(StopReason::Signal(nix::libc::SIGTERM));
            }
            let started_at = Instant::now();
            let result = drive(&mut supervisor, &mut sink);
            if failed_stop {
                assert_eq!(result.unwrap_err().code, LaunchErrorCode::OutputIoError);
            } else {
                result.unwrap();
            }
            assert!(started_at.elapsed() < Duration::from_secs(4));
            let summary = supervisor.summary();
            assert_eq!(summary.outcome, RunOutcome::Failed, "{summary:?}");
            assert!(
                summary.cleanup_errors.iter().any(|error| {
                    error.code == LaunchErrorCode::CleanupFailed && error.stage == "cleanup"
                }),
                "{summary:?}"
            );
            assert_eq!(summary.processes[0].generations, 1);
            assert_eq!(summary.processes[0].restarts, 0);
            assert!(!sink
                .events
                .iter()
                .any(|event| event.event == LaunchEventKind::RestartScheduled));
            // This independent group exits on its own EPIPE. The supervisor
            // closes read ends without sending the escaped group any signal.
            let deadline = Instant::now() + Duration::from_secs(3);
            while !fixture.marker("escaped-writer-finished").exists() {
                assert!(
                    Instant::now() < deadline,
                    "escaped writer did not see EPIPE"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            let writes: u32 = fs::read_to_string(fixture.marker("escaped-writer-count"))
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                writes >= 4,
                "the escaped fixture must keep writing: {writes}"
            );
            assert_reaped(&sink);
        }
    }
}

#[test]
fn absolute_stop_output_deadline_bounds_a_sink_with_continuous_slow_progress() {
    struct SlowProgress {
        accepted: Sink,
        next: Instant,
    }
    impl EventSink for SlowProgress {
        fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
            self.accepted.remember_identity(event);
            if Instant::now() < self.next {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.next = Instant::now() + Duration::from_secs(1);
            self.accepted.emit(event)
        }
        fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
            self.accepted.log(record)
        }
    }
    let fixture = Fixture::new();
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![process("writer", "short-lines")]), None).unwrap();
    let mut sink = SlowProgress {
        accepted: Sink::default(),
        next: Instant::now(),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while sink.accepted.stdout_lines == 0 {
        supervisor
            .poll(Duration::from_millis(5), &mut sink)
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    let start = Instant::now();
    supervisor.request_stop(StopReason::Requested);
    let error = loop {
        match supervisor.poll(Duration::from_millis(5), &mut sink) {
            Err(error) => break error,
            Ok(progress) => assert_ne!(progress, RunProgress::Finished),
        }
        assert!(start.elapsed() < Duration::from_secs(36));
    };
    assert!(start.elapsed() >= Duration::from_secs(32));
    assert_eq!(error.code, LaunchErrorCode::OutputIoError);
    assert!(error
        .message
        .contains("absolute shutdown output-drain deadline"));
    assert!(
        supervisor.summary().cleanup_errors.is_empty(),
        "slow output must not be misclassified as failed group ownership"
    );
    assert_reaped(&sink.accepted);
}

#[test]
fn dropping_supervisor_cleans_all_groups_under_one_deadline() {
    let fixture = Fixture::new();
    let mut supervisor = Supervisor::start(
        fixture.plan(
            (0..8)
                .map(|n| process(&format!("p{n}"), "stubborn"))
                .collect(),
        ),
        None,
    )
    .unwrap();
    let mut sink = Sink::default();
    until(&mut supervisor, &mut sink, |sink| {
        sink.events
            .iter()
            .filter(|event| {
                event.event == LaunchEventKind::ProcessStdout
                    && details(event)["value"]["fixture"].as_str() == Some("fixture-ready")
            })
            .count()
            == 8
    });
    let now = Instant::now();
    drop(supervisor);
    assert!(now.elapsed() < Duration::from_secs(1));
    assert_reaped(&sink);
}

fn assert_process_not_live(pid: i32) {
    #[cfg(target_os = "linux")]
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        let state = stat
            .rsplit_once(") ")
            .unwrap()
            .1
            .split_whitespace()
            .next()
            .unwrap();
        assert!(
            matches!(state, "Z" | "X"),
            "pid {pid} remains live: {state}"
        );
    }
    #[cfg(target_os = "macos")]
    {
        use nix::libc;
        // SAFETY: this is the exact output layout and size PIDTBSDINFO requires.
        let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
        let size = std::mem::size_of_val(&info);
        let result = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size as i32,
            )
        };
        if result == size as i32 {
            assert_eq!(info.pbi_status, 5, "grandchild remains alive");
        } else {
            assert_eq!(nix::errno::Errno::last(), nix::errno::Errno::ESRCH);
        }
    }
}

#[test]
fn bounded_backpressure_retains_events_and_final_summary_until_delivery() {
    struct Backpressure {
        until: Instant,
        accepted: Sink,
    }
    impl EventSink for Backpressure {
        fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
            // Capture while the first start event is offered, even if its
            // eventual delivery is delayed until after leader reap.
            self.accepted.remember_identity(event);
            if Instant::now() < self.until {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.accepted.emit(event)
        }
        fn log(&mut self, record: &ProcessLog) -> io::Result<()> {
            self.accepted.log(record)
        }
    }
    let fixture = Fixture::new();
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![process("one", "echo")]), None).unwrap();
    let mut sink = Backpressure {
        // The leader exits promptly. A slow sink must not turn its remaining
        // pipe bytes into a process-group cleanup failure after two seconds.
        until: Instant::now() + Duration::from_millis(2500),
        accepted: Sink::default(),
    };
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let progress = supervisor
            .poll(Duration::from_millis(5), &mut sink)
            .unwrap();
        if progress == RunProgress::Finished {
            break;
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(supervisor.summary().outcome, RunOutcome::Completed);
    assert!(started(&sink.accepted, "one"));
    assert_eq!(
        sink.accepted.events.last().unwrap().event,
        LaunchEventKind::LaunchFinished
    );
    assert_reaped(&sink.accepted);
}

#[test]
fn diagnostic_sink_failure_is_counted_without_stopping_the_process() {
    struct NoDiagnostics(Sink);
    impl EventSink for NoDiagnostics {
        fn emit(&mut self, event: &LaunchEvent) -> io::Result<()> {
            self.0.emit(event)
        }
        fn log(&mut self, _: &ProcessLog) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
    let fixture = Fixture::new();
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![process("one", "echo")]), None).unwrap();
    let mut sink = NoDiagnostics(Sink::default());
    let deadline = Instant::now() + Duration::from_secs(4);
    while supervisor
        .poll(Duration::from_millis(5), &mut sink)
        .unwrap()
        != RunProgress::Finished
    {
        assert!(Instant::now() < deadline);
    }
    assert_eq!(supervisor.summary().outcome, RunOutcome::Completed);
    assert!(sink
        .0
        .events
        .iter()
        .any(|event| event.event == LaunchEventKind::OutputDropped));
    assert_reaped(&sink.0);
}

#[test]
fn raw_json_numbers_survive_the_process_pipe_and_event_serialization() {
    let fixture = Fixture::new();
    let mut supervisor =
        Supervisor::start(fixture.plan(vec![process("one", "numeric")]), None).unwrap();
    let mut sink = Sink::default();
    drive(&mut supervisor, &mut sink).unwrap();
    let event = sink
        .events
        .iter()
        .find(|event| event.event == LaunchEventKind::ProcessStdout)
        .unwrap();
    let serialized = serde_json::to_string(event).unwrap();
    assert!(serialized.contains(
        r#""value":{"id":12345678901234567890123,"v":1.0000000000000000001,"huge":1e400}"#
    ));
    assert!(sink
        .stderr
        .iter()
        .any(|line| line == r#"{"dup":1,"dup":2}"#));
    assert_reaped(&sink);
}

#[test]
fn preflight_rejects_unbounded_times_and_fixes_relative_environment_paths() {
    let fixture = Fixture::new();
    for time in [86_400.1, 2e16] {
        let mut child = process("one", "echo");
        child["restart"] = json!({"delay":time});
        assert!(prepare(
            &fixture.index(),
            fixture.spec(vec![child]),
            LaunchOverrides::default()
        )
        .is_err());
    }
    let mut spec = fixture.spec(vec![process("one", "echo")]);
    spec.shutdown.sigint_timeout = 50_000.0;
    spec.shutdown.sigterm_timeout = 50_000.0;
    assert!(prepare(&fixture.index(), spec, LaunchOverrides::default()).is_err());
    let config = fixture.marker("session.json");
    fs::write(&config, "{}").unwrap();
    let mut relative = PathBuf::new();
    for _ in std::env::current_dir().unwrap().components().skip(1) {
        relative.push("..");
    }
    relative.push(config.strip_prefix("/").unwrap());
    let mut child = process("one", "echo");
    child["env"]["AOS_ZENOH_CONFIG"] = json!(relative);
    let plan = fixture.plan(vec![child]);
    assert_eq!(
        PathBuf::from(&plan.processes()[0].spec.env["AOS_ZENOH_CONFIG"]),
        config.canonicalize().unwrap()
    );
}

#[test]
fn shared_null_context_overrides_child_environment_in_the_core_consumer() {
    let fixture = Fixture::new();
    let network = fixture.marker("different-network.json5");
    fs::write(&network, "{}").unwrap();
    let mut child = ready(process("one", "echo"));
    child["env"]["AOS_DOMAIN_ID"] = json!("999");
    child["env"]["AOS_ZENOH_CONFIG"] = json!(network);
    let plan = fixture.plan(vec![child]);
    let supervisor = plan.context().unwrap();
    let process = &plan.processes()[0];
    let mut wire = process.bootstrap.clone();
    wire.launch_id = uuid::Uuid::nil().to_string();
    wire.generation = 1;
    let bootstrap =
        aos_core::ProcessBootstrap::from_json(&serde_json::to_string(&wire).unwrap()).unwrap();
    let child_request = bootstrap
        .context_request(aos_core::ContextConfigRequest {
            environment: process
                .spec
                .env
                .iter()
                .filter(|(key, _)| matches!(key.as_str(), "AOS_DOMAIN_ID" | "AOS_ZENOH_CONFIG"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            ..Default::default()
        })
        .unwrap();
    let actual = aos_core::resolve_context_config(&child_request).unwrap();
    assert_eq!(actual.domain_id, supervisor.domain_id);
    assert_eq!(
        actual.zenoh_config.as_ref().map(PathBuf::from),
        supervisor.zenoh_config
    );
    assert_eq!(actual.zenoh_config, None);
    assert_eq!(actual.effective_value()["zenoh_config"]["source"], "launch");
}

#[test]
fn interface_environment_accepts_files_and_directories() {
    let fixture = Fixture::new();
    let interface = fixture.marker("Command.json");
    fs::write(&interface, "{}").unwrap();
    let mut child = process("one", "echo");
    child["env"]["AOS_INTERFACE_PATH"] = json!(std::env::join_paths([&interface, &fixture.root])
        .unwrap()
        .to_str()
        .unwrap());
    let plan = fixture.plan(vec![child]);
    let paths: Vec<_> =
        std::env::split_paths(&plan.processes()[0].spec.env["AOS_INTERFACE_PATH"]).collect();
    assert_eq!(
        paths,
        vec![
            interface.canonicalize().unwrap(),
            fixture.root.canonicalize().unwrap()
        ]
    );
}

#[test]
fn prefix_selection_preserves_explicit_and_deployed_isolation() {
    use std::ffi::OsStr;
    let base = std::env::current_dir().unwrap();
    let install = base.join("venv");
    let explicit = vec![PathBuf::from("only")];
    assert_eq!(
        resolve_package_prefixes(
            Some(&explicit),
            Some(OsStr::new("env")),
            Some(&install),
            &base,
            false
        )
        .unwrap(),
        vec![base.join("only")]
    );
    assert_eq!(
        resolve_package_prefixes(None, Some(OsStr::new("env")), Some(&install), &base, true)
            .unwrap(),
        vec![base.join("env")]
    );
    assert_eq!(
        resolve_package_prefixes(None, Some(OsStr::new("")), Some(&install), &base, false).unwrap(),
        vec![install]
    );
    assert!(
        resolve_package_prefixes(None, Some(OsStr::new("one::two")), None, &base, false).is_err()
    );
}

#[test]
#[ignore = "child fixture launched only by supervisor integration tests"]
fn fixture_process() {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::env::var("AOS_TEST_MODE").expect("fixture mode");
    if mode == "escaped-writer" {
        let marker = PathBuf::from(std::env::var("AOS_TEST_MARKER").unwrap());
        fs::write(&marker, std::process::id().to_string()).unwrap();
        let slow = std::env::var("AOS_TEST_ESCAPED_SLOW").as_deref() == Ok("1");
        let line = if std::env::var("AOS_TEST_ESCAPED_SHORT").as_deref() == Ok("1") {
            "\n".repeat(256)
        } else {
            format!(
                "{{\"escaped\":\"{}\"}}\n",
                if slow {
                    "slow".into()
                } else {
                    "x".repeat(4000)
                }
            )
        };
        let stream = std::env::var("AOS_TEST_ESCAPED_STREAM").unwrap();
        let mut output: Box<dyn Write> = if stream == "stdout" {
            Box::new(io::stdout())
        } else {
            Box::new(io::stderr())
        };
        let mut writes = 0;
        while output.write_all(line.as_bytes()).is_ok() {
            if slow {
                writes += 1;
                fs::write(
                    marker.with_file_name("escaped-writer-count"),
                    writes.to_string(),
                )
                .unwrap();
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        fs::write(marker.with_file_name("escaped-writer-finished"), "EPIPE").unwrap();
        std::process::exit(0);
    }
    if mode == "escaped-parent" {
        use std::os::unix::process::CommandExt;
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "fixture_process", "--ignored", "--nocapture"])
            .env("AOS_TEST_MODE", "escaped-writer");
        // SAFETY: setsid is async-signal-safe; only the fixture child runs this
        // pre-exec closure, with no allocation or shared lock access.
        unsafe {
            command.pre_exec(|| {
                if nix::libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let _escaped = command.spawn().unwrap();
        let marker = PathBuf::from(std::env::var("AOS_TEST_MARKER").unwrap());
        let deadline = Instant::now() + Duration::from_secs(3);
        while !marker.exists() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        if std::env::var("AOS_TEST_ESCAPED_HOLD").unwrap() == "1" {
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        std::process::exit(7);
    }
    if mode == "short-lines" {
        let stdout: String = (0..5000)
            .map(|sequence| format!("{{\"sequence\":{sequence}}}\n"))
            .collect();
        let stderr: String = (0..5000)
            .map(|sequence| format!("diagnostic {sequence}\n"))
            .collect();
        io::stdout().write_all(stdout.as_bytes()).unwrap();
        io::stderr().write_all(stderr.as_bytes()).unwrap();
        std::process::exit(0);
    }
    if mode == "full-pipe-short-lines" {
        let mut output: Box<dyn Write> =
            if std::env::var("AOS_TEST_SHORT_STREAM").as_deref() == Ok("stderr") {
                Box::new(io::stderr())
            } else {
                Box::new(io::stdout())
            };
        output.write_all(b"short-lines-begin\n").unwrap();
        output.write_all(&vec![b'\n'; 65_536]).unwrap();
        std::process::exit(0);
    }
    if mode == "numeric" {
        println!(r#"{{"id":12345678901234567890123,"v":1.0000000000000000001,"huge":1e400}}"#);
        println!(r#"{{"dup":1,"dup":2}}"#);
        std::process::exit(0);
    }
    if matches!(mode.as_str(), "stubborn" | "flood") {
        // SAFETY: this single-purpose child test process intentionally ignores
        // these two signals; no parent test process signal state is changed.
        unsafe {
            nix::sys::signal::signal(
                nix::sys::signal::Signal::SIGINT,
                nix::sys::signal::SigHandler::SigIgn,
            )
            .unwrap();
            nix::sys::signal::signal(
                nix::sys::signal::Signal::SIGTERM,
                nix::sys::signal::SigHandler::SigIgn,
            )
            .unwrap();
        }
        println!("{}", json!({"fixture":"fixture-ready"}));
        io::stdout().flush().unwrap();
        if let Ok(marker) = std::env::var("AOS_TEST_MARKER") {
            fs::write(marker, std::process::id().to_string()).unwrap();
        }
        if mode == "flood" {
            let oversized = vec![b'x'; MAX_OUTPUT_LINE_BYTES + 5000];
            io::stdout().write_all(&oversized).unwrap();
            println!();
            loop {
                io::stdout().write_all(b"{\"line\":\"flood\"}\n").unwrap();
            }
        }
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    if mode == "grandchild" {
        let marker = std::env::var("AOS_TEST_MARKER").unwrap();
        let _child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fixture_process", "--ignored", "--nocapture"])
            .env("AOS_TEST_MODE", "stubborn")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while !std::path::Path::new(&marker).exists() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        std::process::exit(0);
    }
    if mode == "signal" {
        nix::sys::signal::raise(nix::sys::signal::Signal::SIGTERM).unwrap();
        unreachable!();
    }
    if mode == "fail" {
        std::process::exit(7);
    }
    if mode == "marker" {
        fs::write(std::env::var("AOS_TEST_MARKER").unwrap(), "started").unwrap();
    }
    let path = PathBuf::from(std::env::var(PROCESS_CONFIG_ENV).unwrap());
    let bootstrap: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    println!(
        "{}",
        json!({"fixture":{
            "args":std::env::args().collect::<Vec<_>>(),"cwd":std::env::current_dir().unwrap(),
            "bootstrap":bootstrap,"path":path,"mode":fs::metadata(&path).unwrap().permissions().mode()&0o777,
            "directory_mode":fs::metadata(path.parent().unwrap()).unwrap().permissions().mode()&0o777,
            "prefixes":std::env::split_paths(&std::env::var_os(PREFIX_PATH_ENV).unwrap()).collect::<Vec<_>>()
        }})
    );
    println!("plain stdout diagnostic");
    eprintln!("fixture-stderr");
    std::process::exit(0);
}
