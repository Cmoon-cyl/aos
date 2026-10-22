#[path = "../src/hooks.rs"]
mod hooks;

use std::sync::atomic::AtomicBool;
#[cfg(any(unix, windows))]
use std::sync::atomic::Ordering;
#[cfg(any(unix, windows))]
use std::time::Duration;
use std::time::Instant;

use hooks::{Condition, Decision, HookErrorKind, HookRunner};
use serde_json::{json, Value};

fn envelope(message: Value) -> Value {
    json!({
        "message":message,
        "topic":"/sensors/state",
        "sequence":17,
        "received_at":{"time_domain":"steady","time_ns":123456789}
    })
}

fn matches(condition: Value, message: Value) -> bool {
    Condition::parse(&condition)
        .unwrap()
        .evaluate(&message)
        .matched
}

#[test]
fn condition_pointers_preserve_null_missing_empty_keys_and_array_rules() {
    let message = json!({
        "present":null,
        "a/b":{"~key":[4,9]},
        "":{"value":true},
        "01":"literal"
    });
    for path in ["/present", "/a~1b/~0key/1", "//value", "/01", ""] {
        assert!(matches(json!({"path":path,"op":"exists"}), message.clone()));
    }
    for path in [
        "/missing",
        "/a~1b/~0key/01",
        "/a~1b/~0key/-",
        "/a~1b/~0key/2",
    ] {
        let exists = Condition::parse(&json!({"path":path,"op":"exists"}))
            .unwrap()
            .evaluate(&message);
        assert!(!exists.matched);
        assert_eq!(exists.missing_fields, 0);
    }
    assert!(matches(
        json!({"path":"/present","op":"eq","value":null}),
        message.clone()
    ));
    for operation in ["eq", "ne", "gt", "gte", "lt", "lte"] {
        let condition =
            Condition::parse(&json!({"path":"/missing","op":operation,"value":0})).unwrap();
        let result = condition.evaluate(&message);
        assert!(!result.matched, "{operation}");
        assert_eq!(result.missing_fields, 1);
    }
    assert!(matches(
        json!({"path":"","op":"eq","value":null}),
        Value::Null
    ));
    assert!(matches(
        json!({"path":"/a~1b/~0key/1","op":"eq","value":9}),
        message
    ));
}

#[test]
fn conditions_reject_coercions_including_ne_and_validate_numeric_operators() {
    for (actual, expected) in [
        (json!("1"), json!(1)),
        (json!(true), json!(1)),
        (json!(false), json!(0)),
        (Value::Null, json!("null")),
        (json!([]), Value::Null),
        (json!({}), json!(false)),
    ] {
        for operation in ["eq", "ne"] {
            assert!(!matches(
                json!({"path":"","op":operation,"value":expected}),
                actual.clone(),
            ));
        }
    }
    assert!(matches(
        json!({"path":"","op":"ne","value":"b"}),
        json!("a")
    ));
    assert!(matches(
        json!({"path":"","op":"ne","value":false}),
        json!(true)
    ));
    assert!(Condition::parse(&json!({"path":"","op":"gt","value":"a"})).is_err());
    assert!(Condition::parse(&json!({"path":"","op":"lte","value":false})).is_err());
}

#[test]
fn numeric_comparisons_keep_integer_precision_and_mixed_number_ordering() {
    let cases = [
        ("9007199254740993", "9007199254740992", "gt", true),
        ("9007199254740993", "9007199254740992.0", "eq", false),
        ("9007199254740993", "9007199254740992.0", "gt", true),
        ("9007199254740992.0", "9007199254740993", "lt", true),
        ("18446744073709551615", "18446744073709551616.0", "lt", true),
        ("18446744073709551616.0", "18446744073709551615", "gt", true),
        ("9223372036854775807", "9223372036854775808", "lt", true),
        ("-9223372036854775808", "-9223372036854775808.0", "eq", true),
        ("-9223372036854775808", "-1e30", "gt", true),
        ("18446744073709551615", "1e30", "lt", true),
        ("1", "1.0", "eq", true),
        ("0", "-0.0", "eq", true),
        ("1", "1.5", "lt", true),
        ("-1", "-1.5", "gt", true),
        ("1.5", "1", "gte", true),
        ("-1.5", "-1", "lte", true),
        ("1.25", "1.5", "lt", true),
        ("2", "2", "ne", false),
        ("2", "2", "gte", true),
        ("2", "2", "lte", true),
    ];
    for (actual, expected, operation, result) in cases {
        assert_eq!(
            matches(
                json!({"path":"","op":operation,"value":serde_json::from_str::<Value>(expected).unwrap()}),
                serde_json::from_str(actual).unwrap()
            ),
            result,
            "{actual} {operation} {expected}"
        );
    }
}

#[test]
fn all_any_visit_every_leaf_for_missing_field_diagnostics() {
    let condition = Condition::parse(&json!({
        "any":[
            {"path":"/ready","op":"eq","value":true},
            {"all":[
                {"path":"/missing_a","op":"eq","value":null},
                {"path":"/missing_b","op":"ne","value":3},
                {"path":"/missing_c","op":"exists"}
            ]}
        ]
    }))
    .unwrap();
    let result = condition.evaluate(&json!({"ready":true}));
    assert!(result.matched);
    assert_eq!(result.missing_fields, 2);
}

#[test]
fn invalid_condition_and_hook_specs_fail_before_execution() {
    for invalid in [
        json!({"all":[]}),
        json!({"any":[]}),
        json!({"all":[{"path":"","op":"exists"}],"any":[]}),
        json!({"path":"field","op":"exists"}),
        json!({"path":"/field~","op":"exists"}),
        json!({"path":"/field~2","op":"exists"}),
        json!({"path":"","op":"exists","value":true}),
        json!({"path":"","op":"eq"}),
        json!({"path":"","op":"eq","value":[]}),
        json!({"path":"","op":"eval","value":true}),
        json!({"path":"","op":"eq","value":true,"extra":true}),
    ] {
        assert_eq!(
            Condition::parse(&invalid).unwrap_err().kind,
            HookErrorKind::InvalidArgument
        );
    }
    for invalid in [
        json!({"kind":"condition","condition":{"path":"","op":"exists"},"consecutive_matches":true}),
        json!({"kind":"condition","condition":{"path":"","op":"exists"},"consecutive_matches":0}),
        json!({"kind":"script","executable":""}),
        json!({"kind":"script","executable":"\0"}),
        json!({"kind":"script","executable":"unused","argv":"arg"}),
        json!({"kind":"script","executable":"unused","argv":[null]}),
        json!({"kind":"script","executable":"unused","timeout":true}),
        json!({"kind":"script","executable":"unused","timeout":0}),
        json!({"kind":"script","executable":"unused","timeout":1e300}),
        json!({"kind":"script","executable":"unused","max_output_bytes":false}),
        json!({"kind":"script","executable":"unused","max_output_bytes":0}),
        json!({"kind":"script","executable":"unused","shell":true}),
    ] {
        assert_eq!(
            HookRunner::from_json(&invalid).unwrap_err().kind,
            HookErrorKind::InvalidArgument
        );
    }
}

#[test]
fn condition_hook_resets_streak_on_each_miss_and_receives_the_raw_message() {
    let mut hook = HookRunner::from_json(&json!({
        "kind":"condition","condition":{"path":"/hidden","op":"gte","value":3},
        "consecutive_matches":3
    }))
    .unwrap();
    for (index, value) in [3, 4, 2, 3, 4, 5].into_iter().enumerate() {
        let outcome = hook
            .evaluate(
                &envelope(json!({"visible":"shown","hidden":value})),
                &AtomicBool::new(false),
                None,
            )
            .unwrap();
        assert_eq!(outcome.consecutive_matches, [1, 2, 0, 1, 2, 3][index]);
        assert_eq!(
            outcome.decision,
            if index == 5 {
                Decision::Matched
            } else {
                Decision::Continue
            }
        );
    }
    let outcome = hook
        .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
        .unwrap();
    assert_eq!(outcome.consecutive_matches, 0);
    assert_eq!(outcome.missing_fields, 1);
    assert!(hook
        .evaluate(
            &json!({"topic":"/missing_message"}),
            &AtomicBool::new(false),
            None
        )
        .is_err());
}

#[test]
fn condition_hook_obeys_cancellation_and_already_elapsed_observation_deadlines() {
    let mut hook = HookRunner::from_json(&json!({
        "kind":"condition","condition":{"path":"","op":"exists"}
    }))
    .unwrap();
    let input = envelope(json!({"ready":true}));
    assert_eq!(
        hook.evaluate(&input, &AtomicBool::new(true), None)
            .unwrap_err()
            .kind,
        HookErrorKind::Interrupted
    );
    assert_eq!(
        hook.evaluate(&input, &AtomicBool::new(false), Some(Instant::now()))
            .unwrap_err()
            .kind,
        HookErrorKind::OverallDeadline
    );
}

#[cfg(not(any(unix, windows)))]
#[test]
fn selected_interpreter_keeps_unsupported_platform_contract() {
    let error = hooks::run_json_command(
        "unused-interpreter",
        Vec::new(),
        &json!({}),
        &AtomicBool::new(false),
        Instant::now() + std::time::Duration::from_secs(1),
    )
    .unwrap_err();
    assert_eq!(error.kind, HookErrorKind::UnsupportedPlatform);
    assert_eq!(error.code(), "HOOK_UNSUPPORTED_PLATFORM");
}

#[cfg(any(unix, windows))]
mod subprocess {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Mutex, OnceLock, Weak};
    use std::thread;

    use super::*;

    struct Fixture {
        directory: PathBuf,
        executable: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    impl Fixture {
        fn file(&self, label: &str) -> PathBuf {
            self.directory
                .join(format!("{label}-{}", uuid::Uuid::new_v4()))
        }

        fn runner(
            &self,
            mode: &str,
            arguments: &[String],
            timeout: f64,
            limit: usize,
        ) -> HookRunner {
            let mut argv = vec![mode.to_owned()];
            argv.extend_from_slice(arguments);
            HookRunner::from_json(&json!({
                "kind":"script","executable":self.executable,"argv":argv,
                "timeout":timeout,"max_output_bytes":limit
            }))
            .unwrap()
        }
    }

    fn fixture() -> Arc<Fixture> {
        static CACHE: OnceLock<Mutex<Weak<Fixture>>> = OnceLock::new();
        let mut cached = CACHE
            .get_or_init(|| Mutex::new(Weak::new()))
            .lock()
            .unwrap();
        if let Some(fixture) = cached.upgrade() {
            return fixture;
        }
        let directory = std::env::temp_dir().join(format!(
            "aos hook fixture {} {}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&directory).unwrap();
        let source = directory.join("fixture.rs");
        let executable = directory.join(if cfg!(windows) {
            "hook fixture.exe"
        } else {
            "hook fixture"
        });
        fs::write(&source, FIXTURE_SOURCE).unwrap();
        let result = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .arg("--edition=2021")
            .arg(&source)
            .arg("-o")
            .arg(&executable)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        // Exercise the freshly linked helper before starting timing-sensitive
        // hook tests. macOS may validate a new binary on its first execution.
        let ready = Command::new(&executable).arg("ready").output().unwrap();
        assert!(ready.status.success(), "fixture failed to start");
        let fixture = Arc::new(Fixture {
            directory,
            executable,
        });
        *cached = Arc::downgrade(&fixture);
        fixture
    }

    fn path_string(path: &Path) -> String {
        path.to_str().unwrap().into()
    }

    #[test]
    fn selected_interpreter_capture_keeps_json_contract_and_bounded_cleanup() {
        let fixture = fixture();
        let cancel = AtomicBool::new(false);
        let report = hooks::run_json_command(
            &path_string(&fixture.executable),
            vec![
                "output".into(),
                r#"{"status":"ok","native":{"available":true}}"#.into(),
            ],
            &json!({}),
            &cancel,
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(report["native"]["available"], true);
        let pids = fixture.file("doctor-timeout-pid");
        let error = hooks::run_json_command(
            &path_string(&fixture.executable),
            vec!["sleep".into(), path_string(&pids)],
            &json!({}),
            &cancel,
            Instant::now() + Duration::from_millis(150),
        )
        .unwrap_err();
        assert_eq!(error.kind, HookErrorKind::OverallDeadline);
        assert_gone(&read_pids(&pids));
    }

    fn read_pids(path: &Path) -> Vec<i32> {
        fs::read_to_string(path)
            .unwrap_or_default()
            .split_whitespace()
            .map(|value| value.parse().unwrap())
            .collect()
    }

    fn assert_gone(pids: &[i32]) {
        assert!(!pids.is_empty(), "fixture did not record its process IDs");
        for pid in pids {
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                // SAFETY: signal 0 only inspects the fixture PID; it sends no signal.
                #[cfg(unix)]
                let gone = unsafe { libc::kill(*pid, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
                #[cfg(windows)]
                let gone = !aos_launch::windows_process::process_is_running(*pid as u32).unwrap();
                if gone {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "hook process {pid} survived cleanup"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[test]
    fn script_receives_exact_envelope_and_literal_argv_without_shell() {
        let fixture = fixture();
        let captured = fixture.file("input");
        let arguments = fixture.file("argument");
        let marker = fixture.file("must-not-be-created");
        let literal = format!("$(touch '{}'); echo injected", marker.display());
        let mut runner = fixture.runner(
            "capture",
            &[
                path_string(&captured),
                path_string(&arguments),
                literal.clone(),
            ],
            2.0,
            65536,
        );
        let input = envelope(json!({"visible":"keep","hidden":{"value":7}}));
        let outcome = runner
            .evaluate(&input, &AtomicBool::new(false), None)
            .unwrap();
        assert_eq!(outcome.decision, Decision::Matched);
        assert_eq!(outcome.reason.as_deref(), Some("captured"));
        let recorded = fs::read(&captured).unwrap();
        assert_eq!(recorded.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<Value>(&recorded).unwrap(), input);
        assert_eq!(fs::read_to_string(arguments).unwrap(), literal);
        assert!(!marker.exists());
    }

    #[test]
    fn script_duplex_io_does_not_deadlock_with_large_stdin_and_stdout() {
        let fixture = fixture();
        let recorded = fixture.file("duplex");
        let mut runner = fixture.runner("duplex", &[path_string(&recorded)], 4.0, 256 * 1024);
        let input = envelope(json!({"large":"z".repeat(2 * 1024 * 1024)}));
        let outcome = runner
            .evaluate(&input, &AtomicBool::new(false), None)
            .unwrap();
        assert_eq!(outcome.decision, Decision::Continue);
        assert_eq!(outcome.reason.unwrap().len(), 128 * 1024);
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(recorded).unwrap()).unwrap(),
            input
        );
    }

    #[test]
    fn script_stdout_requires_one_strict_result_and_error_never_matches() {
        let fixture = fixture();
        for invalid in [
            "",
            "[]",
            "null",
            "{\"decision\":\"matched\"}\n{\"decision\":\"continue\"}",
            "{\"decision\":\"matched\"} trailing",
            "{\"decision\":\"unknown\"}",
            "{\"reason\":\"missing decision\"}",
            "{\"decision\":\"matched\",\"reason\":null}",
            "{\"decision\":\"matched\",\"extra\":1}",
            "{\"decision\":\"error\",\"decision\":\"matched\"}",
        ] {
            let mut runner = fixture.runner("output", &[invalid.into()], 2.0, 65536);
            assert_eq!(
                runner
                    .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
                    .unwrap_err()
                    .kind,
                HookErrorKind::InvalidOutput,
                "{invalid}"
            );
        }
        let mut runner = fixture.runner(
            "output",
            &["{\"decision\":\"error\",\"reason\":\"bad state\"}".into()],
            2.0,
            65536,
        );
        let error = runner
            .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
            .unwrap_err();
        assert_eq!(error.kind, HookErrorKind::Reported);
        assert_eq!(error.message, "bad state");
        assert_eq!(error.code(), "HOOK_REPORTED_ERROR");
        let valid = "{\"decision\":\"continue\"}\n  ";
        let mut runner = fixture.runner("output", &[valid.into()], 2.0, valid.len());
        assert_eq!(
            runner
                .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
                .unwrap()
                .decision,
            Decision::Continue
        );
    }

    #[test]
    fn spawn_nonzero_and_early_exit_are_explicit_and_do_not_hang_on_stdin() {
        let fixture = fixture();
        let mut missing = HookRunner::from_json(&json!({
            "kind":"script","executable":fixture.file("does-not-exist")
        }))
        .unwrap();
        assert_eq!(
            missing
                .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
                .unwrap_err()
                .kind,
            HookErrorKind::Spawn
        );
        let mut failure = fixture.runner("nonzero", &[], 2.0, 65536);
        let error = failure
            .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
            .unwrap_err();
        assert_eq!(error.kind, HookErrorKind::Exit);
        assert!(error.message.contains("23"));
        assert!(error.stderr.contains("expected fixture failure"));
        let mut early = fixture.runner("early", &[], 2.0, 65536);
        let started = Instant::now();
        assert_eq!(
            early
                .evaluate(
                    &envelope(json!({"large":"x".repeat(2 * 1024 * 1024)})),
                    &AtomicBool::new(false),
                    None
                )
                .unwrap_err()
                .kind,
            HookErrorKind::Exit
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn stdout_and_stderr_limits_kill_and_reap_the_hook_before_returning() {
        let fixture = fixture();
        for mode in ["stdout_flood", "stderr_flood"] {
            let pids = fixture.file("limit-pid");
            let mut runner = fixture.runner(mode, &[path_string(&pids)], 2.0, 1024);
            let error = runner
                .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
                .unwrap_err();
            assert_eq!(error.kind, HookErrorKind::OutputLimit);
            assert!(error.message.contains(if mode == "stdout_flood" {
                "stdout"
            } else {
                "stderr"
            }));
            assert!(error.stderr.len() <= hooks::MAX_STDERR_BYTES);
            assert_gone(&read_pids(&pids));
        }
    }

    #[test]
    fn stdout_requires_utf8_and_binary_stderr_stays_bounded_after_conversion() {
        let fixture = fixture();
        let mut invalid = fixture.runner("binary_stdout", &[], 2.0, 65536);
        assert_eq!(
            invalid
                .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
                .unwrap_err()
                .kind,
            HookErrorKind::InvalidOutput
        );
        let mut diagnostics = fixture.runner("binary_stderr", &[], 2.0, 65536);
        let outcome = diagnostics
            .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
            .unwrap();
        assert_eq!(outcome.decision, Decision::Continue);
        assert!(!outcome.stderr.is_empty());
        assert!(outcome.stderr.len() <= hooks::MAX_STDERR_BYTES);
    }

    #[test]
    fn own_timeout_and_total_deadline_are_distinct_and_reap_blocked_processes() {
        let fixture = fixture();
        for (hook_timeout, total, expected) in [
            (1.0, Duration::from_secs(4), HookErrorKind::HookTimeout),
            (4.0, Duration::from_secs(1), HookErrorKind::OverallDeadline),
        ] {
            let pids = fixture.file("timeout-pid");
            let mut runner = fixture.runner("sleep", &[path_string(&pids)], hook_timeout, 65536);
            let started = Instant::now();
            let error = runner
                .evaluate(
                    &envelope(json!({"large":"x".repeat(2 * 1024 * 1024)})),
                    &AtomicBool::new(false),
                    Some(started + total),
                )
                .unwrap_err();
            assert_eq!(error.kind, expected);
            assert!(started.elapsed() < Duration::from_secs(3));
            assert_gone(&read_pids(&pids));
        }
    }

    #[test]
    fn interruption_kills_a_real_three_process_tree_and_preserves_parent_group() {
        let fixture = fixture();
        let pids = fixture.file("tree-pids");
        let mut runner = fixture.runner("tree", &[path_string(&pids), "2".into()], 5.0, 65536);
        let canceled = Arc::new(AtomicBool::new(false));
        let flag = canceled.clone();
        let path = pids.clone();
        let interrupt = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let pids = read_pids(&path);
                if pids.len() == 3 {
                    // SAFETY: these calls only inspect live fixture process groups.
                    #[cfg(unix)]
                    {
                        let parent_group = unsafe { libc::getpgrp() };
                        for pid in &pids {
                            assert_eq!(unsafe { libc::getpgid(*pid) }, pids[0]);
                            assert_ne!(unsafe { libc::getpgid(*pid) }, parent_group);
                        }
                    }
                    flag.store(true, Ordering::Release);
                    break;
                }
                assert!(Instant::now() < deadline, "fixture tree did not start");
                thread::sleep(Duration::from_millis(5));
            }
        });
        let error = runner
            .evaluate(&envelope(json!({})), &canceled, None)
            .unwrap_err();
        interrupt.join().unwrap();
        assert_eq!(error.kind, HookErrorKind::Interrupted);
        let pids = read_pids(&pids);
        assert_eq!(pids.len(), 3);
        assert_gone(&pids);
    }

    #[test]
    fn timeout_cleans_descendants_that_keep_pipes_open_after_the_leader_exits() {
        let fixture = fixture();
        let pids = fixture.file("orphan-pids");
        let mut runner = fixture.runner("spawn_exit", &[path_string(&pids)], 1.0, 65536);
        let error = runner
            .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
            .unwrap_err();
        assert_eq!(error.kind, HookErrorKind::HookTimeout);
        assert_gone(&read_pids(&pids));
    }

    #[test]
    fn successful_hook_also_cleans_background_children_with_closed_pipes() {
        let fixture = fixture();
        let pids = fixture.file("closed-pids");
        let mut runner = fixture.runner("spawn_closed", &[path_string(&pids)], 2.0, 65536);
        let outcome = runner
            .evaluate(&envelope(json!({})), &AtomicBool::new(false), None)
            .unwrap();
        assert_eq!(outcome.decision, Decision::Continue);
        assert_gone(&read_pids(&pids));
    }

    #[test]
    fn an_elapsed_deadline_and_preexisting_interrupt_do_not_spawn_a_script() {
        let fixture = fixture();
        let pids = fixture.file("must-not-spawn");
        for canceled in [false, true] {
            let mut runner = fixture.runner("sleep", &[path_string(&pids)], 2.0, 65536);
            let error = runner
                .evaluate(
                    &envelope(json!({})),
                    &AtomicBool::new(canceled),
                    Some(Instant::now()),
                )
                .unwrap_err();
            assert_eq!(
                error.kind,
                if canceled {
                    HookErrorKind::Interrupted
                } else {
                    HookErrorKind::OverallDeadline
                }
            );
            assert!(!pids.exists());
        }
    }

    const FIXTURE_SOURCE: &str = r###"
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

fn input() -> Vec<u8> {
    let mut bytes = Vec::new();
    io::stdin().read_to_end(&mut bytes).unwrap();
    bytes
}
fn output(text: &str) {
    print!("{text}");
    io::stdout().flush().unwrap();
}
fn sleep() -> ! {
    loop { std::thread::sleep(Duration::from_secs(30)); }
}
#[cfg(windows)]
fn private_standard_handles() {
    // The fixture explicitly controls inheritance: redirected child stdio must
    // not keep hidden copies of the parent's original inherited handles alive.
    unsafe extern "system" {
        fn GetStdHandle(kind: u32) -> *mut std::ffi::c_void;
        fn SetHandleInformation(handle: *mut std::ffi::c_void, mask: u32, flags: u32) -> i32;
    }
    for kind in [-10_i32, -11, -12] {
        unsafe { SetHandleInformation(GetStdHandle(kind as u32), 1, 0); }
    }
}
fn main() {
    #[cfg(windows)] private_standard_handles();
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "ready" => {}
        "capture" => {
            std::fs::write(&args[2], input()).unwrap();
            std::fs::write(&args[3], &args[4]).unwrap();
            output(r#"{"decision":"matched","reason":"captured"}"#);
        }
        "output" => { let _ = input(); output(&args[2]); }
        "duplex" => {
            output(r#"{"decision":"continue","reason":""#);
            io::stdout().write_all(&vec![b'x'; 128 * 1024]).unwrap();
            output("\"}\n");
            std::fs::write(&args[2], input()).unwrap();
        }
        "nonzero" => {
            let _ = input();
            eprintln!("expected fixture failure");
            std::process::exit(23);
        }
        "binary_stdout" => {
            let _ = input();
            io::stdout().write_all(&[0xff]).unwrap();
        }
        "binary_stderr" => {
            let _ = input();
            io::stderr().write_all(&vec![0xff; 65536]).unwrap();
            output(r#"{"decision":"continue"}"#);
        }
        "early" => std::process::exit(17),
        "sleep" => {
            if let Some(path) = args.get(2) { std::fs::write(path, format!("{}\n", std::process::id())).unwrap(); }
            sleep();
        }
        "stdout_flood" | "stderr_flood" => {
            std::fs::write(&args[2], format!("{}\n", std::process::id())).unwrap();
            let chunk = [b'x'; 8192];
            loop {
                if args[1] == "stdout_flood" { io::stdout().write_all(&chunk).unwrap(); }
                else { io::stderr().write_all(&chunk).unwrap(); }
            }
        }
        "tree" => {
            let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&args[2]).unwrap();
            writeln!(file, "{}", std::process::id()).unwrap();
            let depth: usize = args[3].parse().unwrap();
            if depth > 0 {
                Command::new(std::env::current_exe().unwrap())
                    .args(["tree", &args[2], &(depth - 1).to_string()])
                    .stdin(Stdio::null()).spawn().unwrap();
            }
            sleep();
        }
        "spawn_exit" | "spawn_closed" => {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command.arg("sleep").stdin(Stdio::null());
            if args[1] == "spawn_closed" { command.stdout(Stdio::null()).stderr(Stdio::null()); }
            let child = command.spawn().unwrap();
            std::fs::write(&args[2], format!("{} {}\n", std::process::id(), child.id())).unwrap();
            output(r#"{"decision":"continue"}"#);
        }
        _ => panic!("unknown fixture mode"),
    }
}
"###;
}
