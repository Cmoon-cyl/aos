use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Once;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const PREFLIGHT_DEADLINE: Duration = Duration::from_secs(3);
const COLD_START_DEADLINE: Duration = Duration::from_secs(15);
static COLD_START: Once = Once::new();

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let directory =
            Self(std::env::temp_dir().join(format!("aos-cli-invalid-{}", uuid::Uuid::new_v4())));
        std::fs::create_dir(&directory.0).unwrap();
        directory
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

enum StdinMode {
    Close,
    KeepOpen,
}

struct Probe {
    pid: u32,
    elapsed: Duration,
    deadline: Duration,
    timed_out: bool,
    kill_error: Option<std::io::Error>,
    output: Output,
}

impl Probe {
    fn diagnostic(&self) -> String {
        format!(
            "pid={} elapsed={:?} deadline={:?} timed_out={} status={} kill_error={:?}\nstdout: {}\nstderr: {}",
            self.pid,
            self.elapsed,
            self.deadline,
            self.timed_out,
            self.output.status,
            self.kill_error,
            String::from_utf8_lossy(&self.output.stdout),
            String::from_utf8_lossy(&self.output.stderr),
        )
    }
}

fn drain(mut reader: impl Read + Send + 'static) -> JoinHandle<std::io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
}

fn probe(
    directory: &Path,
    command: [&str; 2],
    bootstrap: &str,
    deadline: Duration,
    stdin_mode: StdinMode,
) -> Probe {
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_aos"))
        .args(command)
        .args(["--request", "-"])
        .env_remove("AOS_DOMAIN_ID")
        .env_remove("AOS_ZENOH_CONFIG")
        .env_remove("AOS_NODE_NAME")
        .env_remove("AOS_NAMESPACE")
        .env_remove("AOS_USE_SIM_TIME")
        .env_remove("AOS_SECURITY_PROFILE")
        .env("AOS_PROCESS_CONFIG", bootstrap)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    // Drain both pipes while the child is alive, including on timeout.
    let stdout = drain(child.stdout.take().unwrap());
    let stderr = drain(child.stderr.take().unwrap());
    // If bootstrap validation were skipped, this nonexistent session file
    // would fail in session_open. The configuration assertion distinguishes
    // that failure without needing a live router.
    let request = json!({"context_config":{"cli":{
        "zenoh_config":directory.join("must-not-open.json5")
    }}});
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(request.to_string().as_bytes()).unwrap();
    let held_stdin = match stdin_mode {
        StdinMode::Close => {
            drop(stdin);
            None
        }
        StdinMode::KeepOpen => Some(stdin),
    };
    let mut timed_out = false;
    let mut kill_error = None;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() >= deadline {
            timed_out = true;
            kill_error = child.kill().err();
            // Always reap before reporting failure or joining the pipe readers.
            break child.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(5));
    };
    drop(held_stdin);
    Probe {
        pid,
        elapsed: started.elapsed(),
        deadline,
        timed_out,
        kill_error,
        output: Output {
            status,
            stdout: stdout.join().unwrap().unwrap(),
            stderr: stderr.join().unwrap().unwrap(),
        },
    }
}

fn assert_configuration_error(probe: &Probe, command: [&str; 2], bootstrap: &str) {
    let diagnostic = probe.diagnostic();
    assert!(
        !probe.timed_out,
        "bootstrap preflight did not finish: {command:?} {bootstrap:?}; {diagnostic}"
    );
    assert_eq!(
        probe.output.status.code(),
        Some(2),
        "{command:?} {bootstrap:?}; {diagnostic}"
    );
    let response: Value = serde_json::from_slice(&probe.output.stdout)
        .unwrap_or_else(|error| panic!("invalid response JSON: {error}; {diagnostic}"));
    assert_eq!(response["ok"], false, "{response}; {diagnostic}");
    assert_eq!(
        response["error"]["code"], "INVALID_ARGUMENT",
        "{response}; {diagnostic}"
    );
    assert_eq!(
        response["error"]["phase"], "configuration",
        "{response}; {diagnostic}"
    );
    let message = response["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("process bootstrap")
            || message.contains("AOS_PROCESS_CONFIG")
            || message.contains("invalid configuration JSON"),
        "{response}; {diagnostic}"
    );
}

fn check_cold_start(directory: &Path) {
    COLD_START.call_once(|| {
        // Cargo can replace debug/aos with a fresh inode even when all crates
        // are Fresh. Exercise this test's actual executable after Cargo exits;
        // an external prewarm before another Cargo invocation is insufficient.
        let result = probe(
            directory,
            ["node", "list"],
            "",
            COLD_START_DEADLINE,
            StdinMode::Close,
        );
        assert_configuration_error(&result, ["node", "list"], "");
        eprintln!("bootstrap cold-start check: {}", result.diagnostic());
    });
}

#[test]
fn invalid_bootstrap_fails_in_configuration_before_session_open() {
    let directory = Directory::new();
    check_cold_start(&directory.0);
    let invalid = directory.0.join("invalid.json");
    std::fs::write(&invalid, "{invalid").unwrap();
    let cases = [
        String::new(),
        "relative.json".into(),
        directory.0.join("missing.json").display().to_string(),
        directory.0.display().to_string(),
        invalid.display().to_string(),
    ];
    for bootstrap in cases {
        for command in [
            ["node", "list"],
            ["doctor", "report"],
            ["config", "resolve"],
        ] {
            let result = probe(
                &directory.0,
                command,
                &bootstrap,
                PREFLIGHT_DEADLINE,
                StdinMode::Close,
            );
            assert_configuration_error(&result, command, &bootstrap);
        }
    }
}

#[test]
fn watchdog_kills_and_reaps_a_child_waiting_for_stdin_eof() {
    let directory = Directory::new();
    check_cold_start(&directory.0);
    let result = probe(
        &directory.0,
        ["node", "list"],
        "",
        PREFLIGHT_DEADLINE,
        StdinMode::KeepOpen,
    );
    let diagnostic = result.diagnostic();
    assert!(result.timed_out, "{diagnostic}");
    assert!(result.kill_error.is_none(), "{diagnostic}");
    assert!(result.elapsed >= PREFLIGHT_DEADLINE, "{diagnostic}");
    assert!(!result.output.status.success(), "{diagnostic}");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            result.output.status.signal(),
            Some(libc::SIGKILL),
            "{diagnostic}"
        );
    }
    eprintln!("bootstrap watchdog negative control: {diagnostic}");
}
