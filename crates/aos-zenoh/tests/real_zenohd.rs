//! Cross-process round trip through an upstream zenohd 1.10.1 process.
//!
//! Ignored by default because it needs external executables:
//!
//! ```sh
//! cargo build -p aos-zenoh --example probe --locked
//! AOS_ZENOHD=/path/to/zenohd AOS_ZENOH_PROBE=target/debug/examples/probe \
//!   cargo test -p aos-zenoh --test real_zenohd --locked -- --ignored --nocapture
//! ```

mod support;

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use support::free_port;

fn required_env(name: &str) -> PathBuf {
    let value = std::env::var_os(name).unwrap_or_else(|| panic!("{name} must be set"));
    let path = PathBuf::from(value);
    assert!(path.is_file(), "{name}={} is not a file", path.display());
    path
}

fn config_path(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

/// Stops the router this test started, by its own PID, even on panic.
struct Router(Option<Child>);

impl Router {
    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let child = self.0.as_mut().expect("router is owned");
        // Test router loss using the owned process handle on every platform.
        // A shell `kill` command is unavailable on native Windows.
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        let status = child.wait()?;
        self.0.take();
        Ok(status)
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn report(name: &str, output: &Output) {
    eprintln!(
        "--- {name}: {:?}\n[stdout]\n{}[stderr]\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "needs AOS_ZENOHD and AOS_ZENOH_PROBE"]
fn processes_exchange_through_upstream_zenohd() {
    let zenohd = required_env("AOS_ZENOHD");
    let probe = required_env("AOS_ZENOH_PROBE");
    let session_config = config_path("zenoh-session.json5");
    let port = free_port();
    let endpoint = format!("tcp/127.0.0.1:{port}");
    let log_path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("zenohd-{port}.log"));
    let log = std::fs::File::create(&log_path).unwrap();

    let version = Command::new(&zenohd).arg("--version").output().unwrap();
    report("zenohd --version", &version);
    assert!(String::from_utf8_lossy(&version.stdout).contains("zenohd v1.10.1"));

    let listen_override = format!(r#"listen/endpoints:["{endpoint}"]"#);
    eprintln!(
        "router: {} --config {} --cfg '{listen_override}' (log {})",
        zenohd.display(),
        config_path("zenoh-router.json5"),
        log_path.display()
    );
    let mut router = Router(Some(
        Command::new(&zenohd)
            .args(["--config", &config_path("zenoh-router.json5")])
            .arg(format!("--cfg={listen_override}"))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "zenohd did not listen on {endpoint}"
        );
        thread::sleep(Duration::from_millis(50));
    }

    let mut listener = Command::new(&probe)
        .args([
            "listen",
            &session_config,
            &endpoint,
            "/aos_probe/chatter",
            "10000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let stdout = listener.stdout.take().unwrap();
    let reader = thread::spawn(move || {
        let mut lines = Vec::new();
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if line.starts_with("probe: ready") {
                let _ = ready_tx.send(());
            }
            lines.push(line);
        }
        lines
    });
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("listener must become ready");

    let talker = Command::new(&probe)
        .args([
            "talk",
            &session_config,
            &endpoint,
            "/aos_probe/chatter",
            r#"{"data":"hello"}"#,
            "30",
        ])
        .output()
        .unwrap();
    report("talker", &talker);
    let listener_status = listener.wait().unwrap();
    let listener_stdout = reader.join().unwrap().join("\n");
    let mut listener_stderr = String::new();
    std::io::Read::read_to_string(&mut listener.stderr.take().unwrap(), &mut listener_stderr)
        .unwrap();
    eprintln!("--- listener: {listener_status:?}\n[stdout]\n{listener_stdout}\n[stderr]\n{listener_stderr}");

    assert!(talker.status.success());
    assert!(listener_status.success());
    assert!(listener_stdout.contains(r#"probe: received {"data":"hello"}"#));
    assert!(listener_stdout.contains("probe: shutdown"));
    let talker_stdout = String::from_utf8_lossy(&talker.stdout);
    let pid_line = |text: &str| {
        text.lines()
            .find(|l| l.contains("zid="))
            .unwrap()
            .to_owned()
    };
    assert_ne!(pid_line(&talker_stdout), pid_line(&listener_stdout));

    let router_status = router.stop().expect("router must exit");
    eprintln!("router after stop: {router_status:?}");

    // The default peer opens without a Router and reconnects in background.
    // A publish returning locally does not prove that a peer received it.
    let started = Instant::now();
    let orphan = Command::new(&probe)
        .args([
            "talk",
            &session_config,
            &endpoint,
            "/aos_probe/chatter",
            "{}",
            "1",
        ])
        .output()
        .unwrap();
    report("talker without router", &orphan);
    assert!(orphan.status.success());
    let orphan_stdout = String::from_utf8_lossy(&orphan.stdout);
    assert!(orphan_stdout.contains("routers=[]"));
    assert!(orphan_stdout.contains("probe: shutdown"));
    assert!(started.elapsed() < Duration::from_secs(10));

    // Fail-fast is still available when explicitly requested. Preserve the
    // negative open check with that profile instead of applying it to default.
    let mut strict_config = zenoh::Config::from_file(&session_config).unwrap();
    strict_config
        .insert_json5("connect/timeout_ms", "3000")
        .unwrap();
    strict_config
        .insert_json5("connect/exit_on_failure", "true")
        .unwrap();
    let strict_path = log_path.with_extension("strict.json");
    std::fs::write(&strict_path, serde_json::to_vec(&strict_config).unwrap()).unwrap();
    let started = Instant::now();
    let strict_orphan = Command::new(&probe)
        .args([
            "talk",
            strict_path.to_str().unwrap(),
            &endpoint,
            "/aos_probe/chatter",
            "{}",
            "1",
        ])
        .output()
        .unwrap();
    report("explicit fail-fast talker without router", &strict_orphan);
    assert_eq!(strict_orphan.status.code(), Some(3));
    assert!(started.elapsed() < Duration::from_secs(10));
    std::fs::remove_file(strict_path).unwrap();
    eprintln!("router log: {}", log_path.display());
}
