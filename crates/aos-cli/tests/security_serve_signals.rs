#![cfg(unix)]

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const STARTUP_TIMEOUT_SECS: u64 = 2;
const SHUTDOWN_TIMEOUT_SECS: u64 = 1;
// Include native startup/close, the production terminal writer's two-second
// budget, and three seconds for process loading and scheduler delays.
const WATCHDOG: Duration =
    Duration::from_secs(STARTUP_TIMEOUT_SECS + SHUTDOWN_TIMEOUT_SECS + 2 + 3);

struct Fixture {
    directory: PathBuf,
    port: u16,
}

impl Fixture {
    fn new() -> Self {
        let directory =
            std::env::temp_dir().join(format!("aos-security-signals-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let fixture = Self {
            directory,
            port: listener.local_addr().unwrap().port(),
        };
        fixture.openssl(&[
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-sha256",
            "-days",
            "2",
            "-subj",
            "/CN=AOS signal test CA",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]);
        fixture.openssl(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            "/CN=router",
            "-keyout",
            "router.key",
            "-out",
            "router.csr",
        ]);
        fs::write(
            fixture.directory.join("router.ext"),
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\nextendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n",
        )
        .unwrap();
        fixture.openssl(&[
            "x509",
            "-req",
            "-sha256",
            "-days",
            "2",
            "-in",
            "router.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-set_serial",
            "1",
            "-extfile",
            "router.ext",
            "-out",
            "router.pem",
        ]);
        fixture
    }

    fn openssl(&self, args: &[&str]) {
        let output = Command::new("openssl")
            .args(args)
            .current_dir(&self.directory)
            .output()
            .expect("OpenSSL is required for CLI native TLS tests");
        assert!(
            output.status.success(),
            "OpenSSL fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn start(&self, unreadable_certificate: bool) -> Router {
        let certificate = self.directory.join("router.pem");
        if unreadable_certificate {
            // A missing path is unreadable even under a root test runner.
            // Profile validation accepts the absolute path; native open fails.
            fs::remove_file(&certificate).unwrap();
        }
        let request = self.directory.join("request.json");
        fs::write(
            &request,
            json!({
                "policy":{"schema_version":1,"domain_id":29,"subjects":[{
                    "id":"reader","common_name":"reader",
                    "nodes":[{"name":"reader","namespace":"/signal_test"}]
                }]},
                "router":{
                    "schema_version":1,
                    "listen_endpoints":[format!("tls/127.0.0.1:{}", self.port)],
                    "root_ca_certificate":self.directory.join("ca.pem"),
                    "certificate":certificate,
                    "private_key":self.directory.join("router.key")
                },
                "startup_timeout":STARTUP_TIMEOUT_SECS,
                "shutdown_timeout":SHUTDOWN_TIMEOUT_SECS
            })
            .to_string(),
        )
        .unwrap();
        let mut pipe = [0; 2];
        assert_eq!(unsafe { libc::pipe(pipe.as_mut_ptr()) }, 0);
        let output = unsafe { File::from_raw_fd(pipe[0]) };
        let filler = unsafe { File::from_raw_fd(pipe[1]) };
        for descriptor in pipe {
            assert_eq!(
                unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) },
                0
            );
        }
        let flags = unsafe { libc::fcntl(output.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(output.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let stderr = self.directory.join("stderr.log");
        let started = Instant::now();
        let child = command()
            .args(["security", "serve", "--request"])
            .arg(request)
            .stdin(Stdio::null())
            .stdout(filler.try_clone().unwrap())
            .stderr(File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        Router {
            child,
            output,
            filler: Some(filler),
            bytes: Vec::new(),
            stderr,
            started,
            #[cfg(target_os = "macos")]
            sample: None,
            #[cfg(target_os = "macos")]
            sample_note: None,
        }
    }

    fn wait_listener_closed(&self) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match TcpListener::bind(("127.0.0.1", self.port)) {
                Ok(listener) => {
                    drop(listener);
                    return;
                }
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {}
                Err(error) => panic!("cannot check owned router listener: {error}"),
            }
            assert!(Instant::now() < deadline, "router listener did not close");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aos"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("AOS_") {
            command.env_remove(name);
        }
    }
    command.env_remove("RUST_LOG");
    command
}

struct Router {
    child: Child,
    output: File,
    filler: Option<File>,
    bytes: Vec<u8>,
    stderr: PathBuf,
    started: Instant,
    #[cfg(target_os = "macos")]
    sample: Option<Child>,
    #[cfg(target_os = "macos")]
    sample_note: Option<String>,
}

impl Router {
    fn sample_if_slow(&mut self, _elapsed: Duration) {
        #[cfg(target_os = "macos")]
        if _elapsed >= WATCHDOG - Duration::from_secs(1)
            && self.sample_note.is_none()
            && matches!(self.child.try_wait(), Ok(None))
        {
            // Start at most one sample of this owned child before the deadline.
            // Never wait for sampling before killing/reaping a timed-out child.
            let start = || -> io::Result<Child> {
                let log = File::create(self.stderr.with_file_name("sample.log"))?;
                Command::new("/usr/bin/sample")
                    .arg(self.child.id().to_string())
                    .args(["1", "1", "-file"])
                    .arg(self.stderr.with_file_name("sample.txt"))
                    .stdin(Stdio::null())
                    .stdout(log.try_clone()?)
                    .stderr(log)
                    .spawn()
            };
            match start() {
                Ok(child) => {
                    self.sample_note = Some(format!("sampling child pid={}", child.id()));
                    self.sample = Some(child);
                }
                Err(error) => self.sample_note = Some(format!("sample unavailable: {error}")),
            }
        }
    }

    fn stop_sample(&mut self) {
        #[cfg(target_os = "macos")]
        if let Some(mut child) = self.sample.take() {
            let kill_error = if child.try_wait().ok().flatten().is_none() {
                child.kill().err()
            } else {
                None
            };
            self.sample_note = Some(format!(
                "sample status={:?} kill_error={kill_error:?}",
                child.wait()
            ));
        }
    }

    fn fail(&mut self, reason: &str, waiting: Instant) -> ! {
        let elapsed = waiting.elapsed();
        let kill_error = if self.child.try_wait().ok().flatten().is_none() {
            self.child.kill().err()
        } else {
            None
        };
        let reaped = self.child.wait();
        self.filler.take();
        self.pump();
        self.stop_sample();
        let stderr = fs::read_to_string(&self.stderr)
            .unwrap_or_else(|error| format!("cannot read captured stderr: {error}"));
        let events: Vec<_> = self
            .bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).map_err(|error| error.to_string()))
            .collect();
        #[cfg(target_os = "macos")]
        let mut sampling = String::new();
        #[cfg(not(target_os = "macos"))]
        let sampling = String::new();
        #[cfg(target_os = "macos")]
        if let Some(note) = &self.sample_note {
            sampling.push_str(&format!("\nsampling: {note}"));
            for name in ["sample.log", "sample.txt"] {
                if let Ok(bytes) = fs::read(self.stderr.with_file_name(name)) {
                    sampling.push_str(&format!(
                        "\n{name} ({} bytes; first 16384 shown): {}",
                        bytes.len(),
                        String::from_utf8_lossy(&bytes[..bytes.len().min(16384)])
                    ));
                }
            }
        }
        panic!(
            "{reason}: pid={} wait_elapsed={elapsed:?} child_elapsed={:?} watchdog={WATCHDOG:?} \
             reaped={reaped:?} kill_error={kill_error:?}; events={events:?}; stderr={stderr}{sampling}",
            self.child.id(),
            self.started.elapsed(),
        );
    }

    fn pump(&mut self) -> bool {
        let mut buffer = [0; 4096];
        loop {
            match self.output.read(&mut buffer) {
                Ok(0) => return true,
                Ok(count) => {
                    self.bytes.extend_from_slice(&buffer[..count]);
                    assert!(
                        self.bytes.len() <= 2 * 1024 * 1024,
                        "unexpected output volume"
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return false,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => panic!("router output read failed: {error}"),
            }
        }
    }

    fn events(&self) -> Vec<Value> {
        self.bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| line.last() == Some(&b'\n') && line.iter().any(|byte| *byte != b'\n'))
            .map(|line| serde_json::from_slice(line).expect("CLI output must remain JSONL"))
            .collect()
    }

    fn ready(&mut self) {
        let waiting = Instant::now();
        loop {
            self.pump();
            let events = self.events();
            if events.iter().any(|event| event["event"] == "router_ready") {
                assert_eq!(events.len(), 1, "{events:?}");
                assert_eq!(events[0]["data"]["domain_id"], 29);
                return;
            }
            if self.child.try_wait().unwrap().is_some() || waiting.elapsed() >= WATCHDOG {
                self.fail("router never became ready", waiting);
            }
            self.sample_if_slow(waiting.elapsed());
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn signal(&mut self, signal: i32) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "owned router already exited"
        );
        assert_eq!(unsafe { libc::kill(self.child.id() as i32, signal) }, 0);
    }

    fn fill_output(&mut self) -> usize {
        let output = self.filler.as_mut().unwrap();
        let descriptor = output.as_raw_fd();
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) },
            0
        );
        let mut total = 0;
        // The CLI is idle after router_ready. Restore blocking mode before
        // signalling; the production writer must block on the full pipe.
        for buffer in [&[b'\n'; 4096][..], &b"\n"[..]] {
            loop {
                match output.write(buffer) {
                    Ok(count) if count > 0 => total += count,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    result => panic!("cannot fill owned output pipe: {result:?}"),
                }
                assert!(total <= 1024 * 1024, "pipe capacity unexpectedly large");
            }
        }
        assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags) }, 0);
        assert!(total > 0);
        total
    }

    fn finish(&mut self) -> ExitStatus {
        self.filler.take();
        let waiting = Instant::now();
        loop {
            let eof = self.pump();
            if let Some(status) = self.child.try_wait().unwrap() {
                if eof {
                    return status;
                }
            }
            if waiting.elapsed() >= WATCHDOG {
                self.fail("router did not finish", waiting);
            }
            self.sample_if_slow(waiting.elapsed());
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        self.stop_sample();
    }
}

fn assert_shutdown(router: &Router, status: ExitStatus, signal: i32) {
    let events = router.events();
    assert_eq!(status.code(), Some(128 + signal), "{status:?}: {events:?}");
    assert_eq!(status.signal(), None);
    let summaries: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "summary")
        .collect();
    assert_eq!(summaries.len(), 1, "{events:?}");
    assert_eq!(summaries[0]["data"]["signal"], signal);
    assert_eq!(summaries[0]["data"]["stage"], "shutdown");
    assert_eq!(summaries[0]["data"]["session_closed"], true);
    assert!(summaries[0]["data"]["cleanup_error"].is_null());
    let errors: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "error")
        .collect();
    assert_eq!(errors.len(), 1, "{events:?}");
    assert_eq!(errors[0]["data"]["code"], "INTERRUPTED");
}

#[test]
fn hangup_emits_one_confirmed_shutdown_summary() {
    let fixture = Fixture::new();
    let mut router = fixture.start(false);
    router.ready();
    router.signal(libc::SIGHUP);
    let status = router.finish();
    assert_shutdown(&router, status, libc::SIGHUP);
    fixture.wait_listener_closed();
}

#[test]
fn quit_emits_one_confirmed_shutdown_summary() {
    let fixture = Fixture::new();
    let mut router = fixture.start(false);
    router.ready();
    router.signal(libc::SIGQUIT);
    let status = router.finish();
    assert_shutdown(&router, status, libc::SIGQUIT);
    fixture.wait_listener_closed();
}

#[test]
fn unreadable_certificate_emits_one_opening_summary() {
    let fixture = Fixture::new();
    let mut router = fixture.start(true);
    let status = router.finish();
    let events = router.events();
    assert_eq!(status.code(), Some(1), "{events:?}");
    assert!(!events.iter().any(|event| event["event"] == "router_ready"));
    let summaries: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "summary")
        .collect();
    assert_eq!(summaries.len(), 1, "{events:?}");
    assert_eq!(summaries[0]["data"]["stage"], "opening");
    assert_eq!(summaries[0]["data"]["signal"], 0);
    assert_eq!(summaries[0]["data"]["session_closed"], false);
    assert!(!summaries[0]["data"]["cleanup_error"]
        .as_str()
        .unwrap()
        .is_empty());
    let errors: Vec<_> = events
        .iter()
        .filter(|event| event["event"] == "error")
        .collect();
    assert_eq!(errors.len(), 1, "{events:?}");
    assert_eq!(errors[0]["data"]["code"], "ROUTER_START_FAILED");
    assert_eq!(errors[0]["data"]["phase"], "session_open");
    fixture.wait_listener_closed();
}

#[test]
fn repeated_term_during_terminal_drain_preserves_summary() {
    let fixture = Fixture::new();
    let mut router = fixture.start(false);
    router.ready();
    let filled_bytes = router.fill_output();
    router.signal(libc::SIGTERM);
    fixture.wait_listener_closed();
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut repeated = 0;
    while Instant::now() < deadline && router.child.try_wait().unwrap().is_none() {
        router.signal(libc::SIGTERM);
        repeated += 1;
        std::thread::sleep(Duration::from_millis(10));
    }
    // Draining starts only after the repeated-signal window. Newline filler
    // occupies the pipe without fabricating any CLI business or summary event.
    let status = router.finish();
    eprintln!("full_pipe_bytes={filled_bytes} repeated_term={repeated} status={status:?}");
    assert!(repeated > 0);
    assert_shutdown(&router, status, libc::SIGTERM);
    fixture.wait_listener_closed();
}
