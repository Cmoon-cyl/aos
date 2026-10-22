//! OS ownership boundary. Never reap a leader until its group's final signal
//! has been sent. A reaped PID/PGID is never a signal target again.
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[cfg(windows)]
use crate::windows_process::{Pipe as ChildStdout, Pipe as ChildStderr, Process as Child};
#[cfg(not(windows))]
use std::process::{Child, ChildStderr, ChildStdout};

#[cfg(unix)]
use nix::{
    errno::Errno,
    fcntl::{fcntl, FcntlArg, OFlag},
    sys::signal::{killpg, Signal},
    unistd::{getpgid, Pid},
};

use crate::{
    LaunchError, LaunchErrorCode, LaunchResult, OutputStream, PlannedProcess, ProcessBootstrap,
    ProcessExit, ProcessIdentity, MAX_DOCUMENT_BYTES, MAX_OUTPUT_LINE_BYTES, PREFIX_PATH_ENV,
    PROCESS_CONFIG_ENV,
};

pub(crate) struct RunDirectory {
    path: PathBuf,
}

impl RunDirectory {
    pub fn create() -> LaunchResult<Self> {
        Self::create_in(std::env::temp_dir())
    }

    fn create_in(root: PathBuf) -> LaunchResult<Self> {
        let path = root.join(format!("aos-launch-{}", uuid::Uuid::new_v4()));
        #[cfg(unix)]
        let mut builder = fs::DirBuilder::new();
        #[cfg(not(unix))]
        let builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&path)
            .map_err(|error| LaunchError::io("bootstrap", error))?;
        let path = match path.canonicalize() {
            Ok(path) => path,
            Err(error) => {
                let _ = fs::remove_dir_all(&path);
                return Err(LaunchError::io("bootstrap", error));
            }
        };
        Ok(Self { path })
    }

    pub fn write(&self, bootstrap: &ProcessBootstrap) -> LaunchResult<PathBuf> {
        let path = self.path.join(format!(
            "{}-{}.json",
            bootstrap.process_id, bootstrap.generation
        ));
        let bytes = serde_json::to_vec(bootstrap)
            .map_err(|error| LaunchError::invalid("bootstrap", error.to_string()))?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(LaunchError::invalid(
                "bootstrap",
                "process bootstrap exceeds 1 MiB",
            ));
        }
        // Validate the exact wire document using the consumer's parser. Keeping
        // two serializable types must never allow launcher/core ABI drift.
        aos_core::ProcessBootstrap::from_json(
            std::str::from_utf8(&bytes).expect("serialized JSON is UTF-8"),
        )
        .map_err(|error| LaunchError::invalid("bootstrap", error.to_string()))?;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .map_err(|error| LaunchError::io("bootstrap", error))?;
        file.write_all(&bytes)
            .map_err(|error| LaunchError::io("bootstrap", error))?;
        // The child is spawned only after the fully written handle is closed.
        drop(file);
        Ok(path)
    }
}

impl Drop for RunDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(crate) struct OutputRecord {
    pub stream: OutputStream,
    pub bytes: Vec<u8>,
    pub truncated_bytes: u64,
}

pub(crate) struct OutputBatch {
    pub records: Vec<OutputRecord>,
}

struct ReapedOutput {
    raw_remaining: usize,
    raw_limit: usize,
    buffered_remaining: usize,
    tail_checked: bool,
}

struct LineReader<R> {
    reader: R,
    stream: OutputStream,
    buffer: Vec<u8>,
    pending: [u8; 4096],
    pending_start: usize,
    pending_end: usize,
    discarded: u64,
    closed: bool,
    reaped_output: Option<ReapedOutput>,
}

impl<R: Read> LineReader<R> {
    fn new(reader: R, stream: OutputStream) -> Self {
        Self {
            reader,
            stream,
            buffer: Vec::new(),
            pending: [0; 4096],
            pending_start: 0,
            pending_end: 0,
            discarded: 0,
            closed: false,
            reaped_output: None,
        }
    }

    #[cfg(any(unix, windows, test))]
    fn bound_after_reap(&mut self, pipe_capacity: usize, readable_bytes: usize) {
        // Once the whole owned group is gone, only bytes already in the pipe
        // can be legitimate. Allow one read chunk of slack, independently for
        // stdout/stderr. This counts new raw reads, including oversized lines,
        // rather than delivered events or the speed of the downstream sink.
        let limit = pipe_capacity.saturating_add(self.pending.len());
        self.reaped_output = Some(ReapedOutput {
            raw_remaining: limit,
            raw_limit: limit,
            // Snapshot only bytes already retained when owned writers are
            // gone. Later writes cannot replenish this parsing allowance.
            buffered_remaining: readable_bytes + self.pending_end - self.pending_start,
            tail_checked: false,
        });
    }

    fn has_reaped_residue(&self) -> bool {
        !self.closed
            && self
                .reaped_output
                .as_ref()
                .is_some_and(|state| state.buffered_remaining > 0 || !state.tail_checked)
    }

    fn drain(&mut self, budget: usize, batch: &mut OutputBatch) -> LaunchResult<()> {
        let mut consumed = 0;
        // Each pipe gets its own line budget. When it is exhausted, retain at
        // most the current 4 KiB chunk and leave remaining bytes in the pipe.
        let record_limit = batch.records.len() + 256;
        while consumed < budget && batch.records.len() < record_limit && !self.closed {
            if self.pending_start == self.pending_end {
                if let Some(state) = &mut self.reaped_output {
                    if state.buffered_remaining == 0 {
                        // One final read distinguishes EOF, WouldBlock and
                        // bytes written after the fixed reap-time snapshot.
                        state.tail_checked = true;
                    }
                }
                let read_len = (budget - consumed).min(self.pending.len());
                match self.reader.read(&mut self.pending[..read_len]) {
                    Ok(0) => {
                        self.closed = true;
                        if !self.buffer.is_empty() || self.discarded > 0 {
                            self.finish_line(batch);
                        }
                        break;
                    }
                    Ok(size) => {
                        if let Some(state) = &mut self.reaped_output {
                            if size > state.buffered_remaining {
                                state.tail_checked = true;
                            }
                            let Some(remaining) = state.raw_remaining.checked_sub(size) else {
                                return Err(LaunchError::new(
                                    LaunchErrorCode::CleanupFailed,
                                    "output_drain",
                                    format!(
                                        "{:?} produced more than {} new bytes after leader reap; \
                                         a writer remains outside the owned process group",
                                        self.stream, state.raw_limit
                                    ),
                                ));
                            };
                            state.raw_remaining = remaining;
                        }
                        self.pending_start = 0;
                        self.pending_end = size;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(LaunchError::io("output", error)),
                }
            }
            let byte = self.pending[self.pending_start];
            self.pending_start += 1;
            consumed += 1;
            if let Some(state) = &mut self.reaped_output {
                state.buffered_remaining = state.buffered_remaining.saturating_sub(1);
            }
            if byte == b'\n' {
                self.finish_line(batch);
            } else if self.discarded > 0 {
                self.discarded = self.discarded.saturating_add(1);
            } else if self.buffer.len() < MAX_OUTPUT_LINE_BYTES {
                self.buffer.push(byte);
            } else {
                self.discarded = self.buffer.len() as u64 + 1;
                self.buffer.clear();
            }
        }
        Ok(())
    }

    fn finish_line(&mut self, batch: &mut OutputBatch) {
        batch.records.push(OutputRecord {
            stream: self.stream,
            bytes: std::mem::take(&mut self.buffer),
            truncated_bytes: self.discarded,
        });
        self.discarded = 0;
    }
}

pub(crate) struct OwnedChild {
    child: Child,
    pub identity: ProcessIdentity,
    pub pgid: i32,
    pub bootstrap_path: PathBuf,
    stdout: Option<LineReader<ChildStdout>>,
    stderr: Option<LineReader<ChildStderr>>,
    pub exit: Option<ProcessExit>,
    pub final_signal_sent: bool,
    pub reaped: bool,
    ownership_lost: bool,
}

impl OwnedChild {
    #[cfg(windows)]
    pub fn owned_job(&self) -> crate::windows_process::OwnedJob {
        self.child.owned_job()
    }
    pub fn spawn(
        plan: &PlannedProcess,
        bootstrap_path: PathBuf,
        launch_id: &str,
        generation: u32,
        prefix_path: &std::ffi::OsStr,
    ) -> LaunchResult<Self> {
        if !cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            return Err(LaunchError::new(
                LaunchErrorCode::Unsupported,
                "spawn",
                "launch currently supports Linux and macOS",
            ));
        }
        let mut command = Command::new(
            plan.entry
                .interpreter
                .as_ref()
                .unwrap_or(&plan.entry.executable),
        );
        if plan.entry.interpreter.is_some() {
            command
                .args(&plan.entry.interpreter_args)
                .arg(&plan.entry.executable);
        }
        command
            .args(&plan.spec.args)
            .current_dir(&plan.cwd)
            .envs(&plan.spec.env)
            .env(PROCESS_CONFIG_ENV, &bootstrap_path)
            .env(PREFIX_PATH_ENV, prefix_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        #[cfg(not(any(windows, target_os = "macos")))]
        let spawned = command.spawn();
        #[cfg(target_os = "macos")]
        let spawned = crate::macos_spawn::spawn_inheriting(&mut command);
        #[cfg(windows)]
        let spawned = Child::spawn(&command, false);
        let mut child = spawned.map_err(|error| {
            LaunchError::new(LaunchErrorCode::SpawnFailed, "spawn", error.to_string())
                .for_process(&plan.spec.id)
        })?;
        let pid = child.id();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        // Construct the RAII owner before fallible descriptor setup.
        let owned = Self {
            child,
            identity: ProcessIdentity {
                launch_id: launch_id.into(),
                process_id: plan.spec.id.clone(),
                generation,
                pid,
            },
            pgid: i32::try_from(pid).expect("OS process id is a positive pid_t"),
            bootstrap_path,
            stdout: stdout.map(|reader| LineReader::new(reader, OutputStream::Stdout)),
            stderr: stderr.map(|reader| LineReader::new(reader, OutputStream::Stderr)),
            exit: None,
            final_signal_sent: false,
            reaped: false,
            ownership_lost: false,
        };
        #[cfg(unix)]
        {
            if let Some(reader) = &owned.stdout {
                nonblocking(&reader.reader)?;
            }
            if let Some(reader) = &owned.stderr {
                nonblocking(&reader.reader)?;
            }
        }
        Ok(owned)
    }

    pub fn observe_exit(&mut self) -> LaunchResult<Option<ProcessExit>> {
        if self.reaped || self.exit.is_some() {
            return Ok(self.exit.clone());
        }
        #[cfg(not(windows))]
        let observed = peek_exit(self.identity.pid);
        #[cfg(windows)]
        let observed = self
            .child
            .try_wait()
            .map(|status| {
                status.map(|status| ProcessExit {
                    code: status.code(),
                    signal: None,
                })
            })
            .map_err(|error| LaunchError::io("observe_exit", error));
        match observed {
            Ok(exit) => {
                self.exit = exit;
                Ok(self.exit.clone())
            }
            Err(error) => {
                // ECHILD means somebody else reaped our leader. No subsequent
                // group signal is safe, because that PID may have been reused.
                self.ownership_lost = true;
                Err(error.for_process(&self.identity.process_id))
            }
        }
    }

    pub fn signal(&mut self, number: i32) -> LaunchResult<()> {
        if self.reaped || self.ownership_lost {
            return Err(LaunchError::new(
                LaunchErrorCode::CleanupFailed,
                "signal",
                "refusing to signal a reaped or unowned process group",
            )
            .for_process(&self.identity.process_id));
        }
        #[cfg(unix)]
        {
            // Apple may return EPERM for a group consisting only of zombies.
            // No signal is needed once every owned member has already exited.
            if self.exit.is_some() && !self.other_group_members()? {
                self.final_signal_sent = true;
                return Ok(());
            }
            let signal =
                Signal::try_from(number).map_err(|error| LaunchError::io("signal", error))?;
            match killpg(Pid::from_raw(self.pgid), signal) {
                Ok(()) | Err(Errno::ESRCH) => {}
                Err(Errno::EPERM)
                    if self.observe_exit()?.is_some() && !self.other_group_members()? =>
                {
                    self.final_signal_sent = true;
                    return Ok(());
                }
                Err(error) => {
                    return Err(LaunchError::new(
                        LaunchErrorCode::CleanupFailed,
                        "signal",
                        error.to_string(),
                    )
                    .for_process(&self.identity.process_id))
                }
            }
            if signal == Signal::SIGKILL {
                self.final_signal_sent = true;
                // If an unsupported entry moved itself out of the original
                // group, still terminate the leader that we have not reaped.
                if self.exit.is_none() {
                    match getpgid(Some(Pid::from_raw(self.identity.pid as i32))) {
                        Ok(group) if group.as_raw() != self.pgid => {
                            self.child
                                .kill()
                                .map_err(|error| LaunchError::io("kill", error))?;
                        }
                        Ok(_) | Err(Errno::ESRCH) => {}
                        Err(error) => return Err(LaunchError::io("group_check", error)),
                    }
                }
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            let result = if number == 9 {
                self.final_signal_sent = true;
                self.child.kill()
            } else if self
                .child
                .active_processes()
                .map_err(|e| LaunchError::io("signal", e))?
                == 0
            {
                Ok(())
            } else {
                self.child.interrupt()
            };
            result.map_err(|e| LaunchError::io("signal", e))
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = number;
            Err(LaunchError::new(
                LaunchErrorCode::Unsupported,
                "signal",
                "Unix process groups required",
            ))
        }
    }

    pub fn reap(&mut self) -> LaunchResult<()> {
        if self.reaped {
            return Ok(());
        }
        if !self.final_signal_sent || self.exit.is_none() || self.ownership_lost {
            return Err(LaunchError::new(
                LaunchErrorCode::CleanupFailed,
                "reap",
                "group cleanup and WNOWAIT exit observation must precede reap",
            ));
        }
        #[cfg(windows)]
        let waited = self.child.reap_ready();
        #[cfg(not(windows))]
        let waited = self.child.wait();
        waited.map_err(|error| {
            self.ownership_lost = true;
            LaunchError::new(LaunchErrorCode::CleanupFailed, "reap", error.to_string())
        })?;
        self.reaped = true;
        #[cfg(any(unix, windows))]
        {
            if let Some(pipe) = &mut self.stdout {
                pipe.bound_after_reap(
                    pipe_capacity(&pipe.reader)?,
                    pipe_readable_bytes(&pipe.reader)?,
                );
            }
            if let Some(pipe) = &mut self.stderr {
                pipe.bound_after_reap(
                    pipe_capacity(&pipe.reader)?,
                    pipe_readable_bytes(&pipe.reader)?,
                );
            }
        }
        Ok(())
    }

    pub fn output(&mut self, budget: usize) -> LaunchResult<OutputBatch> {
        let mut batch = OutputBatch {
            records: Vec::new(),
        };
        if let Some(reader) = &mut self.stdout {
            reader.drain(budget / 2, &mut batch)?;
        }
        if let Some(reader) = &mut self.stderr {
            reader.drain(budget / 2, &mut batch)?;
        }
        Ok(batch)
    }

    pub fn output_closed(&self) -> bool {
        self.stdout.as_ref().is_none_or(|pipe| pipe.closed)
            && self.stderr.as_ref().is_none_or(|pipe| pipe.closed)
    }

    pub fn has_reaped_output_residue(&self) -> bool {
        self.stdout
            .as_ref()
            .is_some_and(LineReader::has_reaped_residue)
            || self
                .stderr
                .as_ref()
                .is_some_and(LineReader::has_reaped_residue)
    }

    /// Check the original group while its unreaped leader reserves the PGID.
    pub fn other_group_members(&self) -> LaunchResult<bool> {
        #[cfg(not(windows))]
        {
            group_has_other_live_members(self.pgid, self.identity.pid)
        }
        #[cfg(windows)]
        {
            let active = self
                .child
                .active_processes()
                .map_err(|e| LaunchError::io("group_check", e))?;
            let leader = u32::from(
                self.child
                    .try_wait()
                    .map_err(|e| LaunchError::io("group_check", e))?
                    .is_none(),
            );
            Ok(active > leader)
        }
    }

    pub fn abandon_cleanup(&mut self) {
        self.ownership_lost = true;
    }
}

#[cfg(unix)]
fn pipe_readable_bytes(fd: &impl std::os::fd::AsRawFd) -> LaunchResult<usize> {
    let mut bytes: nix::libc::c_int = 0;
    // SAFETY: FIONREAD writes one c_int and retains no pointer. The exclusively
    // owned read descriptor is still open, and owned writers have all exited.
    let result = unsafe { nix::libc::ioctl(fd.as_raw_fd(), nix::libc::FIONREAD, &mut bytes) };
    if result == -1 {
        return Err(LaunchError::io(
            "pipe_snapshot",
            std::io::Error::last_os_error(),
        ));
    }
    usize::try_from(bytes).map_err(|error| LaunchError::io("pipe_snapshot", error))
}

#[cfg(target_os = "linux")]
fn pipe_capacity(fd: &impl std::os::fd::AsFd) -> LaunchResult<usize> {
    fcntl(fd, FcntlArg::F_GETPIPE_SZ)
        .map(|capacity| capacity as usize)
        .map_err(|error| LaunchError::io("pipe_capacity", error))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn pipe_capacity(_fd: &impl std::os::fd::AsFd) -> LaunchResult<usize> {
    // macOS exposes no F_GETPIPE_SZ. This conservative allowance exceeds its
    // ordinary pipe capacity; memory remains bounded by LineReader's buffers.
    Ok(1024 * 1024)
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped && !self.ownership_lost {
            let _ = self.signal(9);
            let now = Instant::now();
            let deadline = now.checked_add(Duration::from_millis(250)).unwrap_or(now);
            while Instant::now() < deadline {
                match self.observe_exit() {
                    Ok(Some(_)) => {
                        if matches!(self.other_group_members(), Ok(false)) {
                            let _ = self.reap();
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(5)),
                    Err(_) => break,
                }
            }
        }
        let _ = fs::remove_file(&self.bootstrap_path);
    }
}

#[cfg(target_os = "linux")]
fn group_has_other_live_members(pgid: i32, leader: u32) -> LaunchResult<bool> {
    for entry in fs::read_dir("/proc").map_err(|error| LaunchError::io("group_check", error))? {
        let entry = entry.map_err(|error| LaunchError::io("group_check", error))?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        if pid == leader {
            continue;
        }
        let stat = match fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if vanished_process(&error) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                use std::os::unix::fs::MetadataExt;
                match fs::metadata(entry.path()) {
                    // We may not inspect another user's process under hidepid.
                    // A service must retain visibility of its own UID's group.
                    Ok(metadata) if metadata.uid() != unsafe { nix::libc::geteuid() } => continue,
                    Err(metadata_error) if vanished_process(&metadata_error) => continue,
                    _ => return Err(LaunchError::io("group_check", error)),
                }
            }
            Err(error) => return Err(LaunchError::io("group_check", error)),
        };
        let fields = stat
            .rsplit_once(") ")
            .map(|(_, fields)| fields.split_whitespace().take(3).collect::<Vec<_>>())
            .filter(|fields| fields.len() == 3)
            .ok_or_else(|| {
                LaunchError::new(
                    LaunchErrorCode::CleanupFailed,
                    "group_check",
                    "invalid /proc process stat",
                )
            })?;
        let group = fields[2]
            .parse::<i32>()
            .map_err(|error| LaunchError::io("group_check", error))?;
        if group == pgid && !matches!(fields[0], "Z" | "X") {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn vanished_process(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(nix::libc::ESRCH)
}

#[cfg(target_os = "macos")]
fn group_has_other_live_members(pgid: i32, leader: u32) -> LaunchResult<bool> {
    use nix::libc;
    // PROC_PGRP_ONLY = 2 and SZOMB = 5 in the macOS SDK headers.
    const PROC_PGRP_ONLY: u32 = 2;
    let mut pids = [0_i32; 4096];
    let bytes = std::mem::size_of_val(&pids);
    // SAFETY: writable pid_t array has exactly the supplied size. libproc
    // neither retains the pointer nor writes beyond this buffer.
    let written = unsafe {
        libc::proc_listpids(
            PROC_PGRP_ONLY,
            pgid as u32,
            pids.as_mut_ptr().cast(),
            bytes as i32,
        )
    };
    if written < 0 || written as usize >= bytes {
        return Err(LaunchError::new(
            LaunchErrorCode::CleanupFailed,
            "group_check",
            "cannot enumerate complete process group",
        ));
    }
    let count = written as usize / std::mem::size_of::<i32>();
    for pid in &pids[..count] {
        if *pid <= 0 || *pid as u32 == leader {
            continue;
        }
        // SAFETY: proc_bsdinfo is the output layout required by PIDTBSDINFO.
        let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
        let size = std::mem::size_of_val(&info);
        let result = unsafe {
            libc::proc_pidinfo(
                *pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size as i32,
            )
        };
        if result != size as i32 {
            if Errno::last() == Errno::ESRCH {
                continue;
            }
            return Err(LaunchError::new(
                LaunchErrorCode::CleanupFailed,
                "group_check",
                "cannot read process group member status",
            ));
        }
        if info.pbi_pgid == pgid as u32 && info.pbi_status != 5 {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn group_has_other_live_members(_pgid: i32, _leader: u32) -> LaunchResult<bool> {
    Err(LaunchError::new(
        LaunchErrorCode::Unsupported,
        "group_check",
        "unsupported platform",
    ))
}

#[cfg(unix)]
fn nonblocking(fd: &impl std::os::fd::AsFd) -> LaunchResult<()> {
    let flags = OFlag::from_bits_truncate(
        fcntl(fd, FcntlArg::F_GETFL).map_err(|error| LaunchError::io("pipe", error))?,
    );
    fcntl(fd, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))
        .map_err(|error| LaunchError::io("pipe", error))?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn peek_exit(pid: u32) -> LaunchResult<Option<ProcessExit>> {
    use nix::sys::wait::{waitid, Id, WaitPidFlag, WaitStatus};
    match waitid(
        Id::Pid(Pid::from_raw(pid as i32)),
        WaitPidFlag::WEXITED | WaitPidFlag::WNOHANG | WaitPidFlag::WNOWAIT,
    ) {
        Ok(WaitStatus::Exited(_, code)) => Ok(Some(ProcessExit {
            code: Some(code),
            signal: None,
        })),
        Ok(WaitStatus::Signaled(_, signal, _)) => Ok(Some(ProcessExit {
            code: None,
            signal: Some(signal as i32),
        })),
        Ok(WaitStatus::StillAlive) => Ok(None),
        Ok(_) => Err(LaunchError::new(
            LaunchErrorCode::CleanupFailed,
            "wait",
            "unexpected waitid status",
        )),
        Err(Errno::EINTR) => Ok(None),
        Err(error) => Err(LaunchError::new(
            LaunchErrorCode::CleanupFailed,
            "wait",
            error.to_string(),
        )),
    }
}

#[cfg(target_os = "macos")]
fn peek_exit(pid: u32) -> LaunchResult<Option<ProcessExit>> {
    use nix::libc;
    // nix 0.31.3 exposes WNOWAIT on Apple but gates its waitid wrapper away.
    // SAFETY: the initialized output buffer has the exact libc siginfo_t
    // layout; P_PID selects only our still-unreaped child; no references are
    // retained by waitid. Zero initialization identifies the WNOHANG case.
    let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        let error = Errno::last();
        if error == Errno::EINTR {
            return Ok(None);
        }
        return Err(LaunchError::new(
            LaunchErrorCode::CleanupFailed,
            "wait",
            error.to_string(),
        ));
    }
    if info.si_pid == 0 {
        return Ok(None);
    }
    match info.si_code {
        libc::CLD_EXITED => Ok(Some(ProcessExit {
            code: Some(info.si_status),
            signal: None,
        })),
        libc::CLD_KILLED | libc::CLD_DUMPED => Ok(Some(ProcessExit {
            code: None,
            signal: Some(info.si_status),
        })),
        _ => Err(LaunchError::new(
            LaunchErrorCode::CleanupFailed,
            "wait",
            "unexpected waitid status",
        )),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn peek_exit(_pid: u32) -> LaunchResult<Option<ProcessExit>> {
    Err(LaunchError::new(
        LaunchErrorCode::Unsupported,
        "wait",
        "WNOWAIT support is implemented for Linux and macOS",
    ))
}

#[cfg(windows)]
fn pipe_capacity(_: &crate::windows_process::Pipe) -> LaunchResult<usize> {
    Ok(1024 * 1024)
}
#[cfg(windows)]
fn pipe_readable_bytes(pipe: &crate::windows_process::Pipe) -> LaunchResult<usize> {
    pipe.available()
        .map_err(|e| LaunchError::io("pipe_snapshot", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reap_snapshot_preserves_buffered_short_lines_until_the_tail_is_checked() {
        let mut reader = LineReader::new(
            std::io::Cursor::new(vec![b'\n'; 65_536]),
            OutputStream::Stdout,
        );
        let mut first = OutputBatch {
            records: Vec::new(),
        };
        reader.drain(32 * 1024, &mut first).unwrap();
        assert_eq!(first.records.len(), 256);
        assert_eq!(reader.pending_end - reader.pending_start, 4096 - 256);
        reader.bound_after_reap(65_536, 65_536 - 4096);
        let mut lines = first.records.len();
        while lines < 65_536 {
            assert!(reader.has_reaped_residue());
            let mut batch = OutputBatch {
                records: Vec::new(),
            };
            reader.drain(32 * 1024, &mut batch).unwrap();
            assert_eq!(batch.records.len(), 256);
            lines += batch.records.len();
        }
        // Exhausting the last record quota is not evidence of an open writer.
        // The next visit must be allowed to observe the pipe's EOF.
        assert!(reader.has_reaped_residue());
        let mut tail = OutputBatch {
            records: Vec::new(),
        };
        reader.drain(32 * 1024, &mut tail).unwrap();
        assert!(tail.records.is_empty());
        assert!(reader.closed);
        assert!(!reader.has_reaped_residue());
    }

    #[test]
    fn repeated_record_quota_hits_cannot_replenish_the_reap_snapshot() {
        let mut reader = LineReader::new(std::io::repeat(b'\n'), OutputStream::Stdout);
        reader.bound_after_reap(1024 * 1024, 256);
        for _ in 0..300 {
            let mut batch = OutputBatch {
                records: Vec::new(),
            };
            reader.drain(32 * 1024, &mut batch).unwrap();
            assert_eq!(batch.records.len(), 256);
            assert!(!reader.has_reaped_residue());
        }
        assert!(!reader.closed, "the synthetic escaped writer remains open");
    }

    #[test]
    fn raw_post_reap_budget_counts_both_short_lines_and_unterminated_bytes() {
        for byte in *b"\nx" {
            let mut reader = LineReader::new(std::io::repeat(byte), OutputStream::Stdout);
            reader.bound_after_reap(4096, 0);
            let error = (0..40)
                .find_map(|_| {
                    let mut batch = OutputBatch {
                        records: Vec::new(),
                    };
                    reader.drain(32 * 1024, &mut batch).err()
                })
                .expect("raw reads must fail after the fixed post-reap byte allowance");
            assert_eq!(error.code, LaunchErrorCode::CleanupFailed);
            assert_eq!(error.stage, "output_drain");
            assert!(error.message.contains("8192 new bytes after leader reap"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn readable_pipe_snapshot_counts_only_the_current_kernel_residue() {
        let (read, write) = nix::unistd::pipe().unwrap();
        let mut read = fs::File::from(read);
        let mut write = fs::File::from(write);
        assert_eq!(pipe_readable_bytes(&read).unwrap(), 0);
        write.write_all(&[b'\n'; 513]).unwrap();
        assert_eq!(pipe_readable_bytes(&read).unwrap(), 513);
        read.read_exact(&mut [0; 13]).unwrap();
        assert_eq!(pipe_readable_bytes(&read).unwrap(), 500);
        drop(write);
        assert_eq!(pipe_readable_bytes(&read).unwrap(), 500);
        read.read_exact(&mut [0; 500]).unwrap();
        assert_eq!(pipe_readable_bytes(&read).unwrap(), 0);
    }

    #[test]
    fn relative_temporary_root_yields_a_private_absolute_runtime_directory() {
        let directory = RunDirectory::create_in(PathBuf::from(".")).unwrap();
        let path = directory.path.clone();
        assert!(path.is_absolute());
        assert!(path.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        drop(directory);
        assert!(!path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_disappearance_does_not_hide_permission_or_io_failures() {
        assert!(vanished_process(&std::io::Error::from_raw_os_error(
            nix::libc::ESRCH
        )));
        assert!(vanished_process(&std::io::Error::from_raw_os_error(
            nix::libc::ENOENT
        )));
        assert!(!vanished_process(&std::io::Error::from_raw_os_error(
            nix::libc::EACCES
        )));
        assert!(!vanished_process(&std::io::Error::from_raw_os_error(
            nix::libc::EIO
        )));
    }
}
