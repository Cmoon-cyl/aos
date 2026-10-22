//! Unix implementation of the bounded script runner. Pipe I/O and process
//! control stay on the calling worker; nothing survives as a reader thread.
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};

use super::{
    parse_script_result, Control, HookError, HookErrorKind, HookOutcome, Result, Script,
    MAX_STDERR_BYTES,
};

pub(super) fn run(script: &Script, input: Vec<u8>, control: &Control<'_>) -> Result<HookOutcome> {
    let (output, stderr) = capture(script, input, control)?;
    match parse_script_result(&output) {
        Ok(mut outcome) => {
            outcome.stderr = stderr;
            Ok(outcome)
        }
        Err(mut error) => {
            error.stderr = stderr;
            Err(error)
        }
    }
}

pub(super) fn capture(
    script: &Script,
    input: Vec<u8>,
    control: &Control<'_>,
) -> Result<(Vec<u8>, String)> {
    control.check()?;
    let mut command = Command::new(&script.executable);
    command
        .args(&script.argv)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    #[cfg(target_os = "macos")]
    let spawned = aos_launch::macos_spawn::spawn_inheriting(&mut command);
    #[cfg(not(target_os = "macos"))]
    let spawned = command.spawn();
    let child = spawned.map_err(|error| {
        HookError::new(HookErrorKind::Spawn, format!("start script hook: {error}"))
    })?;
    let mut process = ManagedChild::new(child);
    // Construct the guard before any fallible setup so errors also kill and
    // reap the process group. No application-owned file descriptor is touched.
    let mut pipes = Pipes::new(&mut process.child)?;
    let pumped = pump(&mut process, &mut pipes, &input, script, control);
    let cleanup = process.finish();
    // The group is now stopped. Collect bytes written between the last pump
    // read and the exit notification, without waiting on inherited pipes.
    let drained = pipes.drain(script);
    let result = (|| {
        let status = cleanup?;
        pumped?;
        drained?;
        if !status.success() {
            return Err(HookError::new(
                HookErrorKind::Exit,
                format!("script hook exited unsuccessfully: {status}"),
            ));
        }
        if let Some(error) = pipes.input_error.take() {
            return Err(error);
        }
        control.check()?;
        Ok(std::mem::take(&mut pipes.output))
    })();
    let mut stderr = String::from_utf8_lossy(&pipes.errors).into_owned();
    // Invalid bytes can expand to three-byte replacement characters. Keep the
    // diagnostic text within the same byte limit as the captured stream.
    if stderr.len() > MAX_STDERR_BYTES {
        let mut end = MAX_STDERR_BYTES;
        while !stderr.is_char_boundary(end) {
            end -= 1;
        }
        stderr.truncate(end);
    }
    match result {
        Ok(output) => Ok((output, stderr)),
        Err(mut error) => {
            error.stderr = stderr;
            Err(error)
        }
    }
}

struct ManagedChild {
    child: Child,
    group: libc::pid_t,
    reaped: bool,
}

impl ManagedChild {
    fn new(child: Child) -> Self {
        Self {
            group: child.id() as libc::pid_t,
            child,
            reaped: false,
        }
    }

    fn exited(&self) -> Result<Option<bool>> {
        // WNOWAIT keeps the leader waitable until after group cleanup. Reaping
        // it first would allow its PID/PGID to be reused before killpg.
        // SAFETY: siginfo_t is a C POD and waitid writes within the live value;
        // the PID is this process's owned direct child.
        let mut information: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                self.child.id() as libc::id_t,
                &mut information,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == -1 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(None)
            } else {
                Err(HookError::new(
                    HookErrorKind::Io,
                    format!("inspect script hook exit: {error}"),
                ))
            };
        }
        // SAFETY: waitid succeeded; zero si_pid means no event. si_status is
        // read only for the reported child-exit event.
        if unsafe { information.si_pid() } == 0 {
            return Ok(None);
        }
        Ok(Some(
            information.si_code == libc::CLD_EXITED && unsafe { information.si_status() } == 0,
        ))
    }

    fn finish(&mut self) -> Result<ExitStatus> {
        let group_result = kill_group(self.group);
        // Also cover a direct child that violated the no-daemonization contract.
        // A zombie leader's original exit status is preserved by this signal.
        let _ = self.child.kill();
        let waited = self.child.wait();
        self.reaped = waited.is_ok();
        if let Err(error) = group_result {
            // Darwin filters zombies out of killpg and can return EPERM for a
            // group containing only the exited leader. Accept this only after
            // reaping it and proving the entire group disappeared. Do not send
            // another signal after reaping: the PID/PGID could now be reused.
            #[cfg(target_os = "macos")]
            let disappeared = error.raw_os_error() == Some(libc::EPERM)
                && self.reaped
                && unsafe { libc::kill(-self.group, 0) } == -1
                && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            #[cfg(not(target_os = "macos"))]
            let disappeared = false;
            if !disappeared {
                return Err(HookError::new(
                    HookErrorKind::Cleanup,
                    format!("terminate script hook process group: {error}"),
                ));
            }
        }
        waited.map_err(|error| {
            HookError::new(HookErrorKind::Cleanup, format!("reap script hook: {error}"))
        })
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = kill_group(self.group);
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn kill_group(group: libc::pid_t) -> io::Result<()> {
    // SAFETY: Command::process_group(0) created this positive child PGID; a
    // negative argument targets only that hook group, not the caller/provider.
    if unsafe { libc::kill(-group, libc::SIGKILL) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

struct Pipes {
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    output: Vec<u8>,
    errors: Vec<u8>,
    input_error: Option<HookError>,
}

impl Pipes {
    fn new(child: &mut Child) -> Result<Self> {
        let pipes = Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            output: Vec::new(),
            errors: Vec::new(),
            input_error: None,
        };
        for descriptor in [
            pipes.stdin.as_ref().map(AsRawFd::as_raw_fd),
            pipes.stdout.as_ref().map(AsRawFd::as_raw_fd),
            pipes.stderr.as_ref().map(AsRawFd::as_raw_fd),
        ] {
            let descriptor = descriptor
                .ok_or_else(|| HookError::new(HookErrorKind::Io, "script pipe was not created"))?;
            nonblocking(descriptor)?;
        }
        Ok(pipes)
    }

    fn complete(&self) -> bool {
        self.stdin.is_none() && self.stdout.is_none() && self.stderr.is_none()
    }

    fn drain(&mut self, script: &Script) -> Result<()> {
        loop {
            let output = read_pipe(
                &mut self.stdout,
                &mut self.output,
                script.max_output_bytes,
                "stdout",
            );
            let errors = read_pipe(
                &mut self.stderr,
                &mut self.errors,
                MAX_STDERR_BYTES,
                "stderr",
            );
            // Read both streams even when the first reached its bound, so the
            // captured stderr still explains failures with excessive stdout.
            let progress = output? | errors?;
            if !progress {
                return Ok(());
            }
        }
    }

    fn wait(&self, control: &Control<'_>) -> Result<()> {
        let mut descriptors = [
            libc::pollfd {
                fd: self.stdin.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLOUT,
                revents: 0,
            },
            libc::pollfd {
                fd: self.stdout.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.stderr.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: the array is initialized, remains live for poll, and all
        // nonnegative descriptors are owned pipes. Poll never exceeds 10 ms.
        let result = unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                control.poll_timeout_ms(),
            )
        };
        if result == -1 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(HookError::new(
                    HookErrorKind::Io,
                    format!("poll script hook pipes: {error}"),
                ));
            }
        }
        Ok(())
    }
}

fn nonblocking(descriptor: libc::c_int) -> Result<()> {
    // SAFETY: the descriptor belongs to one of this child's live parent-side
    // pipes. F_SETFL preserves its current flags and adds only O_NONBLOCK.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1
        || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
    {
        return Err(HookError::new(
            HookErrorKind::Io,
            format!("configure script hook pipe: {}", io::Error::last_os_error()),
        ));
    }
    Ok(())
}

fn pump(
    child: &mut ManagedChild,
    pipes: &mut Pipes,
    input: &[u8],
    script: &Script,
    control: &Control<'_>,
) -> Result<()> {
    let mut written: usize = 0;
    loop {
        control.check()?;
        let mut progress = false;
        if let Some(stdin) = pipes.stdin.as_mut() {
            let end = input.len().min(written.saturating_add(64 * 1024));
            match stdin.write(&input[written..end]) {
                Ok(0) => {
                    return Err(HookError::new(
                        HookErrorKind::Io,
                        "hook stdin write made no progress",
                    ));
                }
                Ok(count) => {
                    written += count;
                    progress = true;
                    if written == input.len() {
                        // EOF is part of each one-message invocation.
                        pipes.stdin.take();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                    pipes.stdin.take();
                    pipes.input_error = Some(HookError::new(
                        HookErrorKind::Io,
                        "script closed stdin before receiving the complete JSON message",
                    ));
                    progress = true;
                }
                Err(error) => {
                    return Err(HookError::new(
                        HookErrorKind::Io,
                        format!("write hook stdin: {error}"),
                    ));
                }
            }
        }
        progress |= read_pipe(
            &mut pipes.stdout,
            &mut pipes.output,
            script.max_output_bytes,
            "stdout",
        )?;
        progress |= read_pipe(
            &mut pipes.stderr,
            &mut pipes.errors,
            MAX_STDERR_BYTES,
            "stderr",
        )?;
        if let Some(success) = child.exited()? {
            // A failed leader is already a terminal event. A successful one
            // must also close its inherited streams: surviving descendants
            // cannot turn a half-completed invocation into a successful hook.
            if !success || pipes.complete() {
                return Ok(());
            }
        }
        if !progress {
            pipes.wait(control)?;
        }
    }
}

fn read_pipe<T: Read>(
    pipe: &mut Option<T>,
    bytes: &mut Vec<u8>,
    limit: usize,
    name: &str,
) -> Result<bool> {
    let mut progress = false;
    // Bound work per turn so an endlessly writing child cannot starve checks
    // for cancellation or the earlier of the two steady deadlines.
    for _ in 0..8 {
        let Some(reader) = pipe.as_mut() else {
            break;
        };
        let mut buffer = [0_u8; 8192];
        match reader.read(&mut buffer) {
            Ok(0) => {
                pipe.take();
                return Ok(true);
            }
            Ok(count) => {
                let remaining = limit.saturating_sub(bytes.len());
                bytes.extend_from_slice(&buffer[..count.min(remaining)]);
                if count > remaining {
                    return Err(HookError::new(
                        HookErrorKind::OutputLimit,
                        format!("hook {name} exceeded {limit} bytes"),
                    ));
                }
                progress = true;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(HookError::new(
                    HookErrorKind::Io,
                    format!("read hook {name}: {error}"),
                ));
            }
        }
    }
    Ok(progress)
}
