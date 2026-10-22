//! Windows bounded hook runner; every descendant remains in the owned Job.
use aos_launch::windows_process::{
    Pipe as ChildStdin, Pipe as ChildStdout, Pipe as ChildStderr, Process as Child,
};
use std::io::{self, Read, Write};
use std::process::{Command, ExitStatus};

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
    command.args(&script.argv);
    let child = Child::spawn(&command, true).map_err(|error| {
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
}
impl ManagedChild {
    fn new(child: Child) -> Self {
        Self { child }
    }
    fn exited(&self) -> Result<Option<bool>> {
        self.child
            .try_wait()
            .map(|status| status.map(|s| s.success()))
            .map_err(|e| HookError::new(HookErrorKind::Io, format!("inspect hook exit: {e}")))
    }
    fn finish(&mut self) -> Result<ExitStatus> {
        self.child
            .kill()
            .and_then(|()| self.child.wait())
            .map_err(|e| HookError::new(HookErrorKind::Cleanup, format!("terminate hook job: {e}")))
    }
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
        control.check()?;
        std::thread::sleep(std::time::Duration::from_millis(
            control.poll_timeout_ms() as u64
        ));
        Ok(())
    }
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
