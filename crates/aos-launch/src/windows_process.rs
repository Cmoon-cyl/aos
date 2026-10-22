//! Owned Windows process trees and bounded, nonblocking pipe I/O.
//!
//! The job is assigned atomically by CreateProcessW, before user code runs.
//! Closing the last job handle kills descendants, including those that create
//! a new console group. No operation enumerates or kills processes by PID.
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io::{self, Read, Write},
    mem::{size_of, zeroed},
    os::windows::{ffi::OsStrExt, process::ExitStatusExt},
    process::{Command, ExitStatus},
    ptr::{null, null_mut},
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::*,
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::*,
    System::{Console::*, JobObjects::*, Pipes::*, Threading::*},
};

struct Handle(HANDLE);
impl Handle {
    fn checked(raw: HANDLE) -> io::Result<Self> {
        if raw.is_null() || raw == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(raw))
        }
    }
}
// SAFETY: handles are uniquely owned and Win32 kernel handle operations may be
// performed from any thread. Pipe objects themselves are not shared.
unsafe impl Send for Handle {}
// SAFETY: concurrent queries operate on the same live immutable kernel handle.
unsafe impl Sync for Handle {}
impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn check(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut value: Vec<_> = value.encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "embedded NUL"));
    }
    value.push(0);
    Ok(value)
}

/// A parent pipe uses PIPE_NOWAIT, never a blocking reader/writer thread.
/// In byte mode Windows returns the bytes that fit, including zero if full.
/// The caller pumps both outputs and retries writes under its own deadline.
pub struct Pipe {
    handle: Handle,
}
impl Pipe {
    fn pair(parent_writes: bool) -> io::Result<(Self, Handle)> {
        let name = wide(OsStr::new(&format!(
            r"\\.\pipe\aos-{}",
            uuid::Uuid::new_v4()
        )))?;
        let security = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        // A unique first-instance local pipe prevents another server from
        // replacing this endpoint. Only the child endpoint is inheritable.
        let parent = Handle::checked(unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                if parent_writes {
                    PIPE_ACCESS_OUTBOUND
                } else {
                    PIPE_ACCESS_INBOUND
                } | FILE_FLAG_FIRST_PIPE_INSTANCE,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_NOWAIT | PIPE_REJECT_REMOTE_CLIENTS,
                1,
                65536,
                65536,
                0,
                null(),
            )
        })?;
        let child = Handle::checked(unsafe {
            CreateFileW(
                name.as_ptr(),
                if parent_writes {
                    GENERIC_READ
                } else {
                    GENERIC_WRITE
                },
                0,
                &security,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            )
        })?;
        let connected = unsafe { ConnectNamedPipe(parent.0, null_mut()) };
        if connected == 0 && unsafe { GetLastError() } != ERROR_PIPE_CONNECTED {
            return Err(io::Error::last_os_error());
        }
        Ok((Self { handle: parent }, child))
    }
    pub fn available(&self) -> io::Result<usize> {
        let mut bytes = 0;
        if unsafe {
            PeekNamedPipe(
                self.handle.0,
                null_mut(),
                0,
                null_mut(),
                &mut bytes,
                null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            if matches!(error.raw_os_error(), Some(x) if x == ERROR_BROKEN_PIPE as i32 || x == ERROR_PIPE_NOT_CONNECTED as i32)
            {
                return Ok(0);
            }
            return Err(error);
        }
        Ok(bytes as usize)
    }
}
impl Read for Pipe {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut count = 0;
        if unsafe {
            ReadFile(
                self.handle.0,
                bytes.as_mut_ptr(),
                bytes.len().min(u32::MAX as usize) as u32,
                &mut count,
                null_mut(),
            )
        } != 0
        {
            // A successful zero-byte pipe read represents a zero-byte write,
            // not EOF. Only a broken/disconnected pipe closes this stream.
            return if count == 0 {
                Err(io::ErrorKind::WouldBlock.into())
            } else {
                Ok(count as usize)
            };
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error().map(|n| n as u32) {
            Some(ERROR_NO_DATA) => Err(io::ErrorKind::WouldBlock.into()),
            Some(ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED) => Ok(0),
            _ => Err(error),
        }
    }
}
impl Write for Pipe {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut count = 0;
        check(unsafe {
            WriteFile(
                self.handle.0,
                bytes.as_ptr(),
                bytes.len().min(65536) as u32,
                &mut count,
                null_mut(),
            )
        })?;
        if count == 0 {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(count as usize)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Attributes {
    storage: Vec<usize>,
}
impl Attributes {
    fn new() -> io::Result<Self> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes);
        }
        let mut storage = vec![0_usize; bytes.div_ceil(size_of::<usize>())];
        check(unsafe {
            InitializeProcThreadAttributeList(storage.as_mut_ptr().cast(), 2, 0, &mut bytes)
        })?;
        Ok(Self { storage })
    }
    fn pointer(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_mut_ptr().cast()
    }
    fn set(&mut self, key: usize, value: &mut [HANDLE]) -> io::Result<()> {
        check(unsafe {
            UpdateProcThreadAttribute(
                self.pointer(),
                0,
                key,
                value.as_mut_ptr().cast(),
                size_of_val(value),
                null_mut(),
                null(),
            )
        })
    }
}
impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe {
            DeleteProcThreadAttributeList(self.pointer());
        }
    }
}

/// Retains the process handle and job handle until all owned work is finished.
pub struct Process {
    process: Handle,
    job: std::sync::Arc<Handle>,
    pid: u32,
    pub stdin: Option<Pipe>,
    pub stdout: Option<Pipe>,
    pub stderr: Option<Pipe>,
}
impl Process {
    /// Commands used here inherit the environment and override explicit keys.
    /// Stdio is always owned pipes (stdin is NUL unless `input` is true).
    pub fn spawn(command: &Command, input: bool) -> io::Result<Self> {
        let program = std::path::Path::new(command.get_program());
        if !program.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows owned process requires an absolute executable path",
            ));
        }
        validate_executable(program)?;
        ensure_console()?;
        let application = wide(program.as_os_str())?;
        let mut line = Vec::new();
        for arg in std::iter::once(command.get_program()).chain(command.get_args()) {
            if !line.is_empty() {
                line.push(32);
            }
            quote(arg, &mut line)?;
        }
        line.push(0);
        let mut environment: BTreeMap<Vec<u16>, (std::ffi::OsString, std::ffi::OsString)> =
            std::env::vars_os()
                .map(|(key, value)| {
                    (
                        key.to_string_lossy()
                            .to_uppercase()
                            .encode_utf16()
                            .collect(),
                        (key, value),
                    )
                })
                .collect();
        for (key, value) in command.get_envs() {
            let normalized = key
                .to_string_lossy()
                .to_uppercase()
                .encode_utf16()
                .collect::<Vec<_>>();
            if let Some(value) = value {
                environment.insert(normalized, (key.into(), value.into()));
            } else {
                environment.remove(&normalized);
            }
        }
        let mut block = Vec::new();
        for (_, (key, value)) in environment {
            let mut item = key;
            item.push("=");
            item.push(value);
            block.extend(wide(&item)?);
        }
        if block.is_empty() {
            block.push(0);
        }
        block.push(0);
        let cwd = command
            .get_current_dir()
            .map(|p| wide(p.as_os_str()))
            .transpose()?;
        let job = Handle::checked(unsafe { CreateJobObjectW(null(), null()) })?;
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        check(unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of_val(&limits) as u32,
            )
        })?;
        let (stdout, out) = Pipe::pair(false)?;
        let (stderr, err) = Pipe::pair(false)?;
        let (stdin, inn) = if input {
            let (parent, child) = Pipe::pair(true)?;
            (Some(parent), child)
        } else {
            let security = SECURITY_ATTRIBUTES {
                nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                lpSecurityDescriptor: null_mut(),
                bInheritHandle: 1,
            };
            (
                None,
                Handle::checked(unsafe {
                    CreateFileW(
                        wide(OsStr::new("NUL"))?.as_ptr(),
                        GENERIC_READ,
                        FILE_SHARE_READ | FILE_SHARE_WRITE,
                        &security,
                        OPEN_EXISTING,
                        FILE_ATTRIBUTE_NORMAL,
                        null_mut(),
                    )
                })?,
            )
        };
        let mut attributes = Attributes::new()?;
        let mut handles = [inn.0, out.0, err.0];
        let mut jobs = [job.0];
        attributes.set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize, &mut handles)?;
        attributes.set(PROC_THREAD_ATTRIBUTE_JOB_LIST as usize, &mut jobs)?;
        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = inn.0;
        startup.StartupInfo.hStdOutput = out.0;
        startup.StartupInfo.hStdError = err.0;
        startup.lpAttributeList = attributes.pointer();
        let mut info: PROCESS_INFORMATION = unsafe { zeroed() };
        check(unsafe {
            CreateProcessW(
                application.as_ptr(),
                line.as_mut_ptr(),
                null(),
                null(),
                1,
                EXTENDED_STARTUPINFO_PRESENT
                    | CREATE_UNICODE_ENVIRONMENT
                    | CREATE_NEW_PROCESS_GROUP,
                block.as_ptr().cast(),
                cwd.as_ref().map_or(null(), |v| v.as_ptr()),
                &startup.StartupInfo,
                &mut info,
            )
        })?;
        let process = Handle(info.hProcess);
        drop(Handle(info.hThread));
        Ok(Self {
            process,
            job: std::sync::Arc::new(job),
            pid: info.dwProcessId,
            stdin,
            stdout: Some(stdout),
            stderr: Some(stderr),
        })
    }
    pub fn owned_job(&self) -> OwnedJob {
        OwnedJob(self.job.clone())
    }
    pub fn id(&self) -> u32 {
        self.pid
    }
    pub fn try_wait(&self) -> io::Result<Option<ExitStatus>> {
        match unsafe { WaitForSingleObject(self.process.0, 0) } {
            WAIT_TIMEOUT => Ok(None),
            WAIT_OBJECT_0 => {
                let mut code = 0;
                check(unsafe { GetExitCodeProcess(self.process.0, &mut code) })?;
                Ok(Some(ExitStatus::from_raw(code)))
            }
            _ => Err(io::Error::last_os_error()),
        }
    }
    pub fn active_processes(&self) -> io::Result<u32> {
        let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
        check(unsafe {
            QueryInformationJobObject(
                self.job.0,
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                size_of_val(&accounting) as u32,
                null_mut(),
            )
        })?;
        Ok(accounting.ActiveProcesses)
    }
    pub fn interrupt(&self) -> io::Result<()> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        let signaled = check(unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, self.pid) });
        if signaled.is_err() && self.try_wait()?.is_some() {
            return Ok(());
        }
        signaled
    }
    pub fn kill(&self) -> io::Result<()> {
        check(unsafe { TerminateJobObject(self.job.0, 1) })
    }
    pub fn reap_ready(&self) -> io::Result<ExitStatus> {
        match self.try_wait()? {
            Some(status) if self.active_processes()? == 0 => Ok(status),
            _ => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
    pub fn wait(&self) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(status) = self.try_wait()? {
                if self.active_processes()? == 0 {
                    return Ok(status);
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned Windows job cleanup exceeded two seconds",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

fn quote(arg: &OsStr, output: &mut Vec<u16>) -> io::Result<()> {
    let units = wide(arg)?;
    output.push(34);
    let mut slashes = 0;
    for &unit in &units[..units.len() - 1] {
        if unit == 92 {
            slashes += 1;
            continue;
        }
        output.extend(std::iter::repeat_n(
            92,
            if unit == 34 { slashes * 2 + 1 } else { slashes },
        ));
        slashes = 0;
        output.push(unit);
    }
    output.extend(std::iter::repeat_n(92, slashes * 2));
    output.push(34);
    Ok(())
}

/// Give background supervisors a private hidden console for targeted Ctrl-Break.
/// Call before installing handlers: AllocConsole resets the handler table.
pub fn ensure_console() -> io::Result<()> {
    static CONSOLE: std::sync::OnceLock<Result<(), i32>> = std::sync::OnceLock::new();
    CONSOLE
        .get_or_init(|| {
            if unsafe { GetConsoleCP() } != 0 {
                return Ok(());
            }
            let standard = [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE]
                .map(|kind| (kind, unsafe { GetStdHandle(kind) }));
            if unsafe { AllocConsole() } == 0 {
                return Err(io::Error::last_os_error().raw_os_error().unwrap_or(6));
            }
            for (kind, handle) in standard {
                if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
                    check(unsafe { SetStdHandle(kind, handle) })
                        .map_err(|e| e.raw_os_error().unwrap_or(6))?;
                }
            }
            unsafe {
                windows_sys::Win32::UI::WindowsAndMessaging::ShowWindow(
                    GetConsoleWindow(),
                    windows_sys::Win32::UI::WindowsAndMessaging::SW_HIDE,
                );
            }
            Ok(())
        })
        .map_err(io::Error::from_raw_os_error)
}

/// Read-only proof of membership in one particular live launch generation.
#[derive(Clone)]
pub struct OwnedJob(std::sync::Arc<Handle>);
impl OwnedJob {
    pub fn contains(&self, pid: u32) -> io::Result<bool> {
        let process = match Handle::checked(unsafe {
            OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid)
        }) {
            Ok(process) => process,
            Err(e) if e.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) => return Ok(false),
            Err(e) => return Err(e),
        };
        let mut member = 0;
        check(unsafe { IsProcessInJob(process.0, self.0 .0, &mut member) })?;
        Ok(member != 0 && unsafe { WaitForSingleObject(process.0, 0) } == WAIT_TIMEOUT)
    }
}

/// Read-only liveness query for acceptance diagnostics. Never signals a PID.
pub fn process_is_running(pid: u32) -> io::Result<bool> {
    let process = match Handle::checked(unsafe { OpenProcess(SYNCHRONIZE, 0, pid) }) {
        Ok(process) => process,
        Err(e) if e.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) => return Ok(false),
        Err(e) => return Err(e),
    };
    match unsafe { WaitForSingleObject(process.0, 0) } {
        WAIT_TIMEOUT => Ok(true),
        WAIT_OBJECT_0 => Ok(false),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Capture an output handle before console allocation and without stdio locks.
pub fn duplicate_standard_output(fd: i32) -> io::Result<StandardOutput> {
    use std::os::windows::io::FromRawHandle;
    let kind = match fd {
        1 => STD_OUTPUT_HANDLE,
        2 => STD_ERROR_HANDLE,
        _ => return Err(io::ErrorKind::InvalidInput.into()),
    };
    let original = unsafe { GetStdHandle(kind) };
    if original.is_null() || original == INVALID_HANDLE_VALUE {
        return Err(io::Error::from_raw_os_error(ERROR_INVALID_HANDLE as i32));
    }
    let mut copy = null_mut();
    check(unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            original,
            GetCurrentProcess(),
            &mut copy,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    })?;
    // SAFETY: DuplicateHandle transferred one new non-inheritable handle to us.
    Ok(StandardOutput::new(unsafe {
        std::fs::File::from_raw_handle(copy)
    }))
}

/// Private output handle: byte-exact pipes/files, Unicode interactive console.
pub struct StandardOutput {
    file: std::fs::File,
    console: bool,
}
impl StandardOutput {
    fn new(file: std::fs::File) -> Self {
        use std::os::windows::io::AsRawHandle;
        let mut mode = 0;
        let console = unsafe { GetConsoleMode(file.as_raw_handle(), &mut mode) } != 0;
        Self { file, console }
    }
}
impl Write for StandardOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        use std::os::windows::io::AsRawHandle;
        if !self.console {
            return self.file.write(bytes);
        }
        // The bounded worker slices byte buffers at 4096. Report only a whole
        // UTF-8 prefix so the caller retries the complete trailing character.
        let text = match std::str::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) if error.error_len().is_none() && error.valid_up_to() > 0 => {
                std::str::from_utf8(&bytes[..error.valid_up_to()]).unwrap()
            }
            Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        };
        let wide: Vec<u16> = text.encode_utf16().collect();
        let mut offset = 0;
        while offset < wide.len() {
            let mut written = 0;
            check(unsafe {
                WriteConsoleW(
                    self.file.as_raw_handle(),
                    wide[offset..].as_ptr().cast(),
                    (wide.len() - offset).min(u32::MAX as usize) as u32,
                    &mut written,
                    null(),
                )
            })?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            offset += written as usize;
        }
        Ok(text.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Reject command files and other non-native entries without invoking a shell.
pub fn validate_executable(path: &std::path::Path) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows executable path must be absolute",
        ));
    }
    let name = wide(path.as_os_str())?;
    let mut kind = 0;
    check(unsafe { GetBinaryTypeW(name.as_ptr(), &mut kind) })?;
    if kind != 0 && kind != 6 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows requires a native PE executable or explicit interpreter",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, OnceLock, Weak};

    #[test]
    fn console_unicode_crosses_byte_chunks_with_non_utf8_code_page() {
        use std::os::windows::{io::FromRawHandle, process::CommandExt};
        const CHILD: &str = "AOS_TEST_PRIVATE_CONSOLE";
        if std::env::var_os(CHILD).is_none() {
            let result = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "windows_process::tests::console_unicode_crosses_byte_chunks_with_non_utf8_code_page", "--nocapture"])
                .env(CHILD, "1").creation_flags(DETACHED_PROCESS).output().unwrap();
            assert!(
                result.status.success(),
                "{}{}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
            return;
        }
        // This subprocess owns a fresh hidden console; never change the user's.
        ensure_console().unwrap();
        check(unsafe { SetConsoleOutputCP(936) }).unwrap();
        let handle = Handle::checked(unsafe {
            CreateConsoleScreenBuffer(
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                null(),
                CONSOLE_TEXTMODE_BUFFER,
                null(),
            )
        })
        .unwrap();
        check(unsafe { SetConsoleScreenBufferSize(handle.0, COORD { X: 200, Y: 100 }) }).unwrap();
        check(unsafe { SetConsoleCursorPosition(handle.0, COORD { X: 0, Y: 0 }) }).unwrap();
        let mut duplicate = null_mut();
        check(unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                handle.0,
                GetCurrentProcess(),
                &mut duplicate,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        })
        .unwrap();
        let mut output = StandardOutput::new(unsafe { std::fs::File::from_raw_handle(duplicate) });
        assert!(output.console);
        let text = format!("{}中文", "x".repeat(4095));
        assert_eq!(output.write(&text.as_bytes()[..4096]).unwrap(), 4095);
        assert_eq!(output.write(&text.as_bytes()[4095..]).unwrap(), 6);
        let mut read = 0;
        let mut characters = vec![0_u16; 20000];
        check(unsafe {
            ReadConsoleOutputCharacterW(
                handle.0,
                characters.as_mut_ptr(),
                characters.len() as u32,
                COORD { X: 0, Y: 0 },
                &mut read,
            )
        })
        .unwrap();
        let captured = String::from_utf16(&characters[..read as usize]).unwrap();
        assert_eq!(captured.matches('x').count(), 4095);
        assert!(
            captured.contains("中文"),
            "console UTF-16 content: {captured:?}"
        );
        check(unsafe { SetStdHandle(STD_ERROR_HANDLE, null_mut()) }).unwrap();
        assert_eq!(
            duplicate_standard_output(2).err().unwrap().raw_os_error(),
            Some(6)
        );
        check(unsafe { SetStdHandle(STD_ERROR_HANDLE, INVALID_HANDLE_VALUE) }).unwrap();
        assert_eq!(
            duplicate_standard_output(2).err().unwrap().raw_os_error(),
            Some(6)
        );
    }

    struct Fixture {
        directory: PathBuf,
        executable: PathBuf,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }
    fn fixture() -> Arc<Fixture> {
        static CACHE: OnceLock<Mutex<Weak<Fixture>>> = OnceLock::new();
        let mut cache = CACHE
            .get_or_init(|| Mutex::new(Weak::new()))
            .lock()
            .unwrap();
        if let Some(fixture) = cache.upgrade() {
            return fixture;
        }
        let directory =
            std::env::temp_dir().join(format!("aos native space 测试 {}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let executable = directory.join("fixture.exe");
        let source = directory.join("fixture.rs");
        std::fs::write(&source, r#"
use std::{env, fs, process::Command, time::Duration};
use std::os::windows::process::CommandExt;
fn main() {
    let args: Vec<String> = env::args().collect();
    match args[1].as_str() {
        "report" => fs::write(&args[2], format!("{:?}\n{:?}\n{:?}", &args[3..], env::var("PATH").ok(), env::var("SystemRoot").ok())).unwrap(),
        "tree" => {
            let mut child = Command::new(env::current_exe().unwrap()).args(["hold", &args[2]]).creation_flags(0x200).spawn().unwrap();
            let _ = child.wait();
        },
        "hold" => { fs::write(&args[2], std::process::id().to_string()).unwrap(); std::thread::sleep(Duration::from_secs(60)); },
        _ => panic!("unknown fixture"),
    }
}
"#).unwrap();
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
        let fixture = Arc::new(Fixture {
            directory,
            executable,
        });
        *cache = Arc::downgrade(&fixture);
        fixture
    }

    #[test]
    fn pipe_zero_write_does_not_close_later_data_and_only_disconnect_is_eof() {
        let (mut reader, writer) = Pipe::pair(false).unwrap();
        let mut buffer = [0_u8; 16];
        assert_eq!(
            reader.read(&mut buffer).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut count = 0;
        check(unsafe { WriteFile(writer.0, b"".as_ptr(), 0, &mut count, null_mut()) }).unwrap();
        // Byte pipes may coalesce the zero write. Either way it must not be EOF.
        assert_eq!(
            reader.read(&mut buffer).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        check(unsafe { WriteFile(writer.0, b"after-zero".as_ptr(), 10, &mut count, null_mut()) })
            .unwrap();
        assert_eq!(reader.read(&mut buffer).unwrap(), 10);
        assert_eq!(&buffer[..10], b"after-zero");
        drop(writer);
        assert_eq!(reader.read(&mut buffer).unwrap(), 0);
    }

    #[test]
    fn full_input_pipe_returns_would_block_without_a_reader_thread() {
        let (mut writer, _reader) = Pipe::pair(true).unwrap();
        let started = Instant::now();
        let mut total = 0;
        loop {
            match writer.write(&[b'x'; 8192]) {
                Ok(count) => total += count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                result => panic!("unexpected write: {result:?}"),
            }
            assert!(total <= 1024 * 1024);
        }
        assert!(total > 0 && started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn arguments_and_case_insensitive_environment_round_trip_without_a_shell() {
        let fixture = fixture();
        let report = fixture.directory.join("arguments.txt");
        let arguments = [
            "",
            "one value",
            "a\"b",
            "tail\\",
            "quote\\\"tail\\",
            "$literal;not-a-shell",
            "中文",
        ];
        let original = std::env::var_os("PATH");
        let mut command = Command::new(&fixture.executable);
        command
            .arg("report")
            .arg(&report)
            .args(arguments)
            .env("PATH", "first")
            .env("pAtH", "second")
            .env_remove("sYsTeMrOoT");
        let process = Process::spawn(&command, false).unwrap();
        assert!(process.wait().unwrap().success());
        assert_eq!(
            std::fs::read_to_string(report).unwrap(),
            format!("{:?}\nSome(\"second\")\nNone", arguments)
        );
        assert_eq!(std::env::var_os("PATH"), original);
    }

    #[test]
    fn owned_job_includes_new_console_group_and_excludes_nonmembers_and_exited_processes() {
        let fixture = fixture();
        let marker = fixture.directory.join("descendant.txt");
        let mut command = Command::new(&fixture.executable);
        command.arg("tree").arg(&marker);
        let process = Process::spawn(&command, false).unwrap();
        let job = process.owned_job();
        let deadline = Instant::now() + Duration::from_secs(3);
        let descendant = loop {
            if let Ok(contents) = std::fs::read_to_string(&marker) {
                if let Ok(pid) = contents.parse() {
                    break pid;
                }
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(job.contains(process.id()).unwrap());
        assert!(job.contains(descendant).unwrap());
        assert!(!job.contains(std::process::id()).unwrap());
        process.kill().unwrap();
        process.wait().unwrap();
        assert!(!job.contains(process.id()).unwrap());
        assert!(!process_is_running(descendant).unwrap());
        // No console-group signal is attempted after the retained leader exits.
        process.interrupt().unwrap();
    }
}
