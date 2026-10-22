//! Direct exec for our inherited-environment Command builders on macOS.
//!
//! Darwin's posix_spawnp/execvp can interpret an ENOEXEC file with /bin/sh.
//! Keep the ordinary Command pipe, process-group and error-pipe ownership,
//! but finish the fork child with execve, which has no shell fallback.

use std::{
    collections::BTreeMap,
    ffi::{CString, OsString},
    io,
    os::unix::{ffi::OsStrExt, process::CommandExt},
    process::{Child, Command},
};

use nix::libc;

struct Strings {
    _values: Vec<CString>,
    pointers: Vec<*const libc::c_char>,
}
// SAFETY: pointers refer only to the owned, immutable CString allocations.
// Moving Strings does not move those allocations; no method mutates them.
unsafe impl Send for Strings {}
unsafe impl Sync for Strings {}

impl Strings {
    fn new(values: Vec<CString>) -> Self {
        let mut pointers: Vec<_> = values.iter().map(|value| value.as_ptr()).collect();
        pointers.push(std::ptr::null());
        Self {
            _values: values,
            pointers,
        }
    }
    fn as_ptr(&self) -> *const *const libc::c_char {
        self.pointers.as_ptr()
    }
}

fn cstring(value: &[u8]) -> io::Result<CString> {
    CString::new(value).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
}

/// Internal builders use inherited environment and the default argv[0].
/// Call after all args/env/cwd/stdio/group settings; do not use env_clear/arg0.
/// The caller must keep child-status collection enabled. CLI startup already
/// resets SIGCHLD; a library caller receives an error instead of a global change.
pub fn spawn_inheriting(command: &mut Command) -> io::Result<Child> {
    // Rust's fork/exec error path waits for its failed child. With automatic
    // reaping that wait can panic; reject the incompatible disposition before
    // creating any process, without changing the embedding application's state.
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    if unsafe { libc::sigaction(libc::SIGCHLD, std::ptr::null(), &mut action) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if action.sa_sigaction == libc::SIG_IGN || action.sa_flags & libc::SA_NOCLDWAIT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "direct spawn requires SIGCHLD child-status collection",
        ));
    }
    let program = command.get_program().to_owned();
    let mut environment: BTreeMap<OsString, OsString> = std::env::vars_os().collect();
    for (key, value) in command.get_envs() {
        if let Some(value) = value {
            environment.insert(key.to_owned(), value.to_owned());
        } else {
            environment.remove(key);
        }
    }
    let argv = Strings::new(
        std::iter::once(program.as_os_str())
            .chain(command.get_args())
            .map(|value| cstring(value.as_bytes()))
            .collect::<io::Result<_>>()?,
    );
    let candidates = if program.as_bytes().contains(&b'/') {
        vec![cstring(program.as_bytes())?]
    } else {
        let path = environment
            .get(std::ffi::OsStr::new("PATH"))
            .cloned()
            .unwrap_or_else(|| "/usr/bin:/bin".into());
        std::env::split_paths(&path)
            .map(|directory| cstring(directory.join(&program).as_os_str().as_bytes()))
            .collect::<io::Result<Vec<_>>>()?
    };
    let envp = Strings::new(
        environment
            .into_iter()
            .map(|(key, value)| {
                let mut item = key.into_encoded_bytes();
                item.push(b'=');
                item.extend(value.into_encoded_bytes());
                cstring(&item)
            })
            .collect::<io::Result<_>>()?,
    );
    // SAFETY: every allocation and environment/PATH lookup happens above,
    // before fork. The child only reads immutable buffers, calls execve and
    // reads errno. Returning the OS error lets Command reap a failed child.
    unsafe {
        command.pre_exec(move || {
            let mut denied = false;
            for candidate in &candidates {
                libc::execve(candidate.as_ptr(), argv.as_ptr(), envp.as_ptr());
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::EACCES) => denied = true,
                    Some(libc::ENOENT | libc::ENOTDIR) => {}
                    _ => return Err(error),
                }
            }
            Err(io::Error::from_raw_os_error(if denied {
                libc::EACCES
            } else {
                libc::ENOENT
            }))
        });
    }
    command.spawn()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    #[test]
    fn invalid_image_does_not_execute_shell_text() {
        let directory =
            std::env::temp_dir().join(format!("aos-direct-exec-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let entry = directory.join("plain-text");
        let marker = directory.join("unexpected");
        fs::write(&entry, "printf executed > unexpected\n").unwrap();
        fs::set_permissions(&entry, fs::Permissions::from_mode(0o700)).unwrap();
        let error = spawn_inheriting(Command::new(&entry).current_dir(&directory)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ENOEXEC));
        assert!(!marker.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_interpreter_path_arguments_and_environment_are_preserved() {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "printf '%s\\n' \"$AOS_EXEC_VALUE\" \"${AOS_EXEC_REMOVE-unset}\" \"$1\" \"$2\"",
                "sh",
                "中文 $(id)",
                "",
            ])
            .env("PATH", "/bin:/usr/bin")
            .env("AOS_EXEC_VALUE", "value with spaces")
            .env_remove("AOS_EXEC_REMOVE")
            .stdout(std::process::Stdio::piped());
        let output = spawn_inheriting(&mut command)
            .unwrap()
            .wait_with_output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            "value with spaces\nunset\n中文 $(id)\n\n"
        );
    }

    #[test]
    fn auto_reap_disposition_is_rejected_before_spawning() {
        const CHILD: &str = "AOS_MACOS_EXEC_SIGCHLD_CHILD";
        if std::env::var_os(CHILD).is_some() {
            // Only this isolated subprocess changes the signal disposition.
            unsafe { libc::signal(libc::SIGCHLD, libc::SIG_IGN) };
            let error = spawn_inheriting(&mut Command::new("/usr/bin/true")).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            return;
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "macos_spawn::tests::auto_reap_disposition_is_rejected_before_spawning",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }
}
