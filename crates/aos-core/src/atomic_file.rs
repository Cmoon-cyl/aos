//! Shared snapshot-file replacement for native SDKs and the CLI.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// A successful rename has replaced the destination. When directory syncing is
/// unsupported, the file was synced but crash persistence of the directory
/// entry cannot be confirmed. This does not promise power-loss durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct AtomicWriteReport {
    pub directory_synced: bool,
}

/// Replace a regular file through a private temporary file in its directory.
///
/// On Unix a new destination has mode 0600; existing permission bits are
/// preserved. Symlinks, non-regular destinations and read-only files are
/// rejected. Other platforms retain their filesystem's access-control rules.
///
/// File data is synced before rename. Directory sync failures EINVAL and
/// ENOTSUP are tolerated and reported with `directory_synced = false`. Other
/// errors after rename explicitly state that replacement already happened.
/// Earlier failures preserve the destination and remove this operation's temp
/// file on a best-effort basis.
pub fn write_private_atomic(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<AtomicWriteReport> {
    write_atomic_with(
        path.as_ref(),
        |output| output.write_all(bytes),
        sync_directory,
    )
}

fn write_atomic_with(
    path: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<AtomicWriteReport> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !fs::metadata(parent)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "output parent is not a directory",
        ));
    }
    let permissions = match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output must be a regular file or an unused path",
            ));
        }
        Ok(metadata) if metadata.permissions().readonly() => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "output file is read-only",
            ));
        }
        Ok(metadata) => Some(metadata.permissions()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let temporary = parent.join(format!(
        ".aos-private-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options.open(&temporary)?;
    let mut pending = PendingFile(Some(temporary.clone()));
    write(&mut output)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The incomplete temporary file remains private. Apply the final mode
        // only after writing, and sync that metadata together with the data.
        let mode = permissions
            .as_ref()
            .map_or(0o600, |permissions| permissions.mode() & 0o777);
        output.set_permissions(fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    if let Some(permissions) = permissions {
        output.set_permissions(permissions)?;
    }
    output.sync_all()?;
    drop(output);
    fs::rename(&temporary, path)?;
    // No cleanup of the temporary name after a successful replacement.
    pending.0 = None;
    match sync_parent(parent) {
        Ok(()) => Ok(AtomicWriteReport {
            directory_synced: true,
        }),
        Err(error) if unsupported_directory_sync(&error) => Ok(AtomicWriteReport {
            directory_synced: false,
        }),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!(
                "destination already replaced; durability not confirmed: directory sync failed: {error}"
            ),
        )),
    }
}

struct PendingFile(Option<PathBuf>);
impl Drop for PendingFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
fn sync_directory(parent: &Path) -> io::Result<()> {
    File::open(parent)?.sync_all()
}
#[cfg(not(unix))]
fn sync_directory(_: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "directory sync is unsupported on this platform",
    ))
}

fn unsupported_directory_sync(error: &io::Error) -> bool {
    // EINVAL is the POSIX result returned by filesystems without directory
    // fsync support. Do not suppress arbitrary InvalidInput errors.
    #[cfg(unix)]
    if error.raw_os_error() == Some(22) {
        return true;
    }
    // macOS ENOTSUP is not mapped to ErrorKind::Unsupported by the pinned
    // Rust std implementation, so recognize the native errno explicitly.
    #[cfg(target_os = "macos")]
    if error.raw_os_error() == Some(45) {
        return true;
    }
    #[cfg(target_os = "linux")]
    if error.raw_os_error() == Some(95) {
        return true;
    }
    error.kind() == io::ErrorKind::Unsupported
}

#[cfg(test)]
mod stage3_r2_tests {
    use super::*;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "aos-atomic-review-{}",
                uuid::Uuid::new_v4().simple()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn output(&self) -> PathBuf {
            self.0.join("parameters.json")
        }
        fn entries(&self) -> usize {
            fs::read_dir(&self.0).unwrap().count()
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn failed_partial_write_keeps_the_original_and_removes_its_temporary_file() {
        let directory = Directory::new();
        let path = directory.output();
        fs::write(&path, b"original").unwrap();
        let error = write_atomic_with(
            &path,
            |output| {
                output.write_all(b"partial replacement")?;
                Err(io::Error::other("injected disk write failure"))
            },
            |_| panic!("a failed write must not reach directory sync"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("injected disk write failure"));
        assert_eq!(fs::read(&path).unwrap(), b"original");
        assert_eq!(directory.entries(), 1);
    }

    #[test]
    fn unsupported_directory_sync_reports_replacement_with_weaker_durability() {
        let directory = Directory::new();
        let path = directory.output();
        #[cfg(unix)]
        let errors = vec![
            io::Error::from_raw_os_error(22),
            io::Error::new(io::ErrorKind::Unsupported, "injected ENOTSUP"),
        ];
        #[cfg(not(unix))]
        let errors = vec![io::Error::new(io::ErrorKind::Unsupported, "unsupported")];
        for error in errors {
            fs::write(&path, b"old").unwrap();
            let report = write_atomic_with(
                &path,
                |output| output.write_all(b"replacement"),
                |_| Err(error),
            )
            .unwrap();
            assert!(!report.directory_synced);
            assert_eq!(fs::read(&path).unwrap(), b"replacement");
            assert_eq!(directory.entries(), 1);
        }
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn native_enotsup_is_tolerated_but_unrelated_os_errors_are_not() {
        let enotsup = if cfg!(target_os = "macos") { 45 } else { 95 };
        assert!(unsupported_directory_sync(&io::Error::from_raw_os_error(
            enotsup
        )));
        assert!(!unsupported_directory_sync(&io::Error::from_raw_os_error(
            5
        )));
        assert!(!unsupported_directory_sync(&io::Error::from_raw_os_error(
            13
        )));
    }

    #[test]
    fn post_rename_failures_say_that_the_destination_was_already_replaced() {
        let directory = Directory::new();
        let path = directory.output();
        for kind in [io::ErrorKind::PermissionDenied, io::ErrorKind::InvalidInput] {
            fs::write(&path, b"old").unwrap();
            let error = write_atomic_with(
                &path,
                |output| output.write_all(b"replacement"),
                |_| Err(io::Error::new(kind, "injected directory sync failure")),
            )
            .unwrap_err();
            assert_eq!(error.kind(), kind);
            assert!(error.to_string().contains("destination already replaced"));
            assert!(error.to_string().contains("durability not confirmed"));
            assert_eq!(fs::read(&path).unwrap(), b"replacement");
            assert_eq!(directory.entries(), 1);
        }
    }

    #[test]
    fn non_regular_destinations_and_missing_parents_are_rejected() {
        let directory = Directory::new();
        let path = directory.output();
        fs::create_dir(&path).unwrap();
        assert!(write_private_atomic(&path, b"must not replace a directory").is_err());
        assert!(path.is_dir());
        assert!(write_private_atomic(path.join("missing").join("file"), b"data").is_err());
        assert_eq!(directory.entries(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn private_fresh_mode_existing_permissions_and_atomic_replacement() {
        use std::os::unix::fs::PermissionsExt;
        let directory = Directory::new();
        let path = directory.output();
        write_private_atomic(&path, b"original").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        let previous = directory.0.join("previous_inode");
        fs::hard_link(&path, &previous).unwrap();
        write_private_atomic(&path, b"replacement").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
        assert_eq!(fs::read(&previous).unwrap(), b"original");
        assert_eq!(directory.entries(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_read_only_file_are_not_replaced_or_followed() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let directory = Directory::new();
        let path = directory.output();
        let target = directory.0.join("target");
        fs::write(&target, b"original").unwrap();
        symlink(&target, &path).unwrap();
        assert!(write_private_atomic(&path, b"replacement").is_err());
        assert!(fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(&target).unwrap(), b"original");
        fs::remove_file(&path).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o400)).unwrap();
        assert!(write_private_atomic(&target, b"replacement").is_err());
        assert_eq!(fs::read(&target).unwrap(), b"original");
        assert_eq!(directory.entries(), 1);
    }
}
