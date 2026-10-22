use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    LaunchError, LaunchErrorCode, LaunchResult, MAX_DOCUMENT_BYTES, RUNTIME_ABI, SCHEMA_VERSION,
};

const MAX_PACKAGES: usize = 256;

/// Resolve explicit/environment/installation inputs once, before changing cwd.
/// A deployed process uses only its supervisor's fixed environment prefixes.
/// No branch implicitly inserts cwd as a prefix.
pub fn resolve_package_prefixes(
    explicit: Option<&[PathBuf]>,
    environment: Option<&OsStr>,
    installed_prefix: Option<&Path>,
    base: &Path,
    deployed: bool,
) -> LaunchResult<Vec<PathBuf>> {
    if !base.is_absolute() {
        return Err(LaunchError::invalid(
            "prefixes",
            "prefix base must be absolute",
        ));
    }
    let mut paths = if let Some(paths) = explicit {
        paths.to_vec()
    } else {
        let mut paths = Vec::new();
        if let Some(value) = environment.filter(|value| !value.is_empty()) {
            paths.extend(std::env::split_paths(value));
        }
        if !deployed {
            paths.extend(installed_prefix.map(Path::to_owned));
        }
        paths
    };
    for path in &mut paths {
        if path.as_os_str().is_empty() {
            return Err(LaunchError::invalid(
                "prefixes",
                "prefix lists must not contain empty entries",
            ));
        }
        if path.is_relative() {
            *path = base.join(&*path);
        }
    }
    Ok(paths)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageManifest {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub runtime_abi: String,
    pub entries: BTreeMap<String, EntryManifest>,
    #[serde(default)]
    pub resources: BTreeMap<String, ResourceManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryManifest {
    pub executable: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpreter: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interpreter_args: Vec<String>,
    pub kind: EntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    AosNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifest {
    pub kind: ResourceKind,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Interface,
    Config,
    Launch,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstalledPackage {
    pub prefix: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: PackageManifest,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedEntry {
    pub interpreter: Option<PathBuf>,
    pub interpreter_args: Vec<String>,
    pub package: String,
    pub version: String,
    pub executable: PathBuf,
    pub share_dir: PathBuf,
    pub runtime_abi: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedResource {
    pub package: String,
    pub name: String,
    pub kind: ResourceKind,
    pub path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PackageIndex {
    pub(crate) packages: BTreeMap<String, Vec<InstalledPackage>>,
    prefixes: Vec<PathBuf>,
}

impl PackageIndex {
    /// Explicit prefixes only: no cwd search and no executable/import side
    /// effects. Call `resolve_package_prefixes` to select environment defaults.
    pub fn discover(prefixes: &[PathBuf]) -> LaunchResult<Self> {
        let mut seen = BTreeSet::new();
        let mut canonical_prefixes = Vec::new();
        let mut packages: BTreeMap<String, Vec<InstalledPackage>> = BTreeMap::new();
        let mut count = 0;
        for prefix in prefixes {
            let prefix = absolute_directory(prefix, "discover")?;
            if !seen.insert(prefix.clone()) {
                continue;
            }
            canonical_prefixes.push(prefix.clone());
            let index = prefix.join("share/aos/resource_index/packages");
            if !index
                .try_exists()
                .map_err(|error| LaunchError::io("discover", error))?
            {
                continue;
            }
            let index = canonical_contained(&prefix, &index, "discover")?;
            let mut files = fs::read_dir(&index)
                .map_err(|error| LaunchError::io("discover", error))?
                .collect::<std::io::Result<Vec<_>>>()
                .map_err(|error| LaunchError::io("discover", error))?;
            files.sort_by_key(|entry| entry.file_name());
            for file in files {
                let name = file.file_name();
                let Some(name) = name.to_str() else {
                    return Err(manifest_error("index filename must be UTF-8"));
                };
                if name.starts_with('.') || !name.ends_with(".json") {
                    continue;
                }
                count += 1;
                if count > MAX_PACKAGES {
                    return Err(manifest_error("package index exceeds 256 manifests"));
                }
                let path = canonical_contained(&prefix, &file.path(), "manifest")?;
                let value = strict_file(&path, "manifest")?;
                let manifest: PackageManifest = serde_json::from_value(value)
                    .map_err(|error| manifest_error(error.to_string()))?;
                validate_manifest(&prefix, name, &manifest)?;
                packages
                    .entry(manifest.name.clone())
                    .or_default()
                    .push(InstalledPackage {
                        prefix: prefix.clone(),
                        manifest_path: path,
                        manifest,
                    });
            }
        }
        Ok(Self {
            packages,
            prefixes: canonical_prefixes,
        })
    }

    pub fn packages(&self) -> &BTreeMap<String, Vec<InstalledPackage>> {
        &self.packages
    }

    /// Canonical, deduplicated explicit prefixes in their supplied order.
    pub fn prefixes(&self) -> &[PathBuf] {
        &self.prefixes
    }

    pub fn package(&self, name: &str, version: Option<&str>) -> LaunchResult<InstalledPackage> {
        identifier(name, "package")?;
        let candidates = self.packages.get(name).ok_or_else(|| {
            LaunchError::new(
                LaunchErrorCode::PackageNotFound,
                "package",
                format!("package {name:?} is not installed"),
            )
        })?;
        if candidates.len() != 1 {
            return Err(LaunchError::new(
                LaunchErrorCode::AmbiguousPackage,
                "package",
                format!(
                    "package {name:?} has multiple installed candidates: {}",
                    candidates
                        .iter()
                        .map(|package| format!(
                            "{} ({})",
                            package.prefix.display(),
                            package.manifest.version
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        let package = &candidates[0];
        if version.is_some_and(|expected| expected != package.manifest.version) {
            return Err(LaunchError::new(
                LaunchErrorCode::VersionMismatch,
                "package",
                format!("package {name:?} does not match the requested version"),
            ));
        }
        Ok(package.clone())
    }

    pub fn entry(&self, package: &str, entry: &str) -> LaunchResult<ResolvedEntry> {
        identifier(entry, "entry")?;
        let package = self.package(package, None)?;
        let manifest = package.manifest.entries.get(entry).ok_or_else(|| {
            LaunchError::new(
                LaunchErrorCode::EntryNotFound,
                "entry",
                format!(
                    "entry {entry:?} is not installed in {}",
                    package.manifest.name
                ),
            )
        })?;
        let executable = installed_file(&package.prefix, &manifest.executable, "entry")?;
        if let Some(interpreter) = &manifest.interpreter {
            if !interpreter.is_absolute() || !interpreter.is_file() {
                return Err(manifest_error(
                    "entry interpreter must be an absolute existing file",
                ));
            }
            check_executable(interpreter)?;
            #[cfg(windows)]
            crate::windows_process::validate_executable(interpreter).map_err(|e| {
                LaunchError::invalid("entry", format!("invalid native interpreter: {e}"))
            })?;
        } else {
            if !manifest.interpreter_args.is_empty() {
                return Err(manifest_error(
                    "interpreter_args require an explicit interpreter",
                ));
            }
            check_executable(&executable)?;
            #[cfg(windows)]
            crate::windows_process::validate_executable(&executable).map_err(|e| LaunchError::invalid("entry", format!("Windows entries require a native executable or an explicit interpreter: {e}")))?;
        }
        let share_dir = canonical_contained(
            &package.prefix,
            &package.prefix.join("share").join(&package.manifest.name),
            "entry",
        )?;
        if !share_dir.is_dir() {
            return Err(manifest_error("package share path is not a directory"));
        }
        Ok(ResolvedEntry {
            interpreter: manifest.interpreter.clone(),
            interpreter_args: manifest.interpreter_args.clone(),
            package: package.manifest.name,
            version: package.manifest.version,
            executable,
            share_dir,
            runtime_abi: package.manifest.runtime_abi,
        })
    }

    pub fn resource(&self, package: &str, name: &str) -> LaunchResult<ResolvedResource> {
        identifier(name, "resource")?;
        let package = self.package(package, None)?;
        let resource = package.manifest.resources.get(name).ok_or_else(|| {
            LaunchError::new(
                LaunchErrorCode::ResourceNotFound,
                "resource",
                format!(
                    "resource {name:?} is not installed in {}",
                    package.manifest.name
                ),
            )
        })?;
        Ok(ResolvedResource {
            package: package.manifest.name.clone(),
            name: name.into(),
            kind: resource.kind,
            path: installed_file(&package.prefix, &resource.path, "resource")?,
        })
    }
}

fn validate_manifest(
    prefix: &Path,
    file_name: &str,
    manifest: &PackageManifest,
) -> LaunchResult<()> {
    identifier(&manifest.name, "manifest")?;
    if file_name != format!("{}.json", manifest.name) {
        return Err(manifest_error(
            "manifest filename must match its package name",
        ));
    }
    if manifest.schema_version != SCHEMA_VERSION || manifest.runtime_abi != RUNTIME_ABI {
        return Err(LaunchError::new(
            LaunchErrorCode::Unsupported,
            "manifest",
            "unsupported package schema_version or runtime_abi",
        ));
    }
    if manifest.version.is_empty()
        || manifest.version.len() > 64
        || !manifest
            .version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+-_".contains(&byte))
    {
        return Err(manifest_error(
            "package version must be a nonempty ASCII version string",
        ));
    }
    for (name, entry) in &manifest.entries {
        identifier(name, "manifest")?;
        let path = installed_file(prefix, &entry.executable, "manifest")?;
        check_executable(&path)?;
    }
    for (name, resource) in &manifest.resources {
        identifier(name, "manifest")?;
        installed_file(prefix, &resource.path, "manifest")?;
    }
    let share = canonical_contained(
        prefix,
        &prefix.join("share").join(&manifest.name),
        "manifest",
    )?;
    if !share.is_dir() {
        return Err(manifest_error(
            "package share path must be an installed directory",
        ));
    }
    Ok(())
}

pub(crate) fn identifier(value: &str, stage: &str) -> LaunchResult<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
    {
        return Err(LaunchError::invalid(
            stage,
            format!("invalid identifier {value:?}"),
        ));
    }
    Ok(())
}

fn manifest_error(message: impl Into<String>) -> LaunchError {
    LaunchError::new(LaunchErrorCode::InvalidManifest, "manifest", message)
}

pub(crate) fn strict_file(path: &Path, stage: &str) -> LaunchResult<serde_json::Value> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| LaunchError::io(stage, error))?;
    if !file
        .metadata()
        .map_err(|error| LaunchError::io(stage, error))?
        .is_file()
    {
        return Err(LaunchError::invalid(stage, "expected a regular file"));
    }
    let mut text = String::new();
    file.take((MAX_DOCUMENT_BYTES + 1) as u64)
        .read_to_string(&mut text)
        .map_err(|error| LaunchError::io(stage, error))?;
    if text.len() > MAX_DOCUMENT_BYTES {
        return Err(LaunchError::invalid(stage, "JSON document exceeds 1 MiB"));
    }
    aos_core::parse_config_json(&text)
        .map_err(|error| LaunchError::invalid(stage, error.to_string()))
}

pub(crate) fn absolute_directory(path: &Path, stage: &str) -> LaunchResult<PathBuf> {
    if !path.is_absolute() {
        return Err(LaunchError::invalid(
            stage,
            "directory path must be absolute",
        ));
    }
    let path = path
        .canonicalize()
        .map_err(|error| LaunchError::io(stage, error))?;
    if !path.is_dir() {
        return Err(LaunchError::invalid(stage, "expected a directory"));
    }
    Ok(path)
}

pub(crate) fn absolute_file(path: &Path, stage: &str) -> LaunchResult<PathBuf> {
    if !path.is_absolute() {
        return Err(LaunchError::invalid(stage, "file path must be absolute"));
    }
    let path = path
        .canonicalize()
        .map_err(|error| LaunchError::io(stage, error))?;
    if !path.is_file() {
        return Err(LaunchError::invalid(stage, "expected a regular file"));
    }
    Ok(path)
}

fn canonical_contained(prefix: &Path, path: &Path, stage: &str) -> LaunchResult<PathBuf> {
    let result = path
        .canonicalize()
        .map_err(|error| LaunchError::io(stage, error))?;
    if !result.starts_with(prefix) {
        return Err(LaunchError::invalid(
            stage,
            "installed path escapes its prefix",
        ));
    }
    Ok(result)
}

fn installed_file(prefix: &Path, path: &Path, stage: &str) -> LaunchResult<PathBuf> {
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(LaunchError::invalid(
            stage,
            "installed file path must be relative without dot segments",
        ));
    }
    let path = canonical_contained(prefix, &prefix.join(path), stage)?;
    if !path.is_file() {
        return Err(LaunchError::invalid(
            stage,
            "installed resource must be a regular file",
        ));
    }
    Ok(path)
}

fn check_executable(path: &Path) -> LaunchResult<()> {
    #[cfg(not(unix))]
    let _ = path;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(path)
            .map_err(|error| LaunchError::io("entry", error))?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(manifest_error("entry file is not executable"));
        }
    }
    Ok(())
}
