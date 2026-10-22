//! Installed package discovery and process-local launch supervision.
//!
//! `prepare` validates a complete plan before `Supervisor::start` can spawn.
//! Readiness is supplied by the caller; this crate neither opens a network
//! Context nor installs process-global signal handlers. See
//! `docs/platform/launch.md` for the deployment and ownership contracts.

mod error;
mod output;
mod package;
mod plan;
mod process;
mod supervisor;
mod types;

#[cfg(target_os = "macos")]
#[doc(hidden)]
pub mod macos_spawn;

#[cfg(windows)]
pub mod windows_process;

pub use error::{LaunchError, LaunchErrorCode, LaunchResult};
pub use package::{
    resolve_package_prefixes, EntryKind, EntryManifest, InstalledPackage, PackageIndex,
    PackageManifest, ResolvedEntry, ResolvedResource, ResourceKind, ResourceManifest,
};
pub use plan::{parse_launch, prepare, LaunchPlan, PlannedProcess};
pub use supervisor::Supervisor;
pub use types::*;
