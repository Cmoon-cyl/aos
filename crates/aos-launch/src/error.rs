use std::fmt;

use serde::{Deserialize, Serialize};

pub type LaunchResult<T> = Result<T, LaunchError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchErrorCode {
    InvalidArgument,
    InvalidManifest,
    PackageNotFound,
    AmbiguousPackage,
    EntryNotFound,
    ResourceNotFound,
    VersionMismatch,
    Unsupported,
    Io,
    SpawnFailed,
    ReadinessFailed,
    ReadinessTimeout,
    ProcessFailed,
    RestartExhausted,
    CleanupFailed,
    OutputIoError,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchError {
    pub code: LaunchErrorCode,
    pub message: String,
    pub stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
}

impl LaunchError {
    pub fn new(
        code: LaunchErrorCode,
        stage: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            stage: stage.into(),
            process_id: None,
        }
    }

    pub fn for_process(mut self, process_id: impl Into<String>) -> Self {
        self.process_id = Some(process_id.into());
        self
    }

    pub(crate) fn invalid(stage: &str, message: impl Into<String>) -> Self {
        Self::new(LaunchErrorCode::InvalidArgument, stage, message)
    }

    pub(crate) fn io(stage: &str, error: impl fmt::Display) -> Self {
        Self::new(LaunchErrorCode::Io, stage, error.to_string())
    }
}

impl fmt::Display for LaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.message)
    }
}

impl std::error::Error for LaunchError {}
