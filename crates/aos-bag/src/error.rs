use serde::{Deserialize, Serialize};
use std::{fmt, io};

pub type BagResult<T> = Result<T, BagError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BagErrorKind {
    InvalidArgument,
    Corrupt,
    Incomplete,
    Io,
    DiskFull,
    CapacityLimit,
    ResourceLimit,
    Timeout,
    Closed,
    Transport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BagError {
    pub kind: BagErrorKind,
    pub message: String,
    /// Start of the outer MCAP record being processed, when known.
    pub file_offset: Option<u64>,
}
impl BagError {
    /// Construct an error, including cancellation from a caller-supplied check.
    pub fn new(kind: BagErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            file_offset: None,
        }
    }
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(BagErrorKind::InvalidArgument, message)
    }
    pub(crate) fn corrupt(message: impl Into<String>) -> Self {
        Self::new(BagErrorKind::Corrupt, message)
    }
    pub(crate) fn resource(message: impl Into<String>) -> Self {
        Self::new(BagErrorKind::ResourceLimit, message)
    }
    pub(crate) fn at(mut self, offset: u64) -> Self {
        self.file_offset = Some(offset);
        self
    }
}
impl fmt::Display for BagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)?;
        if let Some(offset) = self.file_offset {
            write!(f, " (MCAP offset {offset})")?;
        }
        Ok(())
    }
}
impl std::error::Error for BagError {}
impl From<io::Error> for BagError {
    fn from(error: io::Error) -> Self {
        Self::new(
            if error
                .get_ref()
                .is_some_and(|e| e.is::<crate::storage::CapacityExceeded>())
            {
                BagErrorKind::CapacityLimit
            } else if error.kind() == io::ErrorKind::StorageFull {
                BagErrorKind::DiskFull
            } else {
                BagErrorKind::Io
            },
            error.to_string(),
        )
    }
}
impl From<aos_core::Error> for BagError {
    fn from(error: aos_core::Error) -> Self {
        use aos_core::Error;
        let kind = match &error {
            Error::Timeout => BagErrorKind::Timeout,
            Error::Closed => BagErrorKind::Closed,
            Error::ResourceLimit(_) => BagErrorKind::ResourceLimit,
            Error::InvalidArgument(_) | Error::Validation(_) | Error::Unsupported(_) => {
                BagErrorKind::InvalidArgument
            }
            _ => BagErrorKind::Transport,
        };
        Self::new(kind, error.to_string())
    }
}
impl From<serde_json::Error> for BagError {
    fn from(error: serde_json::Error) -> Self {
        Self::corrupt(error.to_string())
    }
}
impl From<mcap::McapError> for BagError {
    fn from(error: mcap::McapError) -> Self {
        match error {
            mcap::McapError::Io(error) => error.into(),
            other => Self::corrupt(other.to_string()),
        }
    }
}
