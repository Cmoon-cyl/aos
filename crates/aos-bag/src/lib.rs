//! Bounded MCAP recording, offline inspection/recovery, and Topic-only playback.
mod error;
mod format;
mod offline;
mod player;
mod reader;
mod recorder;
mod storage;
mod types;
pub use error::{BagError, BagErrorKind, BagResult};
pub use offline::{inspect, recover};
pub use player::Player;
pub use recorder::Recorder;
pub use types::*;

#[cfg(test)]
mod player_r1_tests;
#[cfg(test)]
mod runtime_tests;
#[cfg(test)]
mod storage_tests;
