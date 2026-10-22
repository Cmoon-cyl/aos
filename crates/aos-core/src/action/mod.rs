//! Provider-owned ROS-style Action semantics, transported over ordinary entities.
mod client;
mod limits;
mod messages;
#[cfg(test)]
mod resource_test_support;
pub use client::{ActionClient, ClientGoalHandle};
pub use limits::ActionResourceLimits;
mod server;
mod state;
pub use messages::{
    status_qos, validate_goal_id, ActionInterfaces, ActionServerOptions, GoalInfo, Timestamp,
    ERROR_GOAL_TERMINATED, ERROR_NONE, ERROR_REJECTED, ERROR_UNKNOWN_GOAL_ID, ZERO_UUID,
};
pub use server::{ActionServer, CancelRequest, GoalRequest, ServerGoalHandle};
pub use state::{transition, GoalEvent, GoalStatus};
