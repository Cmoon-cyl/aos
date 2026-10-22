// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Rust translation of the eight-entry ROS 2 Jazzy transition map:
// rcl 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0,
// rcl_action/src/rcl_action/goal_state_machine.c. Invalid transitions return an
// AOS error rather than storing UNKNOWN, as rcl_action/goal_handle.c requires.
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GoalStatus {
    Unknown = 0,
    Accepted = 1,
    Executing = 2,
    Canceling = 3,
    Succeeded = 4,
    Canceled = 5,
    Aborted = 6,
}
impl GoalStatus {
    pub fn is_active(self) -> bool {
        matches!(self, Self::Accepted | Self::Executing | Self::Canceling)
    }
    pub fn is_cancelable(self) -> bool {
        matches!(self, Self::Accepted | Self::Executing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalEvent {
    Execute,
    CancelGoal,
    Succeed,
    Abort,
    Canceled,
}

pub fn transition(state: GoalStatus, event: GoalEvent) -> Result<GoalStatus> {
    use GoalEvent as E;
    use GoalStatus as S;
    match (state, event) {
        (S::Accepted, E::Execute) => Ok(S::Executing),
        (S::Accepted | S::Executing, E::CancelGoal) => Ok(S::Canceling),
        (S::Executing | S::Canceling, E::Succeed) => Ok(S::Succeeded),
        (S::Executing | S::Canceling, E::Abort) => Ok(S::Aborted),
        (S::Canceling, E::Canceled) => Ok(S::Canceled),
        _ => Err(Error::InvalidArgument(format!(
            "invalid action transition {state:?} + {event:?}"
        ))),
    }
}
