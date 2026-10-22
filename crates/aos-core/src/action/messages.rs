//! JSON encoding of ROS action_msgs GoalInfo/CancelGoal/GoalStatus contracts.
//! Fixed source: rcl_interfaces 7aa3caf43377ea6ad615bc1040832e2c7566bfbe.
use super::ActionResourceLimits;
use crate::{Durability, Error, Interface, InterfaceKind, QoS, Reliability, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const ZERO_UUID: &str = "00000000000000000000000000000000";
pub const ERROR_NONE: u8 = 0;
pub const ERROR_REJECTED: u8 = 1;
pub const ERROR_UNKNOWN_GOAL_ID: u8 = 2;
pub const ERROR_GOAL_TERMINATED: u8 = 3;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp {
    pub sec: i32,
    pub nanosec: u32,
}
impl Timestamp {
    pub fn now() -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Transport(format!("clock before epoch: {e}")))?;
        Ok(Self {
            sec: i32::try_from(now.as_secs())
                .map_err(|_| Error::InvalidArgument("clock exceeds ROS int32 seconds".into()))?,
            nanosec: now.subsec_nanos(),
        })
    }
    pub fn is_zero(self) -> bool {
        self.sec == 0 && self.nanosec == 0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalInfo {
    pub goal_id: String,
    pub stamp: Timestamp,
}

#[derive(Debug, Clone)]
pub struct ActionServerOptions {
    /// Core follows rcl's 10s default. Python explicitly supplies rclpy's 900s.
    /// Only terminal goals expire; active goals are never timed out by this.
    pub result_timeout: Duration,
    /// Maximum retained pending GetResult requests across all goals on this
    /// server. Client timeout is not a provider-side cancellation signal.
    pub max_result_waiters: usize,
    pub resource_limits: ActionResourceLimits,
}
impl ActionServerOptions {
    pub fn validate(&self) -> Result<()> {
        if !(1..=65_536).contains(&self.max_result_waiters) {
            return Err(Error::InvalidArgument(
                "max_result_waiters must be an integer in 1..=65536".into(),
            ));
        }
        self.resource_limits.validate()
    }
}
impl Default for ActionServerOptions {
    fn default() -> Self {
        Self {
            result_timeout: Duration::from_secs(10),
            max_result_waiters: 1024,
            resource_limits: ActionResourceLimits::default(),
        }
    }
}

pub fn validate_goal_id(id: &str) -> Result<()> {
    if id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "goal_id must be 32 lower-case hexadecimal characters".into(),
        ))
    }
}
pub fn status_qos() -> QoS {
    QoS {
        depth: 1,
        reliability: Reliability::Reliable,
        durability: Durability::TransientLocal,
    }
}

/// The kernel generates five ordinary JSON interfaces from the action type.
/// Every wrapper includes the original identity, so mismatched action hashes
/// cannot mix status or cancellation messages even at the same action name.
#[derive(Debug, Clone)]
pub struct ActionInterfaces {
    pub send_goal: Interface,
    pub get_result: Interface,
    pub cancel_goal: Interface,
    pub feedback: Interface,
    pub status: Interface,
}
impl ActionInterfaces {
    pub fn new(action: &Interface) -> Result<Self> {
        Self::build(action, &mut Interface::from_json)
    }

    pub(crate) fn check_resources(action: &Interface) -> Result<()> {
        // Call the compiler directly: generated kinds are only msg/srv, and
        // checking a parent must not recurse through Interface::from_json.
        Self::build(action, &mut |value| {
            Interface::compile_definition(value, crate::MAX_INTERFACE_SCHEMA_BYTES)
        })?;
        Ok(())
    }

    fn build(
        action: &Interface,
        compile: &mut impl FnMut(Value) -> Result<Interface>,
    ) -> Result<Self> {
        if action.kind() != InterfaceKind::Action {
            return Err(Error::InvalidArgument("expected action Interface".into()));
        }
        let parts: Vec<_> = action.type_name().split('/').collect();
        let prefix = parts[0];
        let name = parts[2];
        let uuid = json!({"type":"string","pattern":"^[0-9a-f]{32}$"});
        let stamp = json!({"type":"object","properties":{"sec":{"type":"integer","minimum":i32::MIN,"maximum":i32::MAX},"nanosec":{"type":"integer","minimum":0,"maximum":999999999}},"required":["sec","nanosec"],"additionalProperties":false});
        let info = json!({"type":"object","properties":{"goal_id":uuid,"stamp":stamp},"required":["goal_id","stamp"],"additionalProperties":false});
        let goal = component(action, "goal");
        let result = component(action, "result");
        let feedback = component(action, "feedback");
        let mut make = |name: String, kind: &str, parts: Value| -> Result<Interface> {
            let mut value = json!({"name":name,"kind":kind,"action_type":action.type_name(),"action_hash":action.type_hash()});
            value
                .as_object_mut()
                .unwrap()
                .extend(parts.as_object().unwrap().clone());
            compile(value)
        };
        Ok(Self {
            send_goal: make(
                format!("{prefix}/srv/{name}_SendGoal"),
                "srv",
                json!({
                    "request":{"type":"object","properties":{"goal_id":uuid,"goal":goal},"required":["goal_id","goal"],"additionalProperties":false},
                    "response":{"type":"object","properties":{"accepted":{"type":"boolean"},"stamp":stamp},"required":["accepted","stamp"],"additionalProperties":false}
                }),
            )?,
            get_result: make(
                format!("{prefix}/srv/{name}_GetResult"),
                "srv",
                json!({
                    "request":{"type":"object","properties":{"goal_id":uuid},"required":["goal_id"],"additionalProperties":false},
                    "response":{"type":"object","properties":{"status":{"type":"integer","minimum":0,"maximum":6},"result":result},"required":["status","result"],"additionalProperties":false}
                }),
            )?,
            cancel_goal: make(
                "aos_interfaces/srv/CancelGoal".into(),
                "srv",
                json!({
                    "request":{"type":"object","properties":{"goal_info":info},"required":["goal_info"],"additionalProperties":false},
                    "response":{"type":"object","properties":{"return_code":{"type":"integer","minimum":0,"maximum":3},"goals_canceling":{"type":"array","items":info}},"required":["return_code","goals_canceling"],"additionalProperties":false}
                }),
            )?,
            feedback: make(
                format!("{prefix}/msg/{name}_FeedbackMessage"),
                "msg",
                json!({
                    "message":{"type":"object","properties":{"goal_id":uuid,"feedback":feedback},"required":["goal_id","feedback"],"additionalProperties":false}
                }),
            )?,
            status: make(
                "aos_interfaces/msg/GoalStatusArray".into(),
                "msg",
                json!({
                    "message":{"type":"object","properties":{"status_list":{"type":"array","items":{"type":"object","properties":{"goal_info":info,"status":{"type":"integer","minimum":1,"maximum":6}},"required":["goal_info","status"],"additionalProperties":false}}},"required":["status_list"],"additionalProperties":false}
                }),
            )?,
        })
    }
}
fn component(interface: &Interface, part: &str) -> Value {
    let mut schema = interface.definition()[part].clone();
    // Preserve local fragment references when embedding a schema below a
    // wrapper property: an identified subschema is its own reference root.
    if let Some(map) = schema.as_object_mut() {
        map.insert(
            "$id".into(),
            json!(format!("urn:aos:{}:{part}", interface.type_hash())),
        );
    }
    schema
}
