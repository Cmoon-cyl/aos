//! Supported QoS policy resolution, based on fixed Jazzy rmw 8cb94a87
//! `rmw/include/rmw/qos_profiles.h` and rcl 18d96c9c Action default_qos.h.
//! Service reliability/durability are deliberately narrower than DDS: Zenoh
//! 1.10.1 replies inherit query congestion settings, not a server override.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{Durability, Error, QoS, Reliability, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedQoS {
    pub profile: QoS,
    pub preset: String,
    pub sources: BTreeMap<String, String>,
}

impl ResolvedQoS {
    pub fn effective_value(&self) -> Value {
        json!({
            "history": "keep_last", "depth": self.profile.depth,
            "reliability": self.profile.reliability, "durability": self.profile.durability,
            "preset": self.preset, "sources": self.sources
        })
    }
}

impl From<QoS> for ResolvedQoS {
    fn from(profile: QoS) -> Self {
        Self {
            profile,
            preset: "explicit".into(),
            sources: ["history", "depth", "reliability", "durability"]
                .into_iter()
                .map(|field| (field.into(), "program".into()))
                .collect(),
        }
    }
}

pub fn qos_preset(name: &str) -> Result<QoS> {
    let (depth, reliability, durability) = match name {
        "default" | "services_default" | "lifecycle_services" | "lifecycle_events" => {
            (10, Reliability::Reliable, Durability::Volatile)
        }
        "sensor_data" => (5, Reliability::BestEffort, Durability::Volatile),
        "parameters" | "parameter_events" => (1000, Reliability::Reliable, Durability::Volatile),
        "action_status" => (1, Reliability::Reliable, Durability::TransientLocal),
        "clock" => (1, Reliability::BestEffort, Durability::Volatile),
        other => return Err(Error::Unsupported(format!("QoS preset {other:?}"))),
    };
    Ok(QoS {
        depth,
        reliability,
        durability,
    })
}

/// Resolve a null/default, depth integer, preset name or strict override object.
/// Bindings must pass user input without first inserting defaults: doing so
/// loses the distinction between preset values and explicit field overrides.
pub fn resolve_qos(request: &Value, default_preset: &str, service: bool) -> Result<ResolvedQoS> {
    let mut overrides = serde_json::Map::new();
    let preset = match request {
        Value::Null => default_preset,
        Value::String(name) => name,
        Value::Number(_) => {
            overrides.insert("depth".into(), request.clone());
            default_preset
        }
        Value::Object(fields) => {
            for (key, value) in fields {
                if !matches!(
                    key.as_str(),
                    "preset" | "history" | "depth" | "reliability" | "durability"
                ) {
                    return Err(Error::Unsupported(format!("QoS field {key:?}")));
                }
                if key != "preset" {
                    overrides.insert(key.clone(), value.clone());
                }
            }
            match fields.get("preset") {
                None => default_preset,
                Some(Value::String(name)) => name,
                Some(_) => {
                    return Err(Error::InvalidArgument("QoS preset must be a string".into()))
                }
            }
        }
        _ => {
            return Err(Error::InvalidArgument(
                "QoS must be a depth integer, preset string, object or null".into(),
            ));
        }
    };
    let mut profile = qos_preset(preset)?;
    let mut sources: BTreeMap<String, String> = ["history", "depth", "reliability", "durability"]
        .into_iter()
        .map(|field| (field.into(), format!("preset:{preset}")))
        .collect();
    for (key, value) in overrides {
        match key.as_str() {
            "history" if value == "keep_last" => {}
            "history" => return Err(Error::Unsupported(format!("QoS history {value}"))),
            "depth" => {
                profile.depth = value
                    .as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        Error::InvalidArgument("QoS depth must be a positive integer".into())
                    })?;
            }
            "reliability" => {
                profile.reliability = match value.as_str() {
                    Some("reliable") => Reliability::Reliable,
                    Some("best_effort") => Reliability::BestEffort,
                    _ => return Err(Error::Unsupported(format!("QoS reliability {value}"))),
                };
            }
            "durability" => {
                profile.durability = match value.as_str() {
                    Some("volatile") => Durability::Volatile,
                    Some("transient_local") => Durability::TransientLocal,
                    _ => return Err(Error::Unsupported(format!("QoS durability {value}"))),
                };
            }
            _ => unreachable!("strict key check above"),
        }
        sources.insert(key, "override".into());
    }
    if service {
        profile.validate_service()?;
    } else {
        profile.validate()?;
    }
    Ok(ResolvedQoS {
        profile,
        preset: preset.into(),
        sources,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionQos {
    pub goal_service: ResolvedQoS,
    pub result_service: ResolvedQoS,
    pub cancel_service: ResolvedQoS,
    pub feedback: ResolvedQoS,
    pub status: ResolvedQoS,
}

impl Default for ActionQos {
    fn default() -> Self {
        Self::from_json(&Value::Null).expect("fixed Action default profiles")
    }
}

impl ActionQos {
    pub fn from_json(value: &Value) -> Result<Self> {
        let empty = serde_json::Map::new();
        let fields = match value {
            Value::Null => &empty,
            Value::Object(fields) => fields,
            _ => {
                return Err(Error::InvalidArgument(
                    "Action QoS must be an object".into(),
                ))
            }
        };
        for key in fields.keys() {
            if !matches!(
                key.as_str(),
                "goal_service" | "result_service" | "cancel_service" | "feedback" | "status"
            ) {
                return Err(Error::Unsupported(format!("Action QoS channel {key:?}")));
            }
        }
        let resolve = |name, preset, service| {
            resolve_qos(fields.get(name).unwrap_or(&Value::Null), preset, service)
        };
        Ok(Self {
            goal_service: resolve("goal_service", "services_default", true)?,
            result_service: resolve("result_service", "services_default", true)?,
            cancel_service: resolve("cancel_service", "services_default", true)?,
            feedback: resolve("feedback", "default", false)?,
            status: resolve("status", "action_status", false)?,
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.goal_service.profile.validate_service()?;
        self.result_service.profile.validate_service()?;
        self.cancel_service.profile.validate_service()?;
        self.feedback.profile.validate()?;
        self.status.profile.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compatibility {
    Compatible,
    Incompatible,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QoSCompatibility {
    pub compatibility: Compatibility,
    pub reasons: Vec<String>,
    /// Graph diagnostics are not a promise that the middleware blocked traffic.
    pub enforcement: String,
}

pub fn qos_compatibility(offered: Option<&QoS>, requested: Option<&QoS>) -> QoSCompatibility {
    let mut result = QoSCompatibility {
        compatibility: Compatibility::Unknown,
        reasons: Vec::new(),
        enforcement: "diagnostic_only".into(),
    };
    let (Some(offered), Some(requested)) = (offered, requested) else {
        result
            .reasons
            .push("effective QoS is not known for both endpoints".into());
        return result;
    };
    if offered.validate().is_err() || requested.validate().is_err() {
        result.reasons.push("invalid advertised QoS".into());
        return result;
    }
    if requested.reliability == Reliability::Reliable
        && offered.reliability == Reliability::BestEffort
    {
        result
            .reasons
            .push("reliable requested, best_effort offered".into());
    }
    if requested.durability == Durability::TransientLocal
        && offered.durability == Durability::Volatile
    {
        result
            .reasons
            .push("transient_local requested, volatile offered".into());
    }
    result.compatibility = if result.reasons.is_empty() {
        Compatibility::Compatible
    } else {
        Compatibility::Incompatible
    };
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_keep_jazzy_values_and_explicit_override_provenance() {
        let value =
            resolve_qos(&json!({"preset":"sensor_data","depth":7}), "default", false).unwrap();
        assert_eq!(value.profile.depth, 7);
        assert_eq!(value.profile.reliability, Reliability::BestEffort);
        assert_eq!(value.sources["depth"], "override");
        assert_eq!(value.sources["reliability"], "preset:sensor_data");
        assert_eq!(
            qos_preset("parameter_events").unwrap().durability,
            Durability::Volatile
        );
        assert_eq!(ActionQos::default().status.profile.depth, 1);
    }

    #[test]
    fn unsupported_policies_and_wrong_numbers_are_never_dropped() {
        for value in [
            json!({"deadline":0}),
            json!({"history":"keep_all"}),
            json!({"depth":false}),
            json!({"depth":0}),
            json!({"depth":1.5}),
            json!({"reliability":"system_default"}),
        ] {
            assert!(resolve_qos(&value, "default", false).is_err(), "{value}");
        }
        assert!(resolve_qos(&json!("sensor_data"), "services_default", true).is_err());
        assert!(resolve_qos(
            &json!({"durability":"transient_local"}),
            "services_default",
            true
        )
        .is_err());
        assert!(
            ActionQos::from_json(&json!({"cancel_service":{"reliability":"best_effort"}})).is_err()
        );
    }

    #[test]
    fn diagnostic_is_directional_and_unknown_is_not_default() {
        let reliable = QoS::default();
        let sensor = qos_preset("sensor_data").unwrap();
        assert_eq!(
            qos_compatibility(Some(&sensor), Some(&reliable)).compatibility,
            Compatibility::Incompatible
        );
        assert_eq!(
            qos_compatibility(Some(&reliable), Some(&sensor)).compatibility,
            Compatibility::Compatible
        );
        assert_eq!(
            qos_compatibility(None, Some(&reliable)).compatibility,
            Compatibility::Unknown
        );
        assert_eq!(qos_compatibility(None, None).enforcement, "diagnostic_only");
    }
}
