//! Shared JSON typesupport for native clients and every language binding.
use std::{collections::BTreeMap, io::Read, path::Path, sync::Arc};

use jsonschema::{Draft, PatternOptions, Validator};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    interface_limits, Error, Result, MAX_INTERFACE_DEFINITION_BYTES,
    MAX_INTERFACE_REGEX_COMPILED_BYTES, MAX_INTERFACE_REGEX_DFA_BYTES, MAX_INTERFACE_SCHEMA_BYTES,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterfaceKind {
    Message,
    Service,
    Action,
}

#[derive(Clone)]
pub struct Interface(Arc<Definition>);
struct Definition {
    value: Value,
    name: String,
    hash: String,
    kind: InterfaceKind,
    validators: BTreeMap<String, PartValidator>,
}
struct PartValidator {
    validator: Validator,
    expanded_cost: interface_limits::ValidationWork,
}

impl std::fmt::Debug for Interface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interface")
            .field("name", &self.type_name())
            .field("hash", &self.type_hash())
            .finish()
    }
}

impl Interface {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let file = std::fs::File::open(path.as_ref())
            .map_err(|e| Error::InvalidArgument(format!("cannot read interface: {e}")))?;
        let mut bytes = Vec::new();
        file.take((MAX_INTERFACE_DEFINITION_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| Error::InvalidArgument(format!("cannot read interface: {e}")))?;
        if bytes.len() > MAX_INTERFACE_DEFINITION_BYTES {
            return Err(Error::ResourceLimit(format!(
                "interface file exceeds {MAX_INTERFACE_DEFINITION_BYTES} bytes"
            )));
        }
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::InvalidArgument(format!("invalid interface JSON: {e}")))?;
        Self::from_json(value)
    }

    pub fn from_json(value: Value) -> Result<Self> {
        Self::from_json_bounded(value, MAX_INTERFACE_SCHEMA_BYTES)
    }

    /// Apply the shared resource profile and an optional tighter per-part
    /// compact JSON byte limit before compiling any untrusted schema.
    pub fn from_json_bounded(value: Value, max_schema_bytes: usize) -> Result<Self> {
        let interface = Self::compile_definition(value, max_schema_bytes)?;
        if interface.kind() == InterfaceKind::Action {
            let default = interface.definition()["result"]
                .get("default")
                .ok_or_else(|| {
                    Error::InvalidArgument(
                        "action result schema requires a top-level default".into(),
                    )
                })?;
            interface.validate("result", default)?;
            // Check the actual wrappers, including their IDs and fixed metadata.
            // This calls compile_definition directly, never from_json recursively.
            crate::ActionInterfaces::check_resources(&interface)?;
        }
        Ok(interface)
    }

    pub(crate) fn compile_definition(value: Value, max_schema_bytes: usize) -> Result<Self> {
        interface_limits::validate_schema_limit(max_schema_bytes)?;
        interface_limits::check_definition(&value)?;
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::InvalidArgument("interface requires string name".into()))?
            .to_owned();
        let kind = match value.get("kind").and_then(Value::as_str) {
            Some("msg") => InterfaceKind::Message,
            Some("srv") => InterfaceKind::Service,
            Some("action") => InterfaceKind::Action,
            _ => {
                return Err(Error::InvalidArgument(
                    "interface kind must be msg, srv or action".into(),
                ))
            }
        };
        let parts: Vec<_> = name.split('/').collect();
        if parts.len() != 3
            || !crate::names::valid_identifier(parts[0])
            || !crate::names::valid_identifier(parts[2])
            || parts[1] != value["kind"].as_str().unwrap()
        {
            return Err(Error::InvalidArgument(
                "type name must be package/kind/Identifier and match kind".into(),
            ));
        }
        let fields: &[&str] = match kind {
            InterfaceKind::Message => &["message"],
            InterfaceKind::Service => &["request", "response"],
            InterfaceKind::Action => &["goal", "result", "feedback"],
        };
        let expanded_costs = interface_limits::check_schemas(&value, fields, max_schema_bytes)?;
        let mut validators = BTreeMap::new();
        for field in fields {
            let schema = value.get(field).ok_or_else(|| {
                Error::InvalidArgument(format!("interface requires {field} schema"))
            })?;
            let validator = jsonschema::options()
                .with_draft(Draft::Draft202012)
                // Local files, remote descriptions and language bindings must
                // agree on one bounded, non-backtracking pattern dialect.
                .with_pattern_options(
                    PatternOptions::regex()
                        .size_limit(MAX_INTERFACE_REGEX_COMPILED_BYTES)
                        .dfa_size_limit(MAX_INTERFACE_REGEX_DFA_BYTES),
                )
                .build(schema)
                .map_err(|e| interface_limits::compilation_error(field, e))?;
            validators.insert(
                (*field).to_owned(),
                PartValidator {
                    validator,
                    expanded_cost: expanded_costs[*field],
                },
            );
        }
        validate_canonical_numbers(&value)?;
        let canonical = serde_jcs::to_vec(&value).map_err(|e| {
            Error::InvalidArgument(format!("interface cannot be RFC 8785 normalized: {e}"))
        })?;
        let hash = format!("{:x}", Sha256::digest(canonical));
        let interface = Self(Arc::new(Definition {
            value,
            name,
            hash,
            kind,
            validators,
        }));
        Ok(interface)
    }

    pub fn definition(&self) -> &Value {
        &self.0.value
    }
    pub fn type_name(&self) -> &str {
        &self.0.name
    }
    pub fn type_hash(&self) -> &str {
        &self.0.hash
    }
    pub fn kind(&self) -> InterfaceKind {
        self.0.kind
    }

    pub fn validate(&self, part: &str, value: &Value) -> Result<()> {
        let validator = self.0.validators.get(part).ok_or_else(|| {
            Error::InvalidArgument(format!("{} has no {part} schema", self.type_name()))
        })?;
        interface_limits::check_validation_work(value, validator.expanded_cost)?;
        if let Err(error) = validator.validator.validate(value) {
            return Err(Error::Validation(format!(
                "{part}{}: {error}",
                error.instance_path()
            )));
        }
        Ok(())
    }

    pub(crate) fn encode(&self, part: &str, value: &Value) -> Result<Vec<u8>> {
        self.validate(part, value)?;
        serde_json::to_vec(value).map_err(|e| Error::Validation(e.to_string()))
    }

    pub(crate) fn decode(&self, part: &str, bytes: &[u8]) -> Result<Value> {
        let value = serde_json::from_slice(bytes)
            .map_err(|e| Error::Validation(format!("invalid JSON: {e}")))?;
        self.validate(part, &value)?;
        Ok(value)
    }
}

// serde_jcs converts integral values through IEEE-754. Reject lossy schema
// numbers before hashing so distinct integer constraints cannot share a hash.
// Business payloads still retain serde_json's integer precision.
fn validate_canonical_numbers(value: &Value) -> Result<()> {
    match value {
        Value::Number(number) => {
            let loss = if let Some(n) = number.as_i64() {
                (n as f64) as i128 != n as i128
            } else if let Some(n) = number.as_u64() {
                (n as f64) as u128 != n as u128
            } else {
                false
            };
            if loss {
                return Err(Error::InvalidArgument("interface integer is not exactly representable by RFC 8785's IEEE-754 number model".into()));
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_canonical_numbers(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_canonical_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Built-in ordinary services have stable interfaces shared by SDK and CLI.
pub fn node_card_interface() -> Interface {
    Interface::from_json(json!({
        "name": "aos_interfaces/srv/GetNodeCard", "kind": "srv",
        "request": {"type": "object", "properties": {}, "additionalProperties": false},
        "response": {"type": "object"}
    }))
    .expect("static NodeCard schema")
}

pub fn type_description_interface() -> Interface {
    Interface::from_json(json!({
        "name": "aos_interfaces/srv/GetTypeDescription", "kind": "srv",
        "request": {"type": "object", "properties": {
            "type_name": {"type": "string"}, "type_hash": {"type": "string"}
        }, "required": ["type_name"], "additionalProperties": false},
        "response": {"type": "object"}
    }))
    .expect("static type-description schema")
}

/// Shared typed clock channel used by the Node time source and bag playback.
pub fn clock_interface() -> Interface {
    Interface::from_json(json!({
        "name":"aos_interfaces/msg/Clock","kind":"msg",
        "message":{"type":"object","additionalProperties":false,"required":["clock"],
            "properties":{"clock":{"type":"object","additionalProperties":false,
                "required":["sec","nanosec"],"properties":{
                    "sec":{"type":"integer","minimum":0,"maximum":9223372036u64},
                    "nanosec":{"type":"integer","minimum":0,"maximum":999999999}
                },"allOf":[{"if":{"properties":{"sec":{"const":9223372036u64}}},
                    "then":{"properties":{"nanosec":{"maximum":854775807}}}}]
            }}
        }
    }))
    .expect("static clock interface")
}
