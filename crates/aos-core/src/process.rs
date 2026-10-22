//! Per-generation deployment input. Parsing and layer composition do not open
//! a transport or start a process. The environment variable is only a locator.
use std::{fs::File, io::Read, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    config::validate_node_layer, parse_config_json, resolve_context_config, ContextConfigRequest,
    Error, NodeConfigRequest, Result,
};

pub const PROCESS_CONFIG_ENV: &str = "AOS_PROCESS_CONFIG";
pub const PROCESS_CONFIG_SCHEMA_VERSION: u32 = 1;
pub const MAX_PROCESS_CONFIG_BYTES: usize = 1024 * 1024;

/// Sources supplied by a launcher; program and environment remain local inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeploymentLayers {
    pub config_files: Vec<String>,
    pub launch: Value,
    pub cli: Value,
}

impl Default for DeploymentLayers {
    fn default() -> Self {
        Self {
            config_files: Vec::new(),
            launch: json!({}),
            cli: json!({}),
        }
    }
}

/// Strict v1 wire document. Use `from_json`, `from_file`, or `from_environment`
/// for untrusted input; direct serde deserialization alone does not validate it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessBootstrap {
    pub schema_version: u32,
    pub launch_id: String,
    pub process_id: String,
    pub generation: u32,
    pub context: DeploymentLayers,
    pub node: DeploymentLayers,
}

/// Self-reported correlation metadata, not authentication or process liveness.
/// A Context combines its validated bootstrap with the actual local PID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub launch_id: String,
    pub process_id: String,
    pub generation: u32,
    pub pid: u32,
}

impl ProcessBootstrap {
    pub fn from_json(text: &str) -> Result<Self> {
        if text.len() > MAX_PROCESS_CONFIG_BYTES {
            return Err(invalid("document exceeds 1 MiB"));
        }
        let value = parse_config_json(text)?;
        let bootstrap: Self = serde_json::from_value(value)
            .map_err(|error| invalid(format!("invalid v1 document: {error}")))?;
        bootstrap.validate()?;
        Ok(bootstrap)
    }

    /// Read a bounded regular file at an absolute path. No path expansion or
    /// cwd-relative interpretation is performed.
    pub fn from_file(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(invalid("file path must be absolute"));
        }
        let metadata = std::fs::metadata(path)
            .map_err(|error| invalid(format!("cannot inspect {}: {error}", path.display())))?;
        check_file(&metadata)?;
        let file = File::open(path)
            .map_err(|error| invalid(format!("cannot open {}: {error}", path.display())))?;
        check_file(
            &file
                .metadata()
                .map_err(|error| invalid(format!("cannot inspect open file: {error}")))?,
        )?;
        let mut text = String::new();
        file.take(MAX_PROCESS_CONFIG_BYTES as u64 + 1)
            .read_to_string(&mut text)
            .map_err(|error| invalid(format!("cannot read UTF-8 document: {error}")))?;
        Self::from_json(&text)
    }

    /// An absent locator is ordinary startup; an empty or invalid locator is
    /// an error. Each standard Context opening reads its own immutable copy.
    pub fn from_environment() -> Result<Option<Self>> {
        std::env::var_os(PROCESS_CONFIG_ENV)
            .map(|path| Self::from_file(Path::new(&path)))
            .transpose()
    }

    /// Validate only the document itself, without reading referenced config
    /// files or selecting a Node. Node-specific checks remain at construction.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PROCESS_CONFIG_SCHEMA_VERSION {
            return Err(invalid("unsupported schema_version"));
        }
        Uuid::parse_str(&self.launch_id).map_err(|_| invalid("launch_id must be a UUID"))?;
        if self.process_id.is_empty()
            || self.process_id.len() > 128
            || !self
                .process_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(invalid(
                "process_id must contain 1..=128 ASCII letters, digits, underscores or hyphens",
            ));
        }
        if self.generation == 0 {
            return Err(invalid("generation must be at least 1"));
        }
        for (name, layers) in [("context", &self.context), ("node", &self.node)] {
            for path in &layers.config_files {
                absolute_path(path, &format!("{name}.config_files"))?;
            }
            for (source, layer) in [("launch", &layers.launch), ("cli", &layers.cli)] {
                if !layer.is_object() {
                    return Err(invalid(format!("{name}.{source} must be an object")));
                }
                if name == "context" {
                    resolve_context_config(&ContextConfigRequest {
                        program: layer.clone(),
                        ..Default::default()
                    })?;
                    if let Some(path) = layer.get("zenoh_config").and_then(Value::as_str) {
                        absolute_path(path, &format!("context.{source}.zenoh_config"))?;
                    }
                } else {
                    validate_node_layer(layer, &format!("process node.{source}"))?;
                }
            }
        }
        Ok(())
    }

    /// Preserve the six existing sources. API files come first, launcher files
    /// follow, and overlapping fields in the same launch/cli source are errors.
    pub fn context_request(&self, mut local: ContextConfigRequest) -> Result<ContextConfigRequest> {
        self.validate()?;
        local.config_files.extend(self.context.config_files.clone());
        local.launch = merge_source(local.launch, &self.context.launch, "context.launch")?;
        local.cli = merge_source(local.cli, &self.context.cli, "context.cli")?;
        Ok(local)
    }

    pub fn node_request(&self, mut local: NodeConfigRequest) -> Result<NodeConfigRequest> {
        self.validate()?;
        local.config_files.extend(self.node.config_files.clone());
        local.launch = merge_source(local.launch, &self.node.launch, "node.launch")?;
        local.cli = merge_source(local.cli, &self.node.cli, "node.cli")?;
        Ok(local)
    }

    pub(crate) fn local_identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            launch_id: self.launch_id.clone(),
            process_id: self.process_id.clone(),
            generation: self.generation,
            pid: std::process::id(),
        }
    }
}

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::InvalidArgument(format!("invalid process bootstrap: {message}"))
}

fn check_file(metadata: &std::fs::Metadata) -> Result<()> {
    if !metadata.is_file() {
        return Err(invalid("path must identify a regular file"));
    }
    if metadata.len() > MAX_PROCESS_CONFIG_BYTES as u64 {
        return Err(invalid("document exceeds 1 MiB"));
    }
    Ok(())
}

fn absolute_path(path: &str, field: &str) -> Result<()> {
    if path.contains('\0') || !Path::new(path).is_absolute() {
        return Err(invalid(format!("{field} must contain absolute paths")));
    }
    Ok(())
}

fn merge_source(mut local: Value, incoming: &Value, source: &str) -> Result<Value> {
    // Existing request APIs use a null root to mean "not provided". Nested
    // nulls are explicit leaves and must not be silently overwritten.
    if local.is_null() {
        local = json!({});
    }
    merge_objects(&mut local, incoming, source)?;
    Ok(local)
}

fn merge_objects(local: &mut Value, incoming: &Value, path: &str) -> Result<()> {
    let (Some(local), Some(incoming)) = (local.as_object_mut(), incoming.as_object()) else {
        return Err(invalid(format!("{path} must be an object")));
    };
    for (key, value) in incoming {
        if let Some(previous) = local.get_mut(key) {
            let path = format!("{path}.{key}");
            if previous.is_object() && value.is_object() {
                merge_objects(previous, value, &path)?;
            } else {
                return Err(invalid(format!(
                    "API and launcher overlap at {path} in the same source"
                )));
            }
        } else {
            local.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}
