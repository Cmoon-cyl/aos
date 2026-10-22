//! Explicit mTLS profiles for clients, local peers and federated Routers.
//!
//! A certificate common name is the router's authenticated subject. Node names,
//! domains, namespaces, caller payloads and goal IDs are resources, not identity.
//! ACL rejection is not reported as a distinct client error by Zenoh 1.10.1:
//! correlate router denial logs with the actual absence of a business effect.

mod federation;
mod keys;
mod policy;
mod profile;

use aos_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use crate::context::{open_resolved_secure_context, open_secure_context};
pub use federation::with_relay_subjects;
pub use policy::compile_policy;
pub use profile::{
    client_config, load_client_profile, router_config, router_policy, validate_client_config,
};

pub const SECURITY_SCHEMA_VERSION: u32 = 1;
pub const MAX_SECURITY_JSON_BYTES: usize = 1024 * 1024;

fn schema_version() -> u32 {
    SECURITY_SCHEMA_VERSION
}
fn connect_timeout_ms() -> u64 {
    3000
}
fn is_false(value: &bool) -> bool {
    !value
}

/// File paths must be absolute. This profile carries paths, never PEM content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTlsProfile {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub domain_id: u32,
    pub router_endpoints: Vec<String>,
    pub root_ca_certificate: String,
    pub certificate: String,
    pub private_key: String,
    /// Strict-client startup deadline. Peer mode uses background reconnect;
    /// it accepts only the default value for backward-compatible decoding.
    #[serde(default = "connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Omitted retains the original strict client profile. Explicit peer mode
    /// only listens on loopback and applies ACLs on the direct peer links too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer: Option<PeerTlsOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterTlsProfile {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub listen_endpoints: Vec<String>,
    pub root_ca_certificate: String,
    pub certificate: String,
    pub private_key: String,
    /// Enable local peer gossip without delegating to any remote Router.
    /// Omitted preserves the original strict-client-only Router profile.
    #[serde(default, skip_serializing_if = "is_false")]
    pub local_peers: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation: Option<RouterFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerTlsOptions {
    pub listen_endpoint: String,
    pub policy: SecurityPolicy,
    pub relays: Vec<RelaySubject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterFederation {
    pub connect_endpoints: Vec<String>,
    pub relays: Vec<RelaySubject>,
}

/// A deployment-controlled Router identity trusted to relay the finite rights
/// of the listed base policy subjects. This is not caller identity forwarding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelaySubject {
    pub id: String,
    pub common_name: String,
    pub forward_subjects: Vec<String>,
}

/// All names are final deployment names, after remapping. The compiler never
/// applies a second remapping layer or accepts caller-written Zenoh wildcards.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityPolicy {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub domain_id: u32,
    pub subjects: Vec<SubjectPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectPolicy {
    pub id: String,
    pub common_name: String,
    /// Finite local Node identities this subject may register. Each includes
    /// its two built-in descriptor services, but no right to call other Nodes.
    pub nodes: Vec<NodeIdentity>,
    #[serde(default)]
    pub endpoints: Vec<EndpointGrant>,
    #[serde(default)]
    pub systems: Vec<SystemGrant>,
    /// The compiler expands only the declarations in this complete policy for
    /// these Nodes. It does not allow arbitrary endpoint types below a prefix.
    #[serde(default)]
    pub graph_nodes: Vec<NodeIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeIdentity {
    pub name: String,
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointGrant {
    /// Must refer to one of this subject's local nodes.
    pub node: NodeIdentity,
    /// Explicit resource boundary, compared by path segments, not text prefix.
    pub namespace: String,
    pub name: String,
    /// Full AOS interface JSON; derived Action interfaces use aos-core's
    /// ActionInterfaces and their real type hashes.
    pub interface: Value,
    pub operations: Vec<EndpointOperation>,
    /// Topic/Service QoS object or Action per-channel overrides, as accepted by
    /// aos-core. Null uses the corresponding core default.
    #[serde(default)]
    pub qos: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointOperation {
    Publish,
    Subscribe,
    Call,
    Provide,
    SendGoal,
    GetResult,
    CancelGoal,
    Feedback,
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemGrant {
    pub node: NodeIdentity,
    pub target: NodeIdentity,
    pub operations: Vec<SystemOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemOperation {
    Describe,
    ParametersRead,
    ParametersWrite,
    ParametersProvide,
    ParameterEventsRead,
    ParameterEventsPublish,
    LifecycleRead,
    LifecycleChange,
    LifecycleProvide,
    LifecycleEventsRead,
    LifecycleEventsPublish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Flow {
    Ingress,
    Egress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Message {
    Put,
    DeclareSubscriber,
    Query,
    DeclareQueryable,
    Reply,
    LivelinessToken,
    DeclareLivelinessSubscriber,
    LivelinessQuery,
}

/// A reviewable reason for a native permission. No delete or anonymous rule
/// can be generated. Repeated permissions may have more than one reason.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ExpandedGrant {
    pub subject_id: String,
    pub operation: String,
    pub resource: String,
    pub flow: Flow,
    pub message: Message,
    pub key_expr: String,
}

/// Constructible only by the validated compiler. Bindings may serialize this
/// result for review; they must not recreate it from unvalidated native ACLs.
#[derive(Debug, Clone, Serialize)]
pub struct CompiledSecurityPolicy {
    schema_version: u32,
    domain_id: u32,
    source_digest: String,
    access_control: Value,
    expanded_grants: Vec<ExpandedGrant>,
}

impl CompiledSecurityPolicy {
    pub fn domain_id(&self) -> u32 {
        self.domain_id
    }
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }
    pub fn access_control(&self) -> &Value {
        &self.access_control
    }
    pub fn expanded_grants(&self) -> &[ExpandedGrant] {
        &self.expanded_grants
    }
}

fn parse<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    if text.len() > MAX_SECURITY_JSON_BYTES {
        return Err(invalid("security JSON exceeds the 1 MiB limit"));
    }
    // serde_json alone silently replaces duplicate object keys in Value.
    let value = aos_core::parse_config_json(text)
        .map_err(|_| invalid("security JSON must be valid JSON without duplicate keys"))?;
    // Serde errors may quote the offending string, including accidentally
    // pasted credentials. Keep profile diagnostics independent of input bytes.
    serde_json::from_value(value)
        .map_err(|_| invalid("security JSON does not match the requested strict schema"))
}

impl ClientTlsProfile {
    pub fn from_json(text: &str) -> Result<Self> {
        parse(text)
    }
}
impl RouterTlsProfile {
    pub fn from_json(text: &str) -> Result<Self> {
        parse(text)
    }
}
impl SecurityPolicy {
    pub fn from_json(text: &str) -> Result<Self> {
        parse(text)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidArgument(message.into())
}
