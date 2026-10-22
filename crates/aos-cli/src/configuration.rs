//! CLI input is resolved by the same core used by the SDK, before network I/O.
use std::collections::BTreeMap;

use aos_core::{
    resolve_context_config, resolve_node_config, Context, ContextConfigRequest, NameKind, Node,
    NodeConfigRequest, NodeOptions, ProcessBootstrap, ResolvedContextConfig, ResolvedName,
    ResolvedNodeConfig,
};
use aos_zenoh::security::ClientTlsProfile;
use serde_json::{json, Value};

use super::{text, CliError, Result};

pub struct InvocationConfig {
    pub context: ResolvedContextConfig,
    pub node: ResolvedNodeConfig,
    pub bootstrap: Option<ProcessBootstrap>,
    pub security: Option<ClientTlsProfile>,
    pub security_profile_path: Option<std::path::PathBuf>,
    pub client_node: Option<ResolvedNodeConfig>,
}

fn environment(keys: &[&str]) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for key in keys {
        match std::env::var(key) {
            Ok(value) => {
                values.insert((*key).to_owned(), value);
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(_) => return Err(CliError::input(format!("{key} must contain Unicode text"))),
        }
    }
    Ok(values)
}

pub(crate) fn client_node(request: &Value) -> Result<Option<ResolvedNodeConfig>> {
    request
        .get("client_node")
        .map(|value| {
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Identity {
                name: String,
                namespace: String,
            }
            let identity: Identity = serde_json::from_value(value.clone())
                .map_err(|error| CliError::input(format!("client_node: {error}")))?;
            Ok::<_, CliError>(resolve_node_config(
                &identity.name,
                &identity.namespace,
                NodeOptions::default(),
                &NodeConfigRequest::default(),
            )?)
        })
        .transpose()
}

impl InvocationConfig {
    pub fn parse(request: &Value, offline: bool, uses_endpoint: bool) -> Result<Self> {
        Self::parse_with_bootstrap(
            request,
            offline,
            uses_endpoint,
            ProcessBootstrap::from_environment()?,
        )
    }

    fn parse_with_bootstrap(
        request: &Value,
        offline: bool,
        uses_endpoint: bool,
        bootstrap: Option<ProcessBootstrap>,
    ) -> Result<Self> {
        let mut node: NodeConfigRequest =
            serde_json::from_value(request.get("node_config").cloned().unwrap_or(json!({})))
                .map_err(|e| CliError::input(format!("node_config: {e}")))?;
        node.environment = environment(if offline {
            &["AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME"]
        } else if uses_endpoint {
            &["AOS_NAMESPACE"]
        } else {
            &[]
        })?;
        let mut context: ContextConfigRequest =
            serde_json::from_value(request.get("context_config").cloned().unwrap_or(json!({})))
                .map_err(|e| CliError::input(format!("context_config: {e}")))?;
        let explicit_security = request
            .get("security_profile")
            .map(|path| {
                path.as_str()
                    .map(std::path::Path::new)
                    .ok_or_else(|| CliError::input("security_profile must be an absolute path"))
            })
            .transpose()?;
        let security = aos_zenoh::security::load_client_profile(explicit_security)?;
        let security_profile_path = explicit_security
            .map(std::path::Path::to_path_buf)
            .or_else(|| std::env::var_os("AOS_SECURITY_PROFILE").map(std::path::PathBuf::from));
        if let Some(profile) = &security {
            if context.program.is_null() {
                context.program = json!({"domain_id":profile.domain_id});
            } else if let Some(program) = context.program.as_object_mut() {
                program
                    .entry("domain_id")
                    .or_insert(json!(profile.domain_id));
            }
        }
        context.environment = environment(&["AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG"])?;
        if let Some(bootstrap) = &bootstrap {
            context = bootstrap.context_request(context)?;
            node = bootstrap.node_request(node)?;
        }
        let name = request["declared_name"].as_str().unwrap_or("aos_cli");
        let namespace = request["declared_namespace"].as_str().unwrap_or("/");
        let client_node = client_node(request)?;
        Ok(Self {
            context: resolve_context_config(&context)?,
            node: resolve_node_config(name, namespace, NodeOptions::default(), &node)?,
            bootstrap,
            security,
            security_profile_path,
            client_node,
        })
    }

    pub fn validate_client_identity(&self, group: &str, verb: &str) -> Result<()> {
        let creates_node = matches!(
            (group, verb),
            ("topic", "pub" | "echo" | "hz" | "bw" | "delay")
                | ("service", "call")
                | ("action", "send_goal" | "get_result" | "cancel")
                | (
                    "param",
                    "get" | "describe" | "list" | "set" | "dump" | "load"
                )
                | ("lifecycle", "get" | "list" | "set")
        );
        if creates_node && self.security.is_some() && self.client_node.is_none() {
            return Err(CliError::input(
                "secure commands that create endpoints require client_node with a policy-authorized name and namespace",
            ));
        }
        Ok(())
    }

    pub fn create_node(&self, context: &Context) -> Result<Node> {
        if self.security.is_some() {
            let identity = self.client_node.clone().ok_or_else(|| {
                CliError::input("secure commands that create endpoints require client_node with a policy-authorized name and namespace")
            })?;
            return Ok(Node::new_resolved(context.clone(), identity)?);
        }
        // The configured identity resolves user endpoints, including ~/ names.
        // A CLI's own introspection services always use a fresh identity so a
        // deployment file or provider environment cannot shadow the provider.
        let identity = resolve_node_config(
            &format!("aos_cli_{}", uuid::Uuid::new_v4().simple()),
            "/",
            NodeOptions::default(),
            &NodeConfigRequest::default(),
        )?;
        Ok(Node::new_resolved(context.clone(), identity)?)
    }

    pub fn target(&self, name: &str, group: &str) -> Result<ResolvedName> {
        let kind = match group {
            "topic" => NameKind::Topic,
            "service" => NameKind::Service,
            "action" => NameKind::Action,
            _ => NameKind::Any,
        };
        Ok(self.node.resolve_name(name, kind)?)
    }

    pub fn report(&self, request: &Value) -> Result<Value> {
        let mut report = json!({
            "context":self.context.effective_value(),
            "node":self.node.effective_value(),
            "network_opened":false,
            "security_profile_selected":self.security.is_some(),
            "client_node":self.client_node.as_ref().map(ResolvedNodeConfig::effective_value),
        });
        if request.get("endpoint_name").is_some() {
            let kind = request["kind"].as_str().unwrap_or("any");
            report["name_resolution"] =
                serde_json::to_value(self.target(text(request, "endpoint_name"), kind)?)
                    .map_err(|e| CliError::input(e.to_string()))?;
            if kind == "any" {
                report["name_resolution"]["scope"] = json!(
                    "Only generic remap rules; see name_resolution_by_kind for actual entities."
                );
                for kind in ["topic", "service", "action"] {
                    report["name_resolution_by_kind"][kind] =
                        serde_json::to_value(self.target(text(request, "endpoint_name"), kind)?)
                            .map_err(|e| CliError::input(e.to_string()))?;
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_bootstrap_preserves_network_node_sources_and_conflicts() {
        let session = std::env::temp_dir().join("bootstrap-session.json5");
        let bootstrap = ProcessBootstrap::from_json(
            &json!({
                "schema_version":1,
                "launch_id":uuid::Uuid::new_v4().to_string(),
                "process_id":"recorder",
                "generation":2,
                "context":{"launch":{"domain_id":37,"zenoh_config":session}},
                "node":{"launch":{"name":"capture","namespace":"/deployed"}}
            })
            .to_string(),
        )
        .unwrap();
        let resolved = InvocationConfig::parse_with_bootstrap(
            &json!({"context_config":{"program":{"domain_id":1}}}),
            false,
            false,
            Some(bootstrap.clone()),
        )
        .unwrap();
        assert_eq!(resolved.context.domain_id, 37);
        assert_eq!(
            resolved.context.effective_value()["domain_id"]["source"],
            "launch"
        );
        assert_eq!(resolved.node.name, "capture");
        assert_eq!(resolved.node.namespace, "/deployed");
        assert_eq!(resolved.bootstrap.unwrap().generation, 2);
        assert!(InvocationConfig::parse_with_bootstrap(
            &json!({"context_config":{"launch":{"domain_id":38}}}),
            false,
            false,
            Some(bootstrap),
        )
        .is_err());
    }

    #[test]
    fn parsed_bootstrap_survives_file_removal_without_shadowing_the_provider() {
        let directory = std::env::temp_dir().join(format!("aos-cli-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let session = directory.join("session.json5");
        std::fs::write(
            &session,
            r#"{
                mode:"peer", connect:{endpoints:[]}, listen:{endpoints:[]},
                scouting:{multicast:{enabled:false},gossip:{enabled:false}}
            }"#,
        )
        .unwrap();
        let path = directory.join("bootstrap.json");
        let launch_id = uuid::Uuid::new_v4().to_string();
        std::fs::write(
            &path,
            json!({
                "schema_version":1, "launch_id":launch_id,
                "process_id":"capture", "generation":3,
                "context":{"launch":{"domain_id":37,"zenoh_config":session}},
                "node":{"launch":{"name":"capture","namespace":"/deployed"}}
            })
            .to_string(),
        )
        .unwrap();
        let configuration = InvocationConfig::parse_with_bootstrap(
            &json!({}),
            false,
            true,
            Some(ProcessBootstrap::from_file(&path).unwrap()),
        )
        .unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            configuration
                .target("work", "service")
                .unwrap()
                .resolved_name,
            "/deployed/work"
        );
        assert_eq!(
            configuration
                .target("~/status", "topic")
                .unwrap()
                .resolved_name,
            "/deployed/capture/status"
        );
        let (context, _) = crate::open_context(&configuration).unwrap();
        assert_eq!(context.configuration().unwrap().domain_id, 37);
        let provider = Node::new(context.clone(), "ignored", "/", NodeOptions::default()).unwrap();
        assert_eq!(provider.fully_qualified_name(), "/deployed/capture");
        let client = configuration.create_node(&context).unwrap();
        assert!(client.fully_qualified_name().starts_with("/aos_cli_"));
        assert_eq!(client.namespace(), "/");
        let identity = json!({
            "launch_id":launch_id,"process_id":"capture","generation":3,
            "pid":std::process::id()
        });
        assert_eq!(client.node_card()["process"], identity);
        assert_eq!(provider.node_card()["process"], identity);
        let nodes: Vec<_> = context
            .graph(std::time::Duration::from_millis(10))
            .unwrap()
            .into_iter()
            .filter(|entity| entity.kind == aos_core::EntityKind::Node)
            .collect();
        assert_eq!(nodes.len(), 2);
        assert_eq!(
            nodes
                .iter()
                .filter(|node| node.node_name == "capture")
                .count(),
            1
        );
        client.destroy_node().unwrap();
        provider.destroy_node().unwrap();
        context.shutdown().unwrap();
    }
}
