use aos_core::{
    resolve_node_config, Endpoint, EntityKind, Interface, NameKind, NodeConfigRequest, NodeOptions,
    Result,
};

use super::{invalid, NodeIdentity};

impl NodeIdentity {
    pub fn fully_qualified_name(&self) -> String {
        format!("{}/{}", self.namespace.trim_end_matches('/'), self.name)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.name.len() > 128 || self.namespace.len() > 512 {
            return Err(invalid("security Node identity is too long"));
        }
        let resolved = resolve_node_config(
            &self.name,
            &self.namespace,
            NodeOptions::default(),
            &NodeConfigRequest::default(),
        )?;
        if resolved.namespace != self.namespace {
            return Err(invalid(
                "security Node namespace must already be normalized",
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_resource(namespace: &str, name: &str) -> Result<()> {
    let identity = NodeIdentity {
        name: "security".into(),
        namespace: namespace.into(),
    };
    identity.validate()?;
    if !name.starts_with('/') || name.len() > 1024 {
        return Err(invalid("security endpoint must be an absolute final name"));
    }
    let config = resolve_node_config(
        "security",
        namespace,
        NodeOptions::default(),
        &NodeConfigRequest::default(),
    )?;
    if config.resolve_name(name, NameKind::Any)?.resolved_name != name {
        return Err(invalid("security endpoint must already be normalized"));
    }
    if namespace != "/"
        && !name
            .strip_prefix(namespace)
            .is_some_and(|rest| rest.starts_with('/'))
    {
        return Err(invalid("security endpoint escapes its resource namespace"));
    }
    Ok(())
}

pub(crate) fn endpoint(name: &str, interface: &Interface) -> Endpoint {
    Endpoint {
        name: name.into(),
        type_name: interface.type_name().into(),
        type_hash: interface.type_hash().into(),
    }
}

pub(crate) fn data_key(domain: u32, endpoint: &Endpoint) -> Result<String> {
    Ok(crate::key::typed_key(domain, endpoint)?.to_string())
}

/// Match only the runtime IDs and one QoS chunk. Every Node/endpoint identity
/// component is literal and follows the same mangle layout as graph.rs.
pub(crate) fn graph_key(
    domain: u32,
    node: &NodeIdentity,
    kind: EntityKind,
    endpoint: Option<&Endpoint>,
) -> Result<String> {
    node.validate()?;
    let kind = match kind {
        EntityKind::Node => "NN",
        EntityKind::Publisher => "MP",
        EntityKind::Subscription => "MS",
        EntityKind::Client => "SC",
        EntityKind::Service => "SS",
        EntityKind::ActionClient => "AC",
        EntityKind::ActionServer => "AS",
    };
    let mut key = format!(
        "aos/@lv/{domain}/*/*/*/{kind}/%/{}/{}",
        node.namespace.replace('/', "%"),
        node.name
    );
    if let Some(endpoint) = endpoint {
        data_key(domain, endpoint)?;
        key.push_str(&format!(
            "/{}/{}/{hash}/*",
            endpoint.name.replace('/', "%"),
            endpoint.type_name.replace('/', "%"),
            hash = endpoint.type_hash
        ));
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_boundaries_are_segments_not_text_prefixes() {
        assert!(validate_resource("/robot", "/robot/nav").is_ok());
        for name in [
            "/robot2/nav",
            "/robot",
            "/robot/../admin",
            "/robot//nav",
            "/robot/*",
            "~/nav",
        ] {
            assert!(validate_resource("/robot", name).is_err(), "{name}");
        }
        assert!(validate_resource("/robot/", "/robot/nav").is_err());
    }
}
