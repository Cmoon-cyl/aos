//! Naming behavior follows ROS 2 Jazzy rcl expand_topic_name.c and
//! rmw name validation. Source: rcl commit 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0 (Apache-2.0); independently
//! implemented in Rust, with AOS errors and no user substitution map.
use crate::{Error, Result};

pub(crate) fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
        && bytes.all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

pub fn validate_node_name(name: &str) -> Result<()> {
    if valid_identifier(name) && name.len() <= 255 {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid node name {name:?}"
        )))
    }
}

pub fn normalize_namespace(namespace: &str) -> Result<String> {
    let namespace = if namespace.is_empty() {
        "/".to_owned()
    } else if namespace.starts_with('/') {
        namespace.to_owned()
    } else {
        format!("/{namespace}")
    };
    if namespace == "/" {
        return Ok(namespace);
    }
    validate_absolute_name(&namespace)?;
    Ok(namespace)
}

pub fn validate_absolute_name(name: &str) -> Result<()> {
    if name.starts_with('/') && name.len() <= 255 && name[1..].split('/').all(valid_identifier) {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "invalid absolute AOS name {name:?}"
        )))
    }
}

pub fn expand_name(name: &str, node_name: &str, namespace: &str) -> Result<String> {
    validate_node_name(node_name)?;
    let namespace = normalize_namespace(namespace)?;
    if name.is_empty() || (name.starts_with('~') && !name.starts_with("~/")) {
        return Err(Error::InvalidArgument(format!(
            "invalid entity name {name:?}"
        )));
    }
    let name = name
        .replace("{node}", node_name)
        .replace("{namespace}", &namespace)
        .replace("{ns}", &namespace);
    let expanded = if let Some(private) = name.strip_prefix("~/") {
        format!("{}/{node_name}/{private}", namespace.trim_end_matches('/'))
    } else if name.starts_with('/') {
        name
    } else {
        format!("{}/{name}", namespace.trim_end_matches('/'))
    };
    validate_absolute_name(&expanded)?;
    Ok(expanded)
}

pub fn is_hidden(name: &str) -> bool {
    name.split('/').any(|part| part.starts_with('_'))
}
