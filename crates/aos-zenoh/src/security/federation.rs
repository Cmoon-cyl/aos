use std::collections::{BTreeMap, BTreeSet};

use aos_core::Result;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::{invalid, CompiledSecurityPolicy, RelaySubject, MAX_SECURITY_JSON_BYTES};

/// Add explicitly trusted Router identities without broadening the base
/// policy's resource keys or operations. Relays may reference base subjects
/// only; delegation through another relay is deliberately unsupported.
pub fn with_relay_subjects(
    base: &CompiledSecurityPolicy,
    relays: &[RelaySubject],
) -> Result<CompiledSecurityPolicy> {
    if base
        .expanded_grants
        .iter()
        .any(|grant| grant.operation.starts_with("relay."))
    {
        return Err(invalid(
            "relay policy must be expanded from an unfederated base policy",
        ));
    }
    if relays.is_empty() || relays.len() > 32 {
        return Err(invalid("federation requires 1..=32 trusted relay subjects"));
    }
    let serialized =
        serde_json::to_vec(relays).map_err(|_| invalid("cannot serialize relay policy"))?;
    if serialized.len() > MAX_SECURITY_JSON_BYTES {
        return Err(invalid("relay policy exceeds the 1 MiB limit"));
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for subject in base.access_control["subjects"].as_array().unwrap() {
        ids.insert(subject["id"].as_str().unwrap().to_owned());
        for name in subject["cert_common_names"].as_array().unwrap() {
            names.insert(name.as_str().unwrap().to_owned());
        }
    }
    let mut rules_by_subject = BTreeMap::new();
    for policy in base.access_control["policies"].as_array().unwrap() {
        for subject in policy["subjects"].as_array().unwrap() {
            rules_by_subject.insert(
                subject.as_str().unwrap(),
                policy["rules"].as_array().unwrap(),
            );
        }
    }
    let mut result = base.clone();
    for relay in relays {
        for value in [&relay.id, &relay.common_name] {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
            {
                return Err(invalid("relay identity must be a literal ASCII identifier"));
            }
        }
        if !ids.insert(relay.id.clone()) || !names.insert(relay.common_name.clone()) {
            return Err(invalid("relay identity conflicts with another subject"));
        }
        let forwarded: BTreeSet<_> = relay.forward_subjects.iter().map(String::as_str).collect();
        if forwarded.is_empty()
            || forwarded.len() > 64
            || forwarded.len() != relay.forward_subjects.len()
        {
            return Err(invalid(
                "relay requires a nonempty unique base subject list",
            ));
        }
        let mut rules = BTreeSet::new();
        for subject in &forwarded {
            let source_rules = rules_by_subject
                .get(subject)
                .ok_or_else(|| invalid("relay references an unknown base subject"))?;
            for rule in source_rules.iter() {
                rules.insert(rule.as_str().unwrap());
            }
        }
        result.access_control["subjects"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id":relay.id,"cert_common_names":[relay.common_name],"link_protocols":["tls"]
            }));
        result.access_control["policies"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "id":format!("aos-policy-{}",relay.id),"subjects":[relay.id],"rules":rules
            }));
        for grant in &base.expanded_grants {
            if forwarded.contains(grant.subject_id.as_str()) {
                let mut forwarded_grant = grant.clone();
                forwarded_grant.subject_id = relay.id.clone();
                forwarded_grant.operation =
                    format!("relay.{}.{}", grant.subject_id, grant.operation);
                result.expanded_grants.push(forwarded_grant);
                if result.expanded_grants.len() > 32_768 {
                    return Err(invalid(
                        "federated policy exceeds expanded permission limit",
                    ));
                }
            }
        }
    }
    result.source_digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&json!({"base_policy_digest":base.source_digest,"relays":relays}))
                .map_err(|_| invalid("cannot serialize federated policy"))?
        )
    );
    zenoh::Config::from_json5(&json!({"access_control":result.access_control}).to_string())
        .map_err(|_| invalid("native ACL rejected federated policy"))?;
    Ok(result)
}
