// Portions of the data key layout are adapted from Hiroz
// d9f979a0404311de12f6884bafaa9b20600eb490 (Apache-2.0 selected).
// Modified for AOS JSON interfaces, ownership and closure. See upstream/source-map.json
// and upstream/licenses/hiroz-LICENSE for source locations and license.

//! Topic key expressions.
//!
//! Layout follows Hiroz's `RmwZenohFormatter::topic_key_expr`
//! (`crates/hiroz-protocol/src/format/rmw_zenoh.rs`, Hiroz
//! d9f979a0404311de12f6884bafaa9b20600eb490), which matches rmw_zenoh:
//! `<domain_id>/<topic without outer slashes>/<type_name>/<type_hash>`.
//! AOS prepends its own `aos` chunk (Spec §§1/4/8), so AOS traffic never
//! matches rmw_zenoh or other unprefixed keys on a shared router.
//! The P0 byte profile keeps `EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH`; typed
//! endpoints use their JSON interface name and hash.
//!
//! Callers pass an already expanded, absolute topic name. ROS name expansion
//! belongs to aos-core; users never build keys themselves.

use aos_core::transport::Endpoint;
use aos_core::{Error, Result};
use zenoh::key_expr::OwnedKeyExpr;

const AOS_PREFIX: &str = "aos";
const EMPTY_TOPIC_TYPE: &str = "EMPTY_TOPIC_TYPE";
const EMPTY_TOPIC_HASH: &str = "EMPTY_TOPIC_HASH";

/// Builds the Zenoh key of one untyped Topic in one domain.
pub(crate) fn topic_key(domain_id: u32, topic: &str) -> Result<OwnedKeyExpr> {
    endpoint_key(domain_id, topic, EMPTY_TOPIC_TYPE, EMPTY_TOPIC_HASH)
}

pub(crate) fn typed_key(domain_id: u32, endpoint: &Endpoint) -> Result<OwnedKeyExpr> {
    let name: Vec<_> = endpoint.type_name.split('/').collect();
    if name.len() != 3
        || !matches!(name[1], "msg" | "srv" | "action")
        || !is_name_chunk(name[0])
        || !is_name_chunk(name[2])
    {
        return Err(invalid(
            "type name must be pkg/(msg|srv|action)/Name".into(),
        ));
    }
    if endpoint.type_hash.len() != 64
        || !endpoint
            .type_hash
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(invalid("type hash must be lowercase SHA256 hex".into()));
    }
    endpoint_key(
        domain_id,
        &endpoint.name,
        &endpoint.type_name,
        &endpoint.type_hash,
    )
}

fn endpoint_key(
    domain_id: u32,
    topic: &str,
    type_name: &str,
    type_hash: &str,
) -> Result<OwnedKeyExpr> {
    let path = topic
        .strip_prefix('/')
        .ok_or_else(|| invalid(format!("topic {topic:?} must be absolute")))?;
    let path = path.strip_suffix('/').unwrap_or(path);
    if !path.split('/').all(is_name_chunk) {
        return Err(invalid(format!("topic {topic:?} is not an expanded name")));
    }
    OwnedKeyExpr::try_from(format!(
        "{AOS_PREFIX}/{domain_id}/{path}/{type_name}/{type_hash}"
    ))
    .map_err(|error| invalid(format!("key expression: {error}")))
}

fn is_name_chunk(chunk: &str) -> bool {
    !chunk.is_empty() && chunk.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn invalid(message: String) -> Error {
    Error::InvalidArgument(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_internal_slashes_domain_and_placeholders() {
        assert_eq!(
            topic_key(0, "/echo/request").unwrap().as_str(),
            "aos/0/echo/request/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH"
        );
        assert_eq!(
            topic_key(42, "/ns/chatter/").unwrap().as_str(),
            "aos/42/ns/chatter/EMPTY_TOPIC_TYPE/EMPTY_TOPIC_HASH"
        );
    }

    #[test]
    fn typed_key_keeps_hiroz_layout_with_json_identity() {
        let endpoint = Endpoint {
            name: "/demo/state".into(),
            type_name: "demo/msg/State".into(),
            type_hash: "a".repeat(64),
        };
        assert_eq!(
            typed_key(42, &endpoint).unwrap().as_str(),
            format!("aos/42/demo/state/demo/msg/State/{}", "a".repeat(64))
        );
    }

    #[test]
    fn rejects_relative_unexpanded_and_wildcard_names() {
        for topic in [
            "chatter",
            "",
            "/",
            "~/chatter",
            "/a//b",
            "/a/*",
            "/a/**",
            "/a/$x",
            "/a?b",
            "/a%b",
        ] {
            assert!(
                matches!(topic_key(0, topic), Err(Error::InvalidArgument(_))),
                "{topic}"
            );
        }
    }
}
