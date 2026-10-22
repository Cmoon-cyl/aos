use std::{collections::BTreeSet, io::Read, net::IpAddr, path::Path};

use aos_core::Result;
use serde_json::{json, Value};

use super::{
    compile_policy, invalid, with_relay_subjects, ClientTlsProfile, CompiledSecurityPolicy,
    RouterTlsProfile, MAX_SECURITY_JSON_BYTES, SECURITY_SCHEMA_VERSION,
};

const _: () = assert!(zenoh_link_tls::AOS_PRIVATE_ROOTS_PATCH_VERSION == 1);
const _: () = assert!(zenoh_link_tls::AOS_TLS_IO_PATCH_VERSION == 1);

/// The only implicit security input. Explicit configuration wins; a configured
/// but invalid profile is an error, never permission to use development mode.
pub fn load_client_profile(explicit: Option<&Path>) -> Result<Option<ClientTlsProfile>> {
    let selected = match explicit {
        Some(path) => Some(path.to_path_buf()),
        None => std::env::var_os("AOS_SECURITY_PROFILE").map(Into::into),
    };
    selected.as_deref().map(read_client_profile).transpose()
}

fn read_client_profile(path: &Path) -> Result<ClientTlsProfile> {
    if path.as_os_str().is_empty() || !path.is_absolute() || path.to_str().is_none() {
        return Err(invalid(
            "security profile must use a nonempty absolute UTF-8 path",
        ));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options
        .open(path)
        .map_err(|_| invalid("cannot open configured security profile"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("cannot inspect configured security profile"))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_SECURITY_JSON_BYTES as u64 {
        return Err(invalid(
            "security profile must be a regular file no larger than 1 MiB",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(invalid("security profile must not be a reparse point"));
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_SECURITY_JSON_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read configured security profile"))?;
    if bytes.len() > MAX_SECURITY_JSON_BYTES {
        return Err(invalid("security profile exceeds the 1 MiB limit"));
    }
    let text =
        std::str::from_utf8(&bytes).map_err(|_| invalid("security profile is not UTF-8 JSON"))?;
    let profile = ClientTlsProfile::from_json(text)?;
    client_config(&profile)?;
    Ok(profile)
}

fn validate_endpoints(endpoints: &[String]) -> Result<()> {
    if endpoints.len() != 1 {
        return Err(invalid(
            "security profile requires exactly one TLS endpoint",
        ));
    }
    for endpoint in endpoints {
        validate_endpoint(endpoint, false, false)?;
    }
    Ok(())
}

fn validate_endpoint(endpoint: &str, allow_zero: bool, loopback: bool) -> Result<()> {
    let address = endpoint
        .strip_prefix("tls/")
        .ok_or_else(|| invalid("security profile endpoints must use tls/"))?;
    if address.is_empty()
        || address.len() > 1024
        || address
            .bytes()
            .any(|b| b.is_ascii_whitespace() || !b.is_ascii() || b"#?;%&=/\\\0".contains(&b))
    {
        return Err(invalid(
            "TLS endpoint attributes, metadata and invalid characters are forbidden",
        ));
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| invalid("TLS endpoint requires host:port"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| invalid("invalid TLS endpoint port"))?;
    if port == 0 && !allow_zero {
        return Err(invalid("TLS endpoint port must be nonzero"));
    }
    let valid_host = if let Some(ip) = host.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        ip.parse::<IpAddr>().is_ok_and(|ip| ip.is_ipv6())
    } else {
        !host.is_empty()
            && host.len() <= 253
            && host.split('.').all(|part| {
                !part.is_empty()
                    && part.len() <= 63
                    && !part.starts_with('-')
                    && !part.ends_with('-')
                    && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
    };
    if !valid_host {
        return Err(invalid("invalid TLS endpoint host"));
    }
    if loopback {
        let literal = host.trim_start_matches('[').trim_end_matches(']');
        if !literal.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()) {
            return Err(invalid(
                "secure peer endpoints must use literal loopback IP addresses",
            ));
        }
    }
    Ok(())
}

fn validate_paths(root: &str, cert: &str, key: &str) -> Result<()> {
    let mut paths = BTreeSet::new();
    for (field, value) in [
        ("root_ca_certificate", root),
        ("certificate", cert),
        ("private_key", key),
    ] {
        if value.is_empty()
            || value.len() > 4096
            || value.contains('\0')
            || !Path::new(value).is_absolute()
        {
            return Err(invalid(format!(
                "security profile {field} must be an absolute file path"
            )));
        }
        if !paths.insert(value) {
            return Err(invalid(
                "CA, certificate and private key must be separate files",
            ));
        }
    }
    Ok(())
}

fn base(mode: &str) -> Value {
    json!({
        "mode": mode,
        "connect": {"endpoints": [], "timeout_ms": 3000, "exit_on_failure": true},
        "listen": {"endpoints": [], "timeout_ms": 0, "exit_on_failure": true},
        "scouting": {
            "multicast": {"enabled": false, "autoconnect": []},
            "gossip": {"enabled": false, "multihop": false, "autoconnect": []}
        },
        "timestamping": {"enabled": true},
        "transport": {
            "unicast": {"max_links": 1},
            "link": {"protocols": ["tls"], "tls": {
                "enable_mtls": true,
                "verify_name_on_connect": true,
                "close_link_on_expiration": true
            }},
            "shared_memory": {"enabled": false}
        },
        "adminspace": {"enabled": false, "permissions": {"read": false, "write": false}},
        "plugins_loading": {"enabled": false}
    })
}

fn native(value: &Value) -> Result<zenoh::Config> {
    zenoh::Config::from_json5(&value.to_string())
        .map_err(|error| invalid(format!("native security configuration: {error}")))
}

/// Construct a TLS-only session configuration. Without `peer`, the legacy
/// single-router client behavior is unchanged. Files and certificate
/// validity is checked by the TLS transport, not by a second crypto stack.
pub fn client_config(profile: &ClientTlsProfile) -> Result<zenoh::Config> {
    if profile.schema_version != SECURITY_SCHEMA_VERSION {
        return Err(invalid(
            "unsupported client security profile schema_version",
        ));
    }
    validate_endpoints(&profile.router_endpoints)?;
    validate_paths(
        &profile.root_ca_certificate,
        &profile.certificate,
        &profile.private_key,
    )?;
    if !(1..=60_000).contains(&profile.connect_timeout_ms) {
        return Err(invalid("connect_timeout_ms must be between 1 and 60000"));
    }
    if profile.peer.is_some() && profile.connect_timeout_ms != super::connect_timeout_ms() {
        return Err(invalid(
            "connect_timeout_ms applies only to strict client profiles; omit it in peer mode",
        ));
    }
    let mut config = base(if profile.peer.is_some() {
        "peer"
    } else {
        "client"
    });
    config["connect"]["endpoints"] = json!(profile.router_endpoints);
    config["connect"]["timeout_ms"] = json!(profile.connect_timeout_ms);
    config["transport"]["link"]["tls"]["root_ca_certificate"] = json!(profile.root_ca_certificate);
    config["transport"]["link"]["tls"]["connect_certificate"] = json!(profile.certificate);
    config["transport"]["link"]["tls"]["connect_private_key"] = json!(profile.private_key);
    if let Some(peer) = &profile.peer {
        validate_endpoint(&profile.router_endpoints[0], false, true)?;
        validate_endpoint(&peer.listen_endpoint, true, true)?;
        if peer.policy.domain_id != profile.domain_id {
            return Err(invalid(
                "peer policy domain differs from secure session profile",
            ));
        }
        let policy = with_relay_subjects(&compile_policy(&peer.policy)?, &peer.relays)?;
        config["listen"]["endpoints"] = json!([peer.listen_endpoint]);
        // Background reconnect preserves local peer operation when the Router
        // is temporarily unavailable. This never retries a business request.
        config["connect"]["timeout_ms"] = json!(-1);
        config["connect"]["exit_on_failure"] = json!(false);
        config["scouting"]["gossip"] = json!({
            "enabled":true,"multihop":false,"target":["router"],
            // The one local Router is explicit. Do not auto-connect to remote
            // Routers advertised over an already federated network.
            "autoconnect":["peer"],"autoconnect_strategy":{"to_peer":"greater-zid"}
        });
        config["transport"]["link"]["tls"]["listen_certificate"] = json!(profile.certificate);
        config["transport"]["link"]["tls"]["listen_private_key"] = json!(profile.private_key);
        config["access_control"] = policy.access_control().clone();
    }
    native(&config)
}

/// The effective Router ACL includes only explicitly configured relay rights.
pub fn router_policy(
    profile: &RouterTlsProfile,
    policy: &CompiledSecurityPolicy,
) -> Result<CompiledSecurityPolicy> {
    match &profile.federation {
        Some(federation) => with_relay_subjects(policy, &federation.relays),
        None => Ok(policy.clone()),
    }
}

/// Generate a TLS-only, default-deny Router. Federation is opt-in and connects
/// only to its explicit TLS endpoints; gossip serves the local peers.
pub fn router_config(
    profile: &RouterTlsProfile,
    policy: &CompiledSecurityPolicy,
) -> Result<zenoh::Config> {
    if profile.schema_version != SECURITY_SCHEMA_VERSION {
        return Err(invalid(
            "unsupported router security profile schema_version",
        ));
    }
    validate_endpoints(&profile.listen_endpoints)?;
    validate_paths(
        &profile.root_ca_certificate,
        &profile.certificate,
        &profile.private_key,
    )?;
    let mut config = base("router");
    config["listen"]["endpoints"] = json!(profile.listen_endpoints);
    config["transport"]["link"]["tls"]["root_ca_certificate"] = json!(profile.root_ca_certificate);
    config["transport"]["link"]["tls"]["listen_certificate"] = json!(profile.certificate);
    config["transport"]["link"]["tls"]["listen_private_key"] = json!(profile.private_key);
    if profile.local_peers || profile.federation.is_some() {
        config["scouting"]["gossip"] = json!({
            "enabled":true,"multihop":false,"target":["router","peer"],"autoconnect":[]
        });
    }
    if let Some(federation) = &profile.federation {
        if federation.connect_endpoints.len() > 16 {
            return Err(invalid(
                "federation supports at most 16 explicit Router endpoints",
            ));
        }
        let mut seen = BTreeSet::new();
        for endpoint in &federation.connect_endpoints {
            validate_endpoint(endpoint, false, false)?;
            if !seen.insert(endpoint) || profile.listen_endpoints.contains(endpoint) {
                return Err(invalid(
                    "federation endpoints must be unique and cannot equal a listener",
                ));
            }
        }
        config["connect"]["endpoints"] = json!(federation.connect_endpoints);
        config["connect"]["timeout_ms"] = json!(-1);
        config["connect"]["exit_on_failure"] = json!(false);
        config["transport"]["link"]["tls"]["connect_certificate"] = json!(profile.certificate);
        config["transport"]["link"]["tls"]["connect_private_key"] = json!(profile.private_key);
    }
    config["access_control"] = router_policy(profile, policy)?.access_control().clone();
    native(&config)
}

/// Validate the final, bootstrap-resolved configuration, before any session is
/// opened. Version 1 accepts the generated profile exactly; all native options
/// outside it are refused, including extra routes and endpoint attributes.
pub fn validate_client_config(config: &zenoh::Config, profile: &ClientTlsProfile) -> Result<()> {
    let expected = client_config(profile)?;
    // These fields are deliberately absent from native Config serialization.
    // Check their presence, never copy secret contents into diagnostics.
    let tls = config.transport().link().tls();
    if tls.root_ca_certificate_base64().is_some()
        || tls.connect_certificate_base64().is_some()
        || tls.connect_private_key_base64().is_some()
        || tls.listen_certificate_base64().is_some()
        || tls.listen_private_key_base64().is_some()
    {
        return Err(invalid(
            "inline TLS credentials are forbidden in the secure profile",
        ));
    }
    let actual =
        serde_json::to_value(config).map_err(|_| invalid("cannot inspect native configuration"))?;
    let expected =
        serde_json::to_value(expected).map_err(|_| invalid("cannot inspect secure profile"))?;
    if actual != expected {
        return Err(invalid(
            "final Zenoh configuration differs from the explicit secure client profile",
        ));
    }
    Ok(())
}
