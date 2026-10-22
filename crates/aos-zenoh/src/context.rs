use aos_core::{
    resolve_context_config, Context, ContextConfigRequest, Error, ProcessBootstrap,
    ResolvedContextConfig, Result,
};

use crate::security::{client_config, validate_client_config, ClientTlsProfile};
use crate::ZenohTransport;

/// Installed default: local router peer profile, independent of the caller cwd.
pub const DEFAULT_SESSION_CONFIG: &str = include_str!("../../../config/zenoh-session.json5");

/// Standard Rust startup. Read `AOS_PROCESS_CONFIG`, preserve deployment
/// sources, resolve the network configuration, then open exactly one session.
/// Other environment input remains explicit in `request.environment`.
pub fn open_context(request: ContextConfigRequest) -> Result<Context> {
    open(request, None)
}

/// Resolve all process/config layers first, then enforce the explicit secure
/// profile before opening the session. A failed secure open never falls back
/// to the development configuration. The profile supplies the default domain;
/// any final domain override must still equal the authorized profile domain.
pub fn open_secure_context(
    mut request: ContextConfigRequest,
    profile: &ClientTlsProfile,
) -> Result<Context> {
    if request.program.is_null() {
        request.program = serde_json::json!({"domain_id": profile.domain_id});
    } else if let Some(program) = request.program.as_object_mut() {
        program
            .entry("domain_id")
            .or_insert_with(|| serde_json::json!(profile.domain_id));
    }
    open(request, Some(profile))
}

/// Adapter entry point for a launcher/CLI that has already read its one
/// bootstrap snapshot and resolved all Context layers. It repeats no source
/// reads or merges, and still applies the same final domain and TLS checks.
pub fn open_resolved_secure_context(
    resolved: ResolvedContextConfig,
    bootstrap: Option<ProcessBootstrap>,
    profile: &ClientTlsProfile,
) -> Result<Context> {
    open_resolved(resolved, bootstrap, Some(profile))
}

fn open(request: ContextConfigRequest, security: Option<&ClientTlsProfile>) -> Result<Context> {
    let bootstrap = ProcessBootstrap::from_environment()?;
    let request = match &bootstrap {
        Some(bootstrap) => bootstrap.context_request(request)?,
        None => request,
    };
    let resolved = resolve_context_config(&request)?;
    open_resolved(resolved, bootstrap, security)
}

fn open_resolved(
    resolved: ResolvedContextConfig,
    bootstrap: Option<ProcessBootstrap>,
    security: Option<&ClientTlsProfile>,
) -> Result<Context> {
    if let Some(profile) = security {
        if resolved.domain_id != profile.domain_id {
            return Err(Error::InvalidArgument(
                "final domain differs from secure client profile".into(),
            ));
        }
    }
    let config = match &resolved.zenoh_config {
        Some(path) => zenoh::Config::from_file(path).map_err(|error| {
            if security.is_some() {
                Error::InvalidArgument(
                    "cannot parse the final secure Zenoh session configuration".into(),
                )
            } else {
                Error::InvalidArgument(format!("zenoh session config {path}: {error}"))
            }
        })?,
        None if security.is_some() => client_config(security.unwrap())?,
        None => zenoh::Config::from_json5(DEFAULT_SESSION_CONFIG).map_err(|error| {
            Error::InvalidArgument(format!("default zenoh session config: {error}"))
        })?,
    };
    if let Some(profile) = security {
        validate_client_config(&config, profile)?;
    }
    let transport = ZenohTransport::open(config, resolved.domain_id)?;
    Context::from_configured_transport(transport, resolved, bootstrap)
}
