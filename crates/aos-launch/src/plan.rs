use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use aos_core::{ContextConfigRequest, Interface, InterfaceKind, NodeConfigRequest, NodeOptions};
use serde_json::{json, Value};

use crate::{
    package::{absolute_directory, absolute_file, identifier, strict_file},
    DeploymentLayers, LaunchError, LaunchErrorCode, LaunchOverrides, LaunchResult, LaunchSpec,
    PackageIndex, ProcessBootstrap, ProcessSpec, ReadyEndpointKind, ReadyLifecycleState,
    ResolvedEntry, ResolvedLaunchContext, ResolvedReadiness, ResolvedReadyEndpoint, ResourceKind,
    ShutdownSpec, PREFIX_PATH_ENV, PROCESS_CONFIG_ENV, SCHEMA_VERSION,
};

#[derive(Debug, Clone)]
pub struct PlannedProcess {
    pub spec: ProcessSpec,
    pub entry: ResolvedEntry,
    pub cwd: PathBuf,
    /// Identity fields are filled by Supervisor for each generation.
    pub bootstrap: ProcessBootstrap,
    pub readiness: Option<ResolvedReadiness>,
}

#[derive(Debug, Clone)]
pub struct LaunchPlan {
    pub(crate) processes: Vec<PlannedProcess>,
    pub(crate) layers: Vec<Vec<usize>>,
    pub(crate) context: Option<ResolvedLaunchContext>,
    pub(crate) shutdown: ShutdownSpec,
    pub(crate) prefix_path: OsString,
}

impl LaunchPlan {
    pub fn processes(&self) -> &[PlannedProcess] {
        &self.processes
    }
    pub fn dependency_layers(&self) -> &[Vec<usize>] {
        &self.layers
    }
    pub fn context(&self) -> Option<&ResolvedLaunchContext> {
        self.context.as_ref()
    }
    pub fn shutdown(&self) -> &ShutdownSpec {
        &self.shutdown
    }
}

pub fn parse_launch(text: &str) -> LaunchResult<LaunchSpec> {
    if text.len() > crate::MAX_DOCUMENT_BYTES {
        return Err(LaunchError::invalid("parse", "launch JSON exceeds 1 MiB"));
    }
    let value = aos_core::parse_config_json(text)
        .map_err(|error| LaunchError::invalid("parse", error.to_string()))?;
    serde_json::from_value(value).map_err(|error| LaunchError::invalid("parse", error.to_string()))
}

pub fn prepare(
    index: &PackageIndex,
    spec: LaunchSpec,
    overrides: LaunchOverrides,
) -> LaunchResult<LaunchPlan> {
    let prefix_path = std::env::join_paths(index.prefixes()).map_err(|error| {
        LaunchError::invalid(
            "prepare",
            format!("prefixes cannot be represented in {PREFIX_PATH_ENV}: {error}"),
        )
    })?;
    if spec.schema_version != SCHEMA_VERSION {
        return Err(LaunchError::new(
            LaunchErrorCode::Unsupported,
            "prepare",
            "unsupported launch schema_version",
        ));
    }
    if spec.processes.is_empty() || spec.processes.len() > 128 {
        return Err(LaunchError::invalid(
            "prepare",
            "launch needs 1..=128 processes",
        ));
    }
    seconds(spec.shutdown.sigint_timeout, "sigint_timeout", false)?;
    seconds(spec.shutdown.sigterm_timeout, "sigterm_timeout", false)?;
    seconds(
        spec.shutdown.sigint_timeout + spec.shutdown.sigterm_timeout,
        "combined shutdown timeout",
        false,
    )?;
    let mut ids = BTreeMap::new();
    let mut fqns = BTreeSet::new();
    for (number, process) in spec.processes.iter().enumerate() {
        identifier(&process.id, "prepare")?;
        if ids.insert(process.id.clone(), number).is_some() {
            return Err(LaunchError::invalid("prepare", "duplicate process id"));
        }
        if let Some(readiness) = &process.readiness {
            if !fqns.insert(&readiness.node) {
                return Err(LaunchError::invalid(
                    "prepare",
                    "duplicate readiness node FQN",
                ));
            }
        }
    }
    for id in overrides.nodes.keys() {
        if !ids.contains_key(id) {
            return Err(LaunchError::invalid(
                "prepare",
                format!("unknown CLI process selector {id:?}"),
            ));
        }
    }
    let needs_probe = spec
        .processes
        .iter()
        .any(|process| process.readiness.is_some());
    if needs_probe && spec.context.is_none() {
        return Err(LaunchError::invalid(
            "prepare",
            "readiness requires an explicit shared context",
        ));
    }
    let mut launch_context = spec
        .context
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| LaunchError::invalid("prepare", error.to_string()))?
        .unwrap_or_else(|| json!({}));
    let mut cli_context = overrides.context;
    normalize_context_path(&mut launch_context)?;
    normalize_context_path(&mut cli_context)?;
    let resolved = aos_core::resolve_context_config(&ContextConfigRequest {
        launch: launch_context.clone(),
        cli: cli_context.clone(),
        ..Default::default()
    })
    .map_err(|error| LaunchError::invalid("prepare", error.to_string()))?;
    let context = needs_probe.then(|| ResolvedLaunchContext {
        domain_id: resolved.domain_id,
        zenoh_config: resolved.zenoh_config.as_ref().map(PathBuf::from),
        effective_config: resolved.effective_value(),
    });

    let layers = dependency_layers(&spec.processes, &ids)?;
    let mut processes = Vec::new();
    for mut process in spec.processes {
        let process_id = process.id.clone();
        let result = (|| {
            index.package(&process.package, process.package_version.as_deref())?;
            let entry = index.entry(&process.package, &process.entry)?;
            let cwd = match &process.cwd {
                Some(path) => absolute_directory(path, "prepare")?,
                None => entry.share_dir.clone(),
            };
            validate_environment(&process.env)?;
            freeze_environment_paths(&mut process.env)?;
            let child_context = aos_core::resolve_context_config(&ContextConfigRequest {
                environment: inherited_environment(
                    &process.env,
                    &["AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG"],
                )?,
                launch: launch_context.clone(),
                cli: cli_context.clone(),
                ..Default::default()
            })
            .map_err(|error| LaunchError::invalid("context", error.to_string()))?;
            if needs_probe
                && (child_context.domain_id != resolved.domain_id
                    || child_context.zenoh_config != resolved.zenoh_config)
            {
                return Err(LaunchError::invalid(
                    "context",
                    "child and supervisor must resolve the same domain_id and zenoh_config",
                ));
            }
            if process.args.len() > 4096
                || process.args.iter().map(String::len).sum::<usize>() > crate::MAX_DOCUMENT_BYTES
                || process.args.iter().any(|arg| arg.contains('\0'))
            {
                return Err(LaunchError::invalid("prepare", "invalid or oversized argv"));
            }
            seconds(process.restart.delay, "restart delay", false)?;
            if process.restart.max_restarts == u32::MAX {
                return Err(LaunchError::invalid(
                    "prepare",
                    "restart count would overflow generation",
                ));
            }
            for path in &mut process.config_files {
                *path = absolute_file(path, "config")?;
                // Validate size, duplicate keys and JSON before the core reads it.
                strict_file(path, "config")?;
            }
            let node_cli = overrides
                .nodes
                .get(&process.id)
                .cloned()
                .unwrap_or_else(|| json!({}));
            let files = process
                .config_files
                .iter()
                .map(|path| utf8_path(path, "config"))
                .collect::<LaunchResult<Vec<_>>>()?;
            aos_core::resolve_node_config(
                "aos_launch_preflight",
                "/",
                NodeOptions::default(),
                &NodeConfigRequest {
                    config_files: files.clone(),
                    environment: node_environment(&process.env)?,
                    launch: process.node.clone(),
                    cli: node_cli.clone(),
                    ..Default::default()
                },
            )
            .map_err(|error| LaunchError::invalid("config", error.to_string()))?;
            let readiness = process
                .readiness
                .as_ref()
                .map(|ready| resolve_readiness(index, ready))
                .transpose()?;
            let bootstrap = ProcessBootstrap {
                schema_version: SCHEMA_VERSION,
                launch_id: String::new(),
                process_id: process.id.clone(),
                generation: 0,
                context: DeploymentLayers {
                    launch: launch_context.clone(),
                    cli: cli_context.clone(),
                    ..Default::default()
                },
                node: DeploymentLayers {
                    config_files: files,
                    launch: process.node.clone(),
                    cli: node_cli,
                },
            };
            // The supervisor fills identity at spawn. Validate the complete
            // remaining wire contract now, before any process can be started.
            let mut template = bootstrap.clone();
            template.launch_id = uuid::Uuid::nil().to_string();
            template.generation = 1;
            let serialized = serde_json::to_string(&template)
                .map_err(|error| LaunchError::invalid("config", error.to_string()))?;
            if serialized.len() + 128 > crate::MAX_DOCUMENT_BYTES {
                return Err(LaunchError::invalid(
                    "config",
                    "process bootstrap exceeds 1 MiB",
                ));
            }
            aos_core::ProcessBootstrap::from_json(&serialized)
                .map_err(|error| LaunchError::invalid("config", error.to_string()))?;
            Ok(PlannedProcess {
                spec: process,
                entry,
                cwd,
                bootstrap,
                readiness,
            })
        })();
        processes.push(result.map_err(|error: LaunchError| error.for_process(process_id))?);
    }
    Ok(LaunchPlan {
        processes,
        layers,
        context,
        shutdown: spec.shutdown,
        prefix_path,
    })
}

pub(crate) fn seconds(value: f64, field: &str, positive: bool) -> LaunchResult<Duration> {
    if !value.is_finite()
        || value < 0.0
        || value > crate::MAX_WAIT_SECONDS
        || (positive && value == 0.0)
    {
        return Err(LaunchError::invalid(
            "prepare",
            format!(
                "{field} must be finite, at most 86400 seconds and {}",
                if positive { "positive" } else { "non-negative" }
            ),
        ));
    }
    let duration = Duration::try_from_secs_f64(value)
        .map_err(|_| LaunchError::invalid("prepare", format!("{field} is out of range")))?;
    Instant::now().checked_add(duration).ok_or_else(|| {
        LaunchError::invalid("prepare", format!("{field} exceeds steady clock range"))
    })?;
    Ok(duration)
}

fn normalize_context_path(value: &mut Value) -> LaunchResult<()> {
    if let Some(path) = value.get_mut("zenoh_config") {
        if let Some(text) = path.as_str() {
            *path = json!(utf8_path(
                &absolute_file(Path::new(text), "context")?,
                "context"
            )?);
        }
    }
    Ok(())
}

fn utf8_path(path: &Path, stage: &str) -> LaunchResult<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| LaunchError::invalid(stage, "deployment path must be UTF-8"))
}

fn validate_environment(environment: &BTreeMap<String, String>) -> LaunchResult<()> {
    if environment
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>()
        > crate::MAX_DOCUMENT_BYTES
    {
        return Err(LaunchError::invalid(
            "prepare",
            "environment overrides exceed 1 MiB",
        ));
    }
    for (key, value) in environment {
        if key.is_empty()
            || key.contains(['=', '\0'])
            || value.contains('\0')
            || key == PROCESS_CONFIG_ENV
            || key == PREFIX_PATH_ENV
        {
            return Err(LaunchError::invalid(
                "prepare",
                format!("invalid or reserved environment key {key:?}"),
            ));
        }
    }
    Ok(())
}

fn freeze_environment_paths(environment: &mut BTreeMap<String, String>) -> LaunchResult<()> {
    let base = std::env::current_dir().map_err(|error| LaunchError::io("prepare", error))?;
    for key in ["AOS_ZENOH_CONFIG", "AOS_INTERFACE_PATH"] {
        let value = if let Some(value) = environment.get(key) {
            value.clone()
        } else {
            match std::env::var(key) {
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => continue,
                Err(_) => {
                    return Err(LaunchError::invalid(
                        "prepare",
                        format!("{key} must be UTF-8"),
                    ))
                }
            }
        };
        if value.is_empty() {
            continue;
        }
        let absolutize = |path: &Path| {
            if path.is_absolute() {
                path.to_owned()
            } else {
                base.join(path)
            }
        };
        let fixed = if key == "AOS_ZENOH_CONFIG" {
            utf8_path(
                &absolute_file(&absolutize(Path::new(&value)), "environment")?,
                "environment",
            )?
        } else {
            let paths = std::env::split_paths(&value)
                .map(|path| {
                    if path.as_os_str().is_empty() {
                        return Err(LaunchError::invalid(
                            "environment",
                            "AOS_INTERFACE_PATH contains an empty entry",
                        ));
                    }
                    let path = absolutize(&path)
                        .canonicalize()
                        .map_err(|error| LaunchError::io("environment", error))?;
                    if !path.is_file() && !path.is_dir() {
                        return Err(LaunchError::invalid(
                            "environment",
                            "interface path must be a regular file or directory",
                        ));
                    }
                    Ok(path)
                })
                .collect::<LaunchResult<Vec<_>>>()?;
            std::env::join_paths(paths)
                .map_err(|error| LaunchError::invalid("environment", error.to_string()))?
                .into_string()
                .map_err(|_| LaunchError::invalid("environment", "interface paths must be UTF-8"))?
        };
        environment.insert(key.into(), fixed);
    }
    Ok(())
}

fn node_environment(
    overrides: &BTreeMap<String, String>,
) -> LaunchResult<BTreeMap<String, String>> {
    inherited_environment(
        overrides,
        &["AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME"],
    )
}

fn inherited_environment(
    overrides: &BTreeMap<String, String>,
    keys: &[&str],
) -> LaunchResult<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for key in keys {
        let value = if let Some(value) = overrides.get(*key) {
            Some(value.clone())
        } else {
            match std::env::var(key) {
                Ok(value) => Some(value),
                Err(std::env::VarError::NotPresent) => None,
                Err(_) => {
                    return Err(LaunchError::invalid(
                        "prepare",
                        format!("{key} must be UTF-8"),
                    ))
                }
            }
        };
        if let Some(value) = value {
            result.insert((*key).to_owned(), value);
        }
    }
    Ok(result)
}

fn resolve_readiness(
    index: &PackageIndex,
    ready: &crate::ReadinessSpec,
) -> LaunchResult<ResolvedReadiness> {
    aos_core::names::validate_absolute_name(&ready.node)
        .map_err(|error| LaunchError::invalid("readiness", error.to_string()))?;
    if ready.node == "/" || ready.endpoints.len() > 128 {
        return Err(LaunchError::invalid(
            "readiness",
            "invalid node or too many readiness endpoints",
        ));
    }
    if ready.lifecycle.as_ref().is_some_and(|lifecycle| {
        lifecycle.autostart && lifecycle.target != ReadyLifecycleState::Active
    }) {
        return Err(LaunchError::invalid(
            "readiness",
            "autostart requires target=active",
        ));
    }
    let timeout = seconds(ready.timeout, "readiness timeout", true)?;
    let mut endpoints = Vec::new();
    let mut names = BTreeSet::new();
    for requirement in &ready.endpoints {
        aos_core::names::validate_absolute_name(&requirement.name)
            .map_err(|error| LaunchError::invalid("readiness", error.to_string()))?;
        if !names.insert((&requirement.name, format!("{:?}", requirement.kind))) {
            return Err(LaunchError::invalid(
                "readiness",
                "duplicate readiness endpoint",
            ));
        }
        let resource = index.resource(
            &requirement.interface.package,
            &requirement.interface.resource,
        )?;
        if resource.kind != ResourceKind::Interface {
            return Err(LaunchError::invalid(
                "readiness",
                "readiness resource is not an interface",
            ));
        }
        let interface = Interface::from_json(strict_file(&resource.path, "interface")?)
            .map_err(|error| LaunchError::invalid("interface", error.to_string()))?;
        let expected = match requirement.kind {
            ReadyEndpointKind::Publisher | ReadyEndpointKind::Subscription => {
                InterfaceKind::Message
            }
            ReadyEndpointKind::Service => InterfaceKind::Service,
            ReadyEndpointKind::Action => InterfaceKind::Action,
        };
        if interface.kind() != expected {
            return Err(LaunchError::invalid(
                "readiness",
                "endpoint kind differs from installed interface kind",
            ));
        }
        endpoints.push(ResolvedReadyEndpoint {
            kind: requirement.kind,
            name: requirement.name.clone(),
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
        });
    }
    Ok(ResolvedReadiness {
        node: ready.node.clone(),
        timeout,
        endpoints,
        lifecycle: ready.lifecycle.clone(),
    })
}

fn dependency_layers(
    processes: &[ProcessSpec],
    ids: &BTreeMap<String, usize>,
) -> LaunchResult<Vec<Vec<usize>>> {
    for process in processes {
        let mut seen = BTreeSet::new();
        for dependency in &process.depends_on {
            let target = ids.get(dependency).ok_or_else(|| {
                LaunchError::invalid("prepare", format!("unknown dependency {dependency:?}"))
            })?;
            if !seen.insert(dependency) || dependency == &process.id {
                return Err(
                    LaunchError::invalid("prepare", "duplicate or self dependency")
                        .for_process(&process.id),
                );
            }
            if processes[*target].readiness.is_none() {
                return Err(LaunchError::invalid(
                    "prepare",
                    "dependency provider must define readiness",
                )
                .for_process(&process.id));
            }
        }
    }
    let mut done = BTreeSet::new();
    let mut layers = Vec::new();
    while done.len() < processes.len() {
        let ready: Vec<_> = processes
            .iter()
            .enumerate()
            .filter(|(index, process)| {
                !done.contains(index)
                    && process
                        .depends_on
                        .iter()
                        .all(|dependency| done.contains(&ids[dependency]))
            })
            .map(|(index, _)| index)
            .collect();
        if ready.is_empty() {
            return Err(LaunchError::invalid(
                "prepare",
                "launch dependency graph contains a cycle",
            ));
        }
        done.extend(ready.iter().copied());
        layers.push(ready);
    }
    Ok(layers)
}
