//! Agent-facing views of one persistent, current-and-future discovery interest.
//! RPCs run outside transport callbacks. A failed description never removes a node.
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aos_core::{transport::GraphWatchBackend, Context, EntityKind, GraphEntity};
use serde_json::{json, Value};

use crate::{configuration::InvocationConfig, full_name, CliError, Result};

const PROBE_BATCH: usize = 8;
const MAX_VIEW_BYTES: usize = 2 * 1024 * 1024;
const POLL: Duration = Duration::from_millis(50);
type View = BTreeMap<String, Value>;

pub fn request_fields(verb: &str) -> Value {
    let mut fields = json!({
        "timeout":{"type":"number","exclusiveMinimum":0,"maximum":86400,"default":if verb == "snapshot" {5} else {30},
            "description":"One total observation and description budget after session open. watch follow=true without timeout is unbounded."},
        "settle_time":{"type":"number","minimum":0,"maximum":10,"default":0.5,
            "description":"Initial discovery window, within timeout; not a global completeness barrier."},
        "probe_timeout":{"type":"number","minimum":0.01,"maximum":5,"default":0.5,
            "description":"Per-NodeCard cap, further limited by remaining total time. At most eight concurrent read-only probes."},
        "refresh_interval":{"type":"number","minimum":0.1,"maximum":300,"default":5,
            "description":"Minimum interval between description attempts; fair batches can take longer on large or slow networks. Graph changes invalidate cached descriptions immediately."},
        "descriptions":{"type":"boolean","default":true},
        "node_name":{"type":"string","pattern":"^/","minLength":2},
        "namespace":{"type":"string","pattern":"^/","minLength":1,
            "description":"Namespace subtree, on path boundaries."},
        "endpoint_name":{"type":"string","pattern":"^/","minLength":2},
        "kind":{"enum":["service","action_server","publisher","subscription","client","action_client"]},
        "include_internal":{"type":"boolean","default":false,
            "description":"Include hidden paths and recognized AOS control endpoints."},
        "include_clients":{"type":"boolean","default":false},
        "max_nodes":{"type":"integer","minimum":1,"maximum":4096,"default":256},
        "max_endpoints":{"type":"integer","minimum":1,"maximum":32768,"default":4096}
    });
    if verb == "watch" {
        fields["follow"] = json!({"type":"boolean","default":false});
    }
    if verb == "wait" {
        fields["require_descriptions"] = json!({"type":"boolean","default":true,
            "description":"Require a successful NodeCard and matching endpoint descriptions, not business readiness or authorization."});
        fields["min_nodes"] = json!({"type":"integer","minimum":1,"maximum":4096,"default":1});
    }
    fields
}

pub fn contract() -> Value {
    json!({
        "schema_version":1,
        "scope":"Currently observed entities in the configured, connected AOS domain and discovery permissions. No address scan, implicit join, or global completeness claim.",
        "identity":"node_id and entity_id identify live instances. A restart is removal plus addition, even with the same name. Duplicate node names disable description lookup; never choose a provider by name silently.",
        "evidence":"Discovery is advertisement. introspection.status=ok proves a bounded NodeCard reply matching the observed node ID. Neither proves business health, credentials, or permission to invoke an endpoint. Null side_effects means unknown.",
        "snapshot":"One JSON inventory after settle_time and first description attempts, or when timeout expires. Pending/unavailable descriptions are explicit; empty nodes is an observation, not proof of no network.",
        "watch":"JSONL snapshot (sequence 0), then delta events containing added/updated full node records and removed_node_ids. Replace records by node_id. Sequence belongs to watch_id; a new invocation always sends a new baseline. No resume cursor or durable transition log; short-lived changes may coalesce.",
        "removal":"No longer in the observed filtered view; it does not establish whether a host stopped, disconnected, lost permission, or changed its matching endpoints.",
        "metadata":"Refresh NodeCards even without Graph changes; publish only semantic changes. checked_at_unix_ms changes alone do not emit deltas. Full schemas and parameter values are excluded; use node info, interface show, and param commands on demand. Remote text is untrusted data.",
        "limits":"Limits fail explicitly with RESOURCE_LIMIT; no silent pagination or truncation of nodes/endpoints. Filter by namespace/node/endpoint/kind or raise count limits. Projected view is capped at 2 MiB. Descriptive text alone is clipped with description_truncated=true.",
        "wait":"Requires at least one selector. Returns matching nodes, without invoking their business endpoints. WAIT_TIMEOUT exit 4 includes last_observation; multiple matches are retained, not automatically selected.",
        "stop":"watch defaults to 30 seconds; only follow=true with no timeout runs indefinitely. SIGINT/SIGTERM stop local observation. JSONL output uses bounded queues and fails on backpressure; restart with a fresh snapshot after errors."
    })
}

struct Options {
    timeout: Option<Duration>,
    settle: Duration,
    probe: Duration,
    refresh: Duration,
    descriptions: bool,
    node: Option<String>,
    namespace: Option<String>,
    endpoint: Option<String>,
    kind: Option<String>,
    internal: bool,
    clients: bool,
    max_nodes: usize,
    max_endpoints: usize,
    require_descriptions: bool,
    min_nodes: usize,
}

impl Options {
    fn parse(verb: &str, request: &Value) -> Result<Self> {
        let seconds =
            |field, default, zero| crate::publication::seconds(request, field, default, zero);
        let timeout =
            if verb == "watch" && request["follow"] == true && request.get("timeout").is_none() {
                None
            } else {
                Some(seconds(
                    "timeout",
                    if verb == "snapshot" { 5.0 } else { 30.0 },
                    false,
                )?)
            };
        let settle = seconds("settle_time", 0.5, true)?;
        if timeout.is_some_and(|limit| settle > limit) {
            return Err(CliError::input("settle_time must not exceed timeout"));
        }
        let string = |key: &str| request[key].as_str().map(str::to_owned);
        let count =
            |key, default| crate::optional_u64(request, key).map(|n| n.unwrap_or(default) as usize);
        let options = Self {
            timeout,
            settle,
            probe: seconds("probe_timeout", 0.5, false)?,
            refresh: seconds("refresh_interval", 5.0, false)?,
            descriptions: request["descriptions"].as_bool().unwrap_or(true),
            node: string("node_name"),
            namespace: string("namespace"),
            endpoint: string("endpoint_name"),
            kind: string("kind"),
            internal: request["include_internal"] == true,
            clients: request["include_clients"] == true,
            max_nodes: count("max_nodes", 256)?,
            max_endpoints: count("max_endpoints", 4096)?,
            require_descriptions: request["require_descriptions"].as_bool().unwrap_or(true),
            min_nodes: count("min_nodes", 1)?,
        };
        if verb == "wait" {
            if options.node.is_none()
                && options.namespace.is_none()
                && options.endpoint.is_none()
                && options.kind.is_none()
            {
                return Err(CliError::input(
                    "graph wait requires node_name, namespace, endpoint_name, or kind",
                ));
            }
            if options.require_descriptions && !options.descriptions {
                return Err(CliError::input(
                    "require_descriptions=true conflicts with descriptions=false",
                ));
            }
            if options.min_nodes > options.max_nodes {
                return Err(CliError::input("min_nodes must not exceed max_nodes"));
            }
        }
        Ok(options)
    }

    fn node_matches(&self, node: &ObservedNode) -> bool {
        self.node.as_ref().is_none_or(|name| name == &node.name)
            && self.namespace.as_ref().is_none_or(|namespace| {
                let namespace = namespace.trim_end_matches('/');
                node.namespace == namespace || node.namespace.starts_with(&format!("{namespace}/"))
            })
    }

    fn endpoint_matches(&self, entity: &GraphEntity) -> bool {
        let Some(endpoint) = &entity.endpoint else {
            return false;
        };
        self.endpoint
            .as_ref()
            .is_none_or(|name| name == &endpoint.name)
            && self
                .kind
                .as_ref()
                .is_none_or(|kind| kind == kind_name(entity.kind))
            && (self.clients
                || self.kind.is_some()
                || !matches!(entity.kind, EntityKind::Client | EntityKind::ActionClient))
            && (self.internal || !internal(entity))
    }
}

pub fn validate(verb: &str, request: &Value) -> Result<()> {
    Options::parse(verb, request).map(|_| ())
}

fn kind_name(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Node => "node",
        EntityKind::Publisher => "publisher",
        EntityKind::Subscription => "subscription",
        EntityKind::Client => "client",
        EntityKind::Service => "service",
        EntityKind::ActionClient => "action_client",
        EntityKind::ActionServer => "action_server",
    }
}

fn internal(entity: &GraphEntity) -> bool {
    let Some(endpoint) = &entity.endpoint else {
        return false;
    };
    if crate::hidden(&endpoint.name) {
        return true;
    }
    let controls = [
        ("get_node_card", "GetNodeCard"),
        ("get_type_description", "GetTypeDescription"),
        ("get_parameters", "GetParameters"),
        ("get_parameter_types", "GetParameterTypes"),
        ("set_parameters", "SetParameters"),
        ("set_parameters_atomically", "SetParametersAtomically"),
        ("describe_parameters", "DescribeParameters"),
        ("list_parameters", "ListParameters"),
        ("change_state", "ChangeState"),
        ("get_state", "GetState"),
        ("get_available_states", "GetAvailableStates"),
        ("get_available_transitions", "GetAvailableTransitions"),
        ("get_transition_graph", "GetTransitionGraph"),
    ];
    (entity.kind == EntityKind::Service
        && controls.iter().any(|(suffix, ty)| {
            endpoint.name == format!("{}/{suffix}", full_name(entity))
                && endpoint.type_name == format!("aos_interfaces/srv/{ty}")
        }))
        || (entity.kind == EntityKind::Publisher
            && endpoint.name == format!("{}/transition_event", full_name(entity))
            && endpoint.type_name == "aos_interfaces/msg/TransitionEvent")
        || (entity.kind == EntityKind::Publisher
            && endpoint.name == "/parameter_events"
            && endpoint.type_name == "aos_interfaces/msg/ParameterEvent")
}

#[derive(Clone, PartialEq, Eq)]
struct ObservedNode {
    name: String,
    namespace: String,
    declared: bool,
    ambiguous: bool,
    entities: Vec<GraphEntity>,
}

fn observe(graph: Vec<GraphEntity>) -> BTreeMap<String, ObservedNode> {
    let mut nodes = BTreeMap::<String, ObservedNode>::new();
    for entity in graph {
        let node = nodes
            .entry(entity.node_id.clone())
            .or_insert_with(|| ObservedNode {
                name: full_name(&entity),
                namespace: entity.namespace.clone(),
                declared: false,
                ambiguous: false,
                entities: Vec::new(),
            });
        node.declared |= entity.kind == EntityKind::Node;
        node.entities.push(entity);
    }
    let mut names = BTreeMap::<String, usize>::new();
    for node in nodes.values() {
        *names.entry(node.name.clone()).or_default() += 1;
    }
    for node in nodes.values_mut() {
        node.ambiguous = names[&node.name] > 1;
        node.entities.sort_by(|a, b| a.id.cmp(&b.id));
    }
    nodes
}

struct Description {
    graph: ObservedNode,
    attempted: Instant,
    checked_at: u64,
    status: &'static str,
    error: Option<&'static str>,
    card: Value,
}

// Keep no arbitrary config, parameters, schema, process environment or remote errors.
fn compact_card(card: &Value) -> Value {
    let mut truncated = false;
    let mut clip = |value: &Value| -> Value {
        match value.as_str() {
            Some(text) => {
                let clipped: String = text.chars().take(1024).collect();
                truncated |= clipped.len() != text.len();
                json!(clipped)
            }
            None => Value::Null,
        }
    };
    let display = clip(&card["display_name"]);
    let summary = clip(&card["summary"]);
    let tags: Vec<_> = card["tags"]
        .as_array()
        .into_iter()
        .flatten()
        .take(32)
        .map(&mut clip)
        .collect();
    let endpoints: Vec<_> = card["endpoints"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|endpoint| {
            json!({"name":endpoint["name"], "kind":endpoint["kind"],
            "type_name":endpoint["type_name"], "type_hash":endpoint["type_hash"],
            "summary":clip(&endpoint["summary"]), "side_effects":clip(&endpoint["side_effects"]),
            "contract_version":clip(&endpoint["contract_version"])})
        })
        .collect();
    truncated |= card["tags"].as_array().is_some_and(|tags| tags.len() > 32);
    let lifecycle = &card["lifecycle"];
    json!({"display_name":display,"summary":summary,"tags":tags,
        "lifecycle":if lifecycle.is_object() {json!({
            "current_state":lifecycle["current_state"],"transitioning":lifecycle["transitioning"],
            "gate_open":lifecycle["gate_open"],"epoch":lifecycle["epoch"]
        })} else {Value::Null},
        "parameter_version":card["effective_config"]["parameter_version"],
        "endpoints":endpoints,"description_truncated":truncated})
}

fn wall_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn resource_limit(message: &str) -> CliError {
    CliError {
        code: "RESOURCE_LIMIT",
        message: message.into(),
        exit: 2,
    }
}

fn selected<'a>(
    nodes: &'a BTreeMap<String, ObservedNode>,
    options: &Options,
) -> Result<Vec<(&'a String, &'a ObservedNode)>> {
    let mut endpoints = 0;
    let mut selected = Vec::new();
    for (id, node) in nodes {
        if !options.node_matches(node) {
            continue;
        }
        let count = node
            .entities
            .iter()
            .filter(|entity| options.endpoint_matches(entity))
            .count();
        if count == 0 && (options.endpoint.is_some() || options.kind.is_some()) {
            continue;
        }
        endpoints += count;
        selected.push((id, node));
        if selected.len() > options.max_nodes || endpoints > options.max_endpoints {
            return Err(resource_limit("Graph view exceeds max_nodes/max_endpoints; narrow selectors or raise the documented limits"));
        }
    }
    Ok(selected)
}

fn project(
    nodes: &BTreeMap<String, ObservedNode>,
    cache: &BTreeMap<String, Description>,
    options: &Options,
) -> Result<View> {
    let mut view = View::new();
    let mut providers = BTreeMap::<(&str, &str), (BTreeSet<&str>, usize)>::new();
    for node in nodes.values() {
        for entity in &node.entities {
            if matches!(entity.kind, EntityKind::Service | EntityKind::ActionServer) {
                if let Some(endpoint) = &entity.endpoint {
                    let (owners, count) = providers
                        .entry((kind_name(entity.kind), &endpoint.name))
                        .or_default();
                    owners.insert(&entity.node_id);
                    *count += 1;
                }
            }
        }
    }
    for (id, node) in selected(nodes, options)? {
        let description = cache
            .get(id)
            .filter(|description| description.graph == *node);
        let status = if node.ambiguous {
            "ambiguous"
        } else if !options.descriptions {
            "disabled"
        } else if !node.declared {
            "pending"
        } else {
            description.map_or("pending", |d| d.status)
        };
        let current = description.filter(|d| status == "ok" && d.status == "ok");
        let card = current.map_or(&Value::Null, |d| &d.card);
        let endpoints: Vec<_> = node
            .entities
            .iter()
            .filter(|e| options.endpoint_matches(e))
            .map(|entity| {
                let endpoint = entity.endpoint.as_ref().expect("selected endpoint");
                let descriptions: Vec<_> = card["endpoints"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|d| {
                        d["name"] == endpoint.name
                            && d["kind"] == kind_name(entity.kind)
                            && d["type_name"] == endpoint.type_name
                            && d["type_hash"] == endpoint.type_hash
                    })
                    .collect();
                // Several resources may share one name/type. Metadata is only usable
                // when every matching description agrees; never pick the first one.
                let description = descriptions
                    .first()
                    .filter(|first| descriptions.iter().all(|d| d == *first));
                let metadata = description.copied().unwrap_or(&Value::Null);
                json!({"entity_id":entity.id,"name":endpoint.name,"kind":kind_name(entity.kind),
                "type_name":endpoint.type_name,"type_hash":endpoint.type_hash,
                "provider_selection":providers.get(&(kind_name(entity.kind),endpoint.name.as_str())).map(|(owners,count)| json!({
                    "status":if *count == 1 {"unique_observed"} else {"multiple_observed"},
                    "node_count":owners.len(),"entity_count":count})),
                "description_status":if description.is_some() {"current"} else {"unknown"},
                "summary":metadata["summary"],"side_effects":metadata["side_effects"],
                "contract_version":metadata["contract_version"]})
            })
            .collect();
        view.insert(id.clone(), json!({"node_id":id,"name":node.name,
            "node_declaration_observed":node.declared,
            "introspection":{"status":status,"error_code":if node.ambiguous {Some("AMBIGUOUS_NODE")} else {description.and_then(|d| d.error)},
                "checked_at_unix_ms":description.map(|d| d.checked_at)},
            "display_name":card["display_name"],"summary":card["summary"],"tags":card["tags"],
            "lifecycle":card["lifecycle"],"parameter_version":card["parameter_version"],
            "description_truncated":card["description_truncated"].as_bool().unwrap_or(false),
            "endpoints":endpoints}));
    }
    if serde_json::to_vec(&view)
        .map_err(|error| CliError::input(error.to_string()))?
        .len()
        > MAX_VIEW_BYTES
    {
        return Err(resource_limit(
            "Graph view exceeds 2 MiB; narrow selectors or disable descriptions",
        ));
    }
    Ok(view)
}

fn probe_batch(
    ctx: &Context,
    nodes: &BTreeMap<String, ObservedNode>,
    cache: &mut BTreeMap<String, Description>,
    options: &Options,
    deadline: Option<Instant>,
    repeat: bool,
) -> Result<bool> {
    if !options.descriptions {
        return Ok(false);
    }
    let now = Instant::now();
    let mut pending: Vec<_> = selected(nodes, options)?
        .into_iter()
        .filter(|(id, node)| {
            node.declared
                && !node.ambiguous
                && cache.get(*id).is_none_or(|old| {
                    old.graph != **node
                        || (repeat && now.duration_since(old.attempted) >= options.refresh)
                })
        })
        .collect();
    // Oldest attempt first, including changed nodes. Repeatedly failing peers
    // cannot starve nodes later in the name/ID order.
    pending.sort_by_key(|(id, _)| cache.get(*id).map(|old| old.attempted));
    pending.truncate(PROBE_BATCH);
    let wait = deadline.map_or(options.probe, |end| {
        options.probe.min(end.saturating_duration_since(now))
    });
    if pending.is_empty() || wait.is_zero() {
        return Ok(false);
    }
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = pending
            .into_iter()
            .map(|(id, node)| {
                scope.spawn(move || {
                    let result = ctx.get_node_card(&node.name, wait);
                    (id.clone(), node.clone(), result, wall_time())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| CliError {
                    code: "INTERNAL_ERROR",
                    message: "NodeCard probe worker panicked".into(),
                    exit: 1,
                })
            })
            .collect::<Result<Vec<_>>>()
    })?;
    for (id, graph, result, checked_at) in results {
        let (status, error, card) = match result {
            Ok(card) if card["id"] == id && card["fully_qualified_name"] == graph.name => {
                ("ok", None, compact_card(&card))
            }
            Ok(_) => ("unavailable", Some("IDENTITY_MISMATCH"), Value::Null),
            Err(error) => ("unavailable", Some(CliError::from(error).code), Value::Null),
        };
        cache.insert(
            id,
            Description {
                graph,
                attempted: Instant::now(),
                checked_at,
                status,
                error,
                card,
            },
        );
    }
    Ok(true)
}

fn meaningful(value: &Value) -> Value {
    let mut value = value.clone();
    value["introspection"]
        .as_object_mut()
        .unwrap()
        .remove("checked_at_unix_ms");
    value
}

fn delta(previous: &View, next: &View) -> Option<Value> {
    let added: Vec<_> = next
        .iter()
        .filter(|(id, _)| !previous.contains_key(*id))
        .map(|(_, node)| node)
        .collect();
    let updated: Vec<_> = next
        .iter()
        .filter(|(id, node)| {
            previous
                .get(*id)
                .is_some_and(|old| meaningful(old) != meaningful(node))
        })
        .map(|(_, node)| node)
        .collect();
    let removed: Vec<_> = previous
        .keys()
        .filter(|id| !next.contains_key(*id))
        .collect();
    if added.is_empty() && updated.is_empty() && removed.is_empty() {
        return None;
    }
    Some(json!({"added":added,"updated":updated,"removed_node_ids":removed}))
}

fn coverage(view: &View, observed: usize) -> Value {
    json!({"observed_nodes":observed,"selected_nodes":view.len(),
        "selected_endpoints":view.values().map(|node| node["endpoints"].as_array().unwrap().len()).sum::<usize>(),
        "descriptions_ok":view.values().filter(|node| node["introspection"]["status"] == "ok").count(),
        "descriptions_pending":view.values().filter(|node| node["introspection"]["status"] == "pending").count(),
        "global_completeness":"unknown","truncated":false})
}

fn inventory(view: &View, observed: usize, scope: &Value, started: Instant) -> Value {
    json!({"schema_version":1,"observed_at_unix_ms":wall_time(),"elapsed_ms":started.elapsed().as_millis(),
        "scope":scope,"coverage":coverage(view,observed),"nodes":view.values().collect::<Vec<_>>()})
}

fn check_stop() -> Result<()> {
    #[cfg(any(unix, windows))]
    {
        let signal = crate::process_commands::received_signal();
        if signal != 0 {
            return Err(CliError {
                code: "INTERRUPTED",
                message: format!("Graph observation stopped by signal {signal}"),
                exit: 128 + signal,
            });
        }
    }
    Ok(())
}

fn pause(watch: &dyn GraphWatchBackend, wait: Duration) -> Result<()> {
    match watch.wait_for_change(wait.min(POLL)) {
        Ok(()) | Err(aos_core::Error::Timeout) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn stream(name: &str, data: &Value) -> Result<()> {
    #[cfg(any(unix, windows))]
    {
        let frame = json!({"schema_version":1,"event":name,"data":data});
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            check_stop()?;
            if crate::process_commands::try_stream_event(&frame)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(CliError {
                    code: "OUTPUT_ERROR",
                    message: "Graph output queue remained full; restart with a fresh snapshot"
                        .into(),
                    exit: 1,
                });
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[cfg(not(any(unix, windows)))]
    crate::event(name, data.clone())
}

#[cfg(test)]
#[path = "graph_commands_tests.rs"]
mod tests;

pub fn execute(
    ctx: &Context,
    router_ids: Option<&[String]>,
    verb: &str,
    request: &Value,
    configuration: &InvocationConfig,
) -> Result<Option<Value>> {
    let options = Options::parse(verb, request)?;
    let started = Instant::now();
    let deadline = options.timeout.map(|duration| started + duration);
    let watch = ctx.watch_graph()?.ok_or_else(|| CliError {
        code: "UNSUPPORTED",
        message: "This transport does not support a persistent Graph watch".into(),
        exit: 2,
    })?;
    let scope = json!({"domain_id":configuration.context.domain_id,"router_ids":router_ids,
        "security_profile_selected":configuration.security.is_some(),
        "network":"configured connected domain and permitted discovery view",
        "business_health":"unverified","business_authorization":"unknown",
        "filters":{"node_name":options.node,"namespace":options.namespace,"endpoint_name":options.endpoint,
            "kind":options.kind,"include_internal":options.internal,"include_clients":options.clients},
        "description_refresh_interval_seconds":options.refresh.as_secs_f64()});
    let mut cache = BTreeMap::new();
    let watch_id = uuid::Uuid::new_v4().simple().to_string();
    let mut sequence = 0_u64;
    let mut previous = None::<View>;
    let result = (|| {
        while started.elapsed() < options.settle {
            check_stop()?;
            pause(
                watch.as_ref(),
                options.settle.saturating_sub(started.elapsed()),
            )?;
        }
        loop {
            check_stop()?;
            #[cfg(any(unix, windows))]
            if verb == "watch" {
                crate::process_commands::check_stream_output()?;
            }
            let mut nodes = observe(watch.snapshot()?);
            cache.retain(|id, _| nodes.contains_key(id));
            let mut view = project(&nodes, &cache, &options)?;
            // Publish discovery before waiting on any NodeCard. Existing nodes
            // and newcomers remain visible even when every RPC times out.
            if verb == "watch" {
                if let Some(previous) = &previous {
                    if let Some(mut change) = delta(previous, &view) {
                        change["watch_id"] = json!(watch_id);
                        change["sequence"] = json!(sequence + 1);
                        change["observed_at_unix_ms"] = json!(wall_time());
                        change["elapsed_ms"] = json!(started.elapsed().as_millis());
                        change["coverage"] = coverage(&view, nodes.len());
                        stream("delta", &change)?;
                        sequence += 1;
                    }
                } else {
                    let mut baseline = inventory(&view, nodes.len(), &scope, started);
                    baseline["watch_id"] = json!(watch_id);
                    baseline["sequence"] = json!(sequence);
                    stream("snapshot", &baseline)?;
                }
                previous = Some(view.clone());
            }
            let expired = deadline.is_some_and(|end| Instant::now() >= end);
            if verb == "watch" && expired {
                return Ok(None);
            }
            if !expired
                && probe_batch(
                    ctx,
                    &nodes,
                    &mut cache,
                    &options,
                    deadline,
                    verb != "snapshot",
                )?
            {
                // Discard metadata if its owner, ambiguity or entity set changed
                // during RPC. Never resurrect an endpoint from a stale NodeCard.
                nodes = observe(watch.snapshot()?);
                cache.retain(|id, _| nodes.contains_key(id));
                view = project(&nodes, &cache, &options)?;
            }
            let pending = view
                .values()
                .any(|node| node["introspection"]["status"] == "pending");
            if verb == "snapshot" && (!pending || deadline.is_some_and(|end| Instant::now() >= end))
            {
                return Ok(Some(inventory(&view, nodes.len(), &scope, started)));
            }
            if verb == "wait" {
                let matches: View = view
                    .iter()
                    .filter(|(_, node)| {
                        !options.require_descriptions
                            || (node["introspection"]["status"] == "ok"
                                && node["endpoints"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .all(|e| e["description_status"] == "current"))
                    })
                    .map(|(id, node)| (id.clone(), node.clone()))
                    .collect();
                if matches.len() >= options.min_nodes {
                    let mut result = inventory(&matches, nodes.len(), &scope, started);
                    result["matched"] = json!(true);
                    result["match_evidence"] = json!(if options.require_descriptions {
                        "introspection"
                    } else {
                        "discovery"
                    });
                    return Ok(Some(result));
                }
                if deadline.is_some_and(|end| Instant::now() >= end) {
                    crate::operation_field(
                        "last_observation",
                        inventory(&view, nodes.len(), &scope, started),
                    );
                    return Err(CliError {code:"WAIT_TIMEOUT", message:"No matching Graph observation met the requested evidence before timeout".into(), exit:4});
                }
            }
            // Re-project immediately after a probe so watch emits metadata
            // updates without waiting for another transport notification.
            if verb == "watch"
                && previous
                    .as_ref()
                    .is_some_and(|old| delta(old, &view).is_some())
            {
                continue;
            }
            pause(
                watch.as_ref(),
                deadline.map_or(POLL, |end| end.saturating_duration_since(Instant::now())),
            )?;
        }
    })();
    #[cfg(any(unix, windows))]
    if verb == "watch" {
        crate::process_commands::set_stream_summary(
            &json!({"schema_version":1,"event":"summary","data":{
                "watch_id":watch_id,"last_sequence":sequence,"elapsed_ms":started.elapsed().as_millis(),
                "reason":if result.is_ok() {"timeout"} else {"error_or_signal"},
                "selected_nodes":previous.as_ref().map_or(0, BTreeMap::len)
            }}),
        )?;
    }
    result
}
