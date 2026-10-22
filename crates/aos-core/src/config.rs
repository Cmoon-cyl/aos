//! Deployment configuration and exact remaps. The order mirrors fixed Jazzy
//! rcl 18d96c9c: node name, namespace, then endpoint expansion, first match only.
//! AOS deliberately rejects wildcard/regex remaps and Action channel splitting.
use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{
    de::{MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::{json, Value};

use crate::{
    names::{expand_name, normalize_namespace, validate_absolute_name, validate_node_name},
    Error, NodeOptions, ParameterOverride, ParameterValue, Result,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfigRequest {
    pub program: Value,
    pub config_files: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub launch: Value,
    pub cli: Value,
}

/// Resolve before opening an adapter. This is intentionally separate from
/// NodeConfigRequest: a Node cannot change an already-open Context's network.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContextConfigRequest {
    pub program: Value,
    pub config_files: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub launch: Value,
    pub cli: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedContextConfig {
    pub domain_id: u32,
    pub zenoh_config: Option<String>,
    values: BTreeMap<String, ConfigValue>,
}
impl ResolvedContextConfig {
    pub fn effective_value(&self) -> Value {
        serde_json::to_value(&self.values).expect("JSON config is serializable")
    }
}
pub fn resolve_context_config(request: &ContextConfigRequest) -> Result<ResolvedContextConfig> {
    let mut values = BTreeMap::from([
        (
            "domain_id".to_owned(),
            ConfigValue {
                value: json!(0),
                source: "default".into(),
            },
        ),
        (
            "zenoh_config".to_owned(),
            ConfigValue {
                value: Value::Null,
                source: "default".into(),
            },
        ),
    ]);
    let mut layers = vec![("program".to_owned(), request.program.clone())];
    for path in &request.config_files {
        let text = std::fs::read_to_string(path).map_err(|e| {
            Error::InvalidArgument(format!("cannot read context config {path:?}: {e}"))
        })?;
        layers.push((format!("file:{path}"), parse_config_json(&text)?));
    }
    let mut env = serde_json::Map::new();
    for (key, value) in &request.environment {
        match key.as_str() {
            "AOS_DOMAIN_ID" | "AOS_ZENOH_CONFIG" if value.is_empty() => continue,
            "AOS_DOMAIN_ID" => {
                let domain = value.parse::<u32>().map_err(|_| {
                    Error::InvalidArgument("AOS_DOMAIN_ID must be a non-negative u32".into())
                })?;
                env.insert("domain_id".into(), json!(domain));
            }
            "AOS_ZENOH_CONFIG" => {
                env.insert("zenoh_config".into(), json!(value));
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "context environment override {key:?} is not allowed"
                )))
            }
        }
    }
    layers.push(("environment".into(), Value::Object(env)));
    layers.push(("launch".into(), request.launch.clone()));
    layers.push(("cli".into(), request.cli.clone()));
    for (source, layer) in layers {
        if layer.is_null() {
            continue;
        }
        let fields = layer.as_object().ok_or_else(|| {
            Error::InvalidArgument(format!("{source} context config must be an object"))
        })?;
        for (key, value) in fields {
            match key.as_str() {
                "domain_id" if value.as_u64().is_some_and(|v| u32::try_from(v).is_ok()) => {}
                "zenoh_config"
                    if value.is_null() || value.as_str().is_some_and(|s| !s.is_empty()) => {}
                "domain_id" | "zenoh_config" => {
                    return Err(Error::InvalidArgument(format!(
                        "invalid context config {key:?}"
                    )))
                }
                _ => {
                    return Err(Error::InvalidArgument(format!(
                        "unknown context config {key:?}"
                    )))
                }
            }
            values.insert(
                key.clone(),
                ConfigValue {
                    value: value.clone(),
                    source: source.clone(),
                },
            );
        }
    }
    Ok(ResolvedContextConfig {
        domain_id: values["domain_id"].value.as_u64().unwrap() as u32,
        zenoh_config: values["zenoh_config"].value.as_str().map(str::to_owned),
        values,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameKind {
    Topic,
    Service,
    Action,
    #[default]
    Any,
}
impl FromStr for NameKind {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "topic" => Ok(Self::Topic),
            "service" => Ok(Self::Service),
            "action" => Ok(Self::Action),
            "any" => Ok(Self::Any),
            _ => Err(Error::InvalidArgument(format!(
                "unknown name kind {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemapRule {
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub kind: NameKind,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedName {
    pub original_name: String,
    pub expanded_name: String,
    pub resolved_name: String,
    pub kind: NameKind,
    pub remap_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValue {
    pub value: Value,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
struct CompiledRemap {
    node: Option<String>,
    kind: NameKind,
    from: String,
    to: String,
    source: String,
    #[serde(skip)]
    priority: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedNodeConfig {
    pub declared_name: String,
    pub declared_namespace: String,
    pub name: String,
    pub namespace: String,
    pub options: NodeOptions,
    pub use_sim_time: bool,
    pub parameters: BTreeMap<String, ParameterOverride>,
    values: BTreeMap<String, ConfigValue>,
    remaps: Vec<CompiledRemap>,
}

impl ResolvedNodeConfig {
    pub fn effective_value(&self) -> Value {
        let mut value = serde_json::to_value(&self.values).expect("JSON config is serializable");
        value["declared_identity"] =
            json!({"name":self.declared_name,"namespace":self.declared_namespace});
        value["remaps"] = serde_json::to_value(&self.remaps).expect("remaps are serializable");
        value["parameters"] = json!(self.parameters);
        value
    }

    pub fn resolve_name(&self, name: &str, kind: NameKind) -> Result<ResolvedName> {
        let expanded_name = expand_name(name, &self.name, &self.namespace)?;
        let rule = self.remaps.iter().find(|rule| {
            (rule.kind == NameKind::Any || rule.kind == kind) && rule.from == expanded_name
        });
        Ok(ResolvedName {
            original_name: name.into(),
            resolved_name: rule.map_or_else(|| expanded_name.clone(), |r| r.to.clone()),
            expanded_name,
            kind,
            remap_source: rule.map(|r| r.source.clone()),
        })
    }
}

/// Parse JSON without losing duplicate object fields before validation.
pub fn parse_config_json(text: &str) -> Result<Value> {
    struct Strict(Value);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Strict;
                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str("JSON")
                }
                fn visit_bool<E>(self, v: bool) -> std::result::Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_i64<E>(self, v: i64) -> std::result::Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_u64<E>(self, v: u64) -> std::result::Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                    serde_json::Number::from_f64(v)
                        .map(|n| Strict(Value::Number(n)))
                        .ok_or_else(|| E::custom("non-finite JSON number"))
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_string<E>(self, v: String) -> std::result::Result<Strict, E> {
                    Ok(Strict(v.into()))
                }
                fn visit_unit<E>(self) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_none<E>(self) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut a: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut values = Vec::new();
                    while let Some(Strict(v)) = a.next_element()? {
                        values.push(v);
                    }
                    Ok(Strict(Value::Array(values)))
                }
                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut a: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut map = serde_json::Map::new();
                    while let Some((key, Strict(value))) = a.next_entry::<String, Strict>()? {
                        if map.insert(key.clone(), value).is_some() {
                            return Err(serde::de::Error::custom(format!(
                                "duplicate config key {key:?}"
                            )));
                        }
                    }
                    Ok(Strict(Value::Object(map)))
                }
            }
            d.deserialize_any(V)
        }
    }
    serde_json::from_str::<Strict>(text)
        .map(|v| v.0)
        .map_err(|e| Error::InvalidArgument(format!("invalid configuration JSON: {e}")))
}

pub fn resolve_node_config(
    name: &str,
    namespace: &str,
    options: NodeOptions,
    request: &NodeConfigRequest,
) -> Result<ResolvedNodeConfig> {
    validate_node_name(name)?;
    let namespace = normalize_namespace(namespace)?;
    let declared_fqn = format!("{}/{name}", namespace.trim_end_matches('/'));
    let mut values: BTreeMap<String, ConfigValue> = [
        ("name", json!(name), "program"),
        ("namespace", json!(namespace), "program"),
        ("display_name", json!(options.display_name), "program"),
        ("summary", json!(options.summary), "program"),
        ("tags", json!(options.tags), "program"),
        ("use_sim_time", json!(false), "default"),
    ]
    .into_iter()
    .map(|(key, value, source)| {
        (
            key.into(),
            ConfigValue {
                value,
                source: source.into(),
            },
        )
    })
    .collect();
    set_parameter_override(
        &mut values,
        "use_sim_time",
        ParameterValue::Bool(false),
        "default",
    );
    let mut rules: Vec<(RemapRule, String, usize)> = Vec::new();
    let mut layers = vec![("program".to_owned(), request.program.clone())];
    for path in &request.config_files {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::InvalidArgument(format!("cannot read config {path:?}: {e}")))?;
        layers.push((format!("file:{path}"), parse_config_json(&text)?));
    }
    let mut env = serde_json::Map::new();
    for (key, value) in &request.environment {
        match key.as_str() {
            "AOS_NODE_NAME" | "AOS_NAMESPACE" | "AOS_USE_SIM_TIME" if value.is_empty() => continue,
            "AOS_NODE_NAME" => {
                env.insert("name".into(), json!(value));
            }
            "AOS_NAMESPACE" => {
                env.insert("namespace".into(), json!(value));
            }
            "AOS_USE_SIM_TIME" => {
                let value = match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(Error::InvalidArgument(
                            "AOS_USE_SIM_TIME must be true or false".into(),
                        ))
                    }
                };
                env.insert("use_sim_time".into(), json!(value));
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "environment override {key:?} is not allowed"
                )))
            }
        }
    }
    layers.push(("environment".into(), Value::Object(env)));
    layers.push(("launch".into(), request.launch.clone()));
    layers.push(("cli".into(), request.cli.clone()));
    for (priority, (source, layer)) in layers.iter().enumerate() {
        if layer.is_null() {
            continue;
        }
        apply_layer(
            layer,
            source,
            priority,
            None,
            &mut values,
            &mut rules,
            (name, &declared_fqn),
        )?;
        if let Some(nodes) = layer.get("nodes") {
            let nodes = nodes
                .as_object()
                .ok_or_else(|| Error::InvalidArgument("nodes must be an object".into()))?;
            // Validate even unmatched entries, rather than hide typos until another Node starts.
            for (selector, node_layer) in nodes {
                validate_selector(selector)?;
                let mut unused_values = BTreeMap::new();
                let mut unused_rules = Vec::new();
                apply_layer(
                    node_layer,
                    source,
                    priority,
                    Some(selector),
                    &mut unused_values,
                    &mut unused_rules,
                    (name, &declared_fqn),
                )?;
            }
            if let (Some(short), Some(absolute)) = (nodes.get(name), nodes.get(&declared_fqn)) {
                reject_selector_alias_conflicts(short, absolute, source)?;
            }
            for selector in [name, &declared_fqn] {
                if let Some(node_layer) = nodes.get(selector) {
                    apply_layer(
                        node_layer,
                        source,
                        priority,
                        Some(selector),
                        &mut values,
                        &mut rules,
                        (name, &declared_fqn),
                    )?;
                }
            }
        }
    }
    let final_name = values["name"]
        .value
        .as_str()
        .expect("validated name")
        .to_owned();
    validate_node_name(&final_name)?;
    let final_namespace = normalize_namespace(
        values["namespace"]
            .value
            .as_str()
            .expect("validated namespace"),
    )?;
    values.get_mut("namespace").unwrap().value = json!(final_namespace);
    let mut remaps = Vec::new();
    let mut duplicate: BTreeMap<(usize, Option<String>, NameKind, String), String> =
        BTreeMap::new();
    for (rule, source, priority) in rules {
        if let Some(selector) = &rule.node {
            validate_selector(selector)?;
            if selector != name && selector != &declared_fqn {
                continue;
            }
        }
        let from = expand_name(&rule.from, &final_name, &final_namespace)?;
        let to = expand_name(&rule.to, &final_name, &final_namespace)?;
        reject_action_channel(&from)?;
        reject_action_channel(&to)?;
        if matches!(rule.kind, NameKind::Any | NameKind::Service)
            && ["get_node_card", "get_type_description"]
                .iter()
                .any(|suffix| {
                    from == format!(
                        "{}/{final_name}/{suffix}",
                        final_namespace.trim_end_matches('/')
                    )
                })
        {
            return Err(Error::InvalidArgument(format!(
                "built-in introspection service {from:?} cannot be remapped"
            )));
        }
        let selector = rule
            .node
            .as_deref()
            .map(|selector| canonical_selector(selector, (name, &declared_fqn)).to_owned());
        let key = (priority, selector.clone(), rule.kind, from.clone());
        if duplicate
            .insert(key, to.clone())
            .is_some_and(|previous| previous != to)
        {
            return Err(Error::InvalidArgument(format!(
                "conflicting remaps for {from:?} in {source}"
            )));
        }
        remaps.push(CompiledRemap {
            node: selector,
            kind: rule.kind,
            from,
            to,
            source,
            priority,
        });
    }
    // stable sort preserves list order inside each source/selector class.
    remaps.sort_by_key(|r| {
        (
            std::cmp::Reverse(r.priority),
            std::cmp::Reverse(r.node.is_some()),
        )
    });
    Ok(ResolvedNodeConfig {
        declared_name: name.into(),
        declared_namespace: namespace,
        name: final_name,
        namespace: final_namespace,
        options: NodeOptions {
            display_name: values["display_name"].value.as_str().map(str::to_owned),
            summary: values["summary"].value.as_str().unwrap().into(),
            tags: serde_json::from_value(values["tags"].value.clone()).expect("validated tags"),
        },
        use_sim_time: values["use_sim_time"].value.as_bool().unwrap(),
        parameters: serde_json::from_value(values["parameters"].value.clone())
            .expect("validated parameter overrides"),
        values,
        remaps,
    })
}

fn validate_selector(selector: &str) -> Result<()> {
    if selector.starts_with('/') {
        validate_absolute_name(selector)
    } else {
        validate_node_name(selector)
    }
}
fn reject_action_channel(name: &str) -> Result<()> {
    if [
        "send_goal",
        "get_result",
        "cancel_goal",
        "feedback",
        "status",
    ]
    .iter()
    .any(|suffix| name.ends_with(&format!("/_action/{suffix}")))
    {
        return Err(Error::InvalidArgument(
            "remap an Action base name, not an internal Action channel".into(),
        ));
    }
    Ok(())
}
/// Syntax/shape preflight for bootstrap input, before the adapter opens.
/// Do not resolve a real identity or load any referenced files here.
pub(crate) fn validate_node_layer(layer: &Value, source: &str) -> Result<()> {
    let identity = ("aos_bootstrap_validation", "/aos_bootstrap_validation");
    apply_layer(
        layer,
        source,
        0,
        None,
        &mut BTreeMap::new(),
        &mut Vec::new(),
        identity,
    )?;
    if let Some(nodes) = layer.get("nodes").and_then(Value::as_object) {
        for (selector, node_layer) in nodes {
            validate_selector(selector)?;
            apply_layer(
                node_layer,
                source,
                0,
                Some(selector),
                &mut BTreeMap::new(),
                &mut Vec::new(),
                identity,
            )?;
        }
    }
    Ok(())
}

fn apply_layer(
    layer: &Value,
    source: &str,
    priority: usize,
    selector: Option<&str>,
    values: &mut BTreeMap<String, ConfigValue>,
    rules: &mut Vec<(RemapRule, String, usize)>,
    identity: (&str, &str),
) -> Result<()> {
    let object = layer
        .as_object()
        .ok_or_else(|| Error::InvalidArgument(format!("{source} config must be an object")))?;
    if object.contains_key("use_sim_time")
        && object
            .get("parameters")
            .and_then(Value::as_object)
            .is_some_and(|p| p.contains_key("use_sim_time"))
    {
        return Err(Error::InvalidArgument(
            "use_sim_time is specified both as a config field and parameter in the same layer"
                .into(),
        ));
    }
    for (key, value) in object {
        match key.as_str() {
            "nodes" if selector.is_none() => {
                if !value.is_object() {
                    return Err(Error::InvalidArgument("nodes must be an object".into()));
                }
                continue;
            }
            "remaps" => {
                let entries: Vec<RemapRule> =
                    serde_json::from_value(value.clone()).map_err(|e| {
                        Error::InvalidArgument(format!("invalid remaps in {source}: {e}"))
                    })?;
                for mut rule in entries {
                    if let Some(node) = &rule.node {
                        validate_selector(node)?;
                    }
                    // Validate syntax even when this rule targets another Node.
                    for name in [&rule.from, &rule.to] {
                        let expanded = expand_name(name, "validation_node", "/validation_ns")?;
                        reject_action_channel(&expanded)?;
                    }
                    if let Some(selector) = selector {
                        if rule
                            .node
                            .as_deref()
                            .is_some_and(|node| !selectors_equivalent(node, selector))
                        {
                            return Err(Error::InvalidArgument(
                                "remap node selector conflicts with containing node config".into(),
                            ));
                        }
                        // A short container can contain a narrower absolute
                        // selector. Preserve that scope instead of broadening
                        // it to another Node with the same short name.
                        let narrowed = rule
                            .node
                            .as_deref()
                            .filter(|node| node.starts_with('/'))
                            .unwrap_or(selector);
                        rule.node = Some(canonical_selector(narrowed, identity).into());
                    }
                    // Expansion rejects wildcards, regex, unknown variables and backreferences.
                    rules.push((rule, source.into(), priority));
                }
                continue;
            }
            "parameters" => {
                let parameters = value.as_object().ok_or_else(|| {
                    Error::InvalidArgument(format!("parameters in {source} must be an object"))
                })?;
                for (name, value) in parameters {
                    if name.is_empty() {
                        return Err(Error::InvalidArgument(
                            "parameter name cannot be empty".into(),
                        ));
                    }
                    let parsed = ParameterValue::from_json(value)?;
                    if name == "use_sim_time" {
                        let ParameterValue::Bool(enabled) = parsed else {
                            return Err(Error::InvalidArgument(
                                "use_sim_time must be a boolean parameter".into(),
                            ));
                        };
                        values.insert(
                            "use_sim_time".into(),
                            ConfigValue {
                                value: json!(enabled),
                                source: source.into(),
                            },
                        );
                    }
                    set_parameter_override(values, name, parsed, source);
                }
                continue;
            }
            "name" if value.is_string() => {
                validate_node_name(value.as_str().unwrap())?;
            }
            "namespace" if value.is_string() => {
                normalize_namespace(value.as_str().unwrap())?;
            }
            "summary" if value.is_string() => {}
            "display_name" if value.is_string() || value.is_null() => {}
            "tags"
                if value
                    .as_array()
                    .is_some_and(|v| v.iter().all(Value::is_string)) => {}
            "use_sim_time" if value.is_boolean() => {}
            "name" | "namespace" | "summary" | "display_name" | "tags" | "use_sim_time" => {
                return Err(Error::InvalidArgument(format!(
                    "invalid value for config {key:?} in {source}"
                )));
            }
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "unknown config key {key:?} in {source}"
                )))
            }
        }
        values.insert(
            key.clone(),
            ConfigValue {
                value: value.clone(),
                source: source.into(),
            },
        );
        if key == "use_sim_time" {
            set_parameter_override(
                values,
                key,
                ParameterValue::Bool(value.as_bool().unwrap()),
                source,
            );
        }
    }
    Ok(())
}

fn set_parameter_override(
    values: &mut BTreeMap<String, ConfigValue>,
    name: &str,
    value: ParameterValue,
    source: &str,
) {
    let parameters = values
        .entry("parameters".into())
        .or_insert_with(|| ConfigValue {
            value: json!({}),
            source: source.into(),
        });
    parameters.value[name] = json!(ParameterOverride {
        value,
        source: source.into()
    });
    parameters.source = source.into();
}

fn selectors_equivalent(left: &str, right: &str) -> bool {
    // Both names have already passed validation. Two absolute selectors must
    // name the same Node; a short selector matches the final path component.
    // This validation must not depend on which Node loads a shared config.
    match (left.starts_with('/'), right.starts_with('/')) {
        (false, false) | (true, true) => left == right,
        (false, true) => right.rsplit('/').next() == Some(left),
        (true, false) => left.rsplit('/').next() == Some(right),
    }
}

fn canonical_selector<'a>(selector: &'a str, identity: (&str, &'a str)) -> &'a str {
    if selector == identity.0 || selector == identity.1 {
        identity.1
    } else {
        selector
    }
}

fn reject_selector_alias_conflicts(short: &Value, absolute: &Value, source: &str) -> Result<()> {
    // Both selectors denote the same declared Node, so neither spelling wins
    // arbitrarily inside one source. Remap collisions are checked after name
    // expansion using the canonical selector key.
    for key in [
        "name",
        "namespace",
        "display_name",
        "summary",
        "tags",
        "use_sim_time",
    ] {
        if let (Some(a), Some(b)) = (short.get(key), absolute.get(key)) {
            let same = if key == "namespace" {
                normalize_namespace(a.as_str().expect("validated namespace"))?
                    == normalize_namespace(b.as_str().expect("validated namespace"))?
            } else {
                a == b
            };
            if !same {
                return Err(Error::InvalidArgument(format!(
                    "conflicting {key:?} for equivalent node selectors in {source}"
                )));
            }
        }
    }
    let short_parameters = selector_parameter_values(short)?;
    let absolute_parameters = selector_parameter_values(absolute)?;
    for (name, value) in short_parameters {
        if absolute_parameters
            .get(&name)
            .is_some_and(|other| other != &value)
        {
            return Err(Error::InvalidArgument(format!(
                "conflicting parameter {name:?} for equivalent node selectors in {source}"
            )));
        }
    }
    Ok(())
}

fn selector_parameter_values(layer: &Value) -> Result<BTreeMap<String, ParameterValue>> {
    let mut values = BTreeMap::new();
    if let Some(parameters) = layer.get("parameters").and_then(Value::as_object) {
        for (name, value) in parameters {
            values.insert(name.clone(), ParameterValue::from_json(value)?);
        }
    }
    // The top-level spelling and the parameter spelling share one identity.
    // apply_layer already rejects both spellings within the same selector.
    if let Some(value) = layer.get("use_sim_time") {
        values.insert("use_sim_time".into(), ParameterValue::from_json(value)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage3_r2_equivalent_selectors_compare_each_parameter_and_sim_time_alias() {
        for (short, absolute) in [
            (json!({"parameters":{"x":1}}), json!({"parameters":{"x":2}})),
            (
                json!({"parameters":{"x":{"type":"integer","value":1}}}),
                json!({"parameters":{"x":{"type":"double","value":1.0}}}),
            ),
            (
                json!({"use_sim_time":true}),
                json!({"parameters":{"use_sim_time":false}}),
            ),
            (
                json!({"parameters":{"use_sim_time":true}}),
                json!({"use_sim_time":false}),
            ),
        ] {
            let result = resolve_node_config(
                "worker",
                "/a",
                NodeOptions::default(),
                &NodeConfigRequest {
                    program: json!({"nodes":{"worker":short,"/a/worker":absolute}}),
                    ..Default::default()
                },
            );
            assert!(matches!(
                result,
                Err(Error::InvalidArgument(message))
                    if message.contains("conflicting parameter")
            ));
        }

        let config = resolve_node_config(
            "worker",
            "/a",
            NodeOptions::default(),
            &NodeConfigRequest {
                program: json!({"nodes":{
                    "worker":{"parameters":{"x":1,"left":"a"},"use_sim_time":true},
                    "/a/worker":{"parameters":{
                        "x":{"type":"integer","value":1},"right":"b","use_sim_time":true
                    }}
                }}),
                // A later source may intentionally override the shared value.
                cli: json!({"parameters":{"x":2}}),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(config.parameters["x"].value, ParameterValue::Integer(2));
        assert_eq!(config.parameters["x"].source, "cli");
        assert_eq!(
            config.parameters["left"].value,
            ParameterValue::String("a".into())
        );
        assert_eq!(
            config.parameters["right"].value,
            ParameterValue::String("b".into())
        );
        assert_eq!(
            config.parameters["use_sim_time"].value,
            ParameterValue::Bool(true)
        );
    }

    #[test]
    fn parameter_overrides_merge_per_name_and_share_sim_time_identity() {
        let config = resolve_node_config("n", "/", NodeOptions::default(), &NodeConfigRequest {
            program: json!({"parameters":{"first":1,"second":"program","use_sim_time":true}}),
            launch: json!({"parameters":{"second":"launch","empty":{"type":"integer_array","value":[]}}}),
            cli: json!({"use_sim_time":false}),
            ..Default::default()
        }).unwrap();
        assert_eq!(config.parameters["first"].value, ParameterValue::Integer(1));
        assert_eq!(config.parameters["first"].source, "program");
        assert_eq!(
            config.parameters["second"].value,
            ParameterValue::String("launch".into())
        );
        assert_eq!(config.parameters["second"].source, "launch");
        assert_eq!(
            config.parameters["empty"].value,
            ParameterValue::IntegerArray(vec![])
        );
        assert_eq!(
            config.parameters["use_sim_time"].value,
            ParameterValue::Bool(false)
        );
        assert_eq!(config.parameters["use_sim_time"].source, "cli");
        assert!(!config.use_sim_time);
        for program in [
            json!({"use_sim_time":true,"parameters":{"use_sim_time":true}}),
            json!({"parameters":{"use_sim_time":1}}),
            json!({"parameters":{"":3}}),
            json!({"parameters":{"empty":[]}}),
            json!({"nodes":{"other":{"parameters":{"":3}}}}),
        ] {
            assert!(resolve_node_config(
                "n",
                "/",
                NodeOptions::default(),
                &NodeConfigRequest {
                    program,
                    ..Default::default()
                }
            )
            .is_err());
        }
    }
    #[test]
    fn validates_identity_in_every_layer_even_when_overridden_or_unselected() {
        for program in [
            json!({"name":"bad-name"}),
            json!({"namespace":"/bad-name"}),
            json!({"nodes":{"other":{"name":"bad-name"}}}),
            json!({"nodes":{"other":{"namespace":"/bad-name"}}}),
        ] {
            assert!(resolve_node_config(
                "worker",
                "/a",
                NodeOptions::default(),
                &NodeConfigRequest {
                    program,
                    cli: json!({"name":"valid","namespace":"/valid"}),
                    ..Default::default()
                }
            )
            .is_err());
        }
    }
    #[test]
    fn equivalent_node_selectors_share_one_conflict_domain() {
        for program in [
            json!({"remaps":[
                {"node":"worker","from":"in","to":"first"},
                {"node":"/a/worker","from":"in","to":"second"}
            ]}),
            json!({"nodes":{
                "worker":{"remaps":[{"from":"in","to":"first"}]},
                "/a/worker":{"remaps":[{"from":"in","to":"second"}]}
            }}),
            json!({"nodes":{"worker":{"summary":"first"},"/a/worker":{"summary":"second"}}}),
            json!({"nodes":{"worker":{"name":"first"},"/a/worker":{"name":"second"}}}),
        ] {
            assert!(resolve_node_config(
                "worker",
                "/a",
                NodeOptions::default(),
                &NodeConfigRequest {
                    program,
                    ..Default::default()
                }
            )
            .is_err());
        }
        let config = resolve_node_config("worker", "/a", NodeOptions::default(), &NodeConfigRequest {
            program: json!({"nodes":{
                "worker":{"namespace":"same"},
                "/a/worker":{"namespace":"/same","remaps":[{"node":"worker","from":"in","to":"out"}]}
            }}), ..Default::default()
        }).unwrap();
        assert_eq!(
            config
                .resolve_name("in", NameKind::Topic)
                .unwrap()
                .resolved_name,
            "/same/out"
        );
        assert_eq!(config.effective_value()["remaps"][0]["node"], "/a/worker");
    }
    #[test]
    fn shared_file_nested_selector_validation_is_independent_of_loading_identity() {
        let path =
            std::env::temp_dir().join(format!("aos-shared-config-{}.json", uuid::Uuid::new_v4()));
        let request = NodeConfigRequest {
            config_files: vec![path.to_str().unwrap().into()],
            ..Default::default()
        };
        for (selector, rule_node) in [
            ("other", "/ns/other"),
            ("/ns/other", "other"),
            ("other", "other"),
            ("/ns/other", "/ns/other"),
        ] {
            let config = json!({"nodes":{selector:{"remaps":[{
                "node":rule_node,"from":"in","to":"out"
            }]}}});
            std::fs::write(&path, config.to_string()).unwrap();
            let other_namespace = if selector == "other" && rule_node == "other" {
                "/x/out"
            } else {
                "/x/in"
            };
            for (name, namespace, expected) in [
                ("worker", "/a", "/a/in"),
                ("other", "/ns", "/ns/out"),
                ("other", "/x", other_namespace),
            ] {
                let resolved =
                    resolve_node_config(name, namespace, NodeOptions::default(), &request).unwrap();
                assert_eq!(
                    resolved
                        .resolve_name("in", NameKind::Topic)
                        .unwrap()
                        .resolved_name,
                    expected,
                    "container={selector}, rule={rule_node}, loader={namespace}/{name}"
                );
            }
        }
        for (selector, rule_node) in [
            ("other", "/ns/different"),
            ("/ns/other", "different"),
            ("other", "different"),
            ("/ns/other", "/different/other"),
        ] {
            let config = json!({"nodes":{selector:{"remaps":[{
                "node":rule_node,"from":"in","to":"out"
            }]}}});
            std::fs::write(&path, config.to_string()).unwrap();
            for (name, namespace) in [("worker", "/a"), ("other", "/ns"), ("other", "/x")] {
                assert!(
                    resolve_node_config(name, namespace, NodeOptions::default(), &request).is_err(),
                    "container={selector}, rule={rule_node}, loader={namespace}/{name}"
                );
            }
        }
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn built_in_introspection_names_cannot_be_remapped_as_services() {
        for suffix in ["get_node_card", "get_type_description"] {
            for kind in ["service", "any"] {
                assert!(resolve_node_config(
                    "worker",
                    "/a",
                    NodeOptions::default(),
                    &NodeConfigRequest {
                        cli: json!({"name":"renamed","namespace":"/b","remaps":[{
                            "kind":kind,"from":format!("~/{suffix}"),"to":"hidden"
                        }]}),
                        ..Default::default()
                    }
                )
                .is_err());
            }
        }
        let config = resolve_node_config(
            "worker",
            "/a",
            NodeOptions::default(),
            &NodeConfigRequest {
                cli: json!({"remaps":[{"kind":"topic","from":"~/get_node_card","to":"topic"}]}),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            config
                .resolve_name("~/get_node_card", NameKind::Service)
                .unwrap()
                .resolved_name,
            "/a/worker/get_node_card"
        );
    }
    #[test]
    fn empty_context_environment_values_mean_unset() {
        let environment = BTreeMap::from([
            ("AOS_DOMAIN_ID".into(), String::new()),
            ("AOS_ZENOH_CONFIG".into(), String::new()),
        ]);
        let defaults = resolve_context_config(&ContextConfigRequest {
            environment: environment.clone(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(defaults.domain_id, 0);
        assert_eq!(defaults.zenoh_config, None);
        let program = resolve_context_config(&ContextConfigRequest {
            program: json!({"domain_id":7,"zenoh_config":"/program.json5"}),
            environment,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(program.domain_id, 7);
        assert_eq!(program.zenoh_config.as_deref(), Some("/program.json5"));
        assert_eq!(program.effective_value()["domain_id"]["source"], "program");
        assert!(resolve_context_config(&ContextConfigRequest {
            environment: BTreeMap::from([("UNKNOWN".into(), String::new())]),
            ..Default::default()
        })
        .is_err());
    }
    #[test]
    fn empty_node_environment_values_mean_unset_and_preserve_provenance() {
        let environment = BTreeMap::from([
            ("AOS_NODE_NAME".into(), String::new()),
            ("AOS_NAMESPACE".into(), String::new()),
            ("AOS_USE_SIM_TIME".into(), String::new()),
        ]);
        let defaults = resolve_node_config(
            "worker",
            "/a",
            NodeOptions::default(),
            &NodeConfigRequest {
                environment: environment.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(defaults.name, "worker");
        assert_eq!(defaults.namespace, "/a");
        assert!(!defaults.use_sim_time);
        assert_eq!(
            defaults.effective_value()["use_sim_time"]["source"],
            "default"
        );
        let program = resolve_node_config(
            "worker",
            "/a",
            NodeOptions::default(),
            &NodeConfigRequest {
                program: json!({"name":"program","namespace":"/program","use_sim_time":true}),
                environment,
                ..Default::default()
            },
        )
        .unwrap();
        for key in ["name", "namespace", "use_sim_time"] {
            assert_eq!(program.effective_value()[key]["source"], "program");
        }
        assert_eq!(program.name, "program");
        assert_eq!(program.namespace, "/program");
        assert!(program.use_sim_time);
        for (key, value) in [
            ("UNKNOWN", ""),
            ("AOS_USE_SIM_TIME", " "),
            ("AOS_NODE_NAME", " "),
        ] {
            assert!(resolve_node_config(
                "worker",
                "/a",
                NodeOptions::default(),
                &NodeConfigRequest {
                    environment: BTreeMap::from([(key.into(), value.into())]),
                    ..Default::default()
                },
            )
            .is_err());
        }
    }
    #[test]
    fn files_apply_in_order_before_environment_launch_and_cli() {
        let root = std::env::temp_dir().join(format!("aos-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let first = root.join("first.json");
        let second = root.join("second.json");
        std::fs::write(
            &first,
            r#"{"name":"file_a","namespace":"/file_a","tags":["a"]}"#,
        )
        .unwrap();
        std::fs::write(&second, r#"{"namespace":"/file_b","tags":["b"]}"#).unwrap();
        let result = resolve_node_config(
            "program",
            "/program",
            NodeOptions::default(),
            &NodeConfigRequest {
                config_files: vec![
                    first.to_str().unwrap().into(),
                    second.to_str().unwrap().into(),
                ],
                environment: BTreeMap::from([("AOS_NODE_NAME".into(), "environment".into())]),
                launch: json!({"name":"launch"}),
                cli: json!({"namespace":"/cli"}),
                ..Default::default()
            },
        );
        std::fs::remove_dir_all(&root).unwrap();
        let config = result.unwrap();
        assert_eq!(config.name, "launch");
        assert_eq!(config.namespace, "/cli");
        assert_eq!(config.options.tags, ["b"]);
        assert_eq!(
            config.effective_value()["tags"]["source"],
            format!("file:{}", second.display())
        );
    }
    #[test]
    fn context_config_is_strict_separate_and_environment_is_not_final() {
        let config = resolve_context_config(&ContextConfigRequest {
            program: json!({"domain_id":1,"zenoh_config":"/safe/config.json5"}),
            environment: BTreeMap::from([("AOS_DOMAIN_ID".into(), "2".into())]),
            cli: json!({"domain_id":3}),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(config.domain_id, 3);
        assert_eq!(config.effective_value()["domain_id"]["source"], "cli");
        assert_eq!(
            config.effective_value()["zenoh_config"]["source"],
            "program"
        );
        for invalid in [
            json!({"domain_id":true}),
            json!({"domain_id":-1}),
            json!({"domain_id":4_294_967_296u64}),
            json!({"name":"node"}),
        ] {
            assert!(resolve_context_config(&ContextConfigRequest {
                cli: invalid,
                ..Default::default()
            })
            .is_err());
        }
        assert!(resolve_node_config(
            "n",
            "/",
            NodeOptions::default(),
            &NodeConfigRequest {
                cli: json!({"domain_id":2}),
                ..Default::default()
            }
        )
        .is_err());
    }
    #[test]
    fn identity_selection_precedence_and_no_chained_remap() {
        let request = NodeConfigRequest {
            program: json!({"name":"new","namespace":"/new","remaps":[{"from":"~/x","to":"~/program"}]}),
            launch: json!({"remaps":[{"from":"~/x","to":"~/generic"},{"node":"worker","from":"~/x","to":"~/selected"},{"from":"~/selected","to":"~/never"}],
                "nodes":{"worker":{"summary":"selected by declared name"},"new":{"summary":"must not select"}}}),
            cli: json!({"name":"final","namespace":"/b"}),
            ..Default::default()
        };
        let config = resolve_node_config("worker", "/a", NodeOptions::default(), &request).unwrap();
        assert_eq!(config.options.summary, "selected by declared name");
        let resolved = config.resolve_name("~/x", NameKind::Topic).unwrap();
        assert_eq!(resolved.expanded_name, "/b/final/x");
        assert_eq!(resolved.resolved_name, "/b/final/selected");
        assert_eq!(resolved.remap_source.as_deref(), Some("launch"));
        assert_eq!(
            config.effective_value()["name"],
            json!({"value":"final","source":"cli"})
        );
    }
    #[test]
    fn kind_rules_do_not_leak_and_arrays_replace() {
        let config=resolve_node_config("n","/",NodeOptions::default(),&NodeConfigRequest {
            program:json!({"tags":["a","b"],"remaps":[{"kind":"service","from":"x","to":"srv"}]}),
            cli:json!({"tags":["c"]}),..Default::default()
        }).unwrap();
        assert_eq!(config.options.tags, ["c"]);
        assert_eq!(
            config
                .resolve_name("x", NameKind::Topic)
                .unwrap()
                .resolved_name,
            "/x"
        );
        assert_eq!(
            config
                .resolve_name("x", NameKind::Service)
                .unwrap()
                .resolved_name,
            "/srv"
        );
    }
    #[test]
    fn rejects_duplicate_keys_conflicts_unknowns_and_action_splits() {
        assert!(parse_config_json(r#"{"name":"a","name":"b"}"#).is_err());
        assert!(parse_config_json(r#"{"nodes":{"a":{"summary":"a","summary":"b"}}}"#).is_err());
        for bad in [
            json!({"surprise":true}),
            json!({"use_sim_time":1}),
            json!({"remaps":[{"from":"a","to":"b"},{"from":"/a","to":"c"}]}),
            json!({"remaps":[{"from":"*/a","to":"b"}]}),
            json!({"remaps":[{"from":"a/_action/status","to":"b"}]}),
        ] {
            assert!(resolve_node_config(
                "n",
                "/",
                NodeOptions::default(),
                &NodeConfigRequest {
                    cli: bad,
                    ..Default::default()
                }
            )
            .is_err());
        }
        assert!(resolve_node_config(
            "n",
            "/",
            NodeOptions::default(),
            &NodeConfigRequest {
                environment: BTreeMap::from([("SECRET".into(), "hidden".into())]),
                ..Default::default()
            }
        )
        .is_err());
    }
}
