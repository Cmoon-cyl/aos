//! One resource admission profile for local files, remote descriptors and bindings.
//! These structural limits are not a CPU deadline or a process memory guarantee.
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use serde_json::Value;

use crate::{Error, Result};

pub const MAX_INTERFACE_DEFINITION_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_INTERFACE_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_INTERFACE_JSON_NODES: usize = 65_536;
pub const MAX_INTERFACE_SCHEMA_NODES: usize = 1_024;
pub const MAX_INTERFACE_EXPANDED_SCHEMA_NODES: usize = 1_024;
/// Resolver context states visited while checking all local references.
pub const MAX_INTERFACE_REFERENCE_STATES: usize = 4_096;
pub const MAX_INTERFACE_DEPTH: usize = 64;
pub const MAX_INTERFACE_REFERENCES: usize = 128;
pub const MAX_INTERFACE_REGEXES: usize = 16;
pub const MAX_INTERFACE_REGEX_SOURCE_BYTES: usize = 64 * 1024;
pub const MAX_INTERFACE_REGEX_COMPILED_BYTES: usize = 1024 * 1024;
pub const MAX_INTERFACE_REGEX_DFA_BYTES: usize = 256 * 1024;
/// Bound the fallback program independently of the payload's byte charge.
const MAX_INTERFACE_REGEX_NFA_STATES: usize = 768;
/// Per-part distinct regex count times the configured compile and DFA limits.
/// This accounting charge excludes other allocations and is not an RSS bound.
pub const MAX_INTERFACE_REGEX_CHARGE_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_INTERFACE_VALIDATION_WORK: usize = 8 * 1024 * 1024;

/// Optional tighter limits for one type-description request. Whole-response
/// bytes and each schema's compact JSON bytes have deliberately different limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeDescriptionLimits {
    pub response_bytes: usize,
    pub schema_bytes: usize,
}

impl Default for TypeDescriptionLimits {
    fn default() -> Self {
        Self {
            response_bytes: MAX_INTERFACE_DEFINITION_BYTES,
            schema_bytes: MAX_INTERFACE_SCHEMA_BYTES,
        }
    }
}

impl TypeDescriptionLimits {
    pub fn validate(&self) -> Result<()> {
        if self.response_bytes == 0 || self.response_bytes > MAX_INTERFACE_DEFINITION_BYTES {
            return Err(Error::InvalidArgument(format!(
                "type-description response_bytes must be in 1..={MAX_INTERFACE_DEFINITION_BYTES}"
            )));
        }
        validate_schema_limit(self.schema_bytes)
    }
}

pub(crate) fn validate_schema_limit(limit: usize) -> Result<()> {
    if limit == 0 || limit > MAX_INTERFACE_SCHEMA_BYTES {
        Err(Error::InvalidArgument(format!(
            "interface schema_bytes must be in 1..={MAX_INTERFACE_SCHEMA_BYTES}"
        )))
    } else {
        Ok(())
    }
}

fn resource_limit(message: impl Into<String>) -> Error {
    Error::ResourceLimit(format!("interface {}", message.into()))
}

/// jsonschema erases regex compiler failures to a generic `format: regex`
/// error. Recheck only that failed pattern with its pinned translation/backend
/// to recover CompiledTooBig, without rejecting successful literal fast paths.
pub(crate) fn compilation_error(field: &str, error: jsonschema::ValidationError<'_>) -> Error {
    if matches!(error.kind(), jsonschema::error::ValidationErrorKind::Format { format } if format == "regex")
    {
        if let Some(pattern) = error.instance().as_str() {
            if let Ok(translated) = jsonschema_regex::to_rust_regex(pattern) {
                let compiled = regex::RegexBuilder::new(translated.as_ref())
                    .size_limit(MAX_INTERFACE_REGEX_COMPILED_BYTES)
                    .dfa_size_limit(MAX_INTERFACE_REGEX_DFA_BYTES)
                    .build();
                if matches!(compiled, Err(regex::Error::CompiledTooBig(_))) {
                    return resource_limit(format!("{field} regex compiled size exceeds {MAX_INTERFACE_REGEX_COMPILED_BYTES} bytes"));
                }
            }
        }
    }
    Error::InvalidArgument(format!("invalid {field} schema: {error}"))
}

pub(crate) fn schema_children(schema: &Value, include_definitions: bool) -> Vec<&Value> {
    let Some(map) = schema.as_object() else {
        return Vec::new();
    };
    let mut children = Vec::new();
    for key in [
        "additionalProperties",
        "unevaluatedProperties",
        "propertyNames",
        "contains",
        "items",
        "unevaluatedItems",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    ] {
        if let Some(child) = map.get(key) {
            children.push(child);
        }
    }
    for key in ["properties", "patternProperties", "dependentSchemas"] {
        if let Some(values) = map.get(key).and_then(Value::as_object) {
            children.extend(values.values());
        }
    }
    if let Some(values) = map.get("dependencies").and_then(Value::as_object) {
        children.extend(values.values().filter(|value| !value.is_array()));
    }
    if include_definitions {
        for key in ["$defs", "definitions"] {
            if let Some(values) = map.get(key).and_then(Value::as_object) {
                children.extend(values.values());
            }
        }
    }
    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(values) = map.get(key).and_then(Value::as_array) {
            children.extend(values);
        }
    }
    children
}

// serde_json's integer and other-number forms select different enum paths in
// pinned jsonschema 0.59.0; 2 and 2.0 may be equal but have different work costs.
const NULL: usize = 0;
const BOOL: usize = 1;
const INTEGER: usize = 2;
const OTHER_NUMBER: usize = 3;
const STRING: usize = 4;
const ARRAY: usize = 5;
const OBJECT: usize = 6;

fn value_kind(value: &Value) -> usize {
    match value {
        Value::Null => NULL,
        Value::Bool(_) => BOOL,
        Value::Number(number) if number.as_i64().is_some() => INTEGER,
        Value::Number(_) => OTHER_NUMBER,
        Value::String(_) => STRING,
        Value::Array(_) => ARRAY,
        Value::Object(_) => OBJECT,
    }
}

/// Conservative per-kind work, separate from structural schema-node limits.
/// Coefficients follow pinned validator paths, not elapsed time or instructions.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidationWork {
    schema_visits: usize,
    values: [usize; 7],
    string_bytes: usize,
    key_values: usize,
    key_bytes: usize,
}

fn capped_add(a: usize, b: usize) -> usize {
    a.saturating_add(b).min(MAX_INTERFACE_VALIDATION_WORK + 1)
}

impl ValidationWork {
    fn add(&mut self, other: Self) {
        self.schema_visits = self
            .schema_visits
            .saturating_add(other.schema_visits)
            .min(MAX_INTERFACE_EXPANDED_SCHEMA_NODES + 1);
        for (left, right) in self.values.iter_mut().zip(other.values) {
            *left = capped_add(*left, right);
        }
        self.string_bytes = capped_add(self.string_bytes, other.string_bytes);
        self.key_values = capped_add(self.key_values, other.key_values);
        self.key_bytes = capped_add(self.key_bytes, other.key_bytes);
    }

    fn equality(&mut self, options: &[Value]) {
        let count = options.len();
        if options.iter().any(|v| v.is_array() || v.is_object()) {
            // A compound comparison can descend through every payload node,
            // string and key. Charge all candidates, including shared prefixes.
            for weight in &mut self.values {
                *weight = capped_add(*weight, count);
            }
            self.string_bytes = capped_add(self.string_bytes, count);
        } else {
            let mut kinds = [false; 7];
            for value in options {
                kinds[value_kind(value)] = true;
                if value.is_number() {
                    kinds[INTEGER] = true;
                    kinds[OTHER_NUMBER] = true;
                }
            }
            for (weight, present) in self.values.iter_mut().zip(kinds) {
                if present {
                    *weight = capped_add(*weight, count);
                }
            }
            if kinds[STRING] {
                self.string_bytes = capped_add(self.string_bytes, count);
            }
        }
    }

    fn for_schema(schema: &Value) -> Self {
        let mut work = Self {
            schema_visits: 1,
            values: [1; 7],
            string_bytes: 0,
            key_values: 0,
            key_bytes: 0,
        };
        let Some(map) = schema.as_object() else {
            return work;
        };
        for keyword in [
            "minLength",
            "maxLength",
            "format",
            "contentEncoding",
            "contentMediaType",
            "contentSchema",
        ] {
            if map.contains_key(keyword) {
                work.string_bytes += 1;
            }
        }
        if let Some(pattern) = map.get("pattern").and_then(Value::as_str) {
            let cost = pattern_work(pattern);
            work.string_bytes = capped_add(work.string_bytes, cost);
            work.values[STRING] = capped_add(work.values[STRING], cost);
        }
        if let Some(value) = map.get("const") {
            work.equality(std::slice::from_ref(value));
        }
        if let Some(items) = map.get("enum").and_then(Value::as_array) {
            if items.len() > 1 && items.iter().all(|v| v.is_null() || v.as_i64().is_some()) {
                // IntegerEnumValidator: i64 binary search; floating-point and
                // out-of-i64 representations use numeric-equality linear scan.
                let logarithmic = usize::BITS as usize - items.len().leading_zeros() as usize;
                work.values[INTEGER] = capped_add(work.values[INTEGER], logarithmic);
                work.values[OTHER_NUMBER] = capped_add(work.values[OTHER_NUMBER], items.len());
            } else if items.len() > 10 && items.iter().all(|v| v.is_null() || v.is_string()) {
                // BigStringEnumValidator hashes once; do not charge enum.len()
                // full string scans for this optimized path.
                work.values[STRING] = capped_add(work.values[STRING], 1);
                work.string_bytes = capped_add(work.string_bytes, 1);
            } else {
                work.equality(items);
            }
        }
        if map.get("uniqueItems").and_then(Value::as_bool) == Some(true) {
            // Hashing/equality touches nested values and their text as well.
            for weight in &mut work.values {
                *weight = capped_add(*weight, 1);
            }
            work.string_bytes = capped_add(work.string_bytes, 1);
        }
        // propertyNames applies ordinary string validators to keys. Conservatively
        // apply the string profile to keys too, including enum/const comparisons.
        work.key_values = work.values[STRING];
        work.key_bytes = work.string_bytes;
        if let Some(properties) = map.get("patternProperties").and_then(Value::as_object) {
            for pattern in properties.keys() {
                let cost = pattern_work(pattern);
                work.key_values = capped_add(work.key_values, cost);
                work.key_bytes = capped_add(work.key_bytes, cost);
            }
        }
        work
    }
}

fn pattern_work(pattern: &str) -> usize {
    match jsonschema_regex::analyze_pattern(pattern) {
        Some(jsonschema_regex::PatternAnalysis::Alternation(alternatives)) => {
            alternatives.len().max(1)
        }
        _ => 1,
    }
}

fn check_regex_program(pattern: &str) -> Result<()> {
    use regex_automata::{nfa::thompson, util::syntax};

    // This preflight owns resource admission only. Let the actual validator
    // report invalid syntax with its schema part and original location.
    let Ok(translated) = jsonschema_regex::to_rust_regex(pattern) else {
        return Ok(());
    };
    // Match regex 1.13.1's string builder and meta forward-NFA configuration.
    // DFA cache size does not affect this construction; the validator retains
    // its existing 256 KiB cache setting. Never match a payload in this check.
    let built = thompson::NFA::compiler()
        .syntax(syntax::Config::new().utf8(true))
        .configure(
            thompson::Config::new()
                .utf8(true)
                .reverse(false)
                .shrink(false)
                .which_captures(thompson::WhichCaptures::All)
                .nfa_size_limit(Some(MAX_INTERFACE_REGEX_COMPILED_BYTES)),
        )
        .build(translated.as_ref());
    let nfa = match built {
        Ok(nfa) => nfa,
        Err(error) => {
            return if error.size_limit().is_some() {
                Err(resource_limit(format!(
                    "regex compiled size exceeds {MAX_INTERFACE_REGEX_COMPILED_BYTES} bytes"
                )))
            } else {
                Ok(())
            };
        }
    };
    if nfa.states().len() > MAX_INTERFACE_REGEX_NFA_STATES {
        return Err(resource_limit(format!(
            "regex NFA states exceed {MAX_INTERFACE_REGEX_NFA_STATES}"
        )));
    }
    Ok(())
}

/// Mirror pinned jsonschema's backend selection within declared schema positions.
/// In particular, unevaluatedProperties recompiles property patterns through its evaluation
/// graph, including literal patterns under combinators and resolved references.
/// IDs are only graph keys; no pointer is dereferenced after the resolver drops.
#[derive(Default)]
struct RegexPrograms {
    distinct: BTreeSet<String>,
    required: BTreeSet<String>,
    property_patterns: BTreeMap<usize, Vec<String>>,
    evaluation_edges: BTreeMap<usize, BTreeSet<usize>>,
    unevaluated: BTreeSet<usize>,
}

impl RegexPrograms {
    fn edge(&mut self, source: &Value, target: &Value) {
        self.evaluation_edges
            .entry(source as *const Value as usize)
            .or_default()
            .insert(target as *const Value as usize);
    }

    fn record(&mut self, schema: &Value) {
        let Some(map) = schema.as_object() else {
            return;
        };
        let identity = schema as *const Value as usize;
        if map
            .get("unevaluatedProperties")
            .is_some_and(|value| value != &Value::Bool(true))
        {
            self.unevaluated.insert(identity);
        }
        for keyword in ["allOf", "anyOf", "oneOf"] {
            if let Some(children) = map.get(keyword).and_then(Value::as_array) {
                for child in children {
                    self.edge(schema, child);
                }
            }
        }
        if let Some(children) = map.get("dependentSchemas").and_then(Value::as_object) {
            for child in children.values() {
                self.edge(schema, child);
            }
        }
        if map
            .get("if")
            .is_some_and(|v| v.is_object() || v.is_boolean())
        {
            for keyword in ["if", "then", "else"] {
                if let Some(child) = map.get(keyword) {
                    self.edge(schema, child);
                }
            }
        }
        if let Some(pattern) = map.get("pattern").and_then(Value::as_str) {
            if jsonschema_regex::analyze_pattern(pattern).is_none() {
                self.required.insert(pattern.into());
            }
        }
        if let Some(properties) = map.get("patternProperties").and_then(Value::as_object) {
            self.property_patterns
                .insert(identity, properties.keys().cloned().collect());
            // additionalProperties false/object optimizes each pattern alone.
            // Otherwise patternProperties uses literals only for an all-literal
            // group; one general pattern sends the entire group to regex.
            let individual = map
                .get("additionalProperties")
                .is_some_and(|value| value == &Value::Bool(false) || value.is_object());
            let all_literals = properties
                .keys()
                .all(|pattern| jsonschema_regex::analyze_pattern(pattern).is_some());
            for pattern in properties.keys() {
                if (!individual && !all_literals)
                    || jsonschema_regex::analyze_pattern(pattern).is_none()
                {
                    self.required.insert(pattern.clone());
                }
            }
        }
    }

    fn check(mut self) -> Result<()> {
        let mut pending: Vec<_> = self.unevaluated.iter().copied().collect();
        let mut visited = BTreeSet::new();
        while let Some(identity) = pending.pop() {
            if !visited.insert(identity) {
                continue;
            }
            if let Some(patterns) = self.property_patterns.get(&identity) {
                self.required.extend(patterns.iter().cloned());
            }
            if let Some(children) = self.evaluation_edges.get(&identity) {
                pending.extend(children);
            }
        }
        for pattern in self.required {
            check_regex_program(&pattern)?;
        }
        Ok(())
    }
}

/// Resolve local fragments with the same pinned library as jsonschema. Cache a
/// subgraph's cost, but charge that cost on *every* reference edge. Definitions
/// do not contribute to a root's expanded cost until reached by a reference.
fn expanded_schema_cost(
    schema: &Value,
    inspect: &mut dyn FnMut(&Value, Option<&Value>) -> Result<()>,
) -> Result<ValidationWork> {
    use referencing::{Draft, List, Registry, Resolver, Uri};
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };
    fn reference_error(error: referencing::Error) -> Error {
        Error::InvalidArgument(format!("invalid local schema reference: {error}"))
    }
    // Retain the resolver's shared URI/list storage: copying an arbitrarily
    // long $id string for every analysis state would amplify preflight memory.
    type CostKey = (usize, Arc<Uri<String>>, List<Uri<String>>);
    fn key(schema: &Value, resolver: &Resolver<'_>) -> CostKey {
        (
            schema as *const Value as usize,
            resolver.base_uri(),
            resolver.dynamic_scope(),
        )
    }
    #[derive(Default)]
    struct Costs<'a> {
        memo: HashMap<CostKey, ValidationWork>,
        active: BTreeSet<usize>,
        analysed: HashSet<CostKey>,
        targets: Vec<(&'a Value, Resolver<'a>)>,
    }
    fn admit_state(costs: &mut Costs<'_>, key: &CostKey) -> Result<()> {
        costs.analysed.insert(key.clone());
        if costs.analysed.len() > MAX_INTERFACE_REFERENCE_STATES {
            return Err(resource_limit(format!(
                "reference analysis states exceed {MAX_INTERFACE_REFERENCE_STATES}"
            )));
        }
        Ok(())
    }
    fn cost<'a>(
        schema: &'a Value,
        resolver: Resolver<'a>,
        costs: &mut Costs<'a>,
        inspect: &mut dyn FnMut(&Value, Option<&Value>) -> Result<()>,
    ) -> Result<ValidationWork> {
        inspect(schema, None)?;
        let identity = schema as *const Value as usize;
        if costs.active.contains(&identity) {
            return Err(resource_limit(
                "recursive schema references are unsupported",
            ));
        }
        let key = key(schema, &resolver);
        if let Some(cost) = costs.memo.get(&key) {
            return Ok(*cost);
        }
        admit_state(costs, &key)?;
        costs.active.insert(identity);
        let mut total = ValidationWork::for_schema(schema);
        for keyword in ["$ref", "$dynamicRef"] {
            if let Some(reference) = schema.get(keyword).and_then(Value::as_str) {
                let resolved = resolver.lookup(reference).map_err(reference_error)?;
                let (target, target_resolver, _) = resolved.into_inner();
                inspect(schema, Some(target))?;
                // Include definitions below a reference target even when that
                // target was originally stored in an annotation/data position.
                costs.targets.push((target, target_resolver.clone()));
                total.add(cost(target, target_resolver, costs, inspect)?);
            }
        }
        for child in schema_children(schema, false) {
            let child_resolver = resolver
                .in_subresource(Draft::Draft202012.create_resource_ref(child))
                .map_err(reference_error)?;
            total.add(cost(child, child_resolver, costs, inspect)?);
        }
        costs.active.remove(&identity);
        costs.memo.insert(key, total);
        Ok(total)
    }
    let base = schema
        .get("$id")
        .and_then(Value::as_str)
        .unwrap_or("json-schema:///");
    let registry = Registry::new()
        .draft(Draft::Draft202012)
        .add(base, schema)
        .map_err(reference_error)?
        .prepare()
        .map_err(reference_error)?;
    let resolver = registry.resolver(referencing::uri::from_str(base).map_err(reference_error)?);
    let mut costs = Costs::default();
    let root_cost = cost(schema, resolver.clone(), &mut costs, inspect)?;
    if root_cost.schema_visits > MAX_INTERFACE_EXPANDED_SCHEMA_NODES {
        return Err(resource_limit(format!(
            "expanded schema visits exceed {MAX_INTERFACE_EXPANDED_SCHEMA_NODES}"
        )));
    }
    // Check unused definitions for recursive/dangling references, but do not
    // add their expanded costs to the root validator's payload work charge.
    let mut pending = vec![(schema, resolver)];
    let mut scanned = HashSet::new();
    loop {
        pending.append(&mut costs.targets);
        let Some((schema, resolver)) = pending.pop() else {
            break;
        };
        let key = key(schema, &resolver);
        if !scanned.insert(key.clone()) {
            continue;
        }
        admit_state(&mut costs, &key)?;
        inspect(schema, None)?;
        if schema.get("$ref").is_some() || schema.get("$dynamicRef").is_some() {
            cost(schema, resolver.clone(), &mut costs, inspect)?;
        }
        for child in schema_children(schema, true) {
            let child_resolver = resolver
                .in_subresource(Draft::Draft202012.create_resource_ref(child))
                .map_err(reference_error)?;
            pending.push((child, child_resolver));
        }
    }
    Ok(root_cost)
}

/// A deterministic admission charge, not an instruction count or CPU deadline.
/// Values pay schema/keyword costs; only text-sensitive keywords charge bytes.
pub(crate) fn check_validation_work(value: &Value, work: ValidationWork) -> Result<()> {
    enum Children<'a> {
        Array(std::slice::Iter<'a, Value>),
        Object(serde_json::map::Iter<'a>),
    }
    let mut remaining = MAX_INTERFACE_VALIDATION_WORK;
    let mut consume = |units: usize| -> Result<()> {
        remaining = remaining.checked_sub(units).ok_or_else(|| Error::ResourceLimit(format!(
            "validation work exceeds {MAX_INTERFACE_VALIDATION_WORK} units ({} expanded schema visits)", work.schema_visits
        )))?;
        Ok(())
    };
    let mut stack = Vec::new();
    let mut current = Some(value);
    loop {
        if let Some(value) = current.take() {
            consume(work.values[value_kind(value)])?;
            match value {
                Value::String(value) => consume(value.len().saturating_mul(work.string_bytes))?,
                Value::Array(values) => stack.push(Children::Array(values.iter())),
                Value::Object(values) => stack.push(Children::Object(values.iter())),
                _ => {}
            }
        }
        let Some(children) = stack.last_mut() else {
            return Ok(());
        };
        current = match children {
            Children::Array(values) => values.next(),
            Children::Object(values) => match values.next() {
                Some((key, value)) => {
                    consume(
                        work.key_values
                            .saturating_add(key.len().saturating_mul(work.key_bytes)),
                    )?;
                    Some(value)
                }
                None => None,
            },
        };
        if current.is_none() {
            stack.pop();
        }
    }
}

/// Count compact JSON without first allocating a serialized copy.
pub(crate) fn check_json_bytes(value: &Value, maximum: usize, label: &str) -> Result<()> {
    struct Counter {
        remaining: usize,
        exceeded: bool,
    }
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.remaining {
                self.exceeded = true;
                return Err(io::Error::other("JSON byte limit"));
            }
            self.remaining -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        remaining: maximum,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut counter, value);
    if counter.exceeded {
        Err(resource_limit(format!("{label} exceeds {maximum} bytes")))
    } else {
        result.map_err(|e| Error::InvalidArgument(format!("invalid interface JSON: {e}")))
    }
}

/// Check depth before serialization, canonicalization or recursive schema walks.
pub(crate) fn check_definition(value: &Value) -> Result<()> {
    let mut pending = vec![(value, 1usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > MAX_INTERFACE_DEPTH {
            return Err(resource_limit(format!(
                "JSON depth exceeds {MAX_INTERFACE_DEPTH}"
            )));
        }
        let children = match value {
            Value::Array(values) => values.len(),
            Value::Object(values) => values.len(),
            _ => 0,
        };
        // Check before extending the frontier, including children not yet visited.
        if children > MAX_INTERFACE_JSON_NODES.saturating_sub(nodes + pending.len()) {
            return Err(resource_limit(format!(
                "JSON nodes exceed {MAX_INTERFACE_JSON_NODES}"
            )));
        }
        match value {
            Value::Array(values) => pending.extend(values.iter().map(|v| (v, depth + 1))),
            Value::Object(values) => pending.extend(values.values().map(|v| (v, depth + 1))),
            _ => {}
        }
    }
    check_json_bytes(value, MAX_INTERFACE_DEFINITION_BYTES, "definition_bytes")
}

/// Visit schema positions only. Defaults, examples and properties named "$ref"
/// are data; patternProperties keys, however, are actual regular expressions.
#[derive(Default)]
struct SchemaResources {
    nodes: usize,
    references: usize,
    regexes: usize,
    regex_source_bytes: usize,
}
impl SchemaResources {
    fn inspect(
        &mut self,
        schema: &Value,
        target: Option<&Value>,
        seen: &mut BTreeSet<usize>,
        programs: &mut RegexPrograms,
    ) -> Result<()> {
        if let Some(target) = target {
            programs.edge(schema, target);
            return Ok(());
        }
        // Structural nodes are counted once per physical object in each root;
        // the expansion calculation independently charges every reference edge.
        if !seen.insert(schema as *const Value as usize) {
            return Ok(());
        }
        self.nodes += 1;
        if self.nodes > MAX_INTERFACE_SCHEMA_NODES {
            return Err(resource_limit(format!(
                "schema nodes exceed {MAX_INTERFACE_SCHEMA_NODES}"
            )));
        }
        let Some(map) = schema.as_object() else {
            return Ok(());
        };
        if let Some(draft) = map.get("$schema") {
            if draft != "https://json-schema.org/draft/2020-12/schema" {
                return Err(Error::Unsupported(
                    "only JSON Schema 2020-12 is supported".into(),
                ));
            }
        }
        // jsonschema 0.59.0's property-evaluation compiler follows this legacy
        // keyword even under Draft 2020-12. The finite profile rejects it before
        // that compiler can create an unaccounted recursive evaluation edge.
        if map.contains_key("$recursiveRef") {
            return Err(resource_limit(
                "legacy $recursiveRef is unsupported by the finite schema profile",
            ));
        }
        for key in ["$ref", "$dynamicRef"] {
            if let Some(reference) = map.get(key) {
                if !reference.as_str().is_some_and(|v| v.starts_with('#')) {
                    return Err(Error::InvalidArgument("external schema references are unsupported; resolve package references before loading".into()));
                }
                self.references += 1;
                if self.references > MAX_INTERFACE_REFERENCES {
                    return Err(resource_limit(format!(
                        "schema references exceed {MAX_INTERFACE_REFERENCES}"
                    )));
                }
            }
        }
        let mut add_pattern = |pattern: &str| -> Result<()> {
            if !programs.distinct.contains(pattern) {
                self.regexes += 1;
                self.regex_source_bytes = self.regex_source_bytes.saturating_add(pattern.len());
                let charge = self.regexes.saturating_mul(
                    MAX_INTERFACE_REGEX_COMPILED_BYTES + MAX_INTERFACE_REGEX_DFA_BYTES,
                );
                if self.regexes > MAX_INTERFACE_REGEXES || charge > MAX_INTERFACE_REGEX_CHARGE_BYTES
                {
                    return Err(resource_limit(format!("distinct regexes exceed {MAX_INTERFACE_REGEXES} or regex charge exceeds {MAX_INTERFACE_REGEX_CHARGE_BYTES} bytes")));
                }
                if self.regex_source_bytes > MAX_INTERFACE_REGEX_SOURCE_BYTES {
                    return Err(resource_limit(format!(
                        "regex source bytes exceed {MAX_INTERFACE_REGEX_SOURCE_BYTES}"
                    )));
                }
                programs.distinct.insert(pattern.to_owned());
            }
            Ok(())
        };
        if let Some(pattern) = map.get("pattern").and_then(Value::as_str) {
            add_pattern(pattern)?;
        }
        if let Some(properties) = map.get("patternProperties").and_then(Value::as_object) {
            for pattern in properties.keys() {
                add_pattern(pattern)?;
            }
        }
        programs.record(schema);
        Ok(())
    }
}

pub(crate) fn check_schemas(
    value: &Value,
    fields: &[&str],
    max_bytes: usize,
) -> Result<BTreeMap<String, ValidationWork>> {
    validate_schema_limit(max_bytes)?;
    // Every byte gate precedes registry preparation and validator compilation.
    for field in fields {
        let schema = value
            .get(field)
            .ok_or_else(|| Error::InvalidArgument(format!("interface requires {field} schema")))?;
        check_json_bytes(schema, max_bytes, &format!("{field} schema_bytes"))?;
    }
    let mut resources = SchemaResources::default();
    let mut expanded_costs = BTreeMap::new();
    for field in fields {
        let schema = &value[*field];
        let mut seen = BTreeSet::new();
        let mut programs = RegexPrograms::default();
        let mut inspect = |schema: &Value, target: Option<&Value>| {
            resources.inspect(schema, target, &mut seen, &mut programs)
        };
        // Validate declared positions before preparing the resolver. Defaults
        // and unknown fields become schema only when a reference reaches them.
        let mut pending = vec![schema];
        while let Some(schema) = pending.pop() {
            inspect(schema, None)?;
            pending.extend(schema_children(schema, true));
        }
        let expanded = expanded_schema_cost(schema, &mut inspect)?;
        programs.check()?;
        expanded_costs.insert((*field).to_owned(), expanded);
    }
    Ok(expanded_costs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn definition_json_node_depth_and_byte_boundaries() {
        check_definition(&json!(vec![Value::Null; MAX_INTERFACE_JSON_NODES - 1])).unwrap();
        let error =
            check_definition(&json!(vec![Value::Null; MAX_INTERFACE_JSON_NODES])).unwrap_err();
        assert!(error.to_string().contains("JSON nodes"));
        let mut nested = Value::Null;
        for _ in 1..MAX_INTERFACE_DEPTH {
            nested = json!([nested]);
        }
        check_definition(&nested).unwrap();
        let error = check_definition(&json!([nested])).unwrap_err();
        assert!(error.to_string().contains("JSON depth"));
        check_json_bytes(&json!("123"), 5, "test").unwrap();
        assert!(check_json_bytes(&json!("123"), 4, "test").is_err());
        let error =
            check_definition(&json!("x".repeat(MAX_INTERFACE_DEFINITION_BYTES))).unwrap_err();
        assert!(error.to_string().contains("definition_bytes"));
    }

    #[test]
    fn references_and_pattern_text_are_counted_before_build() {
        let schema = |n| json!({"message":{"$defs":{"atom":true},"allOf":vec![json!({"$ref":"#/$defs/atom"}); n]}});
        check_schemas(
            &schema(MAX_INTERFACE_REFERENCES),
            &["message"],
            MAX_INTERFACE_SCHEMA_BYTES,
        )
        .unwrap();
        let error = check_schemas(
            &schema(MAX_INTERFACE_REFERENCES + 1),
            &["message"],
            MAX_INTERFACE_SCHEMA_BYTES,
        )
        .unwrap_err();
        assert!(error.to_string().contains("schema references"));
        let schema = |n| json!({"message":{"pattern":format!("^{}", "x".repeat(n - 1))}});
        check_schemas(
            &schema(MAX_INTERFACE_REGEX_SOURCE_BYTES),
            &["message"],
            MAX_INTERFACE_SCHEMA_BYTES,
        )
        .unwrap();
        let error = check_schemas(
            &schema(MAX_INTERFACE_REGEX_SOURCE_BYTES + 1),
            &["message"],
            MAX_INTERFACE_SCHEMA_BYTES,
        )
        .unwrap_err();
        assert!(error.to_string().contains("regex source bytes"));
    }

    #[test]
    fn regex_program_image_uuid_keeps_one_byte_charge() {
        let schema = json!({"message":{"type":"object","properties":{
            "header":{"type":"object","properties":{
                "stamp":{"type":"object","properties":{"sec":{"type":"integer"},"nanosec":{"type":"integer"}}},
                "frame_id":{"type":"string"}}},
            "height":{"type":"integer"},"width":{"type":"integer"},
            "encoding":{"type":"string"},"step":{"type":"integer"},"data":{"type":"string"},
            "id":{"type":"string","pattern":"^[0-9a-f]{32}$"}
        }}});
        let work = check_schemas(&schema, &["message"], MAX_INTERFACE_SCHEMA_BYTES).unwrap();
        // Inspect construction accounting only: no large Image payload is built
        // or matched. The NFA state count must not become a global byte weight.
        assert_eq!(work["message"].string_bytes, 1);
        assert_eq!(work["message"].key_bytes, 1);
    }
}
