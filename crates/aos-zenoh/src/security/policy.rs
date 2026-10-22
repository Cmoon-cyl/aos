use std::collections::{BTreeMap, BTreeSet};

use aos_core::{
    lifecycle_event_interface, node_card_interface, parameter_event_interface, resolve_qos,
    type_description_interface, ActionInterfaces, ActionQos, Durability, Endpoint, EntityKind,
    Interface, InterfaceKind, LifecycleServiceKind, ParameterServiceKind, QoS, Reliability, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    invalid, keys, CompiledSecurityPolicy, EndpointGrant, EndpointOperation as Op, ExpandedGrant,
    Flow, Message, NodeIdentity, SecurityPolicy, SubjectPolicy, SystemGrant,
    SystemOperation as Sys, MAX_SECURITY_JSON_BYTES, SECURITY_SCHEMA_VERSION,
};

const MAX_SUBJECTS: usize = 64;
const MAX_NODES: usize = 256;
const MAX_INPUT_GRANTS: usize = 1024;
const MAX_EXPANDED_GRANTS: usize = 32_768;

fn identifier(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
    {
        return Err(invalid(format!(
            "security {field} must be a nonempty literal ASCII identifier"
        )));
    }
    Ok(())
}

fn unique<T: Ord + Copy>(values: &[T], what: &str) -> Result<()> {
    if values.is_empty()
        || values.len() > 32
        || values.iter().copied().collect::<BTreeSet<_>>().len() != values.len()
    {
        return Err(invalid(format!(
            "{what} requires a nonempty, unique operation list"
        )));
    }
    Ok(())
}

struct Compiler {
    domain: u32,
    grants: BTreeSet<ExpandedGrant>,
    registrations: BTreeMap<NodeIdentity, BTreeSet<String>>,
}

impl Compiler {
    fn grant(
        &mut self,
        subject: &str,
        operation: &str,
        resource: &str,
        flow: Flow,
        message: Message,
        key: &str,
    ) -> Result<()> {
        zenoh::key_expr::OwnedKeyExpr::try_from(key.to_owned())
            .map_err(|_| invalid("compiler generated an invalid key expression"))?;
        self.grants.insert(ExpandedGrant {
            subject_id: subject.into(),
            operation: operation.into(),
            resource: resource.into(),
            flow,
            message,
            key_expr: key.into(),
        });
        if self.grants.len() > MAX_EXPANDED_GRANTS {
            return Err(invalid("security policy exceeds expanded permission limit"));
        }
        Ok(())
    }

    fn register(
        &mut self,
        subject: &str,
        node: &NodeIdentity,
        kind: EntityKind,
        endpoint: Option<&Endpoint>,
    ) -> Result<()> {
        let key = keys::graph_key(self.domain, node, kind, endpoint)?;
        self.registrations
            .entry(node.clone())
            .or_default()
            .insert(key.clone());
        self.grant(
            subject,
            "graph.register",
            &node.fully_qualified_name(),
            Flow::Ingress,
            Message::LivelinessToken,
            &key,
        )?;
        // A router may propagate broad same-domain discovery interests to a
        // provider. This permits requests only; all token ingress is exact.
        let selector = format!("aos/@lv/{}/**", self.domain);
        for message in [
            Message::LivelinessQuery,
            Message::DeclareLivelinessSubscriber,
        ] {
            self.grant(
                subject,
                "graph.discovery_request",
                &node.fully_qualified_name(),
                Flow::Egress,
                message,
                &selector,
            )?;
        }
        Ok(())
    }

    fn service(
        &mut self,
        subject: &str,
        node: &NodeIdentity,
        endpoint: &Endpoint,
        provide: bool,
        operation: &str,
    ) -> Result<()> {
        let key = keys::data_key(self.domain, endpoint)?;
        let messages: &[(Flow, Message)] = if provide {
            &[
                (Flow::Ingress, Message::DeclareQueryable),
                (Flow::Ingress, Message::Reply),
                (Flow::Egress, Message::Query),
            ]
        } else {
            &[
                (Flow::Ingress, Message::Query),
                (Flow::Egress, Message::Reply),
                (Flow::Egress, Message::DeclareQueryable),
            ]
        };
        for &(flow, message) in messages {
            self.grant(subject, operation, &endpoint.name, flow, message, &key)?;
        }
        self.register(
            subject,
            node,
            if provide {
                EntityKind::Service
            } else {
                EntityKind::Client
            },
            Some(endpoint),
        )
    }

    fn topic(
        &mut self,
        subject: &str,
        node: &NodeIdentity,
        endpoint: &Endpoint,
        qos: &QoS,
        publish: bool,
        operation: &str,
    ) -> Result<()> {
        qos.validate()?;
        let key = keys::data_key(self.domain, endpoint)?;
        let messages: &[(Flow, Message)] = if publish {
            &[
                (Flow::Ingress, Message::Put),
                (Flow::Egress, Message::DeclareSubscriber),
            ]
        } else {
            &[
                (Flow::Ingress, Message::DeclareSubscriber),
                (Flow::Egress, Message::Put),
            ]
        };
        for &(flow, message) in messages {
            self.grant(subject, operation, &endpoint.name, flow, message, &key)?;
        }
        self.register(
            subject,
            node,
            if publish {
                EntityKind::Publisher
            } else {
                EntityKind::Subscription
            },
            Some(endpoint),
        )?;
        if qos.durability == Durability::TransientLocal {
            self.advanced(subject, endpoint, &key, qos.reliability, publish, operation)?;
        }
        Ok(())
    }

    fn advanced(
        &mut self,
        subject: &str,
        endpoint: &Endpoint,
        key: &str,
        reliability: Reliability,
        publish: bool,
        operation: &str,
    ) -> Result<()> {
        // zenoh-ext 1.10.1 uses the _ metadata chunk in AOS, whose builders
        // never supply custom metadata. Runtime ZID/EID each occupy one chunk.
        let publisher = format!("{key}/@adv/pub/*/*/_");
        let subscriber = format!("{key}/@adv/sub/*/*/_");
        let history = format!("{key}/@adv/**");
        let detection = format!("{key}/@adv/pub/**");
        let mut rules = if publish {
            vec![
                (Flow::Ingress, Message::DeclareQueryable, publisher.clone()),
                (Flow::Egress, Message::Query, history),
                // Cache replies carry the original Topic key, not @adv.
                (Flow::Ingress, Message::Reply, key.into()),
                (Flow::Ingress, Message::LivelinessToken, publisher.clone()),
                (Flow::Egress, Message::LivelinessQuery, detection.clone()),
                (
                    Flow::Egress,
                    Message::DeclareLivelinessSubscriber,
                    detection.clone(),
                ),
            ]
        } else {
            vec![
                (Flow::Ingress, Message::Query, history),
                (Flow::Egress, Message::Reply, key.into()),
                (Flow::Egress, Message::DeclareQueryable, publisher.clone()),
                (Flow::Ingress, Message::LivelinessQuery, detection.clone()),
                (
                    Flow::Ingress,
                    Message::DeclareLivelinessSubscriber,
                    detection.clone(),
                ),
                (Flow::Egress, Message::LivelinessToken, publisher.clone()),
                (Flow::Ingress, Message::LivelinessToken, subscriber.clone()),
            ]
        };
        if reliability == Reliability::Reliable {
            rules.extend(if publish {
                vec![
                    (Flow::Ingress, Message::Put, publisher.clone()),
                    (Flow::Egress, Message::DeclareSubscriber, detection),
                ]
            } else {
                vec![
                    (Flow::Ingress, Message::DeclareSubscriber, detection),
                    (Flow::Egress, Message::Put, publisher),
                ]
            });
        }
        let operation = format!("{operation}.transient_local");
        for (flow, message, key) in rules {
            self.grant(subject, &operation, &endpoint.name, flow, message, &key)?;
        }
        Ok(())
    }

    fn endpoints(&mut self, subject: &SubjectPolicy, grant: &EndpointGrant) -> Result<()> {
        if !subject.nodes.contains(&grant.node) {
            return Err(invalid(
                "endpoint grant references a Node not owned by its subject",
            ));
        }
        unique(&grant.operations, "endpoint grant")?;
        keys::validate_resource(&grant.namespace, &grant.name)?;
        let interface = Interface::from_json(grant.interface.clone())?;
        let endpoint = keys::endpoint(&grant.name, &interface);
        match interface.kind() {
            InterfaceKind::Message => {
                let qos = resolve_qos(&grant.qos, "default", false)?.profile;
                for op in &grant.operations {
                    let publish = match op {
                        Op::Publish => true,
                        Op::Subscribe => false,
                        _ => return Err(invalid("message grant supports publish/subscribe only")),
                    };
                    self.topic(
                        &subject.id,
                        &grant.node,
                        &endpoint,
                        &qos,
                        publish,
                        if publish {
                            "topic.publish"
                        } else {
                            "topic.subscribe"
                        },
                    )?;
                }
            }
            InterfaceKind::Service => {
                resolve_qos(&grant.qos, "services_default", true)?;
                for op in &grant.operations {
                    let provide = match op {
                        Op::Provide => true,
                        Op::Call => false,
                        _ => return Err(invalid("service grant supports provide/call only")),
                    };
                    self.service(
                        &subject.id,
                        &grant.node,
                        &endpoint,
                        provide,
                        if provide {
                            "service.provide"
                        } else {
                            "service.call"
                        },
                    )?;
                }
            }
            InterfaceKind::Action => self.action(subject, grant, &interface)?,
        }
        Ok(())
    }

    fn action(
        &mut self,
        subject: &SubjectPolicy,
        grant: &EndpointGrant,
        interface: &Interface,
    ) -> Result<()> {
        let types = ActionInterfaces::new(interface)?;
        let qos = ActionQos::from_json(&grant.qos)?;
        qos.validate()?;
        let endpoint = |suffix: &str, interface| {
            keys::endpoint(&format!("{}/_action/{suffix}", grant.name), interface)
        };
        let base = keys::endpoint(&grant.name, interface);
        for op in &grant.operations {
            if *op == Op::Provide {
                self.register(
                    &subject.id,
                    &grant.node,
                    EntityKind::ActionServer,
                    Some(&base),
                )?;
                for (name, ty) in [
                    ("send_goal", &types.send_goal),
                    ("get_result", &types.get_result),
                    ("cancel_goal", &types.cancel_goal),
                ] {
                    self.service(
                        &subject.id,
                        &grant.node,
                        &endpoint(name, ty),
                        true,
                        "action.provide",
                    )?;
                }
                self.topic(
                    &subject.id,
                    &grant.node,
                    &endpoint("feedback", &types.feedback),
                    &qos.feedback.profile,
                    true,
                    "action.feedback.publish",
                )?;
                self.topic(
                    &subject.id,
                    &grant.node,
                    &endpoint("status", &types.status),
                    &qos.status.profile,
                    true,
                    "action.status.publish",
                )?;
            } else {
                self.register(
                    &subject.id,
                    &grant.node,
                    EntityKind::ActionClient,
                    Some(&base),
                )?;
                match op {
                    Op::SendGoal => self.service(&subject.id, &grant.node, &endpoint("send_goal", &types.send_goal), false, "action.send_goal")?,
                    Op::GetResult => self.service(&subject.id, &grant.node, &endpoint("get_result", &types.get_result), false, "action.get_result")?,
                    Op::CancelGoal => self.service(&subject.id, &grant.node, &endpoint("cancel_goal", &types.cancel_goal), false, "action.cancel_goal")?,
                    Op::Feedback => self.topic(&subject.id, &grant.node, &endpoint("feedback", &types.feedback), &qos.feedback.profile, false, "action.feedback.subscribe")?,
                    Op::Status => self.topic(&subject.id, &grant.node, &endpoint("status", &types.status), &qos.status.profile, false, "action.status.subscribe")?,
                    _ => return Err(invalid("action grant supports provide/send_goal/get_result/cancel_goal/feedback/status only")),
                }
            }
        }
        Ok(())
    }

    fn systems(&mut self, subject: &SubjectPolicy, grant: &SystemGrant) -> Result<()> {
        if !subject.nodes.contains(&grant.node) {
            return Err(invalid(
                "system grant references a Node not owned by its subject",
            ));
        }
        grant.target.validate()?;
        unique(&grant.operations, "system grant")?;
        let target = grant.target.fully_qualified_name();
        for op in &grant.operations {
            if matches!(
                op,
                Sys::ParametersProvide
                    | Sys::ParameterEventsPublish
                    | Sys::LifecycleProvide
                    | Sys::LifecycleEventsPublish
            ) && grant.node != grant.target
            {
                return Err(invalid(
                    "a subject may provide system services only for its local Node",
                ));
            }
            match op {
                Sys::Describe => {
                    for (suffix, interface) in [
                        ("get_node_card", node_card_interface()),
                        ("get_type_description", type_description_interface()),
                    ] {
                        self.service(
                            &subject.id,
                            &grant.node,
                            &keys::endpoint(&format!("{target}/{suffix}"), &interface),
                            false,
                            "system.describe",
                        )?;
                    }
                }
                Sys::ParametersRead | Sys::ParametersWrite | Sys::ParametersProvide => {
                    for kind in ParameterServiceKind::ALL {
                        let write = matches!(
                            kind,
                            ParameterServiceKind::SetParameters
                                | ParameterServiceKind::SetParametersAtomically
                        );
                        if *op == Sys::ParametersProvide || write == (*op == Sys::ParametersWrite) {
                            self.service(
                                &subject.id,
                                &grant.node,
                                &keys::endpoint(
                                    &format!("{target}/{}", kind.suffix()),
                                    &kind.interface(),
                                ),
                                *op == Sys::ParametersProvide,
                                if *op == Sys::ParametersProvide {
                                    "parameters.provide"
                                } else if write {
                                    "parameters.write"
                                } else {
                                    "parameters.read"
                                },
                            )?;
                        }
                    }
                    // enable_parameter_services also enables the standard
                    // global /parameter_events publisher. This precise topic
                    // is explicit in the expansion, not a namespace wildcard.
                    if *op == Sys::ParametersProvide {
                        self.parameter_events(subject, grant, true)?;
                    }
                }
                Sys::ParameterEventsRead | Sys::ParameterEventsPublish => {
                    self.parameter_events(subject, grant, *op == Sys::ParameterEventsPublish)?
                }
                Sys::LifecycleRead | Sys::LifecycleChange | Sys::LifecycleProvide => {
                    for kind in LifecycleServiceKind::ALL {
                        let write = kind == LifecycleServiceKind::ChangeState;
                        if *op == Sys::LifecycleProvide || write == (*op == Sys::LifecycleChange) {
                            self.service(
                                &subject.id,
                                &grant.node,
                                &keys::endpoint(
                                    &format!("{target}/{}", kind.suffix()),
                                    &kind.interface(),
                                ),
                                *op == Sys::LifecycleProvide,
                                if *op == Sys::LifecycleProvide {
                                    "lifecycle.provide"
                                } else if write {
                                    "lifecycle.change"
                                } else {
                                    "lifecycle.read"
                                },
                            )?;
                        }
                    }
                    if *op == Sys::LifecycleProvide {
                        self.lifecycle_events(subject, grant, true)?;
                    }
                }
                Sys::LifecycleEventsRead | Sys::LifecycleEventsPublish => {
                    self.lifecycle_events(subject, grant, *op == Sys::LifecycleEventsPublish)?
                }
            }
        }
        Ok(())
    }

    fn parameter_events(
        &mut self,
        subject: &SubjectPolicy,
        grant: &SystemGrant,
        publish: bool,
    ) -> Result<()> {
        self.topic(
            &subject.id,
            &grant.node,
            &keys::endpoint("/parameter_events", &parameter_event_interface()),
            &resolve_qos(&Value::Null, "parameter_events", false)?.profile,
            publish,
            if publish {
                "parameters.events.publish"
            } else {
                "parameters.events.subscribe"
            },
        )
    }

    fn lifecycle_events(
        &mut self,
        subject: &SubjectPolicy,
        grant: &SystemGrant,
        publish: bool,
    ) -> Result<()> {
        self.topic(
            &subject.id,
            &grant.node,
            &keys::endpoint(
                &format!("{}/transition_event", grant.target.fully_qualified_name()),
                &lifecycle_event_interface(),
            ),
            &resolve_qos(&Value::Null, "lifecycle_events", false)?.profile,
            publish,
            if publish {
                "lifecycle.events.publish"
            } else {
                "lifecycle.events.subscribe"
            },
        )
    }
}

/// Compile finite, final AOS resources into Zenoh 1.10.1's CN/TLS subjects and
/// explicit ingress/egress rules. No anonymous, delete, raw-key or wildcard
/// input is accepted. A policy file is a deployment input, not caller data.
pub fn compile_policy(policy: &SecurityPolicy) -> Result<CompiledSecurityPolicy> {
    if policy.schema_version != SECURITY_SCHEMA_VERSION {
        return Err(invalid("unsupported security policy schema_version"));
    }
    let source =
        serde_json::to_vec(policy).map_err(|_| invalid("cannot serialize security policy"))?;
    if source.len() > MAX_SECURITY_JSON_BYTES {
        return Err(invalid("security policy exceeds the 1 MiB limit"));
    }
    if policy.subjects.is_empty() || policy.subjects.len() > MAX_SUBJECTS {
        return Err(invalid("security policy requires 1..=64 subjects"));
    }
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut owners = BTreeMap::new();
    let mut input_grants = 0;
    for subject in &policy.subjects {
        identifier(&subject.id, "subject id")?;
        identifier(&subject.common_name, "certificate common_name")?;
        if !ids.insert(&subject.id) || !names.insert(&subject.common_name) {
            return Err(invalid(
                "subject ids and certificate common names must be unique",
            ));
        }
        for node in &subject.nodes {
            node.validate()?;
            if owners.insert(node.clone(), &subject.id).is_some() {
                return Err(invalid("each Node identity must have one policy owner"));
            }
        }
        input_grants += subject.endpoints.len() + subject.systems.len() + subject.graph_nodes.len();
    }
    if owners.len() > MAX_NODES || input_grants > MAX_INPUT_GRANTS {
        return Err(invalid("security policy exceeds Node or input grant limit"));
    }
    let mut compiler = Compiler {
        domain: policy.domain_id,
        grants: BTreeSet::new(),
        registrations: BTreeMap::new(),
    };
    for subject in &policy.subjects {
        for node in &subject.nodes {
            compiler.register(&subject.id, node, EntityKind::Node, None)?;
            for (suffix, interface) in [
                ("get_node_card", node_card_interface()),
                ("get_type_description", type_description_interface()),
            ] {
                compiler.service(
                    &subject.id,
                    node,
                    &keys::endpoint(
                        &format!("{}/{}", node.fully_qualified_name(), suffix),
                        &interface,
                    ),
                    true,
                    "node.builtin.provide",
                )?;
            }
        }
        for grant in &subject.endpoints {
            compiler.endpoints(subject, grant)?;
        }
        for grant in &subject.systems {
            compiler.systems(subject, grant)?;
        }
    }
    for subject in &policy.subjects {
        let mut graph_nodes = BTreeSet::new();
        for node in &subject.graph_nodes {
            node.validate()?;
            if !graph_nodes.insert(node) {
                return Err(invalid("duplicate graph Node grant"));
            }
            let tokens = compiler
                .registrations
                .get(node)
                .ok_or_else(|| invalid("graph Node is not registered by this complete policy"))?
                .clone();
            let selector = format!("aos/@lv/{}/**", policy.domain_id);
            for message in [
                Message::LivelinessQuery,
                Message::DeclareLivelinessSubscriber,
            ] {
                compiler.grant(
                    &subject.id,
                    "graph.read_request",
                    &node.fully_qualified_name(),
                    Flow::Ingress,
                    message,
                    &selector,
                )?;
            }
            for key in tokens {
                compiler.grant(
                    &subject.id,
                    "graph.read",
                    &node.fully_qualified_name(),
                    Flow::Egress,
                    Message::LivelinessToken,
                    &key,
                )?;
            }
        }
    }
    let mut rules = Vec::new();
    let mut rule_ids: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    // Deduplicate the physical ACL permission without losing its separate
    // human-readable reasons in expanded_grants.
    let permissions: BTreeSet<_> = compiler
        .grants
        .iter()
        .map(|g| {
            (
                g.subject_id.as_str(),
                g.flow,
                g.message,
                g.key_expr.as_str(),
            )
        })
        .collect();
    for (i, (subject, flow, message, key)) in permissions.into_iter().enumerate() {
        let id = format!("aos-rule-{i:05}");
        rules.push(json!({"id":id,"permission":"allow","flows":[flow],"messages":[message],"key_exprs":[key]}));
        rule_ids.entry(subject).or_default().push(id);
    }
    let mut policies = Vec::new();
    let mut subjects = Vec::new();
    for subject in &policy.subjects {
        let rules = rule_ids.get(subject.id.as_str()).ok_or_else(|| {
            invalid("each configured subject must have at least one finite permission")
        })?;
        subjects.push(json!({"id":subject.id,"cert_common_names":[subject.common_name],"link_protocols":["tls"]}));
        policies.push(
            json!({"id":format!("aos-policy-{}",subject.id),"subjects":[subject.id],"rules":rules}),
        );
    }
    let access_control = json!({"enabled":true,"default_permission":"deny","subjects":subjects,"rules":rules,"policies":policies});
    // Parse using the exact native schema so an accidental compiler typo
    // cannot be emitted as a seemingly successful, inert ACL document.
    zenoh::Config::from_json5(&json!({"access_control":access_control}).to_string())
        .map_err(|error| invalid(format!("native ACL rejected compiled policy: {error}")))?;
    Ok(CompiledSecurityPolicy {
        schema_version: SECURITY_SCHEMA_VERSION,
        domain_id: policy.domain_id,
        source_digest: format!("{:x}", Sha256::digest(&source)),
        access_control,
        expanded_grants: compiler.grants.into_iter().collect(),
    })
}
