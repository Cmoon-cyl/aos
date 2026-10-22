//! Logical node ownership, typed entities and ordinary introspection services.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

use crate::{
    clock::ShutdownResource,
    interface::{node_card_interface, type_description_interface},
    lifecycle::{LifecycleLease, LifecycleWorkKind},
    parameter_event_interface, resolve_node_config, resolve_qos,
    transport::{
        ClientBackend, Endpoint, EntityKind, GraphEntity, GraphGuard, PublisherBackend,
        QueueLimits, QueueStats, RequestBackend, ServiceBackend, SubscriptionBackend,
    },
    Clock, ClockType, Context, Error, Interface, InterfaceKind, Lifecycle, NameKind,
    NodeConfigRequest, ParameterServer, ParameterStore, ParameterValue, Rate, ResolvedName,
    ResolvedNodeConfig, ResolvedQoS, Result, TelemetryComponent, TelemetryEvent, TelemetryOutcome,
    TelemetryResource, Timer,
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeOptions {
    pub display_name: Option<String>,
    pub summary: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointOptions {
    pub summary: String,
    pub side_effects: Option<String>,
    pub contract_version: Option<String>,
}
impl EndpointOptions {
    fn validate(&self) -> Result<()> {
        if self
            .side_effects
            .as_deref()
            .is_some_and(|s| !matches!(s, "read_only" | "write" | "external_effect"))
        {
            return Err(Error::InvalidArgument(
                "side_effects must be read_only, write, external_effect or null".into(),
            ));
        }
        Ok(())
    }
}

/// Topic-only views of an Action. These handles cannot submit, cancel, or
/// retrieve goals, and do not own provider goal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionObservationKind {
    Feedback,
    Status,
}
impl ActionObservationKind {
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Feedback => "/_action/feedback",
            Self::Status => "/_action/status",
        }
    }
}

/// Node clones share one logical node. Explicit destruction invalidates all its
/// entities; dropping the last Node also destroys them, even if handles survive.
#[derive(Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
}
struct NodeInner {
    context: Context,
    id: String,
    name: String,
    namespace: String,
    fqn: String,
    options: NodeOptions,
    config: ResolvedNodeConfig,
    lifecycle: Option<Lifecycle>,
    lifecycle_server: Mutex<Option<crate::LifecycleServer>>,
    clock: Clock,
    parameters: ParameterStore,
    parameter_server: Mutex<Option<ParameterServer>>,
    parameter_event_publisher: Mutex<Option<Arc<TypedPublisher>>>,
    telemetry_resource: Arc<TelemetryResource>,
    clock_subscription: Mutex<Option<Arc<TypedSubscription>>>,
    timing_resources: Mutex<Vec<Weak<dyn ShutdownResource>>>,
    closed: AtomicBool,
    graph: Box<dyn GraphGuard>,
    resources: Mutex<BTreeMap<String, Weak<dyn Resource>>>,
    interfaces: Mutex<BTreeMap<(String, String), Interface>>,
    builtins: Mutex<Vec<Arc<BuiltinService>>>,
}

pub(crate) trait Resource: Send + Sync {
    fn close(&self) -> Result<()>;
    fn common(&self) -> &Common;
    fn prepare_activation(&self) -> Result<()> {
        Ok(())
    }
    fn description(&self) -> Value {
        self.common().description()
    }
}
pub(crate) struct Common {
    owner: Weak<NodeInner>,
    context: Context,
    node_id: String,
    telemetry_resource: Option<Arc<TelemetryResource>>,
    id: String,
    endpoint: Endpoint,
    kind: EntityKind,
    options: EndpointOptions,
    interface: Interface,
    qos: Option<ResolvedQoS>,
    name_resolution: ResolvedName,
    lifecycle: Option<Lifecycle>,
    closed: AtomicBool,
    graph: Box<dyn GraphGuard>,
}

impl Node {
    pub fn new(
        context: Context,
        name: &str,
        namespace: &str,
        options: NodeOptions,
    ) -> Result<Self> {
        Self::new_with_config(
            context,
            name,
            namespace,
            options,
            NodeConfigRequest::default(),
        )
    }
    pub fn new_with_config(
        context: Context,
        name: &str,
        namespace: &str,
        options: NodeOptions,
        request: NodeConfigRequest,
    ) -> Result<Self> {
        context.ensure_open()?;
        // Resolve every override before declaring any transport entity.
        let request = match context.process_bootstrap() {
            Some(bootstrap) => bootstrap.node_request(request)?,
            None => request,
        };
        let config = resolve_node_config(name, namespace, options, &request)?;
        Self::new_resolved(context, config)
    }
    /// Low-level entry for an already-composed configuration. The caller owns
    /// bootstrap composition; use `new`/`new_with_config` for automatic input.
    pub fn new_resolved(context: Context, config: ResolvedNodeConfig) -> Result<Self> {
        Self::build(context, config, false)
    }
    pub fn new_lifecycle(
        context: Context,
        name: &str,
        namespace: &str,
        options: NodeOptions,
    ) -> Result<Self> {
        Self::new_lifecycle_with_config(
            context,
            name,
            namespace,
            options,
            NodeConfigRequest::default(),
        )
    }
    pub fn new_lifecycle_with_config(
        context: Context,
        name: &str,
        namespace: &str,
        options: NodeOptions,
        request: NodeConfigRequest,
    ) -> Result<Self> {
        context.ensure_open()?;
        let request = match context.process_bootstrap() {
            Some(bootstrap) => bootstrap.node_request(request)?,
            None => request,
        };
        let config = resolve_node_config(name, namespace, options, &request)?;
        Self::new_resolved_lifecycle(context, config)
    }
    /// Low-level entry for an already-composed managed Node configuration.
    pub fn new_resolved_lifecycle(context: Context, config: ResolvedNodeConfig) -> Result<Self> {
        Self::build(context, config, true)
    }
    fn build(context: Context, config: ResolvedNodeConfig, managed: bool) -> Result<Self> {
        context.ensure_open()?;
        crate::names::validate_node_name(&config.name)?;
        crate::names::normalize_namespace(&config.namespace)?;
        let name = config.name.clone();
        let namespace = config.namespace.clone();
        let options = config.options.clone();
        let clock = context.create_clock(ClockType::Ros)?;
        let id = Uuid::new_v4().simple().to_string();
        let fqn = format!("{}/{name}", namespace.trim_end_matches('/'));
        let lifecycle = managed.then(|| Lifecycle::new(fqn.clone()));
        if let Some(lifecycle) = &lifecycle {
            context.register_shutdown_resource(lifecycle.shutdown_resource())?;
        }
        let parameters = ParameterStore::for_node(
            fqn.clone(),
            id.clone(),
            config.parameters.clone(),
            clock.clone(),
            lifecycle.clone(),
        )?;
        context.register_shutdown_resource(parameters.shutdown_resource())?;
        let graph = context.transport().declare_graph_entity(&GraphEntity {
            id: id.clone(),
            node_id: id.clone(),
            node_name: name.clone(),
            namespace: namespace.clone(),
            kind: EntityKind::Node,
            endpoint: None,
            qos: None,
        })?;
        let telemetry_resource = Arc::new(context.telemetry().resource(TelemetryComponent::Node));
        context.register_shutdown_resource(telemetry_resource.clone())?;
        let node = Self {
            inner: Arc::new(NodeInner {
                context,
                id,
                name: name.clone(),
                fqn,
                namespace,
                options,
                config,
                lifecycle,
                lifecycle_server: Mutex::new(None),
                clock,
                parameters,
                parameter_server: Mutex::new(None),
                parameter_event_publisher: Mutex::new(None),
                telemetry_resource,
                clock_subscription: Mutex::new(None),
                timing_resources: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
                graph,
                resources: Mutex::new(BTreeMap::new()),
                interfaces: Mutex::new(BTreeMap::new()),
                builtins: Mutex::new(Vec::new()),
            }),
        };
        let owner = Arc::downgrade(&node.inner);
        node.inner.parameters.set_sim_time_source(Arc::new(move || {
            let inner = owner.upgrade().ok_or(Error::Closed)?;
            Node { inner }.ensure_clock_subscription()
        }));
        if let Some(lifecycle) = &node.inner.lifecycle {
            let owner = Arc::downgrade(&node.inner);
            lifecycle.set_activation_hook(Arc::new(move || {
                owner
                    .upgrade()
                    .ok_or(Error::Closed)?
                    .prepare_lifecycle_activation()
            }));
        }
        node.add_builtin(
            node_card_interface(),
            "~/get_node_card",
            BuiltinKind::NodeCard,
        )?;
        if node.inner.parameters.get("use_sim_time")? == ParameterValue::Bool(true) {
            node.ensure_clock_subscription()?;
        }
        node.add_builtin(
            type_description_interface(),
            "~/get_type_description",
            BuiltinKind::TypeDescription,
        )?;
        if let Some(lifecycle) = &node.inner.lifecycle {
            let publisher = node.create_control_publisher(
                &crate::lifecycle_event_interface(),
                "~/transition_event",
                resolve_qos(&Value::Null, "lifecycle_events", false)?,
                EndpointOptions {
                    summary: "Authoritative lifecycle state transitions".into(),
                    ..Default::default()
                },
            )?;
            lifecycle.set_event_publisher(Arc::new(publisher));
        }
        node.inner.ensure_open()?;
        node.inner.record("created", TelemetryOutcome::Ok);
        Ok(node)
    }
    pub fn id(&self) -> &str {
        &self.inner.id
    }
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    pub fn namespace(&self) -> &str {
        &self.inner.namespace
    }
    pub fn fully_qualified_name(&self) -> &str {
        &self.inner.fqn
    }
    pub fn resolve_name(&self, name: &str) -> Result<String> {
        Ok(self
            .resolve_name_details(name, NameKind::Any)?
            .resolved_name)
    }
    pub fn resolve_name_details(&self, name: &str, kind: NameKind) -> Result<ResolvedName> {
        self.inner.config.resolve_name(name, kind)
    }
    pub fn effective_config(&self) -> Value {
        self.inner.effective_config()
    }
    pub fn parameters(&self) -> ParameterStore {
        self.inner.parameters.clone()
    }
    /// None for an ordinary Node. Cloned handles share the one Rust state
    /// machine; getting a Node handle never bypasses its managed entity gates.
    pub fn lifecycle(&self) -> Option<Lifecycle> {
        self.inner.lifecycle.clone()
    }
    pub fn enable_lifecycle_services(&self) -> Result<crate::LifecycleServer> {
        let mut server = self.inner.lifecycle_server.lock().unwrap();
        self.inner.ensure_open()?;
        if let Some(existing) = server.as_ref() {
            if existing.is_closed() {
                return Err(Error::Closed);
            }
            return Ok(existing.clone());
        }
        let created = crate::LifecycleServer::new(self)?;
        self.inner.ensure_open()?;
        *server = Some(created.clone());
        Ok(created)
    }
    /// Parameter events have Node lifetime independently of the six services.
    /// SDKs call this even when start_parameter_services is false.
    pub fn enable_parameter_events(&self) -> Result<()> {
        let mut publisher = self.inner.parameter_event_publisher.lock().unwrap();
        self.inner.ensure_open()?;
        if publisher.is_some() {
            return Ok(());
        }
        let created = Arc::new(self.create_control_publisher(
            &parameter_event_interface(),
            "/parameter_events",
            resolve_qos(&Value::Null, "parameter_events", false)?,
            EndpointOptions {
                summary: "Committed Node parameter changes".into(),
                ..Default::default()
            },
        )?);
        self.inner.ensure_open()?;
        self.inner
            .parameters
            .set_event_publisher(Some(created.clone()));
        *publisher = Some(created);
        Ok(())
    }
    /// Explicit in core for legacy-adapter compatibility. Language SDKs may
    /// enable by default; all six services require the parameters QoS preset.
    pub fn enable_parameter_services(&self) -> Result<ParameterServer> {
        self.enable_parameter_events()?;
        let mut server = self.inner.parameter_server.lock().unwrap();
        self.inner.ensure_open()?;
        if let Some(existing) = server.as_ref() {
            if existing.is_closed() {
                return Err(Error::Closed);
            }
            return Ok(existing.clone());
        }
        let created = ParameterServer::new(self)?;
        self.inner.ensure_open()?;
        *server = Some(created.clone());
        Ok(created)
    }
    pub fn clock(&self) -> Clock {
        self.inner.clock.clone()
    }
    pub fn set_use_sim_time(&self, enabled: bool) -> Result<()> {
        self.inner.ensure_open()?;
        self.inner.clock.set_use_sim_time(enabled)
    }
    pub fn create_timer(
        &self,
        period: Duration,
        clock: Option<Clock>,
        autostart: bool,
    ) -> Result<Timer> {
        self.inner.ensure_open()?;
        let clock = clock.unwrap_or_else(|| self.clock());
        let timer = if let Some(lifecycle) = self.lifecycle() {
            Timer::new_managed(clock, period, autostart, lifecycle, &self.inner.fqn)?
        } else {
            Timer::new(clock, period, autostart)?
        };
        self.register_timing(timer.shutdown_resource())?;
        Ok(timer)
    }
    pub fn create_rate(&self, frequency: f64, clock: Option<Clock>) -> Result<Rate> {
        self.inner.ensure_open()?;
        let clock = clock.unwrap_or_else(|| self.clock());
        let rate = if let Some(lifecycle) = self.lifecycle() {
            Rate::new_managed(clock, frequency, lifecycle, &self.inner.fqn)?
        } else {
            Rate::new(clock, frequency)?
        };
        self.register_timing(rate.timer().shutdown_resource())?;
        Ok(rate)
    }
    fn register_timing(&self, resource: Arc<dyn ShutdownResource>) -> Result<()> {
        let mut resources = self.inner.timing_resources.lock().unwrap();
        if let Err(error) = self.inner.ensure_open() {
            resource.shutdown();
            return Err(error);
        }
        self.inner
            .context
            .register_shutdown_resource(resource.clone())?;
        resources.retain(|resource| resource.strong_count() != 0);
        resources.push(Arc::downgrade(&resource));
        Ok(())
    }
    fn ensure_clock_subscription(&self) -> Result<()> {
        let mut slot = self.inner.clock_subscription.lock().unwrap();
        if slot.is_some() {
            return Ok(());
        }
        self.inner.ensure_open()?;
        let subscription = Arc::new(self.create_control_subscription(
            &crate::clock_interface(),
            "/clock",
            resolve_qos(&Value::Null, "clock", false)?,
            EndpointOptions {
                summary: "Node ROS time source".into(),
                ..Default::default()
            },
        )?);
        let worker = subscription.clone();
        let clock = self.clock();
        std::thread::Builder::new()
            .name(format!("aos-clock-{}", self.name()))
            .spawn(move || loop {
                match worker.recv_timeout(Duration::from_millis(100)) {
                    Ok(value) => {
                        let seconds = value["clock"]["sec"].as_i64();
                        let nanos = value["clock"]["nanosec"].as_i64();
                        if let Some(nanos) = seconds
                            .zip(nanos)
                            .and_then(|(sec, ns)| sec.checked_mul(1_000_000_000)?.checked_add(ns))
                        {
                            let _ = clock.update_ros_time(nanos);
                        }
                    }
                    Err(Error::Timeout | Error::Validation(_) | Error::ResourceLimit(_)) => {
                        continue
                    }
                    Err(_) => break,
                }
            })
            .map_err(|e| Error::Transport(format!("cannot start clock worker: {e}")))?;
        *slot = Some(subscription);
        Ok(())
    }
    pub fn node_card(&self) -> Value {
        self.inner.node_card()
    }
    pub fn destroy_node(&self) -> Result<()> {
        self.inner.close()
    }

    pub fn create_publisher(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedPublisher> {
        self.create_publisher_for_role(interface, name, qos.into(), options, false)
    }
    /// Creates only the selected observation Topic, using a complete Action
    /// Interface. Remapping applies once to the Action base, before the fixed
    /// internal suffix is appended.
    pub fn create_action_observation_publisher(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedPublisher> {
        let (interface, name) = self.action_observation(action, action_name, kind)?;
        self.create_publisher(&interface, &name, qos, options)
    }
    /// Creates only a feedback/status subscription; it never creates an
    /// ActionClient or a goal-control endpoint.
    pub fn create_action_observation_subscription(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedSubscription> {
        let (interface, name) = self.action_observation(action, action_name, kind)?;
        self.create_subscription(&interface, &name, qos, options)
    }
    /// As create_action_observation_subscription, requiring an adapter-enforced
    /// local payload budget. Limits are not advertised as Action or Topic QoS.
    pub fn create_action_observation_subscription_with_limits(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
        qos: impl Into<ResolvedQoS>,
        limits: QueueLimits,
        options: EndpointOptions,
    ) -> Result<TypedSubscription> {
        limits.validate()?;
        let (interface, name) = self.action_observation(action, action_name, kind)?;
        self.create_subscription_with_limits(&interface, &name, qos, limits, options)
    }
    fn action_observation(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
    ) -> Result<(Interface, String)> {
        if action_name.split('/').any(|part| part == "_action") {
            return Err(Error::InvalidArgument(
                "Action observation requires an Action base, not an internal endpoint".into(),
            ));
        }
        let interfaces = crate::ActionInterfaces::new(action)?;
        let base = self
            .resolve_name_details(action_name, NameKind::Action)?
            .resolved_name;
        if base.split('/').any(|part| part == "_action") {
            return Err(Error::InvalidArgument(
                "remapped Action observation base cannot be an internal endpoint".into(),
            ));
        }
        let name = format!("{base}{}", kind.suffix());
        crate::names::validate_absolute_name(&name)?;
        let interface = match kind {
            ActionObservationKind::Feedback => interfaces.feedback,
            ActionObservationKind::Status => interfaces.status,
        };
        // Offline recorders must be able to resolve the complete parent even
        // when this Node only owns an observation Topic, not an ActionServer.
        self.inner.ensure_open()?;
        self.inner.interfaces.lock().unwrap().insert(
            (action.type_name().into(), action.type_hash().into()),
            action.clone(),
        );
        Ok((interface, name))
    }
    pub(crate) fn create_control_publisher(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedPublisher> {
        self.create_publisher_for_role(interface, name, qos.into(), options, true)
    }
    fn create_publisher_for_role(
        &self,
        interface: &Interface,
        name: &str,
        qos: ResolvedQoS,
        options: EndpointOptions,
        control: bool,
    ) -> Result<TypedPublisher> {
        require_kind(interface, InterfaceKind::Message)?;
        qos.profile.validate()?;
        let endpoint = self.endpoint(interface, name)?;
        let backend = self
            .inner
            .context
            .transport()
            .create_typed_publisher(&endpoint, &qos.profile)?;
        let common = self.common(
            interface,
            endpoint,
            EntityKind::Publisher,
            Some(qos),
            name,
            options,
        )?;
        let state = Arc::new(PublisherState {
            common: if control {
                common.into_control()
            } else {
                common
            },
            backend,
        });
        self.register(state.clone())?;
        Ok(TypedPublisher { state })
    }
    pub fn create_subscription(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedSubscription> {
        self.create_subscription_for_role(interface, name, qos.into(), options, false, None)
    }
    /// Require a local adapter byte budget. A custom backend that cannot
    /// enforce it returns Unsupported before creating a subscription.
    pub fn create_subscription_with_limits(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        limits: QueueLimits,
        options: EndpointOptions,
    ) -> Result<TypedSubscription> {
        limits.validate()?;
        self.create_subscription_for_role(interface, name, qos.into(), options, false, Some(limits))
    }
    pub(crate) fn create_control_subscription(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<TypedSubscription> {
        self.create_subscription_for_role(interface, name, qos.into(), options, true, None)
    }
    fn create_subscription_for_role(
        &self,
        interface: &Interface,
        name: &str,
        qos: ResolvedQoS,
        options: EndpointOptions,
        control: bool,
        limits: Option<QueueLimits>,
    ) -> Result<TypedSubscription> {
        require_kind(interface, InterfaceKind::Message)?;
        qos.profile.validate()?;
        let endpoint = self.endpoint(interface, name)?;
        let transport = self.inner.context.transport();
        let backend = match limits {
            Some(limits) => {
                transport.create_typed_subscription_with_limits(&endpoint, &qos.profile, limits)?
            }
            None => transport.create_typed_subscription(&endpoint, &qos.profile)?,
        };
        let common = self.common(
            interface,
            endpoint,
            EntityKind::Subscription,
            Some(qos),
            name,
            options,
        )?;
        let state = Arc::new(SubscriptionState {
            common: if control {
                common.into_control()
            } else {
                common
            },
            backend,
            receiving: Mutex::new(()),
            skipped: AtomicU64::new(0),
        });
        self.register(state.clone())?;
        SubscriptionState::start_inactive_worker(&state)?;
        Ok(TypedSubscription { state })
    }
    pub fn create_client(
        &self,
        interface: &Interface,
        name: &str,
        options: EndpointOptions,
    ) -> Result<Client> {
        self.create_client_with_qos(
            interface,
            name,
            resolve_qos(&Value::Null, "services_default", true)?,
            options,
        )
    }
    pub fn create_client_with_qos(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<Client> {
        require_kind(interface, InterfaceKind::Service)?;
        let qos = qos.into();
        qos.profile.validate_service()?;
        let endpoint = self.endpoint(interface, name)?;
        let backend = self
            .inner
            .context
            .transport()
            .create_client_with_qos(&endpoint, &qos.profile)?;
        let state = Arc::new(ClientState {
            common: self.common(
                interface,
                endpoint,
                EntityKind::Client,
                Some(qos),
                name,
                options,
            )?,
            backend,
        });
        self.register(state.clone())?;
        Ok(Client { state })
    }
    pub fn create_service(
        &self,
        interface: &Interface,
        name: &str,
        options: EndpointOptions,
    ) -> Result<Service> {
        self.create_service_with_qos(
            interface,
            name,
            resolve_qos(&Value::Null, "services_default", true)?,
            options,
        )
    }
    pub fn create_service_with_qos(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<Service> {
        self.create_service_for_role(interface, name, qos.into(), options, false)
    }
    pub(crate) fn create_control_service(
        &self,
        interface: &Interface,
        name: &str,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> Result<Service> {
        self.create_service_for_role(interface, name, qos.into(), options, true)
    }
    fn create_service_for_role(
        &self,
        interface: &Interface,
        name: &str,
        qos: ResolvedQoS,
        options: EndpointOptions,
        control: bool,
    ) -> Result<Service> {
        require_kind(interface, InterfaceKind::Service)?;
        qos.profile.validate_service()?;
        let endpoint = self.endpoint(interface, name)?;
        let backend = self
            .inner
            .context
            .transport()
            .create_service_with_qos(&endpoint, &qos.profile)?;
        let common = self.common(
            interface,
            endpoint,
            EntityKind::Service,
            Some(qos),
            name,
            options,
        )?;
        let state = Arc::new(ServiceState {
            common: if control {
                common.into_control()
            } else {
                common
            },
            backend,
            receiving: Mutex::new(()),
        });
        self.register(state.clone())?;
        ServiceState::start_inactive_worker(&state)?;
        Ok(Service { state })
    }
    pub(crate) fn endpoint(&self, interface: &Interface, name: &str) -> Result<Endpoint> {
        self.inner.ensure_open()?;
        Ok(Endpoint {
            name: self
                .resolve_name_details(name, name_kind(interface.kind()))?
                .resolved_name,
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
        })
    }
    pub(crate) fn common(
        &self,
        interface: &Interface,
        endpoint: Endpoint,
        kind: EntityKind,
        qos: Option<ResolvedQoS>,
        original_name: &str,
        options: EndpointOptions,
    ) -> Result<Common> {
        options.validate()?;
        self.inner.ensure_open()?;
        let id = Uuid::new_v4().simple().to_string();
        let graph = self
            .inner
            .context
            .transport()
            .declare_graph_entity(&GraphEntity {
                id: id.clone(),
                node_id: self.id().into(),
                node_name: self.name().into(),
                namespace: self.namespace().into(),
                kind,
                endpoint: Some(endpoint.clone()),
                qos: qos.as_ref().map(|qos| qos.profile.clone()),
            })?;
        self.inner.interfaces.lock().unwrap().insert(
            (interface.type_name().into(), interface.type_hash().into()),
            interface.clone(),
        );
        // Action is a logical entity over five adapter resources. Count that
        // logical resource here; topic/service resources belong to the adapter.
        let telemetry_resource =
            if matches!(kind, EntityKind::ActionServer | EntityKind::ActionClient) {
                let guard = Arc::new(
                    self.inner
                        .context
                        .telemetry()
                        .resource(TelemetryComponent::Action),
                );
                self.inner
                    .context
                    .register_shutdown_resource(guard.clone())?;
                Some(guard)
            } else {
                None
            };
        Ok(Common {
            owner: Arc::downgrade(&self.inner),
            context: self.inner.context.clone(),
            node_id: self.id().to_owned(),
            telemetry_resource,
            id,
            endpoint,
            kind,
            options,
            interface: interface.clone(),
            qos,
            name_resolution: self
                .resolve_name_details(original_name, name_kind(interface.kind()))?,
            lifecycle: self.inner.lifecycle.clone(),
            closed: AtomicBool::new(false),
            graph,
        })
    }
    pub(crate) fn register(&self, resource: Arc<dyn Resource>) -> Result<()> {
        let mut resources = self.inner.resources.lock().unwrap();
        // This check is under the same lock that close uses to drain entities.
        self.inner.ensure_open()?;
        resources.insert(resource.common().id.clone(), Arc::downgrade(&resource));
        Ok(())
    }
    fn add_builtin(&self, interface: Interface, name: &str, kind: BuiltinKind) -> Result<()> {
        let endpoint = self.endpoint(&interface, name)?;
        let qos = resolve_qos(&Value::Null, "services_default", true)?;
        let backend = self
            .inner
            .context
            .transport()
            .create_service_with_qos(&endpoint, &qos.profile)?;
        let resource = Arc::new(BuiltinService {
            common: self.common(
                &interface,
                endpoint,
                EntityKind::Service,
                Some(qos),
                name,
                EndpointOptions {
                    summary: match kind {
                        BuiltinKind::NodeCard => "Describe this node and its live endpoints",
                        BuiltinKind::TypeDescription => {
                            "Describe an interface registered by this node"
                        }
                    }
                    .into(),
                    side_effects: Some("read_only".into()),
                    contract_version: None,
                },
            )?,
            backend,
        });
        self.register(resource.clone())?;
        self.inner.builtins.lock().unwrap().push(resource.clone());
        let owner = Arc::downgrade(&self.inner);
        std::thread::Builder::new()
            .name(format!("aos-introspection-{}", self.name()))
            .spawn(move || {
                loop {
                    if resource.common.closed.load(Ordering::Acquire)
                        || resource.common.context.is_shutdown()
                    {
                        break;
                    }
                    // Do not hold a strong Node reference while waiting, or this
                    // worker could keep the node alive after the last user drops it.
                    let request = match resource.backend.recv_timeout(Duration::from_millis(100)) {
                        Ok(request) => request,
                        Err(Error::Timeout) => continue,
                        Err(Error::Closed) => break,
                        Err(_) => break,
                    };
                    let response = interface
                        .decode("request", request.payload())
                        .and_then(|input| {
                            let node = owner.upgrade().ok_or(Error::Closed)?;
                            node.ensure_open()?;
                            match kind {
                                BuiltinKind::NodeCard => Ok(node.node_card()),
                                BuiltinKind::TypeDescription => node.type_description(&input),
                            }
                        })
                        .and_then(|value| interface.encode("response", &value));
                    let _ = request.reply(response);
                }
            })
            .map_err(|e| Error::Transport(format!("cannot start introspection worker: {e}")))?;
        Ok(())
    }
}

impl NodeInner {
    fn prepare_lifecycle_activation(&self) -> Result<()> {
        self.ensure_open()?;
        let resources: Vec<_> = self
            .resources
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for resource in resources {
            if !resource.common().closed.load(Ordering::Acquire) {
                resource.prepare_activation()?;
            }
        }
        let timing: Vec<_> = self
            .timing_resources
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for resource in timing {
            resource.prepare_lifecycle_activation()?;
        }
        Ok(())
    }
    fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.context.ensure_open()
    }
    fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(Error::Closed);
        }
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.close();
        }
        if let Some(server) = self.lifecycle_server.lock().unwrap().take() {
            let _ = server.close();
        }
        let _ = self.parameters.close();
        if let Some(server) = self.parameter_server.lock().unwrap().take() {
            let _ = server.close();
        }
        let resources: Vec<_> = {
            let mut entries = self.resources.lock().unwrap();
            let resources = entries.values().filter_map(Weak::upgrade).collect();
            entries.clear();
            resources
        };
        let timing: Vec<_> = self
            .timing_resources
            .lock()
            .unwrap()
            .drain(..)
            .filter_map(|resource| resource.upgrade())
            .collect();
        for resource in timing {
            resource.shutdown();
        }
        let _ = self.clock.close();
        let mut error = None;
        for resource in resources {
            if let Err(e) = resource.close() {
                if e != Error::Closed && error.is_none() {
                    error = Some(e);
                }
            }
        }
        if let Err(e) = self.graph.close() {
            if e != Error::Closed && error.is_none() {
                error = Some(e);
            }
        }
        self.builtins.lock().unwrap().clear();
        self.clock_subscription.lock().unwrap().take();
        self.parameter_event_publisher.lock().unwrap().take();
        self.telemetry_resource.close();
        self.record(
            "destroyed",
            error
                .as_ref()
                .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from),
        );
        error.map_or(Ok(()), Err)
    }
    fn record(&self, name: &str, outcome: TelemetryOutcome) {
        let mut event = TelemetryEvent::new(TelemetryComponent::Node, name);
        event.node_id = Some(self.id.clone());
        event.endpoint = Some(self.fqn.clone());
        event.outcome = outcome;
        self.context.telemetry().record(event);
    }
    fn node_card(&self) -> Value {
        let resources: Vec<_> = self
            .resources
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        let mut endpoints: Vec<_> = resources
            .iter()
            .filter(|r| !r.common().closed.load(Ordering::Acquire))
            .map(|r| r.description())
            .collect();
        endpoints.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        let mut card = json!({"id": self.id, "name": self.name, "namespace": self.namespace, "fully_qualified_name": self.fqn,
            "display_name": self.options.display_name, "summary": self.options.summary, "tags": self.options.tags,
            "lifecycle": self.lifecycle.as_ref().and_then(|lifecycle| lifecycle.snapshot().ok()),
            "effective_config": self.effective_config(), "endpoints": endpoints});
        if let Some(identity) = self.context.process_identity() {
            card["process"] = json!(identity);
        }
        card
    }
    fn effective_config(&self) -> Value {
        let mut value = self.config.effective_value();
        if let Ok(snapshot) = self.parameters.snapshot() {
            value["parameter_version"] = json!(snapshot.version);
            for parameter in snapshot.parameters {
                value["parameters"][&parameter.name] = json!({
                    "value": parameter.value, "source": parameter.source,
                    "version": parameter.version, "default_value": parameter.default_value,
                    "descriptor": parameter.descriptor,
                });
                if parameter.name == "use_sim_time" {
                    if let ParameterValue::Bool(enabled) = parameter.value {
                        value["use_sim_time"] =
                            json!({"value": enabled, "source": parameter.source});
                    }
                }
            }
        }
        value
    }
    fn type_description(&self, input: &Value) -> Result<Value> {
        let name = input["type_name"].as_str().unwrap(); // validated by built-in request schema
        let hash = input.get("type_hash").and_then(Value::as_str);
        let interfaces = self.interfaces.lock().unwrap();
        let matches: Vec<_> = interfaces
            .iter()
            .filter(|((n, h), _)| n == name && hash.is_none_or(|wanted| h == wanted))
            .map(|(_, v)| v)
            .collect();
        match matches.as_slice() {
            [interface] => Ok(interface.definition().clone()),
            [] => Err(Error::InvalidArgument(format!(
                "unknown interface {name} with requested hash"
            ))),
            _ => Err(Error::InvalidArgument(format!(
                "interface {name} is ambiguous; supply type_hash"
            ))),
        }
    }
}
impl Drop for NodeInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Common {
    fn into_control(mut self) -> Self {
        self.lifecycle = None;
        self
    }
    pub(crate) fn lifecycle(&self) -> Option<Lifecycle> {
        self.lifecycle.clone()
    }
    pub(crate) fn business_lease(&self, kind: LifecycleWorkKind) -> Result<Option<LifecycleLease>> {
        self.business_lease_in_epoch(kind, None, None)
    }
    pub(crate) fn business_lease_in_epoch(
        &self,
        kind: LifecycleWorkKind,
        entity_id: Option<&str>,
        epoch: Option<u64>,
    ) -> Result<Option<LifecycleLease>> {
        self.ensure_open()?;
        self.lifecycle
            .as_ref()
            .map(|lifecycle| {
                lifecycle.acquire_in_epoch(
                    kind,
                    &self.endpoint.name,
                    entity_id.unwrap_or(&self.id),
                    epoch,
                )
            })
            .transpose()
    }
    pub(crate) fn record(&self, mut event: TelemetryEvent) {
        // Immutable identifiers survive owner invalidation. Recording performs
        // no I/O and is always invoked outside the business state locks.
        event.node_id = Some(self.node_id.clone());
        event.endpoint = Some(self.endpoint.name.clone());
        self.context.telemetry().record(event);
    }
    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.context.ensure_open()?;
        self.owner.upgrade().ok_or(Error::Closed)?.ensure_open()
    }
    /// rclpy 4e607c09 client.py:172-191 / action/client.py:584-603:
    /// poll native availability, return false on timeout, and never invoke work.
    /// Like rmw_zenoh's availability check, AOS uses the graph, requiring the
    /// server kind and the complete endpoint name/type/hash identity.
    pub(crate) fn wait_for_ready(
        &self,
        kind: EntityKind,
        timeout: Option<Duration>,
    ) -> Result<bool> {
        self.wait_for_ready_with_check(kind, timeout, &mut || Ok(()))
    }
    pub(crate) fn wait_for_ready_with_check(
        &self,
        kind: EntityKind,
        timeout: Option<Duration>,
        check: &mut dyn FnMut() -> Result<()>,
    ) -> Result<bool> {
        let started = Instant::now();
        let interval = Duration::from_millis(50);
        self.ensure_open()?;
        let watch = self.context.watch_graph()?;
        loop {
            // Language signal checks run on the caller's thread, outside
            // resource/graph locks. An error ends only this wait; the same
            // discovery interest remains alive throughout successful checks.
            check()?;
            self.ensure_open()?;
            let iteration = Instant::now();
            let remaining = timeout.map(|limit| limit.saturating_sub(started.elapsed()));
            let poll = remaining.map_or(interval, |left| left.min(interval));
            let graph = match &watch {
                Some(watch) => watch.snapshot()?,
                // Compatibility for old/custom adapters. Zenoh supplies a
                // persistent watch and never uses short network queries here.
                None => self.context.transport().graph(poll)?,
            };
            let ready = graph.iter().any(|entity| {
                entity.kind == kind && entity.endpoint.as_ref() == Some(&self.endpoint)
            });
            self.ensure_open()?;
            if ready {
                return Ok(true);
            }
            if timeout.is_some_and(|limit| started.elapsed() >= limit) {
                return Ok(false);
            }
            // This interval bounds local entity/node closure checks only. The
            // network interest lives across the whole caller budget, including
            // an indefinite wait, and is released on every exit path.
            let wait = poll.saturating_sub(iteration.elapsed());
            match &watch {
                Some(watch) => match watch.wait_for_change(wait) {
                    Ok(()) | Err(Error::Timeout) => {}
                    Err(error) => return Err(error),
                },
                None => std::thread::sleep(wait),
            }
        }
    }
    pub(crate) fn close(&self, backend_close: impl FnOnce() -> Result<()>) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Err(Error::Closed);
        }
        // Always close both the transport entity and the graph declaration.
        let backend_result = backend_close();
        let graph_result = self.graph.close();
        if let Some(owner) = self.owner.upgrade() {
            owner.resources.lock().unwrap().remove(&self.id);
        }
        if let Some(resource) = &self.telemetry_resource {
            resource.close();
        }
        backend_result.and(graph_result)
    }
    fn description(&self) -> Value {
        json!({"name": self.endpoint.name, "kind": self.kind, "type_name": self.endpoint.type_name,
            "type_hash": self.endpoint.type_hash, "summary": self.options.summary,
            "side_effects": self.options.side_effects, "contract_version": self.options.contract_version,
            "qos": self.qos.as_ref().map(ResolvedQoS::effective_value),
            "original_name":self.name_resolution.original_name, "resolved_name":self.name_resolution.resolved_name,
            "name_resolution":self.name_resolution,"schema": self.interface.definition()})
    }
}

struct PublisherState {
    common: Common,
    backend: Box<dyn PublisherBackend>,
}
impl Resource for PublisherState {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| self.backend.close())
    }
}
impl Drop for PublisherState {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
pub struct TypedPublisher {
    state: Arc<PublisherState>,
}
impl TypedPublisher {
    pub fn endpoint(&self) -> &Endpoint {
        &self.state.common.endpoint
    }
    pub fn publish(&self, value: &Value) -> Result<()> {
        let _lease = self
            .state
            .common
            .business_lease(LifecycleWorkKind::Publisher)?;
        self.state
            .backend
            .publish(&self.state.common.interface.encode("message", value)?)
    }
    /// Validates JSON under the normal publisher lease, then publishes the
    /// exact supplied bytes. Whitespace and number spelling are preserved.
    pub fn publish_serialized(&self, bytes: &[u8]) -> Result<()> {
        let _lease = self
            .state
            .common
            .business_lease(LifecycleWorkKind::Publisher)?;
        self.state.common.interface.decode("message", bytes)?;
        self.state.backend.publish(bytes)
    }
    pub fn wait_for_subscribers(&self, timeout: Duration) -> Result<()> {
        self.state.common.ensure_open()?;
        let result = self.state.backend.wait_for_subscribers(timeout);
        self.state.common.ensure_open()?;
        result
    }
    pub fn close(&self) -> Result<()> {
        self.state.close()
    }
}
impl Drop for TypedPublisher {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct SubscriptionState {
    common: Common,
    backend: Box<dyn SubscriptionBackend>,
    receiving: Mutex<()>,
    skipped: AtomicU64,
}
impl SubscriptionState {
    fn start_inactive_worker(state: &Arc<Self>) -> Result<()> {
        let Some(lifecycle) = state.common.lifecycle.clone() else {
            return Ok(());
        };
        let weak = Arc::downgrade(state);
        std::thread::Builder::new()
            .name("aos-inactive-subscription".into())
            .spawn(move || loop {
                let Some(state) = weak.upgrade() else {
                    break;
                };
                if state.common.ensure_open().is_err() {
                    break;
                }
                let received = {
                    let _receiving = state.receiving.lock().unwrap();
                    lifecycle.receive_if_inactive(|| state.backend.recv_timeout(Duration::ZERO))
                };
                match received {
                    Ok(Some(_)) => {
                        state.skipped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(Error::Closed) => break,
                    Ok(None) | Err(Error::Timeout) => {}
                    Err(_) => break,
                }
                drop(state);
                std::thread::sleep(Duration::from_millis(5));
            })
            .map_err(|error| {
                Error::Transport(format!("cannot start inactive receiver: {error}"))
            })?;
        Ok(())
    }
    fn discard_queued(&self) -> Result<()> {
        let _receiving = self.receiving.lock().unwrap();
        // The transport KEEP_LAST queue is bounded by this depth. Clearing at
        // most depth removes every message that existed at the activation
        // barrier without allowing a continuous producer to starve activation.
        let depth = self.common.qos.as_ref().unwrap().profile.depth;
        for _ in 0..depth {
            match self.backend.recv_timeout(Duration::ZERO) {
                Ok(_) => {
                    self.skipped.fetch_add(1, Ordering::Relaxed);
                }
                Err(Error::Timeout) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
impl Resource for SubscriptionState {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| self.backend.close())
    }
    fn prepare_activation(&self) -> Result<()> {
        if self.common.lifecycle.is_some() {
            self.discard_queued()?;
        }
        Ok(())
    }
    fn description(&self) -> Value {
        let mut value = self.common.description();
        if self.common.lifecycle.is_some() {
            value["lifecycle_skipped_total"] = self.skipped.load(Ordering::Relaxed).into();
        }
        value
    }
}
impl Drop for SubscriptionState {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
pub struct TypedSubscription {
    state: Arc<SubscriptionState>,
}
/// A serialized receive can reject a payload before JSON parsing or schema
/// validation. The adapter may already have allocated/queued the raw bytes;
/// this result is not a transport ingress memory bound.
#[derive(Debug)]
pub enum BoundedSerializedSample {
    Sample(crate::Sample<Vec<u8>>),
    Oversized { payload_size_bytes: usize },
}
fn validate_bounded_serialized_sample(
    sample: crate::Sample<Vec<u8>>,
    max_bytes: usize,
    decode: impl FnOnce(&[u8]) -> Result<()>,
) -> Result<BoundedSerializedSample> {
    let payload_size_bytes = sample.payload.len().max(sample.payload_size_bytes);
    if payload_size_bytes > max_bytes {
        return Ok(BoundedSerializedSample::Oversized { payload_size_bytes });
    }
    decode(&sample.payload)?;
    Ok(BoundedSerializedSample::Sample(sample))
}
impl TypedSubscription {
    /// Transport queue statistics are preserved unchanged. Lifecycle discards
    /// are reported separately, so they cannot conceal transport overflow.
    pub fn queue_stats(&self) -> Option<QueueStats> {
        self.state.backend.queue_stats()
    }
    pub fn lifecycle_skipped_total(&self) -> u64 {
        self.state.skipped.load(Ordering::Relaxed)
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.state.common.endpoint
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Value> {
        self.recv_sample_timeout(timeout)
            .map(|sample| sample.payload)
    }
    /// Receive validated JSON while preserving the adapter's original
    /// delivery times, serialized payload size, and optional source evidence.
    /// Managed consumers must retain a SubscriptionCall instead.
    pub fn recv_sample_timeout(&self, timeout: Duration) -> Result<crate::Sample<Value>> {
        let sample = self.receive_bytes(timeout)?;
        let payload = self
            .state
            .common
            .interface
            .decode("message", &sample.payload)?;
        Ok(crate::Sample {
            payload,
            payload_size_bytes: sample.payload_size_bytes,
            received: sample.received,
            source: sample.source,
        })
    }
    /// Receives and validates JSON without reserializing it. Adapter metadata
    /// remains attached to the same original bytes. Managed subscriptions must
    /// still use take_message so a callback cannot bypass lifecycle draining.
    pub fn recv_serialized_timeout(&self, timeout: Duration) -> Result<crate::Sample<Vec<u8>>> {
        let sample = self.receive_bytes(timeout)?;
        self.state
            .common
            .interface
            .decode("message", &sample.payload)?;
        Ok(sample)
    }
    /// Rejects oversized adapter samples before decoding. The limit applies to
    /// both the actual byte length and the adapter's reported payload size.
    /// Metadata and original JSON bytes are preserved for admitted samples.
    /// Managed subscriptions still require take_message and its callback lease.
    pub fn recv_serialized_bounded_timeout(
        &self,
        timeout: Duration,
        max_bytes: usize,
    ) -> Result<BoundedSerializedSample> {
        if max_bytes == 0 {
            return Err(Error::InvalidArgument(
                "serialized receive size limit must be positive".into(),
            ));
        }
        let sample = self.receive_bytes(timeout)?;
        validate_bounded_serialized_sample(sample, max_bytes, |payload| {
            self.state
                .common
                .interface
                .decode("message", payload)
                .map(|_| ())
        })
    }
    fn receive_bytes(&self, timeout: Duration) -> Result<crate::Sample<Vec<u8>>> {
        if self.state.common.lifecycle.is_some() {
            return Err(Error::Unsupported(
                "managed subscriptions require take_message and a callback lease".into(),
            ));
        }
        self.state.common.ensure_open()?;
        let sample = self.state.backend.recv_sample_timeout(timeout)?;
        self.state.common.ensure_open()?;
        Ok(sample)
    }
    /// The executor must keep this call until its callback finishes, including
    /// time spent queued in a worker pool. Dropping it releases the drain lease.
    pub fn take_message(&self, timeout: Duration) -> Result<SubscriptionCall> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("receive timeout overflow".into()))?;
        loop {
            self.state.common.ensure_open()?;
            let epoch = self
                .state
                .common
                .lifecycle
                .as_ref()
                .map(Lifecycle::active_epoch)
                .transpose()?
                .flatten();
            if self.state.common.lifecycle.is_some() && epoch.is_none() {
                return Err(Error::Inactive("managed subscription is not active".into()));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            let _receiving = self.state.receiving.lock().unwrap();
            let received = self
                .state
                .backend
                .recv_sample_timeout(left.min(Duration::from_millis(20)));
            let sample = match received {
                Ok(sample) => sample,
                Err(Error::Timeout) if Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            };
            self.state.common.ensure_open()?;
            let lease = self
                .state
                .common
                .lifecycle
                .as_ref()
                .map(|lifecycle| {
                    lifecycle.acquire_in_epoch(
                        LifecycleWorkKind::Subscription,
                        &self.state.common.endpoint.name,
                        &self.state.common.id,
                        epoch,
                    )
                })
                .transpose();
            let lease = match lease {
                Ok(lease) => lease,
                Err(error) => {
                    self.state.skipped.fetch_add(1, Ordering::Relaxed);
                    return Err(error);
                }
            };
            let payload = self
                .state
                .common
                .interface
                .decode("message", &sample.payload)?;
            return Ok(SubscriptionCall {
                sample: crate::Sample {
                    payload,
                    payload_size_bytes: sample.payload_size_bytes,
                    received: sample.received,
                    source: sample.source,
                },
                subscription: self.state.clone(),
                _lease: lease,
            });
        }
    }
    pub fn close(&self) -> Result<()> {
        self.state.close()
    }
}
pub struct SubscriptionCall {
    sample: crate::Sample<Value>,
    subscription: Arc<SubscriptionState>,
    _lease: Option<LifecycleLease>,
}
impl SubscriptionCall {
    pub fn message(&self) -> &Value {
        &self.sample.payload
    }
    /// Adapter receive/source metadata stays attached to the same owned
    /// callback lease, including time queued in an Executor worker pool.
    pub fn sample(&self) -> &crate::Sample<Value> {
        &self.sample
    }
    pub fn is_valid(&self) -> bool {
        self.subscription.common.ensure_open().is_ok()
    }
    pub fn finish(self) {}
}
impl Drop for TypedSubscription {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct ClientState {
    common: Common,
    backend: Option<Box<dyn ClientBackend>>,
}
impl Resource for ClientState {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| {
            self.backend
                .as_ref()
                .map_or(Ok(()), |backend| backend.close())
        })
    }
}
impl Drop for ClientState {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
pub struct Client {
    state: Arc<ClientState>,
}
impl Client {
    pub fn queue_stats(&self) -> Option<QueueStats> {
        self.state
            .backend
            .as_ref()
            .and_then(|backend| backend.queue_stats())
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.state.common.endpoint
    }
    /// Wait until a matching provider is available. None has no deadline;
    /// false means the wait expired. Closing this client returns Error::Closed.
    pub fn wait_for_service(&self, timeout: Option<Duration>) -> Result<bool> {
        self.state
            .common
            .wait_for_ready(EntityKind::Service, timeout)
    }
    /// As wait_for_service, with a caller-thread check between bounded wait
    /// steps. A check error ends only this wait and never closes the client.
    /// The check must not block; it runs outside resource/graph locks.
    pub fn wait_for_service_with_check(
        &self,
        timeout: Option<Duration>,
        mut check: impl FnMut() -> Result<()>,
    ) -> Result<bool> {
        self.state
            .common
            .wait_for_ready_with_check(EntityKind::Service, timeout, &mut check)
    }
    pub fn call(&self, request: &Value, timeout: Duration) -> Result<Value> {
        self.call_wait(request, Some(timeout))
    }
    pub fn call_wait(&self, request: &Value, timeout: Option<Duration>) -> Result<Value> {
        self.state.common.ensure_open()?;
        let bytes = self.state.common.interface.encode("request", request)?;
        let response = match &self.state.backend {
            Some(backend) => backend.call_wait(&bytes, timeout, &self.state.common.closed),
            None => self.state.common.context.transport().call_wait(
                &self.state.common.endpoint,
                &bytes,
                timeout,
                &self.state.common.closed,
            ),
        }?;
        self.state.common.ensure_open()?;
        self.state.common.interface.decode("response", &response)
    }
    pub fn close(&self) -> Result<()> {
        self.state.close()
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct ServiceState {
    common: Common,
    backend: Box<dyn ServiceBackend>,
    receiving: Mutex<()>,
}
impl ServiceState {
    fn start_inactive_worker(state: &Arc<Self>) -> Result<()> {
        let Some(lifecycle) = state.common.lifecycle.clone() else {
            return Ok(());
        };
        let weak = Arc::downgrade(state);
        std::thread::Builder::new()
            .name("aos-inactive-service".into())
            .spawn(move || loop {
                let Some(state) = weak.upgrade() else {
                    break;
                };
                if state.common.ensure_open().is_err() {
                    break;
                }
                let received = {
                    let _receiving = state.receiving.lock().unwrap();
                    lifecycle.receive_if_inactive(|| state.backend.recv_timeout(Duration::ZERO))
                };
                match received {
                    Ok(Some(request)) => {
                        let _ = request.reply(Err(Error::Inactive(format!(
                            "{} does not admit business requests",
                            state.common.endpoint.name
                        ))));
                        continue;
                    }
                    Err(Error::Closed) => break,
                    Ok(None) | Err(Error::Timeout) => {}
                    Err(_) => break,
                }
                drop(state);
                std::thread::sleep(Duration::from_millis(5));
            })
            .map_err(|error| Error::Transport(format!("cannot start inactive service: {error}")))?;
        Ok(())
    }
}
impl Resource for ServiceState {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| self.backend.close())
    }
    fn prepare_activation(&self) -> Result<()> {
        if self.common.lifecycle.is_none() {
            return Ok(());
        }
        let _receiving = self.receiving.lock().unwrap();
        let depth = self.common.qos.as_ref().unwrap().profile.depth;
        for _ in 0..depth {
            match self.backend.recv_timeout(Duration::ZERO) {
                Ok(request) => {
                    let _ = request.reply(Err(Error::Inactive(
                        "request was queued before lifecycle activation".into(),
                    )));
                }
                Err(Error::Timeout) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
impl Drop for ServiceState {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
pub struct Service {
    state: Arc<ServiceState>,
}
impl Service {
    pub fn queue_stats(&self) -> Option<QueueStats> {
        self.state.backend.queue_stats()
    }
    pub fn endpoint(&self) -> &Endpoint {
        &self.state.common.endpoint
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<ServiceRequest> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("receive timeout overflow".into()))?;
        let (backend, lease) = loop {
            self.state.common.ensure_open()?;
            let _receiving = self.state.receiving.lock().unwrap();
            let epoch = match &self.state.common.lifecycle {
                Some(lifecycle) => Some(
                    lifecycle
                        .active_epoch()?
                        .ok_or_else(|| Error::Inactive("managed service is not active".into()))?,
                ),
                None => None,
            };
            let left = deadline.saturating_duration_since(Instant::now());
            let wait = if epoch.is_some() {
                left.min(Duration::from_millis(20))
            } else {
                left
            };
            let backend = match self.state.backend.recv_timeout(wait) {
                Ok(backend) => backend,
                Err(Error::Timeout) if Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            };
            self.state.common.ensure_open()?;
            let lease = self
                .state
                .common
                .lifecycle
                .as_ref()
                .map(|lifecycle| {
                    lifecycle.acquire_in_epoch(
                        LifecycleWorkKind::Service,
                        &self.state.common.endpoint.name,
                        &self.state.common.id,
                        epoch,
                    )
                })
                .transpose();
            match lease {
                Ok(lease) => break (backend, lease),
                Err(error) => {
                    drop(_receiving);
                    let _ = backend.reply(Err(error.clone()));
                    return Err(error);
                }
            }
        };
        self.decode_request(backend, lease)
    }
    /// Control protocols may atomically take a native request while their
    /// parent business gate is inactive. Decoding and any rejection reply
    /// must run after the caller has released its receive/gate locks.
    pub(crate) fn take_inactive_backend(
        &self,
        lifecycle: &Lifecycle,
    ) -> Result<Option<Box<dyn RequestBackend>>> {
        self.state.common.ensure_open()?;
        let _receiving = self.state.receiving.lock().unwrap();
        lifecycle.receive_if_inactive(|| self.state.backend.recv_timeout(Duration::ZERO))
    }
    pub(crate) fn decode_request(
        &self,
        backend: Box<dyn RequestBackend>,
        lease: Option<LifecycleLease>,
    ) -> Result<ServiceRequest> {
        self.state.common.ensure_open()?;
        let request = match self
            .state
            .common
            .interface
            .decode("request", backend.payload())
        {
            Ok(request) => request,
            Err(error) => {
                let _ = backend.reply(Err(error.clone()));
                return Err(error);
            }
        };
        Ok(ServiceRequest {
            request,
            backend,
            service: self.state.clone(),
            replied: AtomicBool::new(false),
            lease: Mutex::new(lease),
            taken_at: Instant::now(),
        })
    }
    pub fn close(&self) -> Result<()> {
        self.state.close()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Owned request handed to an executor. No application callback runs in core.
pub struct ServiceRequest {
    request: Value,
    backend: Box<dyn RequestBackend>,
    service: Arc<ServiceState>,
    replied: AtomicBool,
    lease: Mutex<Option<LifecycleLease>>,
    taken_at: Instant,
}
impl ServiceRequest {
    pub fn request(&self) -> &Value {
        &self.request
    }
    /// Cross-process diagnostic correlation, if supplied by the adapter.
    /// This unverified identifier must not be used for authorization.
    pub fn request_id(&self) -> Option<&str> {
        self.backend.request_id()
    }
    pub fn reply(&self, response: &Value) -> Result<()> {
        self.service.common.ensure_open()?;
        let encoded = self.service.common.interface.encode("response", response)?;
        self.settle(Ok(encoded))
    }
    pub fn reply_error(&self, message: &str) -> Result<()> {
        self.service.common.ensure_open()?;
        self.settle(Err(Error::Remote(message.into())))
    }
    fn settle(&self, response: Result<Vec<u8>>) -> Result<()> {
        if self.replied.swap(true, Ordering::AcqRel) {
            return Err(Error::InvalidArgument("request already replied".into()));
        }
        let response_outcome = response
            .as_ref()
            .err()
            .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from);
        let result = self.backend.reply(response);
        self.lease.lock().unwrap().take();
        let mut event = TelemetryEvent::new(TelemetryComponent::Service, "callback_completed");
        event.request_id = self.request_id().map(str::to_owned);
        event.outcome = result
            .as_ref()
            .err()
            .map_or(response_outcome, TelemetryOutcome::from);
        event.duration_ns = Some(self.taken_at.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        self.service.common.record(event);
        result
    }
}

#[derive(Clone, Copy)]
enum BuiltinKind {
    NodeCard,
    TypeDescription,
}
struct BuiltinService {
    common: Common,
    backend: Box<dyn ServiceBackend>,
}
impl Resource for BuiltinService {
    fn common(&self) -> &Common {
        &self.common
    }
    fn close(&self) -> Result<()> {
        self.common.close(|| self.backend.close())
    }
}
impl Drop for BuiltinService {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn require_kind(interface: &Interface, expected: InterfaceKind) -> Result<()> {
    if interface.kind() == expected {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "expected {expected:?} interface, got {:?}",
            interface.kind()
        )))
    }
}

fn name_kind(kind: InterfaceKind) -> NameKind {
    match kind {
        InterfaceKind::Message => NameKind::Topic,
        InterfaceKind::Service => NameKind::Service,
        InterfaceKind::Action => NameKind::Action,
    }
}

#[cfg(test)]
mod owner_tests {
    use super::*;
    use crate::transport::Transport;
    use std::sync::{atomic::AtomicUsize, Condvar};

    #[derive(Default)]
    struct EmptyReceiver {
        closed: Mutex<bool>,
        changed: Condvar,
    }
    impl EmptyReceiver {
        fn wait(&self, timeout: Duration) -> Error {
            let state = self.closed.lock().unwrap();
            if *state {
                return Error::Closed;
            }
            let (state, _) = self.changed.wait_timeout(state, timeout).unwrap();
            if *state {
                Error::Closed
            } else {
                Error::Timeout
            }
        }
        fn close_inner(&self) -> Result<()> {
            *self.closed.lock().unwrap() = true;
            self.changed.notify_all();
            Ok(())
        }
    }
    impl ServiceBackend for EmptyReceiver {
        fn recv_timeout(&self, timeout: Duration) -> Result<Box<dyn RequestBackend>> {
            Err(self.wait(timeout))
        }
        fn close(&self) -> Result<()> {
            self.close_inner()
        }
    }
    impl SubscriptionBackend for EmptyReceiver {
        fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
            Err(self.wait(timeout))
        }
        fn close(&self) -> Result<()> {
            self.close_inner()
        }
    }
    struct EmptyGuard;
    impl GraphGuard for EmptyGuard {
        fn close(&self) -> Result<()> {
            Ok(())
        }
    }
    struct OwnerTransport(Arc<AtomicUsize>);
    impl Transport for OwnerTransport {
        fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
            Err(Error::Unsupported("not used by owner test".into()))
        }
        fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
            Err(Error::Unsupported("not used by owner test".into()))
        }
        fn create_typed_subscription(
            &self,
            _: &Endpoint,
            _: &crate::QoS,
        ) -> Result<Box<dyn SubscriptionBackend>> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(Box::new(EmptyReceiver::default()))
        }
        fn create_service(&self, _: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
            Ok(Box::new(EmptyReceiver::default()))
        }
        fn declare_graph_entity(&self, _: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
            Ok(Box::new(EmptyGuard))
        }
        fn close(&self) -> Result<()> {
            Ok(())
        }
    }
    #[test]
    fn node_clock_public_setter_creates_source_once_and_respects_owner_close() {
        let count = Arc::new(AtomicUsize::new(0));
        let node = Node::new(
            Context::new(OwnerTransport(count.clone())),
            "n",
            "/",
            NodeOptions::default(),
        )
        .unwrap();
        let clock = node.clock();
        clock.set_use_sim_time(true).unwrap();
        assert_eq!(count.load(Ordering::Acquire), 1);
        assert!(clock.now().unwrap().use_sim_time);
        node.set_use_sim_time(false).unwrap();
        clock.set_use_sim_time(true).unwrap();
        assert_eq!(count.load(Ordering::Acquire), 1);
        node.destroy_node().unwrap();
        assert_eq!(clock.set_use_sim_time(false), Err(Error::Closed));
    }
    #[test]
    fn clock_worker_continues_after_a_resource_refusal() {
        struct Receiver {
            calls: AtomicUsize,
            empty: EmptyReceiver,
        }
        impl SubscriptionBackend for Receiver {
            fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
                match self.calls.fetch_add(1, Ordering::AcqRel) {
                    0 => Err(Error::ResourceLimit("rejected clock sample".into())),
                    1 => Ok(br#"{"clock":{"sec":42,"nanosec":0}}"#.to_vec()),
                    _ => Err(self.empty.wait(timeout)),
                }
            }
            fn close(&self) -> Result<()> {
                self.empty.close_inner()
            }
        }
        struct TransportAfterRefusal(OwnerTransport);
        impl Transport for TransportAfterRefusal {
            fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
                self.0.create_publisher(topic)
            }
            fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
                self.0.create_subscription(topic)
            }
            fn create_typed_subscription(
                &self,
                _: &Endpoint,
                _: &crate::QoS,
            ) -> Result<Box<dyn SubscriptionBackend>> {
                Ok(Box::new(Receiver {
                    calls: AtomicUsize::new(0),
                    empty: EmptyReceiver::default(),
                }))
            }
            fn create_service(&self, endpoint: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
                self.0.create_service(endpoint)
            }
            fn declare_graph_entity(&self, entity: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
                self.0.declare_graph_entity(entity)
            }
            fn close(&self) -> Result<()> {
                self.0.close()
            }
        }
        let transport = TransportAfterRefusal(OwnerTransport(Arc::new(AtomicUsize::new(0))));
        let node = Node::new(
            Context::new(transport),
            "resource_clock",
            "/",
            NodeOptions::default(),
        )
        .unwrap();
        node.set_use_sim_time(true).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !node.clock().now().unwrap().initialized {
            assert!(
                std::time::Instant::now() < deadline,
                "resource refusal stopped the clock worker"
            );
            std::thread::yield_now();
        }
        assert_eq!(node.clock().now().unwrap().nanoseconds, 42_000_000_000);
        node.destroy_node().unwrap();
    }

    #[test]
    fn oversized_serialized_samples_do_not_invoke_the_decoder() {
        let calls = std::cell::Cell::new(0);
        for (bytes, reported) in [(vec![b'x'; 128], 1), (b"{}".to_vec(), 128)] {
            let sample = crate::Sample {
                payload: bytes,
                payload_size_bytes: reported,
                received: None,
                source: None,
            };
            assert!(matches!(
                validate_bounded_serialized_sample(sample, 64, |_| {
                    calls.set(calls.get() + 1);
                    panic!("oversized input reached decoder")
                })
                .unwrap(),
                BoundedSerializedSample::Oversized {
                    payload_size_bytes: 128
                }
            ));
        }
        assert_eq!(calls.get(), 0);
        assert!(matches!(
            validate_bounded_serialized_sample(
                crate::Sample::without_receive_metadata(b"{}".to_vec()),
                2,
                |_| {
                    calls.set(calls.get() + 1);
                    Ok(())
                }
            )
            .unwrap(),
            BoundedSerializedSample::Sample(_)
        ));
        assert_eq!(calls.get(), 1);
    }
}
