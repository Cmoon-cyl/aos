//! Readiness observations for the one process supervisor. Graph is a persistent
//! local watch. Each generation has one RPC worker; poll never waits for one.
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aos_core::{
    transport::GraphWatchBackend, Client, Context, EndpointOptions, EntityKind, GraphEntity,
    LifecycleClient, LifecycleServiceKind, LifecycleTransitionSelector, Node, NodeOptions,
    ResolvedNodeConfig,
};
use aos_launch::{
    LaunchError, LaunchErrorCode, LaunchResult, ProbeFactory, ProcessIdentity, ReadinessPhase,
    ReadinessProbe, ReadinessProgress, ReadyEndpointKind, ReadyLifecycleState,
    ResolvedLaunchContext, ResolvedReadiness, ResolvedReadyEndpoint,
};
use aos_zenoh::ZenohTransport;
use serde_json::{json, Value};

#[derive(Default)]
pub struct CoreProbeFactory {
    diagnostics: Arc<Mutex<Vec<Value>>>,
    security: Option<aos_zenoh::security::ClientTlsProfile>,
    client_node: Option<ResolvedNodeConfig>,
}
const MAX_WORKERS: usize = 128;
const CLOSE_BUDGET: Duration = Duration::from_secs(1);

impl CoreProbeFactory {
    pub fn from_request(request: &Value) -> crate::Result<Self> {
        let path = request["security_profile"]
            .as_str()
            .map(std::path::Path::new);
        let security = aos_zenoh::security::load_client_profile(path)?;
        let client_node = crate::configuration::client_node(request)?;
        Ok(Self {
            security,
            client_node,
            ..Default::default()
        })
    }

    pub fn validate_children(&self, plan: &aos_launch::LaunchPlan) -> crate::Result<()> {
        let inherited = ["AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG"]
            .into_iter()
            .filter_map(|key| std::env::var_os(key).map(|value| (key.to_owned(), value)))
            .collect();
        self.validate_children_with_environment(plan, &inherited)
    }

    fn validate_children_with_environment(
        &self,
        plan: &aos_launch::LaunchPlan,
        inherited: &BTreeMap<String, std::ffi::OsString>,
    ) -> crate::Result<()> {
        let Some(profile) = &self.security else {
            return Ok(());
        };
        if plan.context().is_some() && self.client_node.is_none() {
            return Err(crate::CliError::input(
                "secure supervision requires a policy-authorized client_node for its readiness control session",
            ));
        }
        for process in plan.processes() {
            let path = process.spec.env.get("AOS_SECURITY_PROFILE").ok_or_else(|| {
                crate::CliError::input(format!(
                    "secure supervision requires process {:?} to explicitly select its own AOS_SECURITY_PROFILE",
                    process.spec.id
                ))
            })?;
            let child = aos_zenoh::security::load_client_profile(Some(std::path::Path::new(path)))?
                .expect("explicit profile never selects development mode");
            if child.domain_id != profile.domain_id {
                return Err(crate::CliError::input(format!(
                    "process {:?} security domain differs from the supervisor security profile",
                    process.spec.id
                )));
            }
            if child.router_endpoints != profile.router_endpoints {
                return Err(crate::CliError::input(format!(
                    "process {:?} security Router differs from the supervisor security profile",
                    process.spec.id
                )));
            }
            let mut environment = BTreeMap::new();
            for key in ["AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG"] {
                // An explicit empty child value still shadows its parent's
                // value. The core resolver then applies its empty-value rule.
                let value = match process.spec.env.get(key) {
                    Some(value) => Some(value.as_str()),
                    None => inherited
                        .get(key)
                        .map(|value| {
                            value.to_str().ok_or_else(|| {
                                crate::CliError::input(format!("{key} must contain Unicode text"))
                            })
                        })
                        .transpose()?,
                };
                if let Some(value) = value {
                    environment.insert(key.to_owned(), value.to_owned());
                }
            }
            // Only the deployment layers are known before spawn. A child's
            // program arguments remain subject to the same secure opener at
            // runtime; preflight must not invent their configuration.
            let resolved = aos_core::resolve_context_config(&aos_core::ContextConfigRequest {
                program: json!({"domain_id": child.domain_id}),
                config_files: process.bootstrap.context.config_files.clone(),
                environment,
                launch: process.bootstrap.context.launch.clone(),
                cli: process.bootstrap.context.cli.clone(),
            })?;
            if resolved.domain_id != child.domain_id {
                return Err(crate::CliError::input(format!(
                    "process {:?} resolved domain differs from its security profile",
                    process.spec.id
                )));
            }
            let config = match resolved.zenoh_config {
                Some(path) => zenoh::Config::from_file(path).map_err(|_| {
                    crate::CliError::input(format!(
                        "cannot load process {:?} secure native configuration",
                        process.spec.id
                    ))
                })?,
                None => aos_zenoh::security::client_config(&child)?,
            };
            aos_zenoh::security::validate_client_config(&config, &child)?;
        }
        Ok(())
    }

    pub fn take_diagnostics(&self) -> Vec<Value> {
        std::mem::take(&mut *self.diagnostics.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

fn failure(message: impl Into<String>) -> LaunchError {
    LaunchError::new(LaunchErrorCode::ReadinessFailed, "readiness", message)
}
fn core_error(error: aos_core::Error) -> LaunchError {
    failure(error.to_string())
}
fn timeout() -> LaunchError {
    LaunchError::new(
        LaunchErrorCode::ReadinessTimeout,
        "readiness",
        "readiness deadline expired",
    )
}

impl ProbeFactory for CoreProbeFactory {
    fn create(
        &mut self,
        selection: &ResolvedLaunchContext,
    ) -> LaunchResult<Box<dyn ReadinessProbe>> {
        Ok(Box::new(self.create_core(selection)?))
    }
}

impl CoreProbeFactory {
    fn create_core(&self, selection: &ResolvedLaunchContext) -> LaunchResult<CoreProbe> {
        // This is an explicit supervisor control session. It must not inherit
        // a parent AOS_PROCESS_CONFIG or Node environment identity.
        if let Some(profile) = &self.security {
            let resolved = aos_core::resolve_context_config(&aos_core::ContextConfigRequest {
                program: json!({"domain_id":selection.domain_id,
                    "zenoh_config":selection.zenoh_config}),
                ..Default::default()
            })
            .map_err(core_error)?;
            let context =
                aos_zenoh::security::open_resolved_secure_context(resolved, None, profile)
                    .map_err(core_error)?;
            return CoreProbe::with_identity(
                context,
                self.diagnostics.clone(),
                self.client_node.clone(),
            );
        }
        let config = match &selection.zenoh_config {
            Some(path) => zenoh::Config::from_file(path)
                .map_err(|e| failure(format!("control session configuration: {e}")))?,
            None => zenoh::Config::from_json5(aos_zenoh::DEFAULT_SESSION_CONFIG)
                .map_err(|e| failure(format!("default control session configuration: {e}")))?,
        };
        let context =
            Context::new(ZenohTransport::open(config, selection.domain_id).map_err(core_error)?);
        CoreProbe::with_diagnostics(context, self.diagnostics.clone())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    Card,
    State,
    Configure,
    Activate,
}

enum Operation {
    Card(Arc<Client>),
    State(Arc<LifecycleClient>),
    Change(Arc<LifecycleClient>, u8),
}

struct Job {
    identity: ProcessIdentity,
    sequence: u64,
    node_id: String,
    step: Step,
    deadline: Instant,
    operation: Operation,
}
struct Reply {
    identity: ProcessIdentity,
    sequence: u64,
    node_id: String,
    step: Step,
    result: aos_core::Result<Value>,
}

fn worker(jobs: mpsc::Receiver<Job>, replies: mpsc::SyncSender<Reply>) {
    while let Ok(job) = jobs.recv() {
        let remaining = job.deadline.saturating_duration_since(Instant::now());
        let result = if remaining.is_zero() {
            Err(aos_core::Error::Timeout)
        } else {
            match job.operation {
                Operation::Card(client) => {
                    client.call(&json!({}), remaining.min(Duration::from_millis(500)))
                }
                Operation::State(client) => client
                    .state(remaining.min(Duration::from_millis(500)))
                    .map(|v| json!(v)),
                Operation::Change(client, id) => {
                    // Leave part of the same budget for an observation after
                    // an uncertain response. The transition is never resubmitted.
                    let reserve = (remaining / 4).min(Duration::from_millis(250));
                    client
                        .change_state(
                            LifecycleTransitionSelector {
                                id,
                                label: String::new(),
                            },
                            None,
                            remaining.saturating_sub(reserve),
                        )
                        .map(|v| json!(v))
                }
            }
        };
        if replies
            .send(Reply {
                identity: job.identity,
                sequence: job.sequence,
                node_id: job.node_id,
                step: job.step,
                result,
            })
            .is_err()
        {
            break;
        }
    }
}

struct Observation {
    identity: ProcessIdentity,
    deadline: Instant,
    node_id: Option<String>,
    card: Option<Value>,
    card_client: Option<Arc<Client>>,
    lifecycle_client: Option<Arc<LifecycleClient>>,
    state: Option<String>,
    configure_sent: bool,
    activate_sent: bool,
    next_read: Instant,
    phase: ReadinessPhase,
    error: Option<LaunchError>,
    jobs: Option<mpsc::SyncSender<Job>>,
    worker: Option<JoinHandle<()>>,
    pending: Option<u64>,
    restart_after_exit: bool,
    waiting_reason: Option<String>,
}

impl Observation {
    fn new(
        identity: ProcessIdentity,
        remaining: Duration,
        restart_after_exit: bool,
    ) -> LaunchResult<Self> {
        let now = Instant::now();
        Ok(Self {
            identity,
            deadline: now.checked_add(remaining).ok_or_else(timeout)?,
            node_id: None,
            card: None,
            card_client: None,
            lifecycle_client: None,
            state: None,
            configure_sent: false,
            activate_sent: false,
            next_read: now,
            phase: ReadinessPhase::Waiting,
            error: None,
            jobs: None,
            worker: None,
            pending: None,
            restart_after_exit,
            waiting_reason: None,
        })
    }
    fn close(&self) {
        if let Some(client) = &self.card_client {
            let _ = client.close();
        }
        if let Some(client) = &self.lifecycle_client {
            let _ = client.close();
        }
    }
    fn progress(&self) -> ReadinessProgress {
        ReadinessProgress {
            phase: self.phase,
            node_id: self.node_id.clone(),
        }
    }
    fn timeout(&self) -> LaunchError {
        let mut error = timeout();
        if let Some(reason) = &self.waiting_reason {
            error.message = format!("{}: {reason}", error.message);
        }
        error
    }
}

struct CoreProbe {
    #[cfg(windows)]
    owned_jobs: BTreeMap<String, (ProcessIdentity, aos_launch::windows_process::OwnedJob)>,
    context: Option<Context>,
    watch: Option<Box<dyn GraphWatchBackend>>,
    node: Option<Node>,
    observations: BTreeMap<String, Observation>,
    forgotten: BTreeMap<String, ProcessIdentity>,
    replies: mpsc::Receiver<Reply>,
    reply_sender: mpsc::SyncSender<Reply>,
    retiring: Vec<JoinHandle<()>>,
    sequence: u64,
    closed: bool,
    diagnostics: Arc<Mutex<Vec<Value>>>,
    close_budget: Duration,
    worker_limit: usize,
}

impl CoreProbe {
    #[cfg(test)]
    fn new(context: Context) -> LaunchResult<Self> {
        Self::with_diagnostics(context, Arc::default())
    }
    fn with_diagnostics(
        context: Context,
        diagnostics: Arc<Mutex<Vec<Value>>>,
    ) -> LaunchResult<Self> {
        Self::with_identity(context, diagnostics, None)
    }

    fn with_identity(
        context: Context,
        diagnostics: Arc<Mutex<Vec<Value>>>,
        identity: Option<ResolvedNodeConfig>,
    ) -> LaunchResult<Self> {
        let watch = context
            .watch_graph()
            .map_err(core_error)?
            .ok_or_else(|| failure("this transport does not provide persistent Graph watches"))?;
        let node = match identity {
            Some(identity) => Node::new_resolved(context.clone(), identity),
            None => Node::new(
                context.clone(),
                &format!("aos_launch_{}", uuid::Uuid::new_v4().simple()),
                "/",
                NodeOptions::default(),
            ),
        }
        .map_err(core_error)?;
        let (results, replies) = mpsc::sync_channel(MAX_WORKERS);
        Ok(Self {
            context: Some(context),
            watch: Some(watch),
            node: Some(node),
            #[cfg(windows)]
            owned_jobs: BTreeMap::new(),
            observations: BTreeMap::new(),
            forgotten: BTreeMap::new(),
            replies,
            reply_sender: results,
            retiring: Vec::new(),
            sequence: 0,
            closed: false,
            diagnostics,
            close_budget: CLOSE_BUDGET,
            worker_limit: MAX_WORKERS,
        })
    }

    fn collect(&mut self) -> LaunchResult<()> {
        let mut index = 0;
        while index < self.retiring.len() {
            if self.retiring[index].is_finished() {
                let _ = self.retiring.swap_remove(index).join();
            } else {
                index += 1;
            }
        }
        loop {
            let reply = match self.replies.try_recv() {
                Ok(reply) => reply,
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    return if self.closed {
                        Ok(())
                    } else {
                        Err(failure("readiness worker disconnected"))
                    };
                }
            };
            let Some(observation) = self
                .observations
                .get_mut(&reply.identity.process_id)
                .filter(|o| o.identity == reply.identity)
            else {
                continue;
            };
            if observation.pending != Some(reply.sequence) {
                continue;
            }
            observation.pending = None;
            if observation
                .node_id
                .as_ref()
                .is_some_and(|id| id != &reply.node_id)
            {
                observation.error = Some(failure(
                    "the observed Node ID changed within one process generation",
                ));
                continue;
            }
            observation.next_read = Instant::now() + Duration::from_millis(25);
            match (reply.step, reply.result) {
                (Step::Card, Ok(card)) => {
                    #[cfg(not(windows))]
                    let matches = card_matches(&card, &reply.identity, &reply.node_id);
                    #[cfg(windows)]
                    let matches = if let Some((identity, job)) =
                        self.owned_jobs.get(&reply.identity.process_id)
                    {
                        identity == &reply.identity
                            && card_same_generation(&card, identity, &reply.node_id)
                            && card["process"]["pid"]
                                .as_u64()
                                .and_then(|p| u32::try_from(p).ok())
                                .is_some_and(|pid| job.contains(pid).unwrap_or(false))
                    } else {
                        card_matches(&card, &reply.identity, &reply.node_id)
                    };
                    if matches {
                        observation.node_id = Some(reply.node_id);
                        observation.card = Some(card);
                        observation.phase = ReadinessPhase::Discovered;
                    } else if card_same_generation(&card, &reply.identity, &reply.node_id)
                        && card["process"]["pid"].as_u64() != Some(u64::from(reply.identity.pid))
                    {
                        observation.error = Some(failure(format!(
                            "NodeCard PID {:?} differs from owned leader PID {}; \
                             aos.process.v1 entry wrappers must exec the SDK process",
                            card["process"]["pid"], reply.identity.pid
                        )));
                    } else {
                        // A stale or unrelated response is never readiness.
                        // Only this read-only introspection may be repeated.
                        observation.card = None;
                    }
                }
                (Step::State, Ok(state)) => {
                    observation.state = state["current_state"]["label"].as_str().map(str::to_owned);
                    if observation.state.is_none() {
                        observation.error =
                            Some(failure("lifecycle get_state returned no state label"));
                    }
                }
                (Step::Configure | Step::Activate, Ok(result)) => {
                    if result["success"] != true {
                        observation.error =
                            Some(failure(format!("lifecycle transition rejected: {result}")));
                    } else {
                        // Query the provider state after its report. Neither a
                        // submitted transition nor a success boolean is ready.
                        observation.state = None;
                    }
                }
                (_, Err(aos_core::Error::Timeout | aos_core::Error::NoResponse(_))) => {
                    observation.state = None;
                }
                (_, Err(error)) => {
                    observation.error = Some(core_error(error));
                }
            }
        }
    }

    fn enqueue(
        &mut self,
        process_id: &str,
        node_id: &str,
        step: Step,
        operation: Operation,
    ) -> LaunchResult<()> {
        if self.observations[process_id].pending.is_some() {
            return Ok(());
        }
        let workers = self.retiring.len()
            + self
                .observations
                .values()
                .filter(|o| o.worker.is_some())
                .count();
        let observation = &mut self.observations.get_mut(process_id).unwrap();
        if Instant::now() < observation.next_read {
            return Ok(());
        }
        if observation.jobs.is_none() {
            if workers >= self.worker_limit {
                // Canceled generations retain their slot until their thread
                // exits. Do not exceed the global worker limit during restart.
                observation.waiting_reason = Some(format!(
                    "readiness worker limit {} is occupied by active or retiring generations",
                    self.worker_limit
                ));
                return Ok(());
            }
            let (send, receive) = mpsc::sync_channel(1);
            let results = self.reply_sender.clone();
            observation.worker = Some(std::thread::spawn(move || worker(receive, results)));
            observation.jobs = Some(send);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| failure("readiness sequence overflow"))?;
        let job = Job {
            identity: observation.identity.clone(),
            sequence: self.sequence,
            node_id: node_id.to_owned(),
            step,
            deadline: observation.deadline,
            operation,
        };
        observation
            .jobs
            .as_ref()
            .ok_or_else(|| failure("readiness probe is closed"))?
            .try_send(job)
            .map_err(|_| failure("readiness worker queue is unavailable"))?;
        observation.pending = Some(self.sequence);
        observation.waiting_reason = None;
        match step {
            Step::Configure => {
                observation.configure_sent = true;
                observation.phase = ReadinessPhase::Configuring;
            }
            Step::Activate => {
                observation.activate_sent = true;
                observation.phase = ReadinessPhase::Activating;
            }
            _ => {}
        }
        Ok(())
    }

    fn inspect(
        &mut self,
        expected: &ProcessIdentity,
        requirement: &ResolvedReadiness,
        remaining: Duration,
    ) -> LaunchResult<ReadinessProgress> {
        if self.closed {
            return Err(failure("readiness probe is closed"));
        }
        self.collect()?;
        if remaining.is_zero() {
            return Err(self
                .observations
                .get(&expected.process_id)
                .map_or_else(timeout, Observation::timeout));
        }
        if self
            .observations
            .get(&expected.process_id)
            .is_some_and(|o| o.identity != *expected)
        {
            return Err(failure(
                "new process generation must forget the preceding identity",
            ));
        }
        if !self.observations.contains_key(&expected.process_id) {
            let restart_after_exit = self.forgotten.get(&expected.process_id).is_some_and(|old| {
                old.launch_id == expected.launch_id
                    && old.generation.checked_add(1) == Some(expected.generation)
            });
            self.observations.insert(
                expected.process_id.clone(),
                Observation::new(expected.clone(), remaining, restart_after_exit)?,
            );
        }
        let observation = &self.observations[&expected.process_id];
        if let Some(error) = &observation.error {
            return Err(error.clone().for_process(&expected.process_id));
        }
        if observation.deadline <= Instant::now() {
            return Err(observation.timeout());
        }
        if observation.pending.is_some() {
            return Ok(observation.progress());
        }
        let graph = self
            .watch
            .as_ref()
            .unwrap()
            .snapshot()
            .map_err(core_error)?;
        let nodes: Vec<_> = graph
            .iter()
            .filter(|e| e.kind == EntityKind::Node && crate::full_name(e) == requirement.node)
            .collect();
        if nodes.len() > 1 {
            let message = format!("multiple live Nodes use {}", requirement.node);
            if !observation.restart_after_exit {
                return Err(failure(message));
            }
            let observation = self.observations.get_mut(&expected.process_id).unwrap();
            observation.waiting_reason = Some(message);
            observation.phase = ReadinessPhase::Waiting;
            return Ok(observation.progress());
        }
        let Some(node) = nodes.first() else {
            return Ok(observation.progress());
        };
        if node.node_id.is_empty() || node.id != node.node_id {
            return Err(failure("Graph Node has an invalid owner identity"));
        }
        if observation
            .node_id
            .as_ref()
            .is_some_and(|id| id != &node.node_id)
        {
            return Err(failure(
                "Node identity changed without a process generation change",
            ));
        }
        let node_id = node.node_id.clone();
        let card_interface = aos_core::node_card_interface();
        let card_endpoint = ResolvedReadyEndpoint {
            kind: ReadyEndpointKind::Service,
            name: format!("{}/get_node_card", requirement.node),
            type_name: card_interface.type_name().into(),
            type_hash: card_interface.type_hash().into(),
        };
        let mut waiting_reason = None;
        if !graph_endpoint(
            &graph,
            &node_id,
            &card_endpoint,
            observation.restart_after_exit,
            &mut waiting_reason,
        )? {
            let observation = self.observations.get_mut(&expected.process_id).unwrap();
            observation.waiting_reason = waiting_reason;
            return Ok(observation.progress());
        }
        if observation.card.is_none() {
            let client = match &observation.card_client {
                Some(client) => client.clone(),
                None => {
                    let client = Arc::new(
                        self.node
                            .as_ref()
                            .unwrap()
                            .create_client(
                                &card_interface,
                                &card_endpoint.name,
                                EndpointOptions::default(),
                            )
                            .map_err(core_error)?,
                    );
                    self.observations
                        .get_mut(&expected.process_id)
                        .unwrap()
                        .card_client = Some(client.clone());
                    client
                }
            };
            self.enqueue(
                &expected.process_id,
                &node_id,
                Step::Card,
                Operation::Card(client),
            )?;
            return Ok(self.observations[&expected.process_id].progress());
        }
        let observation = &self.observations[&expected.process_id];
        let card = observation.card.as_ref().unwrap();
        if card["fully_qualified_name"].as_str() != Some(requirement.node.as_str()) {
            return Err(failure(
                "NodeCard FQN does not match the declared readiness Node",
            ));
        }
        let mut endpoints_ready = true;
        for endpoint in &requirement.endpoints {
            endpoints_ready &= graph_endpoint(
                &graph,
                &node_id,
                endpoint,
                observation.restart_after_exit,
                &mut waiting_reason,
            )?;
            if !card_endpoint_matches(card, endpoint)? {
                endpoints_ready = false;
            }
        }
        if !endpoints_ready {
            let client = observation.card_client.as_ref().unwrap().clone();
            if waiting_reason.is_some() {
                let observation = self.observations.get_mut(&expected.process_id).unwrap();
                observation.waiting_reason = waiting_reason;
                return Ok(observation.progress());
            }
            self.enqueue(
                &expected.process_id,
                &node_id,
                Step::Card,
                Operation::Card(client),
            )?;
            return Ok(self.observations[&expected.process_id].progress());
        }
        let Some(lifecycle) = &requirement.lifecycle else {
            let observation = self.observations.get_mut(&expected.process_id).unwrap();
            observation.phase = ReadinessPhase::Ready;
            return Ok(observation.progress());
        };
        for kind in LifecycleServiceKind::ALL {
            let interface = kind.interface();
            let endpoint = ResolvedReadyEndpoint {
                kind: ReadyEndpointKind::Service,
                name: format!("{}/{}", requirement.node, kind.suffix()),
                type_name: interface.type_name().into(),
                type_hash: interface.type_hash().into(),
            };
            if !graph_endpoint(
                &graph,
                &node_id,
                &endpoint,
                observation.restart_after_exit,
                &mut waiting_reason,
            )? {
                let observation = self.observations.get_mut(&expected.process_id).unwrap();
                observation.waiting_reason = waiting_reason;
                return Ok(observation.progress());
            }
        }
        let client = match &observation.lifecycle_client {
            Some(client) => client.clone(),
            None => {
                let client = Arc::new(
                    LifecycleClient::new(self.node.as_ref().unwrap(), &requirement.node)
                        .map_err(core_error)?,
                );
                self.observations
                    .get_mut(&expected.process_id)
                    .unwrap()
                    .lifecycle_client = Some(client.clone());
                client
            }
        };
        let observation = &self.observations[&expected.process_id];
        let target = match lifecycle.target {
            ReadyLifecycleState::Unconfigured => "unconfigured",
            ReadyLifecycleState::Inactive => "inactive",
            ReadyLifecycleState::Active => "active",
        };
        match observation.state.as_deref() {
            Some(state) if state == target => {
                let observation = self.observations.get_mut(&expected.process_id).unwrap();
                observation.phase = ReadinessPhase::Ready;
                return Ok(observation.progress());
            }
            Some("unconfigured") if lifecycle.autostart && target != "unconfigured" => {
                if observation.configure_sent {
                    return Err(failure(
                        "configure was submitted but the provider remains unconfigured",
                    ));
                }
                self.enqueue(
                    &expected.process_id,
                    &node_id,
                    Step::Configure,
                    Operation::Change(client, 1),
                )?;
            }
            Some("inactive") if lifecycle.autostart && target == "active" => {
                if observation.activate_sent {
                    return Err(failure(
                        "activate was submitted but the provider remains inactive",
                    ));
                }
                self.enqueue(
                    &expected.process_id,
                    &node_id,
                    Step::Activate,
                    Operation::Change(client, 3),
                )?;
            }
            Some("configuring" | "activating") => {
                self.observations
                    .get_mut(&expected.process_id)
                    .unwrap()
                    .state = None;
                self.enqueue(
                    &expected.process_id,
                    &node_id,
                    Step::State,
                    Operation::State(client),
                )?;
            }
            Some(state) if lifecycle.autostart => {
                return Err(failure(format!(
                    "autostart cannot reach {target} from {state}"
                )));
            }
            _ => {
                self.observations
                    .get_mut(&expected.process_id)
                    .unwrap()
                    .state = None;
                self.enqueue(
                    &expected.process_id,
                    &node_id,
                    Step::State,
                    Operation::State(client),
                )?;
            }
        }
        Ok(self.observations[&expected.process_id].progress())
    }
}

impl ReadinessProbe for CoreProbe {
    #[cfg(windows)]
    fn register_owned_job(
        &mut self,
        identity: &ProcessIdentity,
        job: aos_launch::windows_process::OwnedJob,
    ) {
        self.owned_jobs
            .insert(identity.process_id.clone(), (identity.clone(), job));
    }
    fn poll(
        &mut self,
        expected: &ProcessIdentity,
        requirement: &ResolvedReadiness,
        remaining: Duration,
    ) -> LaunchResult<ReadinessProgress> {
        self.inspect(expected, requirement, remaining)
            .map_err(|e| e.for_process(&expected.process_id))
    }
    fn forget(&mut self, expected: &ProcessIdentity) {
        #[cfg(windows)]
        if self
            .owned_jobs
            .get(&expected.process_id)
            .is_some_and(|(identity, _)| identity == expected)
        {
            self.owned_jobs.remove(&expected.process_id);
        }
        // Supervisor calls forget only after it observed this leader exit.
        self.forgotten
            .insert(expected.process_id.clone(), expected.clone());
        if self
            .observations
            .get(&expected.process_id)
            .is_some_and(|o| o.identity == *expected)
        {
            if let Some(mut observation) = self.observations.remove(&expected.process_id) {
                // Client close cancels this generation's query. Shared Context
                // stays alive for all other providers.
                observation.close();
                observation.pending = None;
                observation.jobs.take();
                if let Some(worker) = observation.worker.take() {
                    self.retiring.push(worker);
                }
            }
        }
    }
    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        let deadline = Instant::now() + self.close_budget;
        let mut observations = std::mem::take(&mut self.observations);
        let mut retiring = std::mem::take(&mut self.retiring);
        let context = self.context.take().unwrap();
        let watch = self.watch.take();
        let node = self.node.take();
        let (_, empty) = mpsc::sync_channel(1);
        let replies = std::mem::replace(&mut self.replies, empty);
        let workers_at_close =
            retiring.len() + observations.values().filter(|o| o.worker.is_some()).count();
        // The entire close, including transport shutdown and resource drops,
        // has one caller-thread deadline. A broken adapter cannot pin finish.
        let cleanup = std::thread::spawn(move || {
            for observation in observations.values_mut() {
                observation.jobs.take();
                if let Some(worker) = observation.worker.take() {
                    retiring.push(worker);
                }
                observation.close();
            }
            let _ = context.shutdown();
            observations.clear();
            while !retiring.is_empty() {
                while replies.try_recv().is_ok() {}
                let mut index = 0;
                while index < retiring.len() {
                    if retiring[index].is_finished() {
                        let _ = retiring.swap_remove(index).join();
                    } else {
                        index += 1;
                    }
                }
                if !retiring.is_empty() {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            drop((node, watch, context));
        });
        while !cleanup.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        if cleanup.is_finished() {
            let _ = cleanup.join();
        } else {
            self.diagnostics
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(json!({
                    "code":"PROBE_CLOSE_TIMEOUT",
                    "message":"readiness cleanup exceeded its caller deadline; private cleanup threads will end when this CLI exits",
                    "timeout_ms":self.close_budget.as_millis() as u64,
                    "workers_at_close":workers_at_close,
                }));
            // Dropping a JoinHandle detaches it. All owned child processes have
            // already been reaped by Supervisor before its finish calls close.
        }
    }
}

impl Drop for CoreProbe {
    fn drop(&mut self) {
        self.close();
    }
}

fn card_matches(card: &Value, expected: &ProcessIdentity, node_id: &str) -> bool {
    card_same_generation(card, expected, node_id)
        && card["process"]["pid"].as_u64() == Some(u64::from(expected.pid))
}

fn card_same_generation(card: &Value, expected: &ProcessIdentity, node_id: &str) -> bool {
    let process = &card["process"];
    card["id"].as_str() == Some(node_id)
        && process["launch_id"].as_str() == Some(expected.launch_id.as_str())
        && process["process_id"].as_str() == Some(expected.process_id.as_str())
        && process["generation"].as_u64() == Some(u64::from(expected.generation))
}

fn endpoint_kind(kind: ReadyEndpointKind) -> EntityKind {
    match kind {
        ReadyEndpointKind::Publisher => EntityKind::Publisher,
        ReadyEndpointKind::Subscription => EntityKind::Subscription,
        ReadyEndpointKind::Service => EntityKind::Service,
        ReadyEndpointKind::Action => EntityKind::ActionServer,
    }
}

fn graph_endpoint(
    graph: &[GraphEntity],
    node_id: &str,
    expected: &ResolvedReadyEndpoint,
    restart_after_exit: bool,
    waiting_reason: &mut Option<String>,
) -> LaunchResult<bool> {
    let kind = endpoint_kind(expected.kind);
    let candidates: Vec<_> = graph
        .iter()
        .filter(|e| e.kind == kind && e.endpoint.as_ref().is_some_and(|v| v.name == expected.name))
        .collect();
    if matches!(kind, EntityKind::Service | EntityKind::ActionServer)
        && candidates.iter().any(|e| e.node_id != node_id)
    {
        let message = format!("{} has a provider outside the owned Node", expected.name);
        if restart_after_exit {
            *waiting_reason = Some(message);
            return Ok(false);
        }
        return Err(failure(message));
    }
    let owned: Vec<_> = candidates.iter().filter(|e| e.node_id == node_id).collect();
    if matches!(kind, EntityKind::Service | EntityKind::ActionServer) && owned.len() > 1 {
        let message = format!("{} has multiple providers", expected.name);
        if restart_after_exit {
            *waiting_reason = Some(message);
            return Ok(false);
        }
        return Err(failure(message));
    }
    for entity in &owned {
        let endpoint = entity.endpoint.as_ref().unwrap();
        if endpoint.type_name != expected.type_name || endpoint.type_hash != expected.type_hash {
            return Err(failure(format!(
                "{} has a different interface type/hash",
                expected.name
            )));
        }
    }
    Ok(!owned.is_empty())
}

fn card_endpoint_matches(card: &Value, expected: &ResolvedReadyEndpoint) -> LaunchResult<bool> {
    let kind = json!(endpoint_kind(expected.kind));
    let Some(endpoints) = card["endpoints"].as_array() else {
        return Err(failure("NodeCard has no endpoint list"));
    };
    let matches: Vec<_> = endpoints
        .iter()
        .filter(|e| e["name"].as_str() == Some(expected.name.as_str()) && e["kind"] == kind)
        .collect();
    for endpoint in &matches {
        if endpoint["type_name"].as_str() != Some(expected.type_name.as_str())
            || endpoint["type_hash"].as_str() != Some(expected.type_hash.as_str())
        {
            return Err(failure(format!(
                "NodeCard endpoint {} has a different interface",
                expected.name
            )));
        }
    }
    Ok(!matches.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aos_core::transport::{
        ClientBackend, Endpoint, GraphGuard, PublisherBackend, QoS, RequestBackend, ServiceBackend,
        SubscriptionBackend, Transport,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };

    struct SecurityPreflightFixture {
        directory: std::path::PathBuf,
        index: aos_launch::PackageIndex,
        supervisor: std::path::PathBuf,
        child: std::path::PathBuf,
    }

    impl SecurityPreflightFixture {
        fn new() -> Self {
            let directory =
                std::env::temp_dir().join(format!("aos-secure-preflight-{}", uuid::Uuid::new_v4()));
            let prefix = directory.join("installed");
            let index_dir = prefix.join("share/aos/resource_index/packages");
            std::fs::create_dir_all(&index_dir).unwrap();
            std::fs::create_dir_all(prefix.join("share/demo")).unwrap();
            std::fs::create_dir_all(prefix.join("lib/demo")).unwrap();
            let executable = prefix.join("lib/demo/worker");
            #[cfg(windows)]
            std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
            #[cfg(not(windows))]
            std::fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))
                    .unwrap();
            }
            std::fs::write(
                index_dir.join("demo.json"),
                json!({
                    "schema_version":1, "name":"demo", "version":"1.0.0",
                    "runtime_abi":"aos.process.v1", "resources":{},
                    "entries":{"worker":{"executable":"lib/demo/worker", "kind":"aos_node"}}
                })
                .to_string(),
            )
            .unwrap();
            let index = aos_launch::PackageIndex::discover(&[prefix]).unwrap();
            let mut fixture = Self {
                directory,
                index,
                supervisor: Default::default(),
                child: Default::default(),
            };
            fixture.supervisor = fixture.profile("supervisor", 29, "tls/localhost:7447");
            fixture.child = fixture.profile("child", 29, "tls/localhost:7447");
            fixture
        }

        fn profile(&self, name: &str, domain: u32, router: &str) -> std::path::PathBuf {
            let path = self.directory.join(format!("{name}.json"));
            // The absent PEM files also prove that this preflight does not
            // open a native session or implement a second credential parser.
            std::fs::write(
                &path,
                json!({
                    "schema_version":1, "domain_id":domain, "router_endpoints":[router],
                    "root_ca_certificate":self.directory.join("ca.pem"),
                    "certificate":self.directory.join(format!("{name}-cert.pem")),
                    "private_key":self.directory.join(format!("{name}-key.pem"))
                })
                .to_string(),
            )
            .unwrap();
            path
        }

        fn factory(&self, readiness_identity: bool) -> CoreProbeFactory {
            let mut request = json!({"security_profile":self.supervisor});
            if readiness_identity {
                request["client_node"] = json!({"name":"supervisor", "namespace":"/"});
            }
            CoreProbeFactory::from_request(&request).unwrap()
        }

        fn process(&self, id: &str) -> Value {
            json!({
                "id":id, "package":"demo", "entry":"worker",
                "env":{
                    "AOS_SECURITY_PROFILE":self.child,
                    "AOS_DOMAIN_ID":"", "AOS_ZENOH_CONFIG":"",
                    "AOS_NODE_NAME":"", "AOS_NAMESPACE":"", "AOS_USE_SIM_TIME":""
                }
            })
        }

        fn plan(
            &self,
            processes: Vec<Value>,
            context: Value,
            cli_context: Value,
        ) -> aos_launch::LaunchPlan {
            let spec = aos_launch::parse_launch(
                &json!({"schema_version":1, "context":context, "processes":processes}).to_string(),
            )
            .unwrap();
            aos_launch::prepare(
                &self.index,
                spec,
                aos_launch::LaunchOverrides {
                    context: cli_context,
                    ..Default::default()
                },
            )
            .unwrap()
        }

        fn native_config(&self) -> std::path::PathBuf {
            let profile = aos_zenoh::security::load_client_profile(Some(&self.child))
                .unwrap()
                .unwrap();
            let config = aos_zenoh::security::client_config(&profile).unwrap();
            let path = self.directory.join("secure-native.json");
            std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
            path
        }
    }

    impl Drop for SecurityPreflightFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    #[test]
    fn secure_preflight_requires_supervisor_identity_only_for_readiness() {
        let fixture = SecurityPreflightFixture::new();
        let factory = fixture.factory(false);
        let plan = fixture.plan(vec![fixture.process("worker")], Value::Null, json!({}));
        assert!(plan.context().is_none());
        factory.validate_children(&plan).unwrap();

        let mut process = fixture.process("worker");
        process["readiness"] = json!({"node":"/worker"});
        let plan = fixture.plan(
            vec![process],
            json!({"domain_id":29, "zenoh_config":null}),
            json!({}),
        );
        assert!(plan.context().is_some());
        let error = factory.validate_children(&plan).unwrap_err();
        assert!(error.message.contains("client_node"));
        fixture.factory(true).validate_children(&plan).unwrap();
    }

    #[test]
    fn secure_preflight_requires_each_child_profile_even_without_readiness() {
        let fixture = SecurityPreflightFixture::new();
        for profile in [None, Some(json!("")), Some(json!("relative-profile.json"))] {
            let mut process = fixture.process("second");
            match profile {
                Some(path) => process["env"]["AOS_SECURITY_PROFILE"] = path,
                None => {
                    process["env"]
                        .as_object_mut()
                        .unwrap()
                        .remove("AOS_SECURITY_PROFILE");
                }
            }
            let plan = fixture.plan(
                vec![fixture.process("first"), process],
                Value::Null,
                json!({}),
            );
            assert_eq!(
                fixture
                    .factory(false)
                    .validate_children(&plan)
                    .unwrap_err()
                    .exit,
                2
            );
        }
    }

    #[test]
    fn secure_preflight_requires_same_domain_and_controlled_router() {
        let fixture = SecurityPreflightFixture::new();
        for (name, domain, router, expected) in [
            ("other-domain", 30, "tls/localhost:7447", "domain"),
            ("other-router", 29, "tls/localhost:7448", "Router"),
        ] {
            let mut process = fixture.process("second");
            process["env"]["AOS_SECURITY_PROFILE"] = json!(fixture.profile(name, domain, router));
            let plan = fixture.plan(
                vec![fixture.process("first"), process],
                Value::Null,
                json!({}),
            );
            let error = fixture.factory(false).validate_children(&plan).unwrap_err();
            assert!(error.message.contains(expected));
            assert!(error.message.contains("second"));
        }
    }

    #[test]
    fn secure_preflight_resolves_child_environment_launch_and_cli_precedence() {
        let fixture = SecurityPreflightFixture::new();
        let factory = fixture.factory(false);
        let inherited = BTreeMap::from([("AOS_DOMAIN_ID".into(), "30".into())]);
        let plan = fixture.plan(vec![fixture.process("worker")], Value::Null, json!({}));
        factory
            .validate_children_with_environment(&plan, &inherited)
            .unwrap();

        let mut process = fixture.process("worker");
        process["env"]["AOS_DOMAIN_ID"] = json!("30");
        let plan = fixture.plan(vec![process.clone()], Value::Null, json!({}));
        assert!(factory
            .validate_children_with_environment(&plan, &BTreeMap::new())
            .is_err());

        let plan = fixture.plan(
            vec![process.clone()],
            json!({"domain_id":29, "zenoh_config":null}),
            json!({}),
        );
        factory
            .validate_children_with_environment(&plan, &inherited)
            .unwrap();
        let plan = fixture.plan(
            vec![process],
            json!({"domain_id":29, "zenoh_config":null}),
            json!({"domain_id":30}),
        );
        assert!(factory
            .validate_children_with_environment(&plan, &inherited)
            .is_err());
    }

    #[test]
    fn secure_preflight_checks_inherited_domain_when_child_has_no_override() {
        let fixture = SecurityPreflightFixture::new();
        let mut process = fixture.process("worker");
        process["env"]
            .as_object_mut()
            .unwrap()
            .remove("AOS_DOMAIN_ID");
        let plan = fixture.plan(vec![process], Value::Null, json!({}));
        let inherited = BTreeMap::from([("AOS_DOMAIN_ID".into(), "30".into())]);
        let error = fixture
            .factory(false)
            .validate_children_with_environment(&plan, &inherited)
            .unwrap_err();
        assert!(error.message.contains("resolved domain"));
    }

    #[test]
    fn secure_preflight_checks_final_native_configuration_after_all_known_layers() {
        let fixture = SecurityPreflightFixture::new();
        let factory = fixture.factory(false);
        let secure = fixture.native_config();
        let plain = fixture.directory.join("plain.json");
        std::fs::write(&plain, aos_zenoh::DEFAULT_SESSION_CONFIG).unwrap();
        let mut process = fixture.process("worker");
        process["env"]["AOS_ZENOH_CONFIG"] = json!(plain);
        let plan = fixture.plan(vec![process.clone()], Value::Null, json!({}));
        assert!(factory
            .validate_children(&plan)
            .unwrap_err()
            .message
            .contains("final Zenoh"));

        let context = json!({"domain_id":29, "zenoh_config":secure});
        let plan = fixture.plan(vec![process.clone()], context.clone(), json!({}));
        factory.validate_children(&plan).unwrap();
        let plan = fixture.plan(vec![process], context, json!({"zenoh_config":plain}));
        assert!(factory
            .validate_children(&plan)
            .unwrap_err()
            .message
            .contains("final Zenoh"));
    }

    #[test]
    fn secure_preflight_native_parse_errors_do_not_echo_secret_input() {
        let fixture = SecurityPreflightFixture::new();
        let path = fixture.directory.join("invalid-native.json");
        let marker = "DO_NOT_ECHO_PRIVATE_KEY_MATERIAL";
        std::fs::write(&path, format!("{{\"mode\":\"{marker}\"}}")).unwrap();
        let mut process = fixture.process("worker");
        process["env"]["AOS_ZENOH_CONFIG"] = json!(path);
        let plan = fixture.plan(vec![process], Value::Null, json!({}));
        let error = fixture.factory(false).validate_children(&plan).unwrap_err();
        assert!(error.message.contains("secure native configuration"));
        assert!(!error.message.contains(marker));
    }

    #[test]
    fn card_identity_requires_every_generation_field() {
        let identity = ProcessIdentity {
            launch_id: "launch".into(),
            process_id: "worker".into(),
            generation: 2,
            pid: 81,
        };
        let card = json!({"id":"node","process":identity});
        assert!(card_matches(&card, &identity, "node"));
        for field in ["launch_id", "process_id", "generation", "pid"] {
            let mut missing = card.clone();
            missing["process"].as_object_mut().unwrap().remove(field);
            assert!(!card_matches(&missing, &identity, "node"), "{field}");
        }
        let mut old = identity.clone();
        old.generation = 1;
        assert!(!card_matches(&card, &old, "node"));
        assert!(!card_matches(&card, &identity, "old-node"));
        assert!(!card_matches(&json!({"id":"node"}), &identity, "node"));
    }

    #[test]
    fn endpoint_hash_and_node_owner_are_both_required() {
        let endpoint = ResolvedReadyEndpoint {
            kind: ReadyEndpointKind::Service,
            name: "/node/work".into(),
            type_name: "example/srv/Work".into(),
            type_hash: "hash".into(),
        };
        let mut graph = vec![GraphEntity {
            id: "entity".into(),
            node_id: "node".into(),
            node_name: "node".into(),
            namespace: "/".into(),
            kind: EntityKind::Service,
            qos: None,
            endpoint: Some(aos_core::Endpoint {
                name: endpoint.name.clone(),
                type_name: endpoint.type_name.clone(),
                type_hash: endpoint.type_hash.clone(),
            }),
        }];
        assert!(graph_endpoint(&graph, "node", &endpoint, false, &mut None).unwrap());
        graph[0].endpoint.as_mut().unwrap().type_hash = "wrong".into();
        assert!(graph_endpoint(&graph, "node", &endpoint, false, &mut None).is_err());
        graph[0].node_id = "old".into();
        assert!(graph_endpoint(&graph, "node", &endpoint, false, &mut None).is_err());
        assert!(!graph_endpoint(&[], "node", &endpoint, false, &mut None).unwrap());
    }

    struct FixtureState {
        graph: Mutex<Vec<GraphEntity>>,
        card: Mutex<Value>,
        state: Mutex<String>,
        calls: Mutex<Vec<String>>,
        blocked: AtomicBool,
        entered: AtomicBool,
        closed: AtomicBool,
        reject: AtomicBool,
        uncertain_configure: AtomicBool,
        blocked_change: AtomicBool,
        blocked_close: AtomicBool,
        alternate_card: Mutex<Option<Value>>,
    }

    struct FixtureTransport(Arc<FixtureState>);
    struct Guard;
    impl GraphGuard for Guard {
        fn close(&self) -> aos_core::Result<()> {
            Ok(())
        }
    }
    struct Publisher;
    impl PublisherBackend for Publisher {
        fn publish(&self, _: &[u8]) -> aos_core::Result<()> {
            Ok(())
        }
        fn close(&self) -> aos_core::Result<()> {
            Ok(())
        }
    }
    struct Service(AtomicBool);
    impl ServiceBackend for Service {
        fn recv_timeout(&self, timeout: Duration) -> aos_core::Result<Box<dyn RequestBackend>> {
            if self.0.load(Ordering::Acquire) {
                return Err(aos_core::Error::Closed);
            }
            std::thread::sleep(timeout.min(Duration::from_millis(1)));
            Err(aos_core::Error::Timeout)
        }
        fn close(&self) -> aos_core::Result<()> {
            self.0.store(true, Ordering::Release);
            Ok(())
        }
    }
    struct Watch(Arc<FixtureState>);
    impl GraphWatchBackend for Watch {
        fn snapshot(&self) -> aos_core::Result<Vec<GraphEntity>> {
            Ok(self.0.graph.lock().unwrap().clone())
        }
        fn wait_for_change(&self, _: Duration) -> aos_core::Result<()> {
            Err(aos_core::Error::Timeout)
        }
    }
    struct FixtureClient {
        owner: Arc<FixtureState>,
        endpoint: Endpoint,
        closed: AtomicBool,
    }
    impl ClientBackend for FixtureClient {
        fn call_wait(
            &self,
            payload: &[u8],
            timeout: Option<Duration>,
            cancel: &AtomicBool,
        ) -> aos_core::Result<Vec<u8>> {
            let deadline = Instant::now() + timeout.unwrap();
            self.owner.entered.store(true, Ordering::Release);
            self.owner
                .calls
                .lock()
                .unwrap()
                .push(self.endpoint.name.clone());
            while self.owner.blocked.load(Ordering::Acquire)
                || (self.owner.blocked_change.load(Ordering::Acquire)
                    && self.endpoint.name == "/provider/change_state")
            {
                if self.closed.load(Ordering::Acquire)
                    || cancel.load(Ordering::Acquire)
                    || self.owner.closed.load(Ordering::Acquire)
                {
                    return Err(aos_core::Error::Closed);
                }
                if Instant::now() >= deadline {
                    return Err(aos_core::Error::Timeout);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let value = if self.endpoint.name.ends_with("/get_node_card") {
                if self.endpoint.name.starts_with("/other/") {
                    self.owner.alternate_card.lock().unwrap().clone().unwrap()
                } else {
                    self.owner.card.lock().unwrap().clone()
                }
            } else if self.endpoint.name.ends_with("/get_state") {
                let label = self.owner.state.lock().unwrap().clone();
                let id = match label.as_str() {
                    "unconfigured" => 1,
                    "inactive" => 2,
                    "active" => 3,
                    _ => 0,
                };
                let state = json!({"id":id,"label":label});
                json!({"current_state":state,"last_report":null,"snapshot":{
                    "current_state":state,"transitioning":false,"gate_open":id==3,
                    "epoch":0,"in_flight":[]}})
            } else if self.endpoint.name.ends_with("/change_state") {
                let request: Value = serde_json::from_slice(payload).unwrap();
                if self.owner.reject.load(Ordering::Acquire) {
                    json!({"success":false,"report":null,"error":{"code":"REJECTED","message":"fixture"}})
                } else {
                    let configure = request["transition"]["id"] == 1;
                    *self.owner.state.lock().unwrap() =
                        if configure { "inactive" } else { "active" }.into();
                    if configure && self.owner.uncertain_configure.swap(false, Ordering::AcqRel) {
                        return Err(aos_core::Error::Timeout);
                    }
                    json!({"success":true,"report":null,"error":null})
                }
            } else {
                return Err(aos_core::Error::Unsupported("fixture endpoint".into()));
            };
            Ok(serde_json::to_vec(&value).unwrap())
        }
        fn close(&self) -> aos_core::Result<()> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }
    impl Transport for FixtureTransport {
        fn create_publisher(&self, _: &str) -> aos_core::Result<Box<dyn PublisherBackend>> {
            Ok(Box::new(Publisher))
        }
        fn create_subscription(&self, _: &str) -> aos_core::Result<Box<dyn SubscriptionBackend>> {
            Err(aos_core::Error::Unsupported("fixture subscription".into()))
        }
        fn create_typed_publisher(
            &self,
            _: &Endpoint,
            _: &QoS,
        ) -> aos_core::Result<Box<dyn PublisherBackend>> {
            Ok(Box::new(Publisher))
        }
        fn create_service(&self, _: &Endpoint) -> aos_core::Result<Box<dyn ServiceBackend>> {
            Ok(Box::new(Service(AtomicBool::new(false))))
        }
        fn create_client_with_qos(
            &self,
            endpoint: &Endpoint,
            _: &QoS,
        ) -> aos_core::Result<Option<Box<dyn ClientBackend>>> {
            Ok(Some(Box::new(FixtureClient {
                owner: self.0.clone(),
                endpoint: endpoint.clone(),
                closed: AtomicBool::new(false),
            })))
        }
        fn declare_graph_entity(&self, _: &GraphEntity) -> aos_core::Result<Box<dyn GraphGuard>> {
            Ok(Box::new(Guard))
        }
        fn watch_graph(&self) -> aos_core::Result<Option<Box<dyn GraphWatchBackend>>> {
            Ok(Some(Box::new(Watch(self.0.clone()))))
        }
        fn close(&self) -> aos_core::Result<()> {
            while self.0.blocked_close.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.0.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn fixture() -> (
        CoreProbe,
        Arc<FixtureState>,
        ProcessIdentity,
        ResolvedReadiness,
    ) {
        let identity = ProcessIdentity {
            launch_id: "owned".into(),
            process_id: "provider".into(),
            generation: 1,
            pid: 42,
        };
        let node = GraphEntity {
            id: "node".into(),
            node_id: "node".into(),
            node_name: "provider".into(),
            namespace: "/".into(),
            kind: EntityKind::Node,
            endpoint: None,
            qos: None,
        };
        let mut graph = vec![node.clone()];
        let mut interfaces = vec![("get_node_card", aos_core::node_card_interface())];
        interfaces.extend(
            LifecycleServiceKind::ALL
                .into_iter()
                .map(|k| (k.suffix(), k.interface())),
        );
        for (suffix, interface) in interfaces {
            graph.push(GraphEntity {
                id: suffix.into(),
                kind: EntityKind::Service,
                endpoint: Some(Endpoint {
                    name: format!("/provider/{suffix}"),
                    type_name: interface.type_name().into(),
                    type_hash: interface.type_hash().into(),
                }),
                ..node.clone()
            });
        }
        let state = Arc::new(FixtureState {
            graph: Mutex::new(graph),
            card: Mutex::new(
                json!({"id":"node","fully_qualified_name":"/provider","process":identity,"endpoints":[]}),
            ),
            state: Mutex::new("unconfigured".into()),
            calls: Mutex::new(Vec::new()),
            blocked: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reject: AtomicBool::new(false),
            uncertain_configure: AtomicBool::new(false),
            blocked_change: AtomicBool::new(false),
            blocked_close: AtomicBool::new(false),
            alternate_card: Mutex::new(None),
        });
        let context = Context::new(FixtureTransport(state.clone()));
        let probe = CoreProbe::new(context).unwrap();
        let requirement = ResolvedReadiness {
            node: "/provider".into(),
            timeout: Duration::from_secs(3),
            endpoints: Vec::new(),
            lifecycle: Some(aos_launch::LifecycleReadiness {
                target: ReadyLifecycleState::Active,
                autostart: true,
            }),
        };
        (probe, state, identity, requirement)
    }

    fn drive(
        probe: &mut CoreProbe,
        identity: &ProcessIdentity,
        requirement: &ResolvedReadiness,
    ) -> LaunchResult<ReadinessProgress> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let began = Instant::now();
            let progress = probe.poll(
                identity,
                requirement,
                deadline.saturating_duration_since(began),
            )?;
            assert!(
                began.elapsed() < Duration::from_millis(100),
                "poll blocked on a network operation"
            );
            if progress.phase == ReadinessPhase::Ready {
                return Ok(progress);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn autostart_uses_provider_state_and_never_resubmits_an_uncertain_transition() {
        let (mut probe, state, identity, requirement) = fixture();
        state.uncertain_configure.store(true, Ordering::Release);
        assert_eq!(
            drive(&mut probe, &identity, &requirement).unwrap().phase,
            ReadinessPhase::Ready
        );
        let calls = state.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|s| s.ends_with("/change_state"))
                .count(),
            2
        );
        assert!(calls.iter().filter(|s| s.ends_with("/get_state")).count() >= 3);
        drop(calls);
        probe.close();
        assert!(state.closed.load(Ordering::Acquire));
    }

    #[test]
    fn rejected_lifecycle_transition_never_becomes_ready() {
        let (mut probe, state, identity, requirement) = fixture();
        state.reject.store(true, Ordering::Release);
        assert_eq!(
            drive(&mut probe, &identity, &requirement).unwrap_err().code,
            LaunchErrorCode::ReadinessFailed
        );
        assert_eq!(
            state
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|s| s.ends_with("/change_state"))
                .count(),
            1
        );
    }

    #[test]
    fn forget_cancels_old_card_wait_and_old_result_cannot_satisfy_new_generation() {
        let (mut probe, state, mut identity, mut requirement) = fixture();
        requirement.lifecycle = None;
        state.blocked.store(true, Ordering::Release);
        probe
            .poll(&identity, &requirement, Duration::from_secs(30))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !state.entered.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(state.entered.load(Ordering::Acquire));
        let started = Instant::now();
        probe.forget(&identity);
        assert!(
            !probe.context.as_ref().unwrap().is_shutdown(),
            "forget must not close other instances' shared Context"
        );
        identity.generation = 2;
        identity.pid = 43;
        state.card.lock().unwrap()["process"] = json!(identity);
        state.blocked.store(false, Ordering::Release);
        assert_eq!(
            drive(&mut probe, &identity, &requirement)
                .unwrap()
                .node_id
                .as_deref(),
            Some("node")
        );
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "old RPC kept the worker occupied"
        );
    }

    #[test]
    fn close_wakes_and_joins_a_blocked_control_worker() {
        let (mut probe, state, identity, requirement) = fixture();
        state.blocked.store(true, Ordering::Release);
        probe
            .poll(&identity, &requirement, Duration::from_secs(30))
            .unwrap();
        let started = Instant::now();
        probe.close();
        assert!(started.elapsed() < Duration::from_millis(300));
        assert!(probe.retiring.is_empty());
        assert!(probe.observations.is_empty());
        assert!(state.closed.load(Ordering::Acquire));
    }

    #[test]
    fn slow_configure_cannot_consume_an_independent_providers_deadline() {
        let (mut probe, state, identity, requirement) = fixture();
        let other = ProcessIdentity {
            process_id: "other".into(),
            pid: 43,
            ..identity.clone()
        };
        let mut other_requirement = requirement.clone();
        other_requirement.node = "/other".into();
        *state.alternate_card.lock().unwrap() = Some(json!({
            "id":"other-node","fully_qualified_name":"/other","process":other,"endpoints":[]
        }));
        let additional: Vec<_> = state
            .graph
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut entity| {
                entity.id = format!("other-{}", entity.id);
                entity.node_id = "other-node".into();
                entity.node_name = "other".into();
                if let Some(endpoint) = &mut entity.endpoint {
                    endpoint.name = endpoint.name.replace("/provider/", "/other/");
                }
                entity
            })
            .collect();
        state.graph.lock().unwrap().extend(additional);
        state.blocked_change.store(true, Ordering::Release);
        let setup_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            probe
                .poll(&identity, &requirement, Duration::from_secs(30))
                .unwrap();
            if state
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "/provider/change_state")
            {
                break;
            }
            assert!(Instant::now() < setup_deadline);
            std::thread::sleep(Duration::from_millis(2));
        }
        let started = Instant::now();
        assert_eq!(
            drive(&mut probe, &other, &other_requirement).unwrap().phase,
            ReadinessPhase::Ready
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(probe.observations[&identity.process_id].pending.is_some());
        probe.forget(&identity);
        probe.close();
        assert!(probe.retiring.is_empty());
    }

    #[test]
    fn matching_generation_with_child_pid_reports_exec_requirement() {
        let (mut probe, state, identity, requirement) = fixture();
        state.card.lock().unwrap()["process"]["pid"] = json!(identity.pid + 1);
        let error = drive(&mut probe, &identity, &requirement).unwrap_err();
        assert_eq!(error.code, LaunchErrorCode::ReadinessFailed);
        assert!(error.message.contains("must exec"), "{error}");
    }

    #[test]
    fn close_deadline_includes_transport_shutdown_and_reports_detached_cleanup() {
        let (mut probe, state, _, _) = fixture();
        state.blocked_close.store(true, Ordering::Release);
        probe.close_budget = Duration::from_millis(40);
        let started = Instant::now();
        probe.close();
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(!state.closed.load(Ordering::Acquire));
        let diagnostics = probe.diagnostics.lock().unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0]["code"], "PROBE_CLOSE_TIMEOUT");
        assert_eq!(diagnostics[0]["timeout_ms"], 40);
        drop(diagnostics);
        state.blocked_close.store(false, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !state.closed.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(state.closed.load(Ordering::Acquire));
    }

    #[test]
    fn restart_waits_for_old_node_and_service_graph_entries_to_leave() {
        let (mut probe, state, mut identity, mut requirement) = fixture();
        requirement.lifecycle = None;
        drive(&mut probe, &identity, &requirement).unwrap();
        probe.forget(&identity);
        identity.generation += 1;
        identity.pid += 1;
        let old = state.graph.lock().unwrap().clone();
        let new: Vec<_> = old
            .iter()
            .cloned()
            .map(|mut entity| {
                entity.id = format!("new-{}", entity.id);
                entity.node_id = "new-node".into();
                entity
            })
            .collect();
        state.graph.lock().unwrap().extend(new);
        *state.card.lock().unwrap() = json!({
            "id":"new-node","fully_qualified_name":"/provider",
            "process":identity,"endpoints":[]
        });
        assert_eq!(
            probe
                .poll(&identity, &requirement, Duration::from_secs(1))
                .unwrap()
                .phase,
            ReadinessPhase::Waiting
        );
        state
            .graph
            .lock()
            .unwrap()
            .retain(|entity| entity.id != "node");
        assert_ne!(
            probe
                .poll(&identity, &requirement, Duration::from_secs(1))
                .unwrap()
                .phase,
            ReadinessPhase::Ready
        );
        let reason = &probe.observations[&identity.process_id].waiting_reason;
        assert!(reason.as_ref().unwrap().contains("provider outside"));
        state
            .graph
            .lock()
            .unwrap()
            .retain(|entity| entity.node_id == "new-node");
        assert_eq!(
            drive(&mut probe, &identity, &requirement).unwrap().phase,
            ReadinessPhase::Ready
        );
    }

    #[test]
    fn first_generation_conflicts_fail_but_restart_timeout_names_the_conflict() {
        let (mut probe, state, mut identity, requirement) = fixture();
        let mut other = state.graph.lock().unwrap()[0].clone();
        other.id = "other-node".into();
        other.node_id = "other-node".into();
        state.graph.lock().unwrap().push(other);
        let error = probe
            .poll(&identity, &requirement, Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.code, LaunchErrorCode::ReadinessFailed);
        probe.forget(&identity);
        identity.generation += 1;
        probe
            .poll(&identity, &requirement, Duration::from_millis(20))
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));
        let error = probe
            .poll(&identity, &requirement, Duration::ZERO)
            .unwrap_err();
        assert_eq!(error.code, LaunchErrorCode::ReadinessTimeout);
        assert!(error.message.contains("multiple live Nodes"));
    }

    #[test]
    fn retiring_worker_capacity_is_named_in_readiness_timeout() {
        let (mut probe, _, identity, requirement) = fixture();
        probe.worker_limit = 1;
        let (release, wait) = mpsc::channel();
        probe.retiring.push(std::thread::spawn(move || {
            let _ = wait.recv();
        }));
        probe
            .poll(&identity, &requirement, Duration::from_millis(20))
            .unwrap();
        let error = probe
            .poll(&identity, &requirement, Duration::ZERO)
            .unwrap_err();
        assert!(error.message.contains("worker limit 1"), "{error}");
        release.send(()).unwrap();
        probe.close();
        assert!(probe.diagnostics.lock().unwrap().is_empty());
    }

    fn real_router(endpoint: &str) -> zenoh::Session {
        use zenoh::Wait;
        let config = zenoh::Config::from_json5(&format!(
            r#"{{
            mode:"router", listen:{{endpoints:["{endpoint}"]}},
            scouting:{{multicast:{{enabled:false}},gossip:{{enabled:false}}}}
        }}"#
        ))
        .unwrap();
        zenoh::open(config).wait().unwrap()
    }

    fn real_client_config(router: &zenoh::Session) -> zenoh::Config {
        use zenoh::Wait;
        let endpoint = router.info().locators().wait()[0].to_string();
        zenoh::Config::from_json5(&format!(
            r#"{{
            mode:"client", connect:{{endpoints:["{endpoint}"]}}, listen:{{endpoints:[]}},
            scouting:{{multicast:{{enabled:false}},gossip:{{enabled:false}}}}
        }}"#
        ))
        .unwrap()
    }

    #[test]
    fn real_lifecycle_request_with_no_reply_is_canceled_within_close_budget() {
        let _router = real_router("tcp/127.0.0.1:0");
        let config = real_client_config(&_router);
        let provider_context = Context::new(ZenohTransport::open(config.clone(), 74015).unwrap());
        let provider = Node::new(
            provider_context.clone(),
            "silent",
            "/",
            NodeOptions::default(),
        )
        .unwrap();
        let service = provider
            .create_service(
                &LifecycleServiceKind::ChangeState.interface(),
                "~/change_state",
                EndpointOptions::default(),
            )
            .unwrap();
        let mut probe =
            CoreProbe::new(Context::new(ZenohTransport::open(config, 74015).unwrap())).unwrap();
        let identity = ProcessIdentity {
            launch_id: "owned".into(),
            process_id: "silent".into(),
            generation: 1,
            pid: 42,
        };
        probe.observations.insert(
            identity.process_id.clone(),
            Observation::new(identity.clone(), Duration::from_secs(86400), false).unwrap(),
        );
        let client =
            Arc::new(LifecycleClient::new(probe.node.as_ref().unwrap(), "/silent").unwrap());
        probe
            .observations
            .get_mut(&identity.process_id)
            .unwrap()
            .lifecycle_client = Some(client.clone());
        // Wait for the real adapter's service discovery, without replacing it
        // with the fixture ClientBackend used by the other probe tests.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !probe
            .watch
            .as_ref()
            .unwrap()
            .snapshot()
            .unwrap()
            .iter()
            .any(|entity| {
                entity.kind == EntityKind::Service
                    && entity
                        .endpoint
                        .as_ref()
                        .is_some_and(|endpoint| endpoint.name == "/silent/change_state")
            })
        {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        probe
            .enqueue(
                &identity.process_id,
                "silent-node",
                Step::Configure,
                Operation::Change(client, 1),
            )
            .unwrap();
        let _unanswered_request = service.recv_timeout(Duration::from_secs(5)).unwrap();
        let started = Instant::now();
        probe.close();
        assert!(started.elapsed() < CLOSE_BUDGET);
        assert!(probe.diagnostics.lock().unwrap().is_empty());
        assert!(probe.retiring.is_empty());
        assert!(probe.observations.is_empty());
        provider_context.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires exclusive tcp/127.0.0.1:7447; run explicitly in native acceptance"]
    fn real_default_profile_reaches_owned_node_readiness() {
        use zenoh::Wait;
        drop(
            std::net::TcpListener::bind(("127.0.0.1", 7447)).expect(
                "default-profile readiness requires an isolated host with port 7447 unused",
            ),
        );
        #[cfg(windows)]
        let router_profile = include_str!("../../../config/zenoh-router-windows.json5");
        #[cfg(not(windows))]
        let router_profile = include_str!("../../../config/zenoh-router.json5");
        let _router = zenoh::open(zenoh::Config::from_json5(router_profile).unwrap())
            .wait()
            .unwrap();
        let launch_id = uuid::Uuid::new_v4().to_string();
        let bootstrap = aos_core::ProcessBootstrap::from_json(
            &json!({
                "schema_version":1,"launch_id":launch_id,"process_id":"default-node","generation":1,
                "context":{},"node":{}
            })
            .to_string(),
        )
        .unwrap();
        let configuration = aos_core::resolve_context_config(&aos_core::ContextConfigRequest {
            program: json!({"domain_id":74016,"zenoh_config":null}),
            ..Default::default()
        })
        .unwrap();
        let context = Context::from_configured_transport(
            ZenohTransport::open(
                zenoh::Config::from_json5(aos_zenoh::DEFAULT_SESSION_CONFIG).unwrap(),
                74016,
            )
            .unwrap(),
            configuration,
            Some(bootstrap),
        )
        .unwrap();
        let _provider = Node::new(
            context.clone(),
            "default_probe",
            "/",
            NodeOptions::default(),
        )
        .unwrap();
        let identity = ProcessIdentity {
            launch_id,
            process_id: "default-node".into(),
            generation: 1,
            pid: std::process::id(),
        };
        let requirement = ResolvedReadiness {
            node: "/default_probe".into(),
            timeout: Duration::from_secs(3),
            endpoints: vec![],
            lifecycle: None,
        };
        let factory = CoreProbeFactory::default();
        let mut probe = factory
            .create_core(&ResolvedLaunchContext {
                domain_id: 74016,
                zenoh_config: None,
                effective_config: json!({}),
            })
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let progress = probe
                .poll(
                    &identity,
                    &requirement,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "{error}; graph={:?}; card={:?}",
                        probe.watch.as_ref().unwrap().snapshot().unwrap(),
                        probe
                            .observations
                            .get(&identity.process_id)
                            .and_then(|o| o.card.as_ref())
                    )
                });
            if progress.phase == ReadinessPhase::Ready {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        probe.close();
        assert!(factory.take_diagnostics().is_empty());
        context.shutdown().unwrap();
    }
}
