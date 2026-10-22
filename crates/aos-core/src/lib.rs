//! AOS common runtime. Communication abstractions belong here; adapters depend
//! on this crate, never the other way around.
#![doc = include_str!("../README.md")]

pub mod action;
mod atomic_file;
mod clock;
mod config;
mod context;
mod error;
mod interface;
mod interface_limits;
mod lifecycle;
mod lifecycle_service;
pub mod names;
mod node;
mod parameter;
mod parameter_service;
mod process;
mod qos;
mod sample;
mod telemetry;
pub mod transport;

pub use action::{
    ActionClient, ActionInterfaces, ActionResourceLimits, ActionServer, ActionServerOptions,
    CancelRequest, ClientGoalHandle, GoalInfo, GoalRequest, GoalStatus, ServerGoalHandle,
    Timestamp,
};
pub use atomic_file::{write_private_atomic, AtomicWriteReport};
pub use clock::{Clock, ClockJump, ClockType, Rate, TimePoint, Timer, TimerCall, TimerInfo};
pub use config::{
    parse_config_json, resolve_context_config, resolve_node_config, ConfigValue,
    ContextConfigRequest, NameKind, NodeConfigRequest, RemapRule, ResolvedContextConfig,
    ResolvedName, ResolvedNodeConfig,
};
pub use context::{Context, Publisher, Subscription};
pub use error::{Error, Result};
pub use interface::{
    clock_interface, node_card_interface, type_description_interface, Interface, InterfaceKind,
};
pub use interface_limits::{
    TypeDescriptionLimits, MAX_INTERFACE_DEFINITION_BYTES, MAX_INTERFACE_DEPTH,
    MAX_INTERFACE_EXPANDED_SCHEMA_NODES, MAX_INTERFACE_JSON_NODES, MAX_INTERFACE_REFERENCES,
    MAX_INTERFACE_REFERENCE_STATES, MAX_INTERFACE_REGEXES, MAX_INTERFACE_REGEX_CHARGE_BYTES,
    MAX_INTERFACE_REGEX_COMPILED_BYTES, MAX_INTERFACE_REGEX_DFA_BYTES,
    MAX_INTERFACE_REGEX_SOURCE_BYTES, MAX_INTERFACE_SCHEMA_BYTES, MAX_INTERFACE_SCHEMA_NODES,
    MAX_INTERFACE_VALIDATION_WORK,
};
pub use lifecycle::{
    lifecycle_event_interface, Lifecycle, LifecycleCallbackReturn, LifecycleEvent,
    LifecycleInFlight, LifecycleProgress, LifecycleSnapshot, LifecycleState,
    LifecycleStateDescription, LifecycleTransition, LifecycleTransitionDescription,
    LifecycleTransitionId, LifecycleTransitionReport, LifecycleWorkKind,
    DEFAULT_LIFECYCLE_DRAIN_TIMEOUT,
};
pub use lifecycle_service::{
    LifecycleChangeRequest, LifecycleChangeStateReply, LifecycleChangeStateRequest,
    LifecycleClient, LifecycleServer, LifecycleServiceError, LifecycleServiceKind,
    LifecycleStateReply, LifecycleTransitionSelector,
};
pub use node::{
    ActionObservationKind, BoundedSerializedSample, Client, EndpointOptions, Node, NodeOptions,
    Service, ServiceRequest, SubscriptionCall, TypedPublisher, TypedSubscription,
};
pub use parameter::{
    FloatingPointRange, IntegerRange, ListParametersResult, Parameter, ParameterCommit,
    ParameterDescriptor, ParameterError, ParameterEvent, ParameterOverride, ParameterRecord,
    ParameterResult, ParameterSnapshot, ParameterStore, ParameterTransaction, ParameterType,
    ParameterValue, DEFAULT_PARAMETER_TRANSACTION_TIMEOUT,
};
pub use parameter_service::{
    parameter_event_interface, ListParametersRequest, ParameterClient, ParameterNamesRequest,
    ParameterQueryReply, ParameterServer, ParameterServiceKind, ParameterServiceRequest,
    ParameterSetResult, ParameterUpdateMode, SetParametersAtomicallyReply,
    SetParametersAtomicallyRequest, SetParametersReply, SetParametersRequest,
};
pub use process::{
    DeploymentLayers, ProcessBootstrap, ProcessIdentity, MAX_PROCESS_CONFIG_BYTES,
    PROCESS_CONFIG_ENV, PROCESS_CONFIG_SCHEMA_VERSION,
};
pub use qos::{
    qos_compatibility, qos_preset, resolve_qos, ActionQos, Compatibility, QoSCompatibility,
    ResolvedQoS,
};
pub use sample::{ReceiveMetadata, ReceiveScope, Sample, SourceMetadata, SourceTimestamp};
pub use telemetry::{
    Telemetry, TelemetryComponent, TelemetryCorrelationSource, TelemetryEvent, TelemetryMetric,
    TelemetryMetricKey, TelemetryOutcome, TelemetryRecord, TelemetryResource, TelemetrySnapshot,
    MAX_TELEMETRY_METRIC_KEYS, MAX_TELEMETRY_RECORDS,
};
pub use transport::{
    Durability, Endpoint, EntityKind, GraphEntity, QoS, QueueByteStats, QueueLimits, Reliability,
};

/// Version shared by the native SDK and future CLI.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
