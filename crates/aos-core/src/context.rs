use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use std::time::Duration;

use crate::transport::{GraphWatchBackend, PublisherBackend, SubscriptionBackend, Transport};
use crate::{
    clock::ShutdownResource, Clock, ClockType, Error, ProcessBootstrap, ProcessIdentity,
    ResolvedContextConfig, Result, Telemetry, TelemetryRecord, TelemetrySnapshot,
};

/// Shared ownership of a process-local communication session.
///
/// Clones and entities keep the session alive. Dropping one Context does not
/// invalidate its surviving entities; use `shutdown` for explicit invalidation.
/// Dropping the last owner closes the session. Shutdown is terminal, including
/// when backend cleanup fails.
#[derive(Clone)]
pub struct Context {
    inner: Arc<Inner>,
}

struct Inner {
    transport: Box<dyn Transport>,
    telemetry: Telemetry,
    closed: AtomicBool,
    shutdown_resources: Mutex<Vec<Weak<dyn ShutdownResource>>>,
    configuration: Option<ResolvedContextConfig>,
    process_bootstrap: Option<ProcessBootstrap>,
    process_identity: Option<ProcessIdentity>,
}

impl Context {
    /// Take exclusive ownership of an already-open adapter session.
    ///
    /// Opening/configuration errors are returned by the adapter before this
    /// call. The same session must not be installed in independent Contexts.
    /// This low-level constructor does not read a process bootstrap and cannot
    /// configure an already-open network session. Use the adapter's standard
    /// `open_context(ContextConfigRequest)` for automatic startup injection.
    pub fn new(transport: impl Transport) -> Self {
        Self::from_parts(transport, None, None)
    }

    /// Adopt a transport already opened using this resolved configuration.
    /// This accepts pre-parsed inputs; it does not reopen or configure a network
    /// session. The adapter must merge/resolve the bootstrap before opening.
    pub fn from_configured_transport(
        transport: impl Transport,
        configuration: ResolvedContextConfig,
        bootstrap: Option<ProcessBootstrap>,
    ) -> Result<Self> {
        if let Some(bootstrap) = &bootstrap {
            bootstrap.validate()?;
        }
        Ok(Self::from_parts(transport, Some(configuration), bootstrap))
    }

    fn from_parts(
        transport: impl Transport,
        configuration: Option<ResolvedContextConfig>,
        process_bootstrap: Option<ProcessBootstrap>,
    ) -> Self {
        let telemetry = transport.telemetry().unwrap_or_default();
        let process_identity = process_bootstrap
            .as_ref()
            .map(ProcessBootstrap::local_identity);
        Self {
            inner: Arc::new(Inner {
                transport: Box::new(transport),
                telemetry,
                closed: AtomicBool::new(false),
                shutdown_resources: Mutex::new(Vec::new()),
                configuration,
                process_bootstrap,
                process_identity,
            }),
        }
    }

    pub fn configuration(&self) -> Option<&ResolvedContextConfig> {
        self.inner.configuration.as_ref()
    }

    pub fn process_bootstrap(&self) -> Option<&ProcessBootstrap> {
        self.inner.process_bootstrap.as_ref()
    }

    /// Self-reported launch correlation and the PID captured at construction.
    /// These fields are not authenticated and do not prove process liveness.
    pub fn process_identity(&self) -> Option<&ProcessIdentity> {
        self.inner.process_identity.as_ref()
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Diagnostics remain readable after shutdown. Clones share the same
    /// counters and bounded record buffer as a supporting adapter.
    pub fn telemetry(&self) -> Telemetry {
        self.inner.telemetry.clone()
    }

    pub fn telemetry_snapshot(&self) -> TelemetrySnapshot {
        self.inner.telemetry.snapshot()
    }

    pub fn enable_log_capture(&self, capacity: usize) -> Result<()> {
        self.inner.telemetry.enable_log_capture(capacity)
    }

    pub fn disable_log_capture(&self) {
        self.inner.telemetry.disable_log_capture();
    }

    pub fn drain_telemetry(&self) -> Vec<TelemetryRecord> {
        self.inner.telemetry.drain_records()
    }

    /// Explicit caller-thread output to stderr only. This may block on I/O;
    /// executors and CLI operation loops choose their own flush boundaries.
    pub fn flush_logs_to_stderr(&self) -> std::io::Result<usize> {
        self.inner.telemetry.flush_stderr()
    }

    /// Stop new work and close the session.
    ///
    /// The winning caller gets the backend cleanup result. Later/concurrent
    /// callers get `Error::Closed`, which means shutdown has begun, not that
    /// another caller's cleanup has finished. No operation is retried.
    /// An already-in-flight publish may have been submitted before shutdown.
    pub fn shutdown(&self) -> Result<()> {
        self.inner.shutdown()
    }
    /// A clock registered with this Context is invalidated by shutdown even
    /// while a Timer or Rate is waiting in another thread.
    pub fn create_clock(&self, kind: ClockType) -> Result<Clock> {
        let clock = Clock::new(kind);
        self.register_shutdown_resource(clock.shutdown_resource())?;
        Ok(clock)
    }
    pub(crate) fn register_shutdown_resource(
        &self,
        resource: Arc<dyn ShutdownResource>,
    ) -> Result<()> {
        let mut resources = self.inner.shutdown_resources.lock().unwrap();
        if self.is_shutdown() {
            resource.shutdown();
            return Err(Error::Closed);
        }
        resources.retain(|resource| resource.strong_count() != 0);
        resources.push(Arc::downgrade(&resource));
        Ok(())
    }

    /// Create a publisher for an absolute probe topic such as `/echo/request`.
    pub fn create_publisher(&self, topic: &str) -> Result<Publisher> {
        self.ensure_open()?;
        validate_topic(topic)?;
        let publisher = Publisher {
            backend: self.inner.transport.create_publisher(topic)?,
            context: self.clone(),
            closed: AtomicBool::new(false),
        };
        // Shutdown may have won while the adapter declared the entity. The
        // local wrapper's Drop cleans up that losing declaration.
        self.ensure_open()?;
        Ok(publisher)
    }

    /// Create a subscription before publishing to avoid a volatile-startup loss.
    pub fn create_subscription(&self, topic: &str) -> Result<Subscription> {
        self.ensure_open()?;
        validate_topic(topic)?;
        let subscription = Subscription {
            backend: self.inner.transport.create_subscription(topic)?,
            context: self.clone(),
            closed: AtomicBool::new(false),
        };
        self.ensure_open()?;
        Ok(subscription)
    }

    pub fn graph(&self, timeout: Duration) -> Result<Vec<crate::GraphEntity>> {
        self.ensure_open()?;
        let graph = self.transport().graph(timeout)?;
        self.ensure_open()?;
        Ok(graph)
    }

    /// Observe current and future graph entities until the watch is dropped.
    /// Each snapshot is the locally discovered live set, not a global barrier.
    /// The watch keeps its Context alive; explicit shutdown still invalidates it.
    /// None denotes a legacy/custom backend with snapshot polling only.
    pub fn watch_graph(&self) -> Result<Option<Box<dyn GraphWatchBackend>>> {
        self.ensure_open()?;
        let watch = self.transport().watch_graph()?;
        // A losing declaration is dropped if shutdown won during creation.
        self.ensure_open()?;
        Ok(watch.map(|backend| {
            Box::new(ContextGraphWatch {
                backend,
                context: self.clone(),
            }) as Box<dyn GraphWatchBackend>
        }))
    }

    /// Default raw NodeCard reply ceiling, before JSON decoding.
    pub const MAX_NODE_CARD_RESPONSE_BYTES: usize = 1024 * 1024;

    pub fn get_node_card(&self, node_fqn: &str, timeout: Duration) -> Result<serde_json::Value> {
        self.get_node_card_bounded(node_fqn, timeout, Self::MAX_NODE_CARD_RESPONSE_BYTES)
    }

    /// Tighten the default raw reply limit. Native adapters can reject before
    /// copying; legacy adapters are checked after returning a Vec. The core
    /// always checks again before decoding, including custom adapter overrides.
    pub fn get_node_card_bounded(
        &self,
        node_fqn: &str,
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<serde_json::Value> {
        if max_response_bytes == 0 || max_response_bytes > Self::MAX_NODE_CARD_RESPONSE_BYTES {
            return Err(Error::InvalidArgument(format!(
                "NodeCard reply limit must be in 1..={}",
                Self::MAX_NODE_CARD_RESPONSE_BYTES
            )));
        }
        let interface = crate::node_card_interface();
        self.call_introspection_bounded(
            node_fqn,
            "get_node_card",
            &interface,
            &serde_json::json!({}),
            timeout,
            max_response_bytes,
        )
    }

    pub fn get_type_description(
        &self,
        node_fqn: &str,
        type_name: &str,
        type_hash: Option<&str>,
        timeout: Duration,
    ) -> Result<crate::Interface> {
        self.get_type_description_bounded(
            node_fqn,
            type_name,
            type_hash,
            timeout,
            crate::TypeDescriptionLimits::default(),
        )
    }

    /// Bound the raw response before decoding and all schema parts before
    /// compilation. Adapters may enforce an even smaller local reply limit.
    pub fn get_type_description_bounded(
        &self,
        node_fqn: &str,
        type_name: &str,
        type_hash: Option<&str>,
        timeout: Duration,
        limits: crate::TypeDescriptionLimits,
    ) -> Result<crate::Interface> {
        limits.validate()?;
        let interface = crate::type_description_interface();
        let mut request = serde_json::json!({"type_name": type_name});
        if let Some(hash) = type_hash {
            request["type_hash"] = serde_json::json!(hash);
        }
        let definition = self.call_introspection_bounded(
            node_fqn,
            "get_type_description",
            &interface,
            &request,
            timeout,
            limits.response_bytes,
        )?;
        let actual = crate::Interface::from_json_bounded(definition, limits.schema_bytes)?;
        if actual.type_name() != type_name
            || type_hash.is_some_and(|hash| actual.type_hash() != hash)
        {
            return Err(Error::Validation(
                "type description does not match requested name/hash".into(),
            ));
        }
        Ok(actual)
    }

    fn call_introspection_bounded(
        &self,
        node_fqn: &str,
        suffix: &str,
        interface: &crate::Interface,
        request: &serde_json::Value,
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<serde_json::Value> {
        self.ensure_open()?;
        crate::names::validate_absolute_name(node_fqn)?;
        let endpoint = crate::Endpoint {
            name: format!("{node_fqn}/{suffix}"),
            type_name: interface.type_name().into(),
            type_hash: interface.type_hash().into(),
        };
        let payload = interface.encode("request", request)?;
        let response =
            self.transport()
                .call_bounded(&endpoint, &payload, timeout, max_response_bytes)?;
        self.ensure_open()?;
        // A custom adapter can override call_bounded. The core gate remains
        // mandatory even when that override returns an oversized Vec.
        if response.len() > max_response_bytes {
            return Err(Error::QueueOverflow(format!(
                "{suffix} response exceeds {max_response_bytes} bytes before JSON decoding"
            )));
        }
        interface.decode("response", &response)
    }

    pub(crate) fn transport(&self) -> &dyn Transport {
        self.inner.transport.as_ref()
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        ensure_open(&self.inner.closed)
    }
}

struct ContextGraphWatch {
    // Release the backend interest before the final Context reference.
    backend: Box<dyn GraphWatchBackend>,
    context: Context,
}

impl GraphWatchBackend for ContextGraphWatch {
    fn snapshot(&self) -> Result<Vec<crate::GraphEntity>> {
        self.context.ensure_open()?;
        let graph = self.backend.snapshot()?;
        self.context.ensure_open()?;
        Ok(graph)
    }

    fn wait_for_change(&self, timeout: Duration) -> Result<()> {
        self.context.ensure_open()?;
        let result = self.backend.wait_for_change(timeout);
        self.context.ensure_open()?;
        result
    }
}

impl Inner {
    fn shutdown(&self) -> Result<()> {
        begin_close(&self.closed)?;
        let resources: Vec<_> = self
            .shutdown_resources
            .lock()
            .unwrap()
            .drain(..)
            .filter_map(|resource| resource.upgrade())
            .collect();
        for resource in resources {
            resource.shutdown();
        }
        self.transport.close()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        report_drop_error("context", self.shutdown());
    }
}

/// A process-local publisher, safe to hold in a native binding.
pub struct Publisher {
    // Release the backend before the final Context reference.
    backend: Box<dyn PublisherBackend>,
    context: Context,
    closed: AtomicBool,
}

impl Publisher {
    /// Submit bytes once; this is not a delivery or business acknowledgement.
    pub fn publish(&self, payload: &[u8]) -> Result<()> {
        self.context.ensure_open()?;
        ensure_open(&self.closed)?;
        self.backend.publish(payload)
    }

    /// Terminal close; repeated/concurrent calls return `Error::Closed`.
    pub fn close(&self) -> Result<()> {
        begin_close(&self.closed)?;
        self.backend.close()
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        report_drop_error("publisher", self.close());
    }
}

/// An owned receiver; no application callbacks run inside core or the adapter.
pub struct Subscription {
    backend: Box<dyn SubscriptionBackend>,
    context: Context,
    closed: AtomicBool,
}

impl Subscription {
    /// Receive owned bytes. Native bindings must release the GIL while waiting.
    ///
    /// Closing the subscription or context interrupts the wait. Data returned
    /// by the adapter during shutdown is discarded if closure is observed here.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.context.ensure_open()?;
        ensure_open(&self.closed)?;
        let result = self.backend.recv_timeout(timeout);
        self.context.ensure_open()?;
        ensure_open(&self.closed)?;
        result
    }

    /// Terminal close that wakes pending receivers.
    pub fn close(&self) -> Result<()> {
        begin_close(&self.closed)?;
        self.backend.close()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        report_drop_error("subscription", self.close());
    }
}

fn ensure_open(closed: &AtomicBool) -> Result<()> {
    if closed.load(Ordering::Acquire) {
        Err(Error::Closed)
    } else {
        Ok(())
    }
}

fn begin_close(closed: &AtomicBool) -> Result<()> {
    if closed.swap(true, Ordering::AcqRel) {
        Err(Error::Closed)
    } else {
        Ok(())
    }
}

fn report_drop_error(entity: &str, result: Result<()>) {
    if let Err(error) = result {
        if error != Error::Closed {
            // Destructors cannot return errors; explicit close is the checked
            // path. Preserve cleanup diagnostics without panicking on stderr.
            use std::io::Write;
            let record = serde_json::json!({
                "schema_version":1,"event":"cleanup_error","entity":entity,
                "outcome":crate::TelemetryOutcome::from(&error),
                "message":"Implicit cleanup failed; use explicit close or shutdown to inspect the error."
            });
            let _ = writeln!(std::io::stderr().lock(), "{record}");
        }
    }
}

fn validate_topic(topic: &str) -> Result<()> {
    let valid = topic.strip_prefix('/').is_some_and(|path| {
        path.split('/').all(|segment| {
            let mut bytes = segment.bytes();
            bytes
                .next()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
                && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
    });
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "expected an absolute topic with identifier segments, got {topic:?}"
        )))
    }
}

#[cfg(test)]
mod telemetry_tests {
    use super::*;
    use crate::{TelemetryComponent, TelemetryEvent};

    struct Backend(Option<Telemetry>);
    impl Transport for Backend {
        fn telemetry(&self) -> Option<Telemetry> {
            self.0.clone()
        }
        fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
            Err(Error::Unsupported("test publisher".into()))
        }
        fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
            Err(Error::Unsupported("test subscription".into()))
        }
        fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn context_and_adapter_share_capture_and_snapshot_after_shutdown() {
        let adapter = Telemetry::new();
        let context = Context::new(Backend(Some(adapter.clone())));
        let clone = context.clone();
        context.enable_log_capture(2).unwrap();
        adapter.record(TelemetryEvent::new(
            TelemetryComponent::Transport,
            "session_opened",
        ));
        assert_eq!(clone.telemetry_snapshot().events_total, 1);
        assert_eq!(clone.telemetry_snapshot().context_id, adapter.context_id());
        context.shutdown().unwrap();
        assert_eq!(clone.drain_telemetry().len(), 1);
        assert_eq!(adapter.snapshot().log_queued, 0);
        clone.disable_log_capture();
        assert!(!adapter.snapshot().log_capture_enabled);
    }

    #[test]
    fn unsupported_backends_get_distinct_local_fallbacks() {
        let first = Context::new(Backend(None));
        let second = Context::new(Backend(None));
        assert_ne!(
            first.telemetry().context_id(),
            second.telemetry().context_id()
        );
        first
            .telemetry()
            .record(TelemetryEvent::new(TelemetryComponent::Node, "created"));
        assert_eq!(first.telemetry_snapshot().events_total, 1);
        assert_eq!(second.telemetry_snapshot().events_total, 0);
    }
}
