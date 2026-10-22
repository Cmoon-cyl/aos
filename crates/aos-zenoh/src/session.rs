use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use aos_core::transport::{
    ClientBackend, Endpoint, GraphEntity, GraphGuard, GraphWatchBackend, PublisherBackend, QoS,
    ServiceBackend, SubscriptionBackend, Transport,
};
use aos_core::{
    Error, QueueLimits, Result, Telemetry, TelemetryComponent, TelemetryEvent, TelemetryOutcome,
    TelemetryResource,
};
use std::time::Duration;
use zenoh::Wait;

use crate::graph::{self, ZenohGraphGuard, ZenohGraphWatch};
use crate::key::{topic_key, typed_key};
use crate::pubsub::{ZenohPublisher, ZenohSubscription};
use crate::queue::{CloseWake, Queue};
use crate::service::{self, ZenohClient, ZenohService};

/// One process-local Zenoh session implementing [`Transport`].
///
/// The configuration is injected by the caller; the checked-in
/// `config/zenoh-session.json5` starts a local peer and connects to the local
/// Router in the background, including when that Router starts later. An open
/// session alone is not evidence of remote connectivity. Install it in one
/// `aos_core::Context`.
pub struct ZenohTransport {
    session: zenoh::Session,
    domain_id: u32,
    shared: Arc<Shared>,
    _resource: Arc<TelemetryResource>,
}

/// State that entities observe without keeping the Zenoh session alive.
#[derive(Default)]
pub(crate) struct Shared {
    live: Mutex<Live>,
    pub(crate) telemetry: Telemetry,
    pub(crate) queue_limits: QueueLimits,
}

#[derive(Default)]
struct Live {
    closed: bool,
    queues: Vec<Weak<dyn CloseWake>>,
}

impl Shared {
    pub(crate) fn new(telemetry: Telemetry, queue_limits: QueueLimits) -> Self {
        Self {
            telemetry,
            queue_limits,
            live: Mutex::default(),
        }
    }

    pub(crate) fn resource(&self, component: TelemetryComponent) -> Result<Arc<TelemetryResource>> {
        let resource = Arc::new(self.telemetry.resource(component));
        self.register(&resource)?;
        Ok(resource)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.live().closed
    }

    fn live(&self) -> MutexGuard<'_, Live> {
        // The lock guards plain bookkeeping and never runs user code.
        self.live.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a queue so session close can wake its receivers. Registration
    /// and close share one lock, so a queue is either refused or woken.
    pub(crate) fn register<T: CloseWake + 'static>(&self, queue: &Arc<T>) -> Result<()> {
        let mut live = self.live();
        if live.closed {
            return Err(Error::Closed);
        }
        live.queues.retain(|queue| queue.strong_count() > 0);
        let wake: Arc<dyn CloseWake> = queue.clone();
        live.queues.push(Arc::downgrade(&wake));
        Ok(())
    }
}

impl CloseWake for TelemetryResource {
    fn wake_close(&self) {
        self.close();
    }
}

impl ZenohTransport {
    /// Opens a session; `domain_id` isolates logical communication domains
    /// and is not an authentication boundary.
    pub fn open(config: zenoh::Config, domain_id: u32) -> Result<Self> {
        Self::open_with_telemetry(config, domain_id, Telemetry::default())
    }

    /// Configure local payload limits for all Topic, Service and Action
    /// transport paths in this session. This does not alter wire QoS.
    pub fn open_with_queue_limits(
        config: zenoh::Config,
        domain_id: u32,
        limits: QueueLimits,
    ) -> Result<Self> {
        Self::open_with_telemetry_and_queue_limits(config, domain_id, Telemetry::default(), limits)
    }

    /// Accepts a caller-owned diagnostic handle, including capture configured
    /// before opening. No configuration values or arbitrary errors are recorded.
    pub fn open_with_telemetry(
        config: zenoh::Config,
        domain_id: u32,
        telemetry: Telemetry,
    ) -> Result<Self> {
        Self::open_with_telemetry_and_queue_limits(
            config,
            domain_id,
            telemetry,
            QueueLimits::default(),
        )
    }

    pub fn open_with_telemetry_and_queue_limits(
        config: zenoh::Config,
        domain_id: u32,
        telemetry: Telemetry,
        limits: QueueLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let session = zenoh::open(config).wait().map_err(|error| {
            let mut event =
                TelemetryEvent::new(TelemetryComponent::Transport, "zenoh.session.open");
            event.outcome = TelemetryOutcome::Transport;
            telemetry.record(event);
            Error::Transport(format!("zenoh::open: {error}"))
        })?;
        let shared = Arc::new(Shared::new(telemetry, limits));
        let resource = shared.resource(TelemetryComponent::Transport)?;
        shared.telemetry.record(TelemetryEvent::new(
            TelemetryComponent::Transport,
            "zenoh.session.open",
        ));
        Ok(Self {
            session,
            domain_id,
            shared,
            _resource: resource,
        })
    }

    pub fn telemetry(&self) -> Telemetry {
        self.shared.telemetry.clone()
    }

    /// Zenoh ID of this session, for diagnostics.
    pub fn zid(&self) -> String {
        self.session.info().zid().wait().to_string()
    }

    /// Zenoh IDs of the routers this session is currently connected to.
    pub fn router_zids(&self) -> Vec<String> {
        self.session
            .info()
            .routers_zid()
            .wait()
            .map(|id| id.to_string())
            .collect()
    }
}

impl Transport for ZenohTransport {
    fn telemetry(&self) -> Option<Telemetry> {
        Some(self.telemetry())
    }

    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
        let key = topic_key(self.domain_id, topic)?;
        if self.shared.is_closed() {
            return Err(Error::Closed);
        }
        let publisher =
            ZenohPublisher::declare(&self.session, key, &QoS::default(), self.shared.clone())?;
        Ok(Box::new(publisher))
    }

    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
        let key = topic_key(self.domain_id, topic)?;
        let queue = Arc::new(Queue::with_limits(
            QoS::default().depth,
            self.shared.queue_limits,
        ));
        self.shared.register(&queue)?;
        // If close wins after registration, the queue is already closed and
        // the entity is returned invalidated, or the declaration fails.
        let subscription = ZenohSubscription::declare(
            &self.session,
            key,
            queue,
            &QoS::default(),
            self.shared.clone(),
        )?;
        Ok(Box::new(subscription))
    }

    fn create_typed_publisher(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn PublisherBackend>> {
        if self.shared.is_closed() {
            return Err(Error::Closed);
        }
        let key = typed_key(self.domain_id, endpoint)?;
        Ok(Box::new(ZenohPublisher::declare(
            &self.session,
            key,
            qos,
            self.shared.clone(),
        )?))
    }

    fn create_typed_subscription(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        self.create_typed_subscription_with_limits(endpoint, qos, self.shared.queue_limits)
    }

    fn create_typed_subscription_with_limits(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
        limits: QueueLimits,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        qos.validate()?;
        limits.validate()?;
        let key = typed_key(self.domain_id, endpoint)?;
        let queue = Arc::new(Queue::with_limits(qos.depth, limits));
        self.shared.register(&queue)?;
        Ok(Box::new(ZenohSubscription::declare(
            &self.session,
            key,
            queue,
            qos,
            self.shared.clone(),
        )?))
    }

    fn create_service(&self, endpoint: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        self.create_service_with_qos(endpoint, &QoS::default())
    }

    fn create_service_with_qos(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Box<dyn ServiceBackend>> {
        let key = typed_key(self.domain_id, endpoint)?;
        Ok(Box::new(ZenohService::declare(
            &self.session,
            key,
            qos,
            self.shared.clone(),
        )?))
    }

    fn create_client_with_qos(
        &self,
        endpoint: &Endpoint,
        qos: &QoS,
    ) -> Result<Option<Box<dyn ClientBackend>>> {
        Ok(Some(Box::new(ZenohClient::new(
            &self.session,
            typed_key(self.domain_id, endpoint)?,
            qos,
            self.shared.clone(),
            endpoint.name.ends_with("/_action/get_result"),
        )?)))
    }

    fn call(&self, endpoint: &Endpoint, payload: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        service::call(
            &self.session,
            typed_key(self.domain_id, endpoint)?,
            &self.shared,
            payload,
            timeout,
        )
    }

    fn call_bounded(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Vec<u8>> {
        service::call_bounded(
            &self.session,
            typed_key(self.domain_id, endpoint)?,
            &self.shared,
            payload,
            timeout,
            max_response_bytes,
        )
    }

    fn call_cancelable(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        service::call_with_cancel(
            &self.session,
            typed_key(self.domain_id, endpoint)?,
            &self.shared,
            payload,
            timeout,
            Some(cancel),
        )
    }

    fn call_wait(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Option<Duration>,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        if timeout.is_none() && !endpoint.name.ends_with("/_action/get_result") {
            return Err(Error::Unsupported(
                "service wait without deadline is reserved for Action GetResult".into(),
            ));
        }
        service::call_wait(
            &self.session,
            typed_key(self.domain_id, endpoint)?,
            &self.shared,
            payload,
            timeout,
            Some(cancel),
        )
    }

    fn declare_graph_entity(&self, entity: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        Ok(Box::new(ZenohGraphGuard::declare(
            &self.session,
            self.domain_id,
            entity,
            self.shared.clone(),
        )?))
    }

    fn graph(&self, timeout: Duration) -> Result<Vec<GraphEntity>> {
        graph::snapshot(&self.session, self.domain_id, &self.shared, timeout)
    }

    fn watch_graph(&self) -> Result<Option<Box<dyn GraphWatchBackend>>> {
        Ok(Some(Box::new(ZenohGraphWatch::declare(
            &self.session,
            self.domain_id,
            self.shared.clone(),
        )?)))
    }

    fn close(&self) -> Result<()> {
        let queues = {
            let mut live = self.shared.live();
            if live.closed {
                return Err(Error::Closed);
            }
            live.closed = true;
            std::mem::take(&mut live.queues)
        };
        // Wake receivers first: Zenoh's close is bounded but not instant.
        for queue in queues.iter().filter_map(Weak::upgrade) {
            queue.wake_close();
        }
        let result = self
            .session
            .close()
            .wait()
            .map_err(|error| Error::Transport(format!("zenoh session close: {error}")));
        let mut event = TelemetryEvent::new(TelemetryComponent::Transport, "zenoh.session.close");
        event.outcome = result
            .as_ref()
            .err()
            .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from);
        self.shared.telemetry.record(event);
        result
    }
}

pub(crate) fn failure(shared: &Shared, context: &str, error: zenoh::Error) -> Error {
    if shared.is_closed() {
        Error::Closed
    } else {
        Error::Transport(format!("{context}: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_graph_watch_declaration_releases_registered_queue() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        config.insert_json5("listen/endpoints", "[]").unwrap();
        let transport = ZenohTransport::open(config, 0).unwrap();
        let existing = transport.shared.live().queues.clone();
        // Force the middleware declaration to fail after Shared registration,
        // rather than taking the earlier already-closed registration path.
        transport.session.close().wait().unwrap();
        assert!(transport.watch_graph().is_err());
        let registered: Vec<_> = transport
            .shared
            .live()
            .queues
            .iter()
            .filter(|queue| !existing.iter().any(|prior| prior.ptr_eq(queue)))
            .cloned()
            .collect();
        assert!(!registered.is_empty());
        assert!(registered.iter().all(|queue| queue.strong_count() == 0));
    }

    #[test]
    fn dropping_graph_watch_releases_interest_and_callback_state() {
        let mut config = zenoh::Config::default();
        config
            .insert_json5("scouting/multicast/enabled", "false")
            .unwrap();
        config.insert_json5("listen/endpoints", "[]").unwrap();
        let transport = ZenohTransport::open(config, 0).unwrap();
        let existing = transport.shared.live().queues.clone();
        let watch = transport.watch_graph().unwrap().unwrap();
        // Telemetry also registers a session resource. Identify this watch's
        // new registrations rather than assuming its queue occupies slot 0.
        let registered: Vec<_> = transport
            .shared
            .live()
            .queues
            .iter()
            .filter(|queue| !existing.iter().any(|prior| prior.ptr_eq(queue)))
            .cloned()
            .collect();
        assert!(registered.iter().any(|queue| queue.strong_count() == 2));
        drop(watch);
        assert!(registered.iter().all(|queue| queue.strong_count() == 0));
        transport.close().unwrap();
    }
}
