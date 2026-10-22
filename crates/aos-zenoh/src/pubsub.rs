// Portions of the QoS configuration are adapted from Hiroz
// d9f979a0404311de12f6884bafaa9b20600eb490 (Apache-2.0 selected).
// Modified for AOS JSON interfaces, ownership and closure. See upstream/source-map.json
// and upstream/licenses/hiroz-LICENSE for source locations and license.

//! Opaque Topic payloads over Zenoh Advanced pub/sub.
//! QoS mappings are adapted from Hiroz d9f979a0404311de12f6884bafaa9b20600eb490,
//! `crates/hiroz/src/pubsub.rs` (`apply_transient_local_pub/sub`, publisher build).
//! JSON typesupport and application callbacks stay outside this adapter.
//!
//! Reliable maps to Zenoh's Reliable routing marker and Block congestion;
//! best_effort maps to BestEffort and Drop. The marker alone does not enable
//! retransmission in Zenoh 1.10.1. Transient-local cache and receive history
//! both use depth, with Advanced pub/sub recovery only for reliable history.

use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime};

use aos_core::transport::{
    Durability, PublisherBackend, QoS, QueueStats, Reliability, SubscriptionBackend,
};
use aos_core::{
    Error, ReceiveMetadata, ReceiveScope, Result, Sample, SourceMetadata, SourceTimestamp,
    TelemetryComponent, TelemetryOutcome, TelemetryResource,
};
use zenoh::key_expr::OwnedKeyExpr;
use zenoh::Wait;
use zenoh_ext::{
    AdvancedPublisher, AdvancedPublisherBuilderExt, AdvancedSubscriber,
    AdvancedSubscriberBuilderExt, CacheConfig, HistoryConfig, MissDetectionConfig, RecoveryConfig,
};

use crate::byte_budget::{check_send, Rejection};
use crate::queue::Queue;
use crate::session::{failure, Shared};
use crate::telemetry::EndpointTelemetry;

pub(crate) struct ZenohPublisher {
    // Write-locked only by close, so publishes run concurrently.
    publisher: RwLock<Option<AdvancedPublisher<'static>>>,
    waiters: Mutex<Vec<Weak<Queue<()>>>>,
    shared: Arc<Shared>,
    telemetry: EndpointTelemetry,
    resource: Arc<TelemetryResource>,
}

impl ZenohPublisher {
    pub(crate) fn declare(
        session: &zenoh::Session,
        key: OwnedKeyExpr,
        qos: &QoS,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        qos.validate()?;
        let resource = shared.resource(TelemetryComponent::Topic)?;
        let telemetry = EndpointTelemetry::new(
            shared.telemetry.clone(),
            TelemetryComponent::Topic,
            key.as_str(),
        );
        let (reliability, congestion) = match qos.reliability {
            Reliability::Reliable => (
                zenoh::qos::Reliability::Reliable,
                zenoh::qos::CongestionControl::Block,
            ),
            Reliability::BestEffort => (
                zenoh::qos::Reliability::BestEffort,
                zenoh::qos::CongestionControl::Drop,
            ),
        };
        let mut builder = session
            .declare_publisher(key)
            .reliability(reliability)
            .congestion_control(congestion)
            .advanced();
        if qos.durability == Durability::TransientLocal {
            builder = builder
                .publisher_detection()
                .cache(CacheConfig::default().max_samples(qos.depth));
            if qos.reliability == Reliability::Reliable {
                builder = builder.sample_miss_detection(
                    MissDetectionConfig::default().sporadic_heartbeat(Duration::from_millis(500)),
                );
            }
        }
        let publisher = builder
            .wait()
            .map_err(|error| failure(&shared, "declare publisher", error))?;
        telemetry.record(telemetry.event("zenoh.publisher.declare"));
        Ok(Self {
            publisher: RwLock::new(Some(publisher)),
            waiters: Mutex::new(Vec::new()),
            shared,
            telemetry,
            resource,
        })
    }
}

impl ZenohPublisher {
    pub(crate) fn wait_for_match(&self, timeout: Duration) -> Result<()> {
        let started = Instant::now();
        let queue = Arc::new(Queue::new(1));
        self.shared.register(&queue)?;
        let listener = {
            let publisher = self
                .publisher
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            let publisher = publisher.as_ref().ok_or(Error::Closed)?;
            let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
            waiters.retain(|queue| queue.strong_count() > 0);
            waiters.push(Arc::downgrade(&queue));
            drop(waiters);
            let sink = queue.clone();
            // Subscribe before sampling status so a matching transition cannot be missed.
            let listener = publisher
                .matching_listener()
                .callback(move |status| {
                    if status.matching() {
                        sink.push(());
                    }
                })
                .wait()
                .map_err(|error| failure(&self.shared, "listen for subscriber", error))?;
            if publisher
                .matching_status()
                .wait()
                .map_err(|error| failure(&self.shared, "subscriber status", error))?
                .matching()
            {
                queue.push(());
            }
            listener
        };
        let result = queue.recv_timeout(timeout.saturating_sub(started.elapsed()));
        queue.close();
        drop(listener);
        result
    }
}

impl PublisherBackend for ZenohPublisher {
    fn wait_for_subscribers(&self, timeout: Duration) -> Result<()> {
        self.wait_for_match(timeout)
    }
    fn publish(&self, payload: &[u8]) -> Result<()> {
        let result = (|| {
            if self.shared.is_closed() {
                return Err(Error::Closed);
            }
            let publisher = self
                .publisher
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            let publisher = publisher.as_ref().ok_or(Error::Closed)?;
            check_send(self.shared.queue_limits, payload.len(), "TOPIC")?;
            publisher
                .put(payload.to_vec())
                .wait()
                .map_err(|error| failure(&self.shared, "publisher put", error))
        })();
        let mut event = self.telemetry.event("zenoh.topic.publish");
        event.outcome = result
            .as_ref()
            .err()
            .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from);
        event.bytes = Some(payload.len() as u64);
        self.telemetry.record(event);
        result
    }

    fn close(&self) -> Result<()> {
        self.resource.close();
        let publisher = self
            .publisher
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(Error::Closed)?;
        for queue in self
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
            .filter_map(|queue| queue.upgrade())
        {
            queue.close();
        }
        if self.shared.is_closed() {
            // Session close already released the entity.
            return Err(Error::Closed);
        }
        publisher
            .undeclare()
            .wait()
            .map_err(|error| failure(&self.shared, "undeclare publisher", error))
    }
}

pub(crate) struct ZenohSubscription {
    queue: Arc<Queue<Sample<Vec<u8>>>>,
    subscriber: Mutex<Option<AdvancedSubscriber<()>>>,
    shared: Arc<Shared>,
    resource: Arc<TelemetryResource>,
}

impl ZenohSubscription {
    pub(crate) fn declare(
        session: &zenoh::Session,
        key: OwnedKeyExpr,
        queue: Arc<Queue<Sample<Vec<u8>>>>,
        qos: &QoS,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        let sink = queue.clone();
        qos.validate()?;
        let resource = shared.resource(TelemetryComponent::Topic)?;
        let telemetry = EndpointTelemetry::new(
            shared.telemetry.clone(),
            TelemetryComponent::Topic,
            key.as_str(),
        );
        let trace = telemetry.clone();
        let mut builder = session
            .declare_subscriber(key)
            .callback(move |sample| {
                let steady = Instant::now();
                let system = SystemTime::now();
                let bytes = sample.payload().len();
                let mut event = trace.event("zenoh.topic.receive");
                event.bytes = Some(bytes as u64);
                trace.record(event);
                let result = sink.try_push_with(
                    bytes,
                    || receive_sample(sample, steady, system),
                    |_evicted| {
                        let mut event = trace.event("zenoh.topic.queue_evicted");
                        event.outcome = TelemetryOutcome::QueueOverflow;
                        trace.record(event);
                    },
                );
                if matches!(result, Err(Rejection::Oversized | Rejection::Capacity)) {
                    let mut event = trace.event("zenoh.topic.queue_rejected");
                    event.outcome = TelemetryOutcome::QueueOverflow;
                    trace.record(event);
                }
            })
            .advanced();
        if qos.durability == Durability::TransientLocal {
            builder = builder
                .history(
                    HistoryConfig::default()
                        .detect_late_publishers()
                        .max_samples(qos.depth),
                )
                .query_timeout(Duration::from_millis(u64::MAX))
                .subscriber_detection();
            if qos.reliability == Reliability::Reliable {
                builder = builder.recovery(RecoveryConfig::default().heartbeat());
            }
        }
        let subscriber = builder.wait().map_err(|error| {
            queue.close();
            failure(&shared, "declare subscriber", error)
        })?;
        telemetry.record(telemetry.event("zenoh.subscription.declare"));
        Ok(Self {
            queue,
            subscriber: Mutex::new(Some(subscriber)),
            shared,
            resource,
        })
    }
}

impl SubscriptionBackend for ZenohSubscription {
    fn queue_stats(&self) -> Option<QueueStats> {
        Some(self.queue.stats())
    }

    fn recv_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.recv_sample_timeout(timeout)
            .map(|sample| sample.payload)
    }

    fn recv_sample_timeout(&self, timeout: Duration) -> Result<Sample<Vec<u8>>> {
        self.queue.recv_timeout(timeout)
    }

    fn close(&self) -> Result<()> {
        self.queue.close();
        self.resource.close();
        let subscriber = self
            .subscriber
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(Error::Closed)?;
        if self.shared.is_closed() {
            return Err(Error::Closed);
        }
        subscriber
            .undeclare()
            .wait()
            .map_err(|error| failure(&self.shared, "undeclare subscriber", error))
    }
}

fn receive_sample(
    sample: zenoh::sample::Sample,
    steady: Instant,
    system: SystemTime,
) -> Sample<Vec<u8>> {
    // Capture before payload copying and our KEEP_LAST queue. AdvancedSubscriber
    // may have performed history/recovery ordering before delivering here.
    let payload = sample.payload().to_bytes().into_owned();
    let timestamp = sample.timestamp().map(|timestamp| SourceTimestamp {
        kind: "zenoh_hlc".into(),
        value: timestamp.get_time().as_u64().to_string(),
        clock_id: timestamp.get_id().to_string(),
        comparable: false,
        // Zenoh 1.10.1 routers can insert missing HLC timestamps and replace
        // timestamps too far in the future. Presence proves neither publisher
        // origin nor synchronization with this subscriber's system clock.
        origin: "router_or_publisher".into(),
    });
    let source_info = sample.source_info();
    let source = if timestamp.is_some() || source_info.is_some() {
        Some(SourceMetadata {
            timestamp,
            publisher_id: source_info.map(|info| {
                let id = info.source_id();
                format!("{}/{}", id.zid(), id.eid())
            }),
            sequence: source_info.map(|info| u64::from(info.source_sn())),
        })
    } else {
        None
    };
    Sample {
        payload_size_bytes: payload.len(),
        payload,
        received: Some(ReceiveMetadata {
            steady,
            system,
            scope: ReceiveScope::AdapterDelivery,
        }),
        source,
    }
}

impl Drop for ZenohSubscription {
    fn drop(&mut self) {
        // Dropping the Zenoh subscriber undeclares it; also wake any waiter
        // that still holds the queue.
        self.queue.close();
    }
}
