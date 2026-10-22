//! Bounded process-local diagnostics shared by core and transport adapters.
//!
//! Recording performs no I/O and never calls application code. JSONL capture
//! is explicit; a caller drains it on an operation or executor thread. Records
//! contain identifiers and finite outcome codes, never payloads, configuration,
//! or arbitrary error messages. Identifiers are diagnostic claims, not proof
//! of identity or authorization. Steady times are local to this handle.
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::{Error, Result};

pub const MAX_TELEMETRY_RECORDS: usize = 4096;
pub const MAX_TELEMETRY_METRIC_KEYS: usize = 64;
const MAX_EVENT_NAME: usize = 64;
const MAX_IDENTIFIER: usize = 128;
const MAX_ENDPOINT: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryComponent {
    Context,
    Node,
    Topic,
    Service,
    Action,
    Parameter,
    Lifecycle,
    Graph,
    Clock,
    Transport,
}

/// Finite outcome labels keep metrics bounded and exclude error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryOutcome {
    Ok,
    Rejected,
    Inactive,
    Closed,
    Timeout,
    InvalidArgument,
    ResourceLimit,
    Validation,
    Unsupported,
    Remote,
    QueueOverflow,
    NoResponse,
    Transport,
}

impl From<&Error> for TelemetryOutcome {
    fn from(error: &Error) -> Self {
        match error {
            Error::Closed => Self::Closed,
            Error::Inactive(_) => Self::Inactive,
            Error::Timeout => Self::Timeout,
            Error::InvalidArgument(_) => Self::InvalidArgument,
            Error::ResourceLimit(_) => Self::ResourceLimit,
            Error::Validation(_) => Self::Validation,
            Error::Unsupported(_) => Self::Unsupported,
            Error::Remote(_) => Self::Remote,
            Error::QueueOverflow(_) => Self::QueueOverflow,
            Error::NoResponse(_) => Self::NoResponse,
            Error::Transport(_) => Self::Transport,
        }
    }
}

/// Describes evidence for the request ID on this event, not authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryCorrelationSource {
    OutgoingAttachment,
    IncomingAttachment,
    ReplyAttachment,
    QueryContext,
    MissingAttachment,
    InvalidAttachment,
    MismatchedAttachment,
}

/// A deliberately narrow event shape for current and future platform features.
/// Request/goal/node/endpoint identifiers never become metric labels.
#[derive(Debug, Clone, Serialize)]
pub struct TelemetryEvent {
    pub component: TelemetryComponent,
    /// A lower-case ASCII event name, using digits, `_` or `.`, at most 64 bytes.
    pub event: String,
    pub outcome: TelemetryOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_source: Option<TelemetryCorrelationSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ns: Option<u64>,
}

impl TelemetryEvent {
    pub fn new(component: TelemetryComponent, event: impl Into<String>) -> Self {
        Self {
            component,
            event: event.into(),
            outcome: TelemetryOutcome::Ok,
            node_id: None,
            endpoint: None,
            request_id: None,
            goal_id: None,
            correlation_source: None,
            bytes: None,
            duration_ns: None,
        }
    }

    fn valid(&self) -> bool {
        !self.event.is_empty()
            && self.event.len() <= MAX_EVENT_NAME
            && self.event.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.".contains(&byte)
            })
            && [&self.node_id, &self.request_id, &self.goal_id]
                .into_iter()
                .all(|value| {
                    value.as_ref().is_none_or(|value| {
                        !value.is_empty()
                            && value.len() <= MAX_IDENTIFIER
                            && value
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || b"_-:.".contains(&byte))
                    })
                })
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                !endpoint.is_empty()
                    && endpoint.len() <= MAX_ENDPOINT
                    && !endpoint.chars().any(char::is_control)
            })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryRecord {
    pub schema_version: u8,
    pub process_id: u32,
    pub context_id: String,
    pub sequence: u64,
    /// Decimal nanoseconds, kept as a string to avoid JSON number truncation.
    pub system_time_unix_ns: Option<String>,
    /// Elapsed monotonic time since this telemetry handle was created.
    pub steady_time_ns: u64,
    pub identity_trust: &'static str,
    #[serde(flatten)]
    pub event: TelemetryEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct TelemetryMetricKey {
    pub component: TelemetryComponent,
    pub event: String,
    pub outcome: TelemetryOutcome,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryMetric {
    #[serde(flatten)]
    pub key: TelemetryMetricKey,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetrySnapshot {
    pub schema_version: u8,
    pub process_id: u32,
    pub context_id: String,
    pub events_total: u64,
    pub failures_total: u64,
    pub bytes_total: u64,
    pub duration_ns_total: u64,
    pub rejected_events_total: u64,
    pub metric_keys_dropped_total: u64,
    pub metrics: Vec<TelemetryMetric>,
    /// Active owned resources; explicit close and final Drop decrement once.
    pub active_resources: BTreeMap<TelemetryComponent, u64>,
    pub log_capture_enabled: bool,
    pub log_capacity: usize,
    pub log_queued: usize,
    /// Records evicted by KEEP_LAST or not fully written after an I/O failure.
    pub log_dropped_total: u64,
    pub log_write_failures_total: u64,
}

#[derive(Clone)]
pub struct Telemetry {
    inner: Arc<Inner>,
}

struct Inner {
    process_id: u32,
    context_id: String,
    started: Instant,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    events_total: u64,
    failures_total: u64,
    bytes_total: u64,
    duration_ns_total: u64,
    rejected_events_total: u64,
    metric_keys_dropped_total: u64,
    metrics: BTreeMap<TelemetryMetricKey, u64>,
    active_resources: BTreeMap<TelemetryComponent, u64>,
    log_capture_enabled: bool,
    log_capacity: usize,
    records: VecDeque<TelemetryRecord>,
    log_dropped_total: u64,
    log_write_failures_total: u64,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                process_id: std::process::id(),
                context_id: uuid::Uuid::new_v4().to_string(),
                started: Instant::now(),
                state: Mutex::new(State::default()),
            }),
        }
    }
}

impl Telemetry {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub fn context_id(&self) -> &str {
        &self.inner.context_id
    }

    /// The adapter must actually send this identifier on the request. Merely
    /// allocating it does not establish remote receipt or authentication.
    pub fn new_request_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Records an event without I/O. Invalid or oversized fields are rejected
    /// and counted; diagnostics must not change the business operation result.
    pub fn record(&self, event: TelemetryEvent) -> bool {
        let mut state = self.lock();
        if !event.valid() {
            state.rejected_events_total = state.rejected_events_total.saturating_add(1);
            return false;
        }
        state.events_total = state.events_total.saturating_add(1);
        if event.outcome != TelemetryOutcome::Ok {
            state.failures_total = state.failures_total.saturating_add(1);
        }
        state.bytes_total = state.bytes_total.saturating_add(event.bytes.unwrap_or(0));
        state.duration_ns_total = state
            .duration_ns_total
            .saturating_add(event.duration_ns.unwrap_or(0));
        let key = TelemetryMetricKey {
            component: event.component,
            event: event.event.clone(),
            outcome: event.outcome,
        };
        if let Some(count) = state.metrics.get_mut(&key) {
            *count = count.saturating_add(1);
        } else if state.metrics.len() < MAX_TELEMETRY_METRIC_KEYS {
            state.metrics.insert(key, 1);
        } else {
            state.metric_keys_dropped_total = state.metric_keys_dropped_total.saturating_add(1);
        }
        if state.log_capture_enabled {
            if state.records.len() == state.log_capacity {
                state.records.pop_front();
                state.log_dropped_total = state.log_dropped_total.saturating_add(1);
            }
            let record = TelemetryRecord {
                schema_version: 1,
                process_id: self.inner.process_id,
                context_id: self.inner.context_id.clone(),
                sequence: state.events_total,
                system_time_unix_ns: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_nanos().to_string()),
                steady_time_ns: self
                    .inner
                    .started
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64,
                identity_trust: "unauthenticated",
                event,
            };
            state.records.push_back(record);
        }
        true
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        let state = self.lock();
        TelemetrySnapshot {
            schema_version: 1,
            process_id: self.inner.process_id,
            context_id: self.inner.context_id.clone(),
            events_total: state.events_total,
            failures_total: state.failures_total,
            bytes_total: state.bytes_total,
            duration_ns_total: state.duration_ns_total,
            rejected_events_total: state.rejected_events_total,
            metric_keys_dropped_total: state.metric_keys_dropped_total,
            metrics: state
                .metrics
                .iter()
                .map(|(key, count)| TelemetryMetric {
                    key: key.clone(),
                    count: *count,
                })
                .collect(),
            active_resources: state.active_resources.clone(),
            log_capture_enabled: state.log_capture_enabled,
            log_capacity: state.log_capacity,
            log_queued: state.records.len(),
            log_dropped_total: state.log_dropped_total,
            log_write_failures_total: state.log_write_failures_total,
        }
    }

    pub fn enable_log_capture(&self, capacity: usize) -> Result<()> {
        if !(1..=MAX_TELEMETRY_RECORDS).contains(&capacity) {
            return Err(Error::InvalidArgument(format!(
                "telemetry log capacity must be between 1 and {MAX_TELEMETRY_RECORDS}"
            )));
        }
        let mut state = self.lock();
        state.log_capacity = capacity;
        state.log_capture_enabled = true;
        while state.records.len() > capacity {
            state.records.pop_front();
            state.log_dropped_total = state.log_dropped_total.saturating_add(1);
        }
        Ok(())
    }

    /// Stops capture but preserves already queued records for an explicit drain.
    pub fn disable_log_capture(&self) {
        self.lock().log_capture_enabled = false;
    }

    pub fn drain_records(&self) -> Vec<TelemetryRecord> {
        self.lock().records.drain(..).collect()
    }

    /// Drains one bounded batch. I/O happens outside the record lock and may
    /// block the calling thread. Never call this from a transport callback.
    /// Failed output is not retried; incomplete/unwritten records are counted.
    pub fn drain_jsonl(&self, output: &mut impl Write) -> io::Result<usize> {
        let records = self.drain_records();
        for (index, record) in records.iter().enumerate() {
            let result = (|| {
                let mut bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
                bytes.push(b'\n');
                output.write_all(&bytes)
            })();
            if let Err(error) = result {
                let mut state = self.lock();
                state.log_write_failures_total = state.log_write_failures_total.saturating_add(1);
                state.log_dropped_total = state
                    .log_dropped_total
                    .saturating_add((records.len() - index) as u64);
                return Err(error);
            }
        }
        if let Err(error) = output.flush() {
            let mut state = self.lock();
            state.log_write_failures_total = state.log_write_failures_total.saturating_add(1);
            return Err(error);
        }
        Ok(records.len())
    }

    /// Writes only to stderr. Capture remains opt-in and no background writer
    /// or shutdown join is installed.
    pub fn flush_stderr(&self) -> io::Result<usize> {
        self.drain_jsonl(&mut io::stderr().lock())
    }

    pub fn resource(&self, component: TelemetryComponent) -> TelemetryResource {
        let mut state = self.lock();
        let count = state.active_resources.entry(component).or_default();
        *count = count.saturating_add(1);
        TelemetryResource {
            telemetry: self.clone(),
            component,
            closed: AtomicBool::new(false),
        }
    }
}

/// One resource gauge entry, safely decremented by either close or final Drop.
pub struct TelemetryResource {
    telemetry: Telemetry,
    component: TelemetryComponent,
    closed: AtomicBool,
}

impl TelemetryResource {
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut state = self.telemetry.lock();
        if let Some(count) = state.active_resources.get_mut(&self.component) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active_resources.remove(&self.component);
            }
        }
    }
}

impl Drop for TelemetryResource {
    fn drop(&mut self) {
        self.close();
    }
}

impl crate::clock::ShutdownResource for TelemetryResource {
    fn shutdown(&self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> TelemetryEvent {
        TelemetryEvent::new(TelemetryComponent::Service, "request_sent")
    }

    #[test]
    fn default_metrics_do_not_capture_records_or_label_request_identity() {
        let telemetry = Telemetry::new();
        for number in 0..1000 {
            let mut event = event();
            event.endpoint = Some(format!("/endpoint/{number}"));
            event.request_id = Some(Telemetry::new_request_id());
            event.bytes = Some(3);
            telemetry.record(event);
        }
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.events_total, 1000);
        assert_eq!(snapshot.bytes_total, 3000);
        assert_eq!(snapshot.metrics.len(), 1);
        assert_eq!(snapshot.metrics[0].count, 1000);
        assert!(!snapshot.log_capture_enabled);
        assert!(telemetry.drain_records().is_empty());
    }

    #[test]
    fn capture_retains_bounded_latest_records_and_reports_loss() {
        let telemetry = Telemetry::new();
        assert!(telemetry.enable_log_capture(0).is_err());
        assert!(telemetry
            .enable_log_capture(MAX_TELEMETRY_RECORDS + 1)
            .is_err());
        telemetry.enable_log_capture(2).unwrap();
        for _ in 0..4 {
            telemetry.record(event());
        }
        assert_eq!(telemetry.snapshot().log_queued, 2);
        assert_eq!(telemetry.snapshot().log_dropped_total, 2);
        telemetry.disable_log_capture();
        telemetry.record(event());
        let records = telemetry.drain_records();
        assert_eq!(
            records
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        assert_eq!(telemetry.snapshot().log_queued, 0);
        assert!(!telemetry.snapshot().log_capture_enabled);
    }

    #[test]
    fn metric_cardinality_and_event_field_sizes_are_bounded() {
        let telemetry = Telemetry::new();
        for number in 0..100 {
            telemetry.record(TelemetryEvent::new(
                TelemetryComponent::Lifecycle,
                format!("event_{number}"),
            ));
        }
        let mut oversized = event();
        oversized.endpoint = Some("x".repeat(MAX_ENDPOINT + 1));
        assert!(!telemetry.record(oversized));
        assert!(!telemetry.record(TelemetryEvent::new(
            TelemetryComponent::Service,
            "message\ninjection",
        )));
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.metrics.len(), MAX_TELEMETRY_METRIC_KEYS);
        assert_eq!(snapshot.metric_keys_dropped_total, 36);
        assert_eq!(snapshot.rejected_events_total, 2);
        assert_eq!(snapshot.events_total, 100);
    }

    #[test]
    fn jsonl_excludes_error_text_and_marks_identity_unverified() {
        let telemetry = Telemetry::new();
        telemetry.enable_log_capture(2).unwrap();
        let mut event = event();
        event.outcome = TelemetryOutcome::from(&Error::Remote("secret_payload".into()));
        event.request_id = Some(Telemetry::new_request_id());
        telemetry.record(event);
        let mut output = Vec::new();
        assert_eq!(telemetry.drain_jsonl(&mut output).unwrap(), 1);
        let text = String::from_utf8(output).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(!text.contains("secret_payload"));
        let record: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(record["outcome"], "remote");
        assert_eq!(record["identity_trust"], "unauthenticated");
        assert_eq!(record["context_id"], telemetry.context_id());
        assert!(record.get("payload").is_none());
        assert_eq!(telemetry.snapshot().failures_total, 1);
    }

    #[test]
    fn failed_output_is_counted_and_is_not_retried() {
        struct Failure;
        impl Write for Failure {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let telemetry = Telemetry::new();
        telemetry.enable_log_capture(2).unwrap();
        telemetry.record(event());
        telemetry.record(event());
        assert!(telemetry.drain_jsonl(&mut Failure).is_err());
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.log_write_failures_total, 1);
        assert_eq!(snapshot.log_dropped_total, 2);
        assert_eq!(snapshot.log_queued, 0);
    }

    #[test]
    fn output_does_not_hold_the_record_lock() {
        struct ReentrantWriter(Telemetry);
        impl Write for ReentrantWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                assert!(self.0.inner.state.try_lock().is_ok());
                self.0.record(event());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let telemetry = Telemetry::new();
        telemetry.enable_log_capture(2).unwrap();
        telemetry.record(event());
        assert_eq!(
            telemetry
                .drain_jsonl(&mut ReentrantWriter(telemetry.clone()))
                .unwrap(),
            1
        );
        assert_eq!(telemetry.snapshot().log_queued, 1);
    }

    #[test]
    fn resource_close_and_drop_remove_gauges_once() {
        let telemetry = Telemetry::new();
        let first = telemetry.resource(TelemetryComponent::Service);
        let second = telemetry.resource(TelemetryComponent::Service);
        assert_eq!(
            telemetry.snapshot().active_resources[&TelemetryComponent::Service],
            2
        );
        first.close();
        first.close();
        drop(first);
        assert_eq!(
            telemetry.snapshot().active_resources[&TelemetryComponent::Service],
            1
        );
        drop(second);
        assert!(telemetry.snapshot().active_resources.is_empty());
    }
}
