// Portions of the query/reply configuration are adapted from Hiroz
// d9f979a0404311de12f6884bafaa9b20600eb490 (Apache-2.0 selected).
// Modified for AOS JSON interfaces, ownership and closure. See upstream/source-map.json
// and upstream/licenses/hiroz-LICENSE for source locations and license.

//! Query/reply adapted from Hiroz d9f979a0404311de12f6884bafaa9b20600eb490
//! `crates/hiroz/src/service.rs`. Zenoh owns request correlation. Its callback
//! enqueues an owned Query, never a Python or application callback.
//!
//! Request KEEP_LAST eviction sends a best-effort versioned error reply before
//! dropping the Query. It rejects execution only at the evicting provider,
//! so a client keeps it provisional until all providers finish. A later
//! business reply can still win; a real deadline remains Timeout. Inactive
//! providers use the same provisional rule. If all providers finish early
//! without a business reply, the first recorded rejection determines the error.
//! Completed-reply eviction reports local QueueOverflow: provider work may
//! already have run. Neither error triggers a retry. Query completion without
//! a reply is NoResponse if observed before the deadline, otherwise Timeout;
//! NoResponse does not establish whether provider work ran.
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use aos_core::transport::{ClientBackend, QoS, QueueStats, RequestBackend, ServiceBackend};
use aos_core::{
    Error, Result, TelemetryComponent, TelemetryCorrelationSource, TelemetryOutcome,
    TelemetryResource,
};
use zenoh::key_expr::OwnedKeyExpr;
use zenoh::query::{ConsolidationMode, Query, QueryTarget, Queryable, ReplyError};
use zenoh::Wait;

use crate::byte_budget::{check_send, Rejection};
use crate::queue::{FinishOnDrop, Queue};
use crate::reply_queue::{FinishReplyOnDrop, ReplyEvent, ReplyQueue, ReplyValue};
use crate::session::{failure, Shared};
use crate::telemetry::{attachment, Correlation, EndpointTelemetry, QueryTelemetry};

/// GetResult protocol ceiling from rmw_zenoh
/// dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e,
/// rmw_zenoh_cpp/src/detail/rmw_client_data.cpp:138-149.
/// This avoids Zenoh's ordinary 10-second query timeout for deferred Action
/// results. It is the protocol's maximum milliseconds, not mathematical infinity.
const GET_RESULT_PROTOCOL_TIMEOUT: Duration = Duration::from_millis(u64::MAX);

// This encoding and exact payload form the versioned adapter error protocol.
// Ordinary provider errors, including text containing this code, stay Remote.
const SERVICE_ERROR_ENCODING: &str = "application/vnd.aos.service-error.v1";
const SERVICE_QUEUE_OVERFLOW: &str = "SERVICE_QUEUE_OVERFLOW";
const SERVICE_INACTIVE: &str = "SERVICE_INACTIVE";
const SERVICE_REQUEST_TOO_LARGE: &str = "SERVICE_REQUEST_TOO_LARGE";
const SERVICE_REQUEST_CAPACITY: &str = "SERVICE_REQUEST_CAPACITY";
const SERVICE_REPLY_TOO_LARGE: &str = "SERVICE_REPLY_TOO_LARGE";

pub(crate) struct ZenohService {
    queryable: Mutex<Option<Queryable<()>>>,
    queue: Arc<Queue<Query>>,
    shared: Arc<Shared>,
    telemetry: EndpointTelemetry,
    resource: Arc<TelemetryResource>,
}

impl ZenohService {
    pub(crate) fn declare(
        session: &zenoh::Session,
        key: OwnedKeyExpr,
        qos: &QoS,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        qos.validate_service()?;
        let queue = Arc::new(Queue::with_limits(qos.depth, shared.queue_limits));
        shared.register(&queue)?;
        let sink = queue.clone();
        let resource = shared.resource(TelemetryComponent::Service)?;
        let telemetry = EndpointTelemetry::new(
            shared.telemetry.clone(),
            TelemetryComponent::Service,
            key.as_str(),
        );
        let trace = telemetry.clone();
        let queryable = session
            .declare_queryable(key)
            .complete(true)
            .callback(move |query| {
                let bytes = query.payload().map_or(0, |payload| payload.len());
                let mut event = trace.event("zenoh.service.request_received");
                Correlation::incoming(query.attachment()).apply(&mut event);
                event.bytes = Some(bytes as u64);
                trace.record(event);
                let mut incoming = Some(query);
                let result = sink.try_push_with(
                    bytes,
                    || incoming.take().unwrap(),
                    |evicted| reject_request(evicted, SERVICE_QUEUE_OVERFLOW, &trace, true),
                );
                if let Some(query) = incoming {
                    match result {
                        Err(Rejection::Oversized) => {
                            reject_request(query, SERVICE_REQUEST_TOO_LARGE, &trace, false);
                        }
                        Err(Rejection::Capacity) => {
                            reject_request(query, SERVICE_REQUEST_CAPACITY, &trace, false);
                        }
                        // Close releases a late Query outside the queue lock.
                        _ => {}
                    }
                }
            })
            .wait()
            .map_err(|error| {
                queue.close();
                failure(&shared, "declare service", error)
            })?;
        telemetry.record(telemetry.event("zenoh.service.declare"));
        Ok(Self {
            queryable: Mutex::new(Some(queryable)),
            queue,
            shared,
            telemetry,
            resource,
        })
    }
}

fn reject_request(query: Query, code: &'static str, trace: &EndpointTelemetry, evicted: bool) {
    // This rejects only this provider. Another matching provider can still
    // execute the same request. No queue lock is held while replying/dropping.
    let mut event = trace.event(if evicted {
        "zenoh.service.request_evicted"
    } else {
        "zenoh.service.request_rejected"
    });
    Correlation::incoming(query.attachment()).apply(&mut event);
    event.outcome = TelemetryOutcome::QueueOverflow;
    trace.record(event);
    let _ = query
        .reply_err(code)
        .encoding(SERVICE_ERROR_ENCODING)
        .wait();
}

impl ServiceBackend for ZenohService {
    fn recv_timeout(&self, timeout: Duration) -> Result<Box<dyn RequestBackend>> {
        let query = self.queue.recv_timeout(timeout)?;
        let payload = query
            .payload()
            .map(|payload| payload.to_bytes().into_owned())
            .unwrap_or_default();
        let correlation = Correlation::incoming(query.attachment());
        let mut event = self.telemetry.event("zenoh.service.request_taken");
        correlation.apply(&mut event);
        self.telemetry.record(event);
        Ok(Box::new(ZenohRequest {
            query: Mutex::new(Some(query)),
            payload,
            queue: self.queue.clone(),
            shared: self.shared.clone(),
            telemetry: self.telemetry.clone(),
            correlation,
        }))
    }

    fn close(&self) -> Result<()> {
        self.queue.close();
        self.resource.close();
        let queryable = self
            .queryable
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(Error::Closed)?;
        if self.shared.is_closed() {
            return Err(Error::Closed);
        }
        queryable
            .undeclare()
            .wait()
            .map_err(|error| failure(&self.shared, "undeclare service", error))
    }

    fn queue_stats(&self) -> Option<QueueStats> {
        Some(self.queue.stats())
    }
}

impl Drop for ZenohService {
    fn drop(&mut self) {
        self.queue.close();
    }
}

struct ZenohRequest {
    query: Mutex<Option<Query>>,
    payload: Vec<u8>,
    queue: Arc<Queue<Query>>,
    shared: Arc<Shared>,
    telemetry: EndpointTelemetry,
    correlation: Correlation,
}

impl RequestBackend for ZenohRequest {
    fn request_id(&self) -> Option<&str> {
        self.correlation.request_id.as_deref()
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn reply(&self, response: Result<Vec<u8>>) -> Result<()> {
        let query = self
            .query
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(Error::Closed)?;
        if self.shared.is_closed() || self.queue.is_closed() {
            return Err(Error::Closed);
        }
        let mut event = self.telemetry.event("zenoh.service.reply_sent");
        self.correlation.apply(&mut event);
        let mut local_limit_error = None;
        let result = match response {
            Ok(payload) => {
                event.bytes = Some(payload.len() as u64);
                if let Some(error) = reply_limit_error(&self.shared, payload.len()) {
                    local_limit_error = Some(error);
                    query
                        .reply_err(SERVICE_REPLY_TOO_LARGE)
                        .encoding(SERVICE_ERROR_ENCODING)
                        .wait()
                } else {
                    let mut reply = query.reply(query.key_expr().clone(), payload);
                    if let Some(id) = self.request_id() {
                        reply = reply.attachment(attachment(id));
                        event.correlation_source =
                            Some(TelemetryCorrelationSource::ReplyAttachment);
                    }
                    reply.wait()
                }
            }
            Err(Error::Inactive(_)) => {
                event.outcome = TelemetryOutcome::Inactive;
                event.correlation_source = Some(TelemetryCorrelationSource::QueryContext);
                query
                    .reply_err(SERVICE_INACTIVE)
                    .encoding(SERVICE_ERROR_ENCODING)
                    .wait()
            }
            // Router-generated timeouts use BYTES; session-generated ones use STRING.
            // Give provider failures a separate encoding, even for text "Timeout".
            Err(error) => {
                event.outcome = TelemetryOutcome::from(&error);
                event.correlation_source = Some(TelemetryCorrelationSource::QueryContext);
                let payload = error.to_string().into_bytes();
                if let Some(error) = reply_limit_error(&self.shared, payload.len()) {
                    local_limit_error = Some(error);
                    query
                        .reply_err(SERVICE_REPLY_TOO_LARGE)
                        .encoding(SERVICE_ERROR_ENCODING)
                        .wait()
                } else {
                    query
                        .reply_err(payload)
                        .encoding(zenoh::bytes::Encoding::TEXT_PLAIN)
                        .wait()
                }
            }
        }
        .map_err(|error| failure(&self.shared, "reply service", error))
        .and_then(|()| local_limit_error.map_or(Ok(()), Err));
        if let Err(error) = &result {
            event.outcome = TelemetryOutcome::from(error);
        }
        self.telemetry.record(event);
        result
    }
}

fn reply_limit_error(shared: &Shared, bytes: usize) -> Option<Error> {
    (bytes > shared.queue_limits.message_bytes).then(|| {
        Error::QueueOverflow(format!(
            "{SERVICE_REPLY_TOO_LARGE}: {bytes} response bytes exceed the local limit of {}; \
             this response payload was not submitted, but provider work may have executed; \
             no automatic retry",
            shared.queue_limits.message_bytes
        ))
    })
}

/// Zenoh 1.10.1 session and router deadlines use STRING and BYTES respectively.
/// Provider failures use TEXT_PLAIN, so provider text "Timeout" stays Remote.
pub(crate) fn is_query_timeout(error: &ReplyError) -> bool {
    (error.encoding() == &zenoh::bytes::Encoding::ZENOH_STRING
        || error.encoding() == &zenoh::bytes::Encoding::ZENOH_BYTES)
        && error.payload().len() == b"Timeout".len()
        && error.payload().to_bytes().as_ref() == b"Timeout"
}

fn service_error_is(error: &ReplyError, code: &str) -> bool {
    error.encoding() == &zenoh::bytes::Encoding::from(SERVICE_ERROR_ENCODING)
        && error.payload().len() == code.len()
        && error.payload().to_bytes().as_ref() == code.as_bytes()
}

fn reply_payload_size(reply: &zenoh::query::Reply) -> usize {
    match reply.result() {
        Ok(sample) => sample.payload().len(),
        // Fixed adapter/middleware control errors carry no business payload.
        Err(error)
            if is_query_timeout(error) || service_error_is(error, SERVICE_REPLY_TOO_LARGE) =>
        {
            0
        }
        Err(error) => error.payload().len(),
    }
}

fn copy_reply(reply: zenoh::query::Reply) -> ReplyValue {
    match reply.into_result() {
        Ok(sample) => ReplyValue::Success(sample.payload().to_bytes().into_owned()),
        Err(error) if is_query_timeout(&error) => ReplyValue::Timeout,
        Err(error) if service_error_is(&error, SERVICE_REPLY_TOO_LARGE) => {
            ReplyValue::ProviderResponseTooLarge
        }
        Err(error) => ReplyValue::Remote(error.payload().to_bytes().into_owned()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ProviderRejection {
    QueueOverflow = 1,
    Inactive = 2,
    RequestTooLarge = 3,
    RequestCapacity = 4,
}

impl ProviderRejection {
    pub(crate) fn outcome(self) -> TelemetryOutcome {
        match self {
            Self::QueueOverflow | Self::RequestTooLarge | Self::RequestCapacity => {
                TelemetryOutcome::QueueOverflow
            }
            Self::Inactive => TelemetryOutcome::Inactive,
        }
    }
}

pub(crate) fn provider_rejection(error: &ReplyError) -> Option<ProviderRejection> {
    if error.encoding() != &zenoh::bytes::Encoding::from(SERVICE_ERROR_ENCODING)
        || error.payload().len() > SERVICE_REQUEST_TOO_LARGE.len()
    {
        return None;
    }
    match error.payload().to_bytes().as_ref() {
        code if code == SERVICE_QUEUE_OVERFLOW.as_bytes() => Some(ProviderRejection::QueueOverflow),
        code if code == SERVICE_INACTIVE.as_bytes() => Some(ProviderRejection::Inactive),
        code if code == SERVICE_REQUEST_TOO_LARGE.as_bytes() => {
            Some(ProviderRejection::RequestTooLarge)
        }
        code if code == SERVICE_REQUEST_CAPACITY.as_bytes() => {
            Some(ProviderRejection::RequestCapacity)
        }
        _ => None,
    }
}

/// The first observed provider rejection is provisional, never a retry signal.
#[derive(Default)]
struct ProvisionalRejection(AtomicU8);

impl ProvisionalRejection {
    fn remember(&self, rejection: ProviderRejection) {
        let _ = self
            .0
            .compare_exchange(0, rejection as u8, Ordering::AcqRel, Ordering::Acquire);
    }

    fn current(&self) -> Option<ProviderRejection> {
        match self.0.load(Ordering::Acquire) {
            1 => Some(ProviderRejection::QueueOverflow),
            2 => Some(ProviderRejection::Inactive),
            3 => Some(ProviderRejection::RequestTooLarge),
            4 => Some(ProviderRejection::RequestCapacity),
            _ => None,
        }
    }
}

fn query_finished_error(
    started: Instant,
    timeout: Option<Duration>,
    finished_at: Instant,
    provider_rejection: Option<ProviderRejection>,
) -> Error {
    if timeout.is_some_and(|timeout| finished_at.saturating_duration_since(started) >= timeout) {
        Error::Timeout
    } else if let Some(
        rejection @ (ProviderRejection::QueueOverflow
        | ProviderRejection::RequestTooLarge
        | ProviderRejection::RequestCapacity),
    ) = provider_rejection
    {
        let code = match rejection {
            ProviderRejection::RequestTooLarge => SERVICE_REQUEST_TOO_LARGE,
            ProviderRejection::RequestCapacity => SERVICE_REQUEST_CAPACITY,
            _ => SERVICE_QUEUE_OVERFLOW,
        };
        Error::QueueOverflow(format!(
            "{code}: the rejecting provider discarded this request before executing its handler; \
             other providers may have received it; no automatic retry"
        ))
    } else if provider_rejection == Some(ProviderRejection::Inactive) {
        Error::Inactive(
            "SERVICE_INACTIVE: a provider rejected this request before admitting business work; \
             other providers may have received it; no automatic retry"
                .into(),
        )
    } else {
        Error::NoResponse(
            "query finished without a response; provider execution is unknown; no automatic retry"
                .into(),
        )
    }
}

pub(crate) struct ZenohClient {
    session: zenoh::Session,
    key: OwnedKeyExpr,
    shared: Arc<Shared>,
    replies: Arc<ReplyQueue>,
    allow_no_deadline: bool,
    telemetry: EndpointTelemetry,
    resource: Arc<TelemetryResource>,
}

impl ZenohClient {
    pub(crate) fn new(
        session: &zenoh::Session,
        key: OwnedKeyExpr,
        qos: &QoS,
        shared: Arc<Shared>,
        allow_no_deadline: bool,
    ) -> Result<Self> {
        qos.validate_service()?;
        let replies = Arc::new(ReplyQueue::with_limits(qos.depth, shared.queue_limits));
        shared.register(&replies)?;
        let resource = shared.resource(TelemetryComponent::Service)?;
        let telemetry = EndpointTelemetry::new(
            shared.telemetry.clone(),
            TelemetryComponent::Service,
            key.as_str(),
        );
        telemetry.record(telemetry.event("zenoh.client.declare"));
        Ok(Self {
            session: session.clone(),
            key,
            shared,
            replies,
            allow_no_deadline,
            telemetry,
            resource,
        })
    }

    // The persistent client queue is intentional: concurrent call_async users
    // can leave completed replies waiting. Per-call depth or metadata-only
    // depth would not implement a Client's KEEP_LAST profile.
    fn begin_query(&self, payload: &[u8], timeout: Option<Duration>) -> Result<PendingQuery> {
        if self.shared.is_closed() || self.replies.stats().closed {
            return Err(Error::Closed);
        }
        if timeout.is_none() && !self.allow_no_deadline {
            return Err(Error::Unsupported(
                "service wait without deadline is reserved for Action GetResult".into(),
            ));
        }
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(Error::Timeout);
        }
        check_send(self.shared.queue_limits, payload.len(), "SERVICE_REQUEST")?;
        let id = self.replies.register()?;
        let pending = PendingQuery {
            id,
            replies: self.replies.clone(),
            cancellation: zenoh::cancellation::CancellationToken::default(),
            started: Instant::now(),
            timeout,
            provider_rejection: Arc::default(),
            telemetry: QueryTelemetry::new(self.telemetry.clone()),
        };
        let sink = FinishReplyOnDrop {
            queue: self.replies.clone(),
            id,
        };
        let settled = AtomicBool::new(false);
        let rejection = pending.provider_rejection.clone();
        let trace = pending.telemetry.clone();
        let submitted = self
            .session
            .get(self.key.clone())
            .payload(payload.to_vec())
            .attachment(attachment(&pending.telemetry.request_id))
            .timeout(timeout.unwrap_or(GET_RESULT_PROTOCOL_TIMEOUT))
            .target(if timeout.is_none() {
                QueryTarget::AllComplete
            } else {
                QueryTarget::All
            })
            // Zenoh 1.10.1 replies inherit the query congestion control;
            // ReplyBuilder::congestion_control itself has no effect.
            .congestion_control(zenoh::qos::CongestionControl::Block)
            .consolidation(ConsolidationMode::None)
            .cancellation_token(pending.cancellation.clone())
            .callback(move |reply| {
                if let Some(code) = reply.result().err().and_then(provider_rejection) {
                    // QueryTarget::All may reach another provider that can
                    // still complete this request. A rejection is provisional
                    // and does not occupy completed-reply KEEP_LAST depth.
                    rejection.remember(code);
                    trace.reply(&reply);
                    return;
                }
                trace.reply(&reply);
                if settled.swap(true, Ordering::AcqRel) {
                    return;
                }
                let bytes = reply_payload_size(&reply);
                let _ = sink
                    .queue
                    .try_push_with(sink.id, bytes, || copy_reply(reply));
            })
            .wait()
            .map_err(|error| failure(&self.shared, "query service", error));
        if let Err(error) = submitted {
            pending.telemetry.finished(&Err(error.clone()));
            return Err(error);
        }
        pending.telemetry.submitted(payload.len());
        Ok(pending)
    }
}

impl ClientBackend for ZenohClient {
    fn call_wait(
        &self,
        payload: &[u8],
        timeout: Option<Duration>,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>> {
        if cancel.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        self.begin_query(payload, timeout)?.wait(cancel)
    }

    fn queue_stats(&self) -> Option<QueueStats> {
        Some(self.replies.stats())
    }

    fn close(&self) -> Result<()> {
        if self.replies.stats().closed {
            return Err(Error::Closed);
        }
        self.replies.close();
        self.resource.close();
        Ok(())
    }
}

impl Drop for ZenohClient {
    fn drop(&mut self) {
        self.replies.close();
    }
}

// This split stays inside the adapter: it lets issue and consumption share
// one Client queue without adding a second RPC lifecycle to aos-core.
struct PendingQuery {
    id: u64,
    replies: Arc<ReplyQueue>,
    cancellation: zenoh::cancellation::CancellationToken,
    started: Instant,
    timeout: Option<Duration>,
    provider_rejection: Arc<ProvisionalRejection>,
    telemetry: QueryTelemetry,
}

impl PendingQuery {
    fn wait(self, cancel: &AtomicBool) -> Result<Vec<u8>> {
        let response = self.wait_result(cancel);
        self.telemetry.finished(&response);
        response
    }

    fn wait_result(&self, cancel: &AtomicBool) -> Result<Vec<u8>> {
        loop {
            if cancel.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let remaining = self
                .timeout
                .map(|timeout| timeout.saturating_sub(self.started.elapsed()));
            let wait = remaining
                .unwrap_or(Duration::from_millis(50))
                .min(Duration::from_millis(50));
            match self.replies.recv_timeout(self.id, wait) {
                Ok(ReplyEvent::Response(response)) => return response,
                Ok(ReplyEvent::Finished(finished_at)) => {
                    return Err(query_finished_error(
                        self.started,
                        self.timeout,
                        finished_at,
                        self.provider_rejection.current(),
                    ));
                }
                Err(Error::Timeout)
                    if self
                        .timeout
                        .is_none_or(|timeout| self.started.elapsed() < timeout) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for PendingQuery {
    fn drop(&mut self) {
        // Cancel, timeout and Drop also remove completed entries, so abandoned
        // calls cannot keep occupying the client's reply depth.
        self.replies.remove(self.id);
        // Local query cancellation never cancels provider work or retries it.
        let _ = self.cancellation.cancel().wait();
    }
}

pub(crate) fn call(
    session: &zenoh::Session,
    key: OwnedKeyExpr,
    shared: &Arc<Shared>,
    payload: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>> {
    call_with_cancel(session, key, shared, payload, timeout, None)
}

pub(crate) fn call_with_cancel(
    session: &zenoh::Session,
    key: OwnedKeyExpr,
    shared: &Arc<Shared>,
    payload: &[u8],
    timeout: Duration,
    cancel: Option<&AtomicBool>,
) -> Result<Vec<u8>> {
    call_wait(session, key, shared, payload, Some(timeout), cancel)
}

pub(crate) fn call_bounded(
    session: &zenoh::Session,
    key: OwnedKeyExpr,
    shared: &Arc<Shared>,
    payload: &[u8],
    timeout: Duration,
    max_response_bytes: usize,
) -> Result<Vec<u8>> {
    if max_response_bytes == 0 {
        return Err(Error::InvalidArgument(
            "reply byte limit must be positive".into(),
        ));
    }
    call_wait_bounded(
        session,
        key,
        shared,
        payload,
        Some(timeout),
        None,
        Some(max_response_bytes),
    )
}

/// None is reserved by ZenohTransport for the ROS GetResult special case.
pub(crate) fn call_wait(
    session: &zenoh::Session,
    key: OwnedKeyExpr,
    shared: &Arc<Shared>,
    payload: &[u8],
    timeout: Option<Duration>,
    cancel: Option<&AtomicBool>,
) -> Result<Vec<u8>> {
    call_wait_bounded(session, key, shared, payload, timeout, cancel, None)
}

fn call_wait_bounded(
    session: &zenoh::Session,
    key: OwnedKeyExpr,
    shared: &Arc<Shared>,
    payload: &[u8],
    timeout: Option<Duration>,
    cancel: Option<&AtomicBool>,
    max_response_bytes: Option<usize>,
) -> Result<Vec<u8>> {
    if cancel.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err(Error::Closed);
    }
    if shared.is_closed() {
        return Err(Error::Closed);
    }
    check_send(shared.queue_limits, payload.len(), "SERVICE_REQUEST")?;
    // Queue reservation runs before copy_reply for both successful and error
    // replies. A per-call cap never relaxes the adapter's existing byte gate.
    let queue = Arc::new(Queue::with_limits(
        1,
        bounded_reply_limits(shared.queue_limits, max_response_bytes),
    ));
    shared.register(&queue)?;
    if timeout.is_some_and(|timeout| timeout.is_zero()) {
        return Err(Error::Timeout);
    }
    let sink = FinishOnDrop(queue.clone());
    let settled = AtomicBool::new(false);
    let started = Instant::now();
    let cancellation = zenoh::cancellation::CancellationToken::default();
    let provisional = Arc::new(ProvisionalRejection::default());
    let rejection = provisional.clone();
    let telemetry = QueryTelemetry::new(EndpointTelemetry::new(
        shared.telemetry.clone(),
        TelemetryComponent::Service,
        key.as_str(),
    ));
    let trace = telemetry.clone();
    let submitted = session
        .get(key)
        .payload(payload.to_vec())
        .attachment(attachment(&telemetry.request_id))
        .timeout(timeout.unwrap_or(GET_RESULT_PROTOCOL_TIMEOUT))
        .target(if timeout.is_none() {
            QueryTarget::AllComplete
        } else {
            QueryTarget::All
        })
        .consolidation(ConsolidationMode::None)
        .cancellation_token(cancellation.clone())
        .callback(move |reply| {
            if let Some(code) = reply.result().err().and_then(provider_rejection) {
                rejection.remember(code);
                trace.reply(&reply);
                return;
            }
            trace.reply(&reply);
            if settled.swap(true, Ordering::AcqRel) {
                return;
            }
            let bytes = reply_payload_size(&reply);
            if let Err(rejection) = sink.0.try_push_with(bytes, || copy_reply(reply), drop) {
                sink.0.fail(rejection.local_reply_error());
            }
        })
        .wait()
        .map_err(|error| failure(shared, "query service", error));
    if let Err(error) = submitted {
        telemetry.finished(&Err(error.clone()));
        return Err(error);
    }
    telemetry.submitted(payload.len());
    let response = loop {
        if cancel.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            break Err(Error::Closed);
        }
        let remaining = timeout.map(|timeout| timeout.saturating_sub(started.elapsed()));
        let wait = match remaining {
            Some(remaining) if cancel.is_none() => remaining,
            Some(remaining) => remaining.min(Duration::from_millis(50)),
            None => Duration::from_millis(50),
        };
        match queue.recv_timeout(wait) {
            Ok(response) => break response.into_result(),
            Err(Error::Timeout) => {
                if let Some(finished_at) = queue.finished_at() {
                    break Err(query_finished_error(
                        started,
                        timeout,
                        finished_at,
                        provisional.current(),
                    ));
                }
                if timeout.is_none_or(|timeout| started.elapsed() < timeout) {
                    continue;
                }
                break Err(Error::Timeout);
            }
            Err(error) => break Err(error),
        }
    };
    queue.close();
    // Cancel only the local query wait, not provider execution or business work.
    let _ = cancellation.cancel().wait();
    telemetry.finished(&response);
    response
}

fn bounded_reply_limits(
    mut limits: aos_core::QueueLimits,
    maximum: Option<usize>,
) -> aos_core::QueueLimits {
    if let Some(maximum) = maximum {
        limits.message_bytes = limits.message_bytes.min(maximum);
    }
    limits
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
