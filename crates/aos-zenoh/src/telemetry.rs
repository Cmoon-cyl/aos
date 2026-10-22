//! Diagnostic attachments never affect routing, authorization, or business results.
//! `zenoh.*` events identify the actual wire key in `endpoint`. They deliberately
//! omit Node identity, which is owned by core rather than inferred from the wire.
use std::time::Instant;

use aos_core::{
    Result, Telemetry, TelemetryComponent, TelemetryCorrelationSource, TelemetryEvent,
    TelemetryOutcome,
};
use zenoh::bytes::ZBytes;

const MAX_ATTACHMENT_BYTES: usize = 128;

#[derive(Clone)]
pub(crate) struct EndpointTelemetry {
    pub(crate) telemetry: Telemetry,
    component: TelemetryComponent,
    endpoint: String,
}

impl EndpointTelemetry {
    pub(crate) fn new(
        telemetry: Telemetry,
        component: TelemetryComponent,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            telemetry,
            component,
            endpoint: endpoint.into(),
        }
    }

    pub(crate) fn event(&self, name: &str) -> TelemetryEvent {
        let mut event = TelemetryEvent::new(self.component, name);
        event.endpoint = Some(self.endpoint.clone());
        event
    }

    pub(crate) fn record(&self, event: TelemetryEvent) {
        self.telemetry.record(event);
    }
}

#[derive(Clone)]
pub(crate) struct Correlation {
    pub(crate) request_id: Option<String>,
    pub(crate) source: TelemetryCorrelationSource,
}

impl Correlation {
    pub(crate) fn incoming(attachment: Option<&ZBytes>) -> Self {
        let Some(attachment) = attachment else {
            return Self {
                request_id: None,
                source: TelemetryCorrelationSource::MissingAttachment,
            };
        };
        let request_id = decode_attachment(attachment);
        Self {
            source: if request_id.is_some() {
                TelemetryCorrelationSource::IncomingAttachment
            } else {
                TelemetryCorrelationSource::InvalidAttachment
            },
            request_id,
        }
    }

    pub(crate) fn apply(&self, event: &mut TelemetryEvent) {
        event.request_id.clone_from(&self.request_id);
        event.correlation_source = Some(self.source);
    }
}

pub(crate) fn attachment(request_id: &str) -> Vec<u8> {
    // IDs here are locally generated or strictly decoded below, not free text.
    format!(r#"{{"schema_version":1,"request_id":"{request_id}"}}"#).into_bytes()
}

fn decode_attachment(attachment: &ZBytes) -> Option<String> {
    if attachment.len() > MAX_ATTACHMENT_BYTES {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&attachment.to_bytes()).ok()?;
    let fields = value.as_object()?;
    if fields.len() != 2 || fields.get("schema_version")?.as_u64()? != 1 {
        return None;
    }
    let id = fields.get("request_id")?.as_str()?;
    if id.len() != 36
        || !id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
    {
        return None;
    }
    Some(id.to_owned())
}

/// One local call's diagnostics, not a second request state machine.
#[derive(Clone)]
pub(crate) struct QueryTelemetry {
    pub(crate) endpoint: EndpointTelemetry,
    pub(crate) request_id: String,
    started: Instant,
}

impl QueryTelemetry {
    pub(crate) fn new(endpoint: EndpointTelemetry) -> Self {
        Self {
            endpoint,
            request_id: Telemetry::new_request_id(),
            started: Instant::now(),
        }
    }

    pub(crate) fn event(&self, name: &str) -> TelemetryEvent {
        let mut event = self.endpoint.event(name);
        event.request_id = Some(self.request_id.clone());
        event.correlation_source = Some(TelemetryCorrelationSource::QueryContext);
        event
    }

    pub(crate) fn submitted(&self, bytes: usize) {
        let mut event = self.event("zenoh.service.query_submitted");
        event.correlation_source = Some(TelemetryCorrelationSource::OutgoingAttachment);
        event.bytes = Some(bytes as u64);
        self.endpoint.record(event);
    }

    pub(crate) fn reply(&self, reply: &zenoh::query::Reply) {
        let mut event = self.event("zenoh.service.reply_received");
        match reply.result() {
            Ok(sample) => {
                let correlation = Correlation::incoming(sample.attachment());
                event.correlation_source = Some(match correlation.request_id.as_deref() {
                    Some(id) if id == self.request_id => {
                        TelemetryCorrelationSource::ReplyAttachment
                    }
                    Some(_) => TelemetryCorrelationSource::MismatchedAttachment,
                    None => correlation.source,
                });
                event.bytes = Some(sample.payload().len() as u64);
            }
            Err(error) => {
                event.outcome = if crate::service::is_query_timeout(error) {
                    TelemetryOutcome::Timeout
                } else if let Some(rejection) = crate::service::provider_rejection(error) {
                    rejection.outcome()
                } else {
                    TelemetryOutcome::Remote
                };
                // Zenoh 1.10.1 ReplyErrBuilder has no attachment method.
                // Correlation is the actual local query callback's context.
            }
        }
        self.endpoint.record(event);
    }

    pub(crate) fn finished(&self, result: &Result<Vec<u8>>) {
        let mut event = self.event("zenoh.service.call_finished");
        event.outcome = result
            .as_ref()
            .err()
            .map_or(TelemetryOutcome::Ok, TelemetryOutcome::from);
        event.duration_ns = Some(self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        self.endpoint.record(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachments_are_bounded_versioned_and_do_not_accept_free_text_ids() {
        let id = Telemetry::new_request_id();
        assert_eq!(
            Correlation::incoming(Some(&ZBytes::from(attachment(&id)))).request_id,
            Some(id.clone())
        );
        assert_eq!(
            Correlation::incoming(None).source,
            TelemetryCorrelationSource::MissingAttachment
        );
        for value in [
            vec![b' '; MAX_ATTACHMENT_BYTES + 1],
            br#"{"schema_version":1,"request_id":"secret\nvalue"}"#.to_vec(),
            format!(r#"{{"schema_version":2,"request_id":"{id}"}}"#).into_bytes(),
            format!(r#"{{"schema_version":1,"request_id":"{id}","extra":0}}"#).into_bytes(),
            br#"{"schema_version":1}"#.to_vec(),
        ] {
            let correlation = Correlation::incoming(Some(&ZBytes::from(value)));
            assert!(correlation.request_id.is_none());
            assert_eq!(
                correlation.source,
                TelemetryCorrelationSource::InvalidAttachment
            );
        }
    }
}
