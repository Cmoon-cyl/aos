use std::sync::mpsc;
use std::thread;

use super::*;

const LIMIT: Duration = Duration::from_secs(5);
const KEY: &str = "aos/service/qos";

struct Network {
    _router: zenoh::Session,
    caller: zenoh::Session,
    provider: zenoh::Session,
    client_config: zenoh::Config,
}

impl Network {
    fn open() -> Self {
        let router_config = zenoh::Config::from_json5(
            r#"{
                mode: "router",
                listen: { endpoints: ["tcp/127.0.0.1:0"] },
                scouting: { multicast: { enabled: false }, gossip: { enabled: false } }
            }"#,
        )
        .unwrap();
        let router = zenoh::open(router_config).wait().unwrap();
        let endpoint = router.info().locators().wait()[0].to_string();
        let config = zenoh::Config::from_json5(&format!(
            r#"{{
                mode: "client",
                connect: {{ endpoints: ["{endpoint}"] }},
                listen: {{ endpoints: [] }},
                scouting: {{ multicast: {{ enabled: false }}, gossip: {{ enabled: false }} }}
            }}"#
        ))
        .unwrap();
        let caller = zenoh::open(config.clone()).wait().unwrap();
        let provider = zenoh::open(config.clone()).wait().unwrap();
        Self {
            _router: router,
            caller,
            provider,
            client_config: config,
        }
    }

    fn client(&self, depth: usize) -> ZenohClient {
        self.client_with_no_deadline(depth, false)
    }

    fn client_with_no_deadline(&self, depth: usize, allow_no_deadline: bool) -> ZenohClient {
        ZenohClient::new(
            &self.caller,
            KEY.try_into().unwrap(),
            &QoS {
                depth,
                ..QoS::default()
            },
            Arc::default(),
            allow_no_deadline,
        )
        .unwrap()
    }

    fn wait_for_service(&self) {
        let querier = self.caller.declare_querier(KEY).wait().unwrap();
        until(|| querier.matching_status().wait().unwrap().matching());
    }
}

fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not met");
        thread::sleep(Duration::from_millis(5));
    }
}

fn bounded_shared(message_bytes: usize, queue_bytes: usize) -> Arc<Shared> {
    Arc::new(Shared::new(
        aos_core::Telemetry::default(),
        aos_core::QueueLimits {
            message_bytes,
            queue_bytes,
        },
    ))
}

#[test]
fn bounded_reply_queue_rejects_before_the_copy_closure() {
    let limits = bounded_reply_limits(
        aos_core::QueueLimits {
            message_bytes: 64,
            queue_bytes: 128,
        },
        Some(8),
    );
    let queue: Queue<Vec<u8>> = Queue::with_limits(1, limits);
    let rejected = queue.try_push_with(9, || panic!("oversized reply must not be copied"), drop);
    assert!(rejected.is_err());
    let limits = bounded_reply_limits(
        aos_core::QueueLimits {
            message_bytes: 4,
            queue_bytes: 128,
        },
        Some(8),
    );
    assert_eq!(limits.message_bytes, 4);
}

#[test]
fn real_bounded_reply_honors_both_call_and_adapter_caps_for_success_and_errors() {
    let network = Network::open();
    let _provider = network
        .provider
        .declare_queryable(KEY)
        .callback(|query| {
            let payload = query.payload().unwrap().to_bytes();
            let reply = vec![b'x'; payload[0] as usize];
            if payload[1] == 0 {
                query.reply(KEY, reply).wait().unwrap();
            } else {
                query.reply_err(reply).wait().unwrap();
            }
        })
        .wait()
        .unwrap();
    network.wait_for_service();
    for (adapter_cap, response_bytes, error_reply, oversized) in [
        (64, 32, false, false),
        (64, 33, false, true),
        (16, 17, false, true),
        (64, 32, true, false),
        (64, 33, true, true),
    ] {
        let result = call_bounded(
            &network.caller,
            KEY.try_into().unwrap(),
            &bounded_shared(adapter_cap, 128),
            &[response_bytes, u8::from(error_reply)],
            LIMIT,
            32,
        );
        if oversized {
            assert!(matches!(result, Err(Error::QueueOverflow(_))), "{result:?}");
        } else if error_reply {
            assert!(matches!(result, Err(Error::Remote(_))), "{result:?}");
        } else {
            assert_eq!(result.unwrap(), vec![b'x'; response_bytes as usize]);
        }
    }
}

#[test]
fn real_context_node_card_rejects_oversize_then_accepts_valid_reply() {
    use std::sync::atomic::AtomicUsize;

    use aos_core::{Context, Endpoint, QueueLimits};

    const NODE: &str = "/node_card_source";
    let network = Network::open();
    let response_limit = Context::MAX_NODE_CARD_RESPONSE_BYTES;
    assert_eq!(response_limit, 1024 * 1024);
    let interface = aos_core::node_card_interface();
    let endpoint = Endpoint {
        name: format!("{NODE}/get_node_card"),
        type_name: interface.type_name().into(),
        type_hash: interface.type_hash().into(),
    };
    let key = crate::key::typed_key(0, &endpoint).unwrap();
    let healthy_card = serde_json::json!({
        "id": "node-card-limit-fixture",
        "name": "node_card_source",
        "namespace": "/",
        "fully_qualified_name": NODE,
        "display_name": "NodeCard limit fixture",
        "summary": "healthy after oversized reply",
        "tags": [],
        "lifecycle": null,
        "effective_config": {},
        "endpoints": []
    });
    let healthy_payload = serde_json::to_vec(&healthy_card).unwrap();
    let healthy_bytes = healthy_payload.len();
    let mut oversized_card = healthy_card.clone();
    oversized_card["summary"] = serde_json::json!("");
    let envelope_bytes = serde_json::to_vec(&oversized_card).unwrap().len();
    oversized_card["summary"] = serde_json::json!("x".repeat(response_limit + 1 - envelope_bytes));
    let oversized_payload = serde_json::to_vec(&oversized_card).unwrap();
    let oversized_bytes = oversized_payload.len();
    assert_eq!(oversized_bytes, response_limit + 1);
    interface.validate("response", &oversized_card).unwrap();
    interface.validate("response", &healthy_card).unwrap();

    // Both are valid JSON replies below the adapter's ordinary message cap.
    // Only Context's per-call 1 MiB limit should reject the first response.
    let adapter_limits = QueueLimits::default();
    assert!(oversized_bytes < adapter_limits.message_bytes);
    let requests = Arc::new(AtomicUsize::new(0));
    let observed_requests = requests.clone();
    let reply_key = key.clone();
    let _provider = network
        .provider
        .declare_queryable(key.clone())
        .callback(move |query| {
            assert_eq!(query.payload().unwrap().to_bytes().as_ref(), b"{}");
            let request = observed_requests.fetch_add(1, Ordering::AcqRel);
            let payload = if request == 0 {
                oversized_payload.clone()
            } else {
                healthy_payload.clone()
            };
            query.reply(reply_key.clone(), payload).wait().unwrap();
        })
        .wait()
        .unwrap();
    let matching = network.caller.declare_querier(key).wait().unwrap();
    until(|| matching.matching_status().wait().unwrap().matching());
    let transport = crate::ZenohTransport::open_with_queue_limits(
        network.client_config.clone(),
        0,
        adapter_limits,
    )
    .unwrap();
    let context = Context::new(transport);

    let first_error = context.get_node_card(NODE, LIMIT).unwrap_err();
    let Error::QueueOverflow(ref message) = first_error else {
        panic!("expected an adapter reply byte refusal, got {first_error:?}");
    };
    // A core post-Vec fallback uses a different diagnostic. Require the exact
    // adapter code to prove the Context -> call_bounded -> Zenoh receive path.
    let first_code = message.split_once(':').map(|(code, _)| code);
    assert_eq!(first_code, Some("REPLY_MESSAGE_TOO_LARGE"));
    assert_eq!(requests.load(Ordering::Acquire), 1);

    let second_card = context.get_node_card(NODE, LIMIT).unwrap();
    assert_eq!(second_card, healthy_card);
    assert_eq!(requests.load(Ordering::Acquire), 2);
    println!(
        "node-card-response-evidence {}",
        serde_json::json!({
            "transport": "tcp",
            "node": NODE,
            "response_limit_bytes": response_limit,
            "adapter_message_limit_bytes": adapter_limits.message_bytes,
            "oversized_json_bytes": oversized_bytes,
            "healthy_json_bytes": healthy_bytes,
            "first_result": {"kind": "QueueOverflow", "code": first_code, "detail": message},
            "second_result": {"kind": "Ok", "node_card": second_card},
            "provider_requests": requests.load(Ordering::Acquire)
        })
    );
    context.shutdown().unwrap();
}

#[test]
fn real_request_byte_rejection_is_provisional_and_another_provider_can_succeed() {
    for persistent in [false, true] {
        for other_provider in [false, true] {
            let network = Network::open();
            let restricted = ZenohService::declare(
                &network.provider,
                KEY.try_into().unwrap(),
                &QoS::default(),
                bounded_shared(16, 32),
            )
            .unwrap();
            let allowed = other_provider.then(|| {
                ZenohService::declare(
                    &network.provider,
                    KEY.try_into().unwrap(),
                    &QoS::default(),
                    Arc::default(),
                )
                .unwrap()
            });
            if let Some(allowed) = &allowed {
                wait_for_two_services(&network, &restricted, allowed);
            } else {
                network.wait_for_service();
            }
            let client = network.client(2);
            let payload = vec![b'x'; 32];
            let pending = persistent.then(|| client.begin_query(&payload, Some(LIMIT)).unwrap());
            let legacy = (!persistent).then(|| {
                let session = network.caller.clone();
                let payload = payload.clone();
                thread::spawn(move || {
                    call_wait(
                        &session,
                        KEY.try_into().unwrap(),
                        &Arc::default(),
                        &payload,
                        Some(LIMIT),
                        None,
                    )
                })
            });
            until(|| {
                restricted
                    .queue_stats()
                    .unwrap()
                    .bytes
                    .unwrap()
                    .oversized_messages_total
                    == 1
            });
            assert!(matches!(
                restricted.recv_timeout(Duration::ZERO),
                Err(Error::Timeout)
            ));
            if let Some(allowed) = &allowed {
                let request = allowed.recv_timeout(LIMIT).unwrap();
                assert_eq!(request.payload(), payload);
                request.reply(Ok(b"success".to_vec())).unwrap();
            }
            let result = match pending {
                Some(pending) => pending.wait(&AtomicBool::new(false)),
                None => legacy.unwrap().join().unwrap(),
            };
            if other_provider {
                assert_eq!(result, Ok(b"success".to_vec()));
            } else {
                assert!(matches!(result, Err(Error::QueueOverflow(message))
                    if message.contains(SERVICE_REQUEST_TOO_LARGE) && message.contains("other providers")));
            }
            let bytes = restricted.queue_stats().unwrap().bytes.unwrap();
            assert_eq!(
                (
                    bytes.queued_bytes,
                    bytes.reserved_bytes,
                    bytes.oversized_bytes_total
                ),
                (0, 0, 32)
            );
        }
    }
}

#[test]
fn real_client_and_legacy_reply_paths_reject_remote_oversize_before_copy() {
    for persistent in [false, true] {
        for remote_error in [false, true] {
            let network = Network::open();
            let service = network
                .provider
                .declare_queryable(KEY)
                .complete(true)
                .wait()
                .unwrap();
            network.wait_for_service();
            let shared = bounded_shared(4, 8);
            let client = ZenohClient::new(
                &network.caller,
                KEY.try_into().unwrap(),
                &QoS::default(),
                shared.clone(),
                false,
            )
            .unwrap();
            let pending = persistent.then(|| client.begin_query(b"req", Some(LIMIT)).unwrap());
            let legacy = (!persistent).then(|| {
                let (session, shared) = (network.caller.clone(), shared.clone());
                thread::spawn(move || {
                    call_wait(
                        &session,
                        KEY.try_into().unwrap(),
                        &shared,
                        b"req",
                        Some(LIMIT),
                        None,
                    )
                })
            });
            let request = service.recv_timeout(LIMIT).unwrap().unwrap();
            if remote_error {
                request
                    .reply_err(vec![0xff; 5])
                    .encoding(zenoh::bytes::Encoding::TEXT_PLAIN)
                    .wait()
                    .unwrap();
            } else {
                request.reply(KEY, vec![b'x'; 5]).wait().unwrap();
            }
            drop(request);
            let result = match pending {
                Some(pending) => pending.wait(&AtomicBool::new(false)),
                None => legacy.unwrap().join().unwrap(),
            };
            assert!(matches!(result, Err(Error::QueueOverflow(message))
                if message.contains("REPLY_MESSAGE_TOO_LARGE") && message.contains("may have executed")));
            if persistent {
                let bytes = client.queue_stats().unwrap().bytes.unwrap();
                assert_eq!(
                    (
                        bytes.queued_bytes,
                        bytes.reserved_bytes,
                        bytes.oversized_messages_total,
                        bytes.oversized_bytes_total
                    ),
                    (0, 0, 1, 5)
                );
                assert!(client.replies.stats().queued == 0);
            }
        }
    }
}

#[test]
fn real_oversize_request_is_not_submitted_and_oversize_provider_reply_reports_execution_uncertainty(
) {
    let network = Network::open();
    let shared = bounded_shared(4, 8);
    let service = ZenohService::declare(
        &network.provider,
        KEY.try_into().unwrap(),
        &QoS::default(),
        shared.clone(),
    )
    .unwrap();
    network.wait_for_service();
    let restricted = ZenohClient::new(
        &network.caller,
        KEY.try_into().unwrap(),
        &QoS::default(),
        shared.clone(),
        false,
    )
    .unwrap();
    assert!(matches!(restricted.begin_query(b"large", Some(LIMIT)),
        Err(Error::QueueOverflow(message)) if message.contains("was not submitted")));
    assert!(
        matches!(call_wait(&network.caller, KEY.try_into().unwrap(), &shared,
        b"large", Some(LIMIT), None), Err(Error::QueueOverflow(message)) if message.contains("was not submitted"))
    );
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(50)),
        Err(Error::Timeout)
    ));
    assert_eq!(service.queue_stats().unwrap().received_total, 0);
    for provider_error in [false, true] {
        let client = network.client(1);
        let pending = client.begin_query(b"req", Some(LIMIT)).unwrap();
        let request = service.recv_timeout(LIMIT).unwrap();
        let response = if provider_error {
            Err(Error::Remote("large".into()))
        } else {
            Ok(b"large".to_vec())
        };
        assert!(
            matches!(request.reply(response), Err(Error::QueueOverflow(message))
            if message.contains(SERVICE_REPLY_TOO_LARGE) && message.contains("may have executed"))
        );
        assert!(
            matches!(pending.wait(&AtomicBool::new(false)), Err(Error::QueueOverflow(message))
            if message.contains(SERVICE_REPLY_TOO_LARGE) && message.contains("may have executed"))
        );
    }
}

#[test]
fn foreign_request_attachments_are_optional_and_never_change_service_results() {
    let network = Network::open();
    let shared = Arc::new(Shared::default());
    shared.telemetry.enable_log_capture(64).unwrap();
    let service = ZenohService::declare(
        &network.provider,
        KEY.try_into().unwrap(),
        &QoS::default(),
        shared.clone(),
    )
    .unwrap();
    network.wait_for_service();
    let id = aos_core::Telemetry::new_request_id();
    let cases = [
        (None, None, TelemetryCorrelationSource::MissingAttachment),
        (
            Some(b"untrusted-attachment-secret".to_vec()),
            None,
            TelemetryCorrelationSource::InvalidAttachment,
        ),
        (
            Some(attachment(&id)),
            Some(id.as_str()),
            TelemetryCorrelationSource::IncomingAttachment,
        ),
    ];
    for (attached, expected_id, expected_source) in cases {
        let mut query = network.caller.get(KEY).payload(b"payload-secret".to_vec());
        if let Some(attached) = attached {
            query = query.attachment(attached);
        }
        let replies = query.wait().unwrap();
        let request = service.recv_timeout(LIMIT).unwrap();
        assert_eq!(request.request_id(), expected_id);
        request.reply(Ok(b"response-secret".to_vec())).unwrap();
        let response = replies
            .recv_timeout(LIMIT)
            .unwrap()
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(response.payload().to_bytes().as_ref(), b"response-secret");
        assert_eq!(
            Correlation::incoming(response.attachment())
                .request_id
                .as_deref(),
            expected_id
        );
        let records = shared.telemetry.drain_records();
        let received = records
            .iter()
            .find(|r| r.event.event == "zenoh.service.request_received")
            .unwrap();
        assert_eq!(received.event.correlation_source, Some(expected_source));
        let json = serde_json::to_string(&records).unwrap();
        assert!(!json.contains("secret"));
    }
}

#[test]
fn reply_attachment_mismatch_is_diagnostic_and_cannot_replace_local_correlation() {
    let network = Network::open();
    let queryable = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(4);
    client.shared.telemetry.enable_log_capture(128).unwrap();
    for expected in [
        TelemetryCorrelationSource::ReplyAttachment,
        TelemetryCorrelationSource::MissingAttachment,
        TelemetryCorrelationSource::InvalidAttachment,
        TelemetryCorrelationSource::MismatchedAttachment,
    ] {
        let pending = client.begin_query(b"request-secret", Some(LIMIT)).unwrap();
        let id = pending.telemetry.request_id.clone();
        let query = queryable.recv_timeout(LIMIT).unwrap().unwrap();
        assert_eq!(
            Correlation::incoming(query.attachment()).request_id,
            Some(id.clone())
        );
        let mut reply = query.reply(KEY, b"business-result".to_vec());
        let attached = match expected {
            TelemetryCorrelationSource::ReplyAttachment => Some(attachment(&id)),
            TelemetryCorrelationSource::InvalidAttachment => Some(b"attachment-secret".to_vec()),
            TelemetryCorrelationSource::MismatchedAttachment => {
                Some(attachment(&aos_core::Telemetry::new_request_id()))
            }
            _ => None,
        };
        if let Some(attached) = attached {
            reply = reply.attachment(attached);
        }
        reply.wait().unwrap();
        assert_eq!(
            pending.wait(&AtomicBool::new(false)).unwrap(),
            b"business-result"
        );
        let records = client.shared.telemetry.drain_records();
        let received = records
            .iter()
            .find(|r| r.event.event == "zenoh.service.reply_received")
            .unwrap();
        assert_eq!(received.event.request_id.as_deref(), Some(id.as_str()));
        assert_eq!(received.event.correlation_source, Some(expected));
        assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
    }
}

#[test]
fn real_service_request_depth_keeps_latest_and_rejects_evicted_requests() {
    for depth in [1, 2] {
        let network = Network::open();
        let service = ZenohService::declare(
            &network.provider,
            KEY.try_into().unwrap(),
            &QoS {
                depth,
                ..QoS::default()
            },
            Arc::default(),
        )
        .unwrap();
        network.wait_for_service();
        let mut replies = Vec::new();
        for number in 0u8..4 {
            replies.push(
                network
                    .caller
                    .get(KEY)
                    .payload(vec![number])
                    .timeout(LIMIT)
                    .wait()
                    .unwrap(),
            );
            until(|| service.queue_stats().unwrap().received_total == u64::from(number) + 1);
        }
        let stats = service.queue_stats().unwrap();
        assert_eq!(stats.depth, depth);
        assert_eq!(stats.queued, depth);
        assert_eq!(stats.dropped_total, (4 - depth) as u64);
        for replies in replies.iter().take(4 - depth) {
            let error = replies
                .recv_timeout(LIMIT)
                .unwrap()
                .unwrap()
                .into_result()
                .unwrap_err();
            assert_eq!(
                error.encoding(),
                &zenoh::bytes::Encoding::from(SERVICE_ERROR_ENCODING)
            );
            assert_eq!(
                error.payload().to_bytes().as_ref(),
                SERVICE_QUEUE_OVERFLOW.as_bytes()
            );
        }
        for number in (4 - depth) as u8..4 {
            let request = service.recv_timeout(Duration::ZERO).unwrap();
            assert_eq!(request.payload(), &[number]);
            request.reply(Ok(request.payload().to_vec())).unwrap();
            let response = replies[number as usize]
                .recv_timeout(LIMIT)
                .unwrap()
                .unwrap();
            assert_eq!(
                response.result().unwrap().payload().to_bytes().as_ref(),
                &[number]
            );
        }
        assert!(matches!(
            service.recv_timeout(Duration::ZERO),
            Err(Error::Timeout)
        ));
        service.close().unwrap();
        let closed = service.queue_stats().unwrap();
        assert_eq!(closed.queued, 0);
        assert_eq!(closed.received_total, 4);
        assert_eq!(closed.dropped_total, stats.dropped_total);
        assert!(closed.closed);
    }
}

#[test]
fn real_client_depth_applies_to_completed_replies_and_not_pending_calls() {
    for depth in [1, 2] {
        let network = Network::open();
        let service = network
            .provider
            .declare_queryable(KEY)
            .complete(true)
            .wait()
            .unwrap();
        network.wait_for_service();
        let client = network.client(depth);
        let mut pending = Vec::new();
        let mut requests = Vec::new();
        for number in 0u8..4 {
            pending.push(client.begin_query(&[number], Some(LIMIT)).unwrap());
            requests.push(service.recv_timeout(LIMIT).unwrap().unwrap());
        }
        assert_eq!(client.queue_stats().unwrap().queued, 0);
        assert_eq!(client.queue_stats().unwrap().dropped_total, 0);

        // Hold consumption after actual router replies to make depth pressure
        // deterministic without adding a public split-RPC API or a test hook.
        for (number, request) in requests.iter().enumerate() {
            request.reply(KEY, vec![number as u8]).wait().unwrap();
            until(|| client.queue_stats().unwrap().received_total == number as u64 + 1);
        }
        let stats = client.queue_stats().unwrap();
        assert_eq!(stats.queued, depth);
        assert_eq!(stats.received_total, 4);
        assert_eq!(stats.dropped_total, (4 - depth) as u64);
        for (number, query) in pending.into_iter().enumerate() {
            let result = query.wait(&AtomicBool::new(false));
            if number < 4 - depth {
                assert!(matches!(
                    result,
                    Err(Error::QueueOverflow(message))
                        if message.contains("provider may have executed")
                            && message.contains("no automatic retry")
                ));
            } else {
                assert_eq!(result, Ok(vec![number as u8]));
            }
        }
        assert_eq!(client.queue_stats().unwrap().queued, 0);
        assert_eq!(
            client.queue_stats().unwrap().dropped_total,
            stats.dropped_total
        );
    }
}

#[test]
fn real_service_evictions_reach_client_as_queue_overflow_without_retry() {
    let network = Network::open();
    let service = ZenohService::declare(
        &network.provider,
        KEY.try_into().unwrap(),
        &QoS {
            depth: 2,
            ..QoS::default()
        },
        Arc::default(),
    )
    .unwrap();
    network.wait_for_service();
    let client = network.client(8);
    let mut pending = Vec::new();
    for number in 0u8..6 {
        pending.push(client.begin_query(&[number], Some(LIMIT)).unwrap());
        until(|| service.queue_stats().unwrap().received_total == u64::from(number) + 1);
    }
    for (number, pending) in pending.into_iter().enumerate() {
        if number < 4 {
            assert!(matches!(
                pending.wait(&AtomicBool::new(false)),
                Err(Error::QueueOverflow(message))
                    if message.starts_with("SERVICE_QUEUE_OVERFLOW:")
                        && message.contains("before executing its handler")
                        && message.contains("other providers may have received")
                        && message.contains("no automatic retry")
            ));
        } else {
            let request = service.recv_timeout(LIMIT).unwrap();
            assert_eq!(request.payload(), &[number as u8]);
            request.reply(Ok(request.payload().to_vec())).unwrap();
            assert_eq!(
                pending.wait(&AtomicBool::new(false)),
                Ok(vec![number as u8])
            );
        }
    }
    assert!(matches!(
        service.recv_timeout(Duration::from_millis(50)),
        Err(Error::Timeout)
    ));
    assert_eq!(service.queue_stats().unwrap().received_total, 6);
    assert_eq!(service.queue_stats().unwrap().dropped_total, 4);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
}

#[test]
fn real_reply_error_protocol_requires_exact_encoding_and_code_for_both_clients() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(1);
    for persistent in [false, true] {
        for (encoding, payload, rejection) in [
            ("text/plain", SERVICE_QUEUE_OVERFLOW, None),
            ("text/plain", SERVICE_INACTIVE, None),
            ("text/plain", "Timeout", None),
            (SERVICE_ERROR_ENCODING, "UNKNOWN_ERROR", None),
            (SERVICE_ERROR_ENCODING, "SERVICE_QUEUE_OVERFLOW extra", None),
            (SERVICE_ERROR_ENCODING, "SERVICE_INACTIVE extra", None),
            (
                "application/vnd.aos.service-error.v2",
                SERVICE_QUEUE_OVERFLOW,
                None,
            ),
            (
                "application/vnd.aos.service-error.v2",
                SERVICE_INACTIVE,
                None,
            ),
            (
                SERVICE_ERROR_ENCODING,
                SERVICE_QUEUE_OVERFLOW,
                Some(ProviderRejection::QueueOverflow),
            ),
            (
                SERVICE_ERROR_ENCODING,
                SERVICE_INACTIVE,
                Some(ProviderRejection::Inactive),
            ),
            (
                SERVICE_ERROR_ENCODING,
                SERVICE_REQUEST_TOO_LARGE,
                Some(ProviderRejection::RequestTooLarge),
            ),
            (
                SERVICE_ERROR_ENCODING,
                SERVICE_REQUEST_CAPACITY,
                Some(ProviderRejection::RequestCapacity),
            ),
        ] {
            let pending = persistent.then(|| client.begin_query(b"protocol", Some(LIMIT)).unwrap());
            let legacy = (!persistent).then(|| {
                let session = network.caller.clone();
                thread::spawn(move || {
                    call_wait(
                        &session,
                        KEY.try_into().unwrap(),
                        &Arc::default(),
                        b"protocol",
                        Some(LIMIT),
                        None,
                    )
                })
            });
            let request = service.recv_timeout(LIMIT).unwrap().unwrap();
            request
                .reply_err(payload)
                .encoding(encoding)
                .wait()
                .unwrap();
            drop(request);
            let result = if let Some(pending) = pending {
                pending.wait(&AtomicBool::new(false))
            } else {
                legacy.unwrap().join().unwrap()
            };
            match rejection {
                Some(
                    ProviderRejection::QueueOverflow
                    | ProviderRejection::RequestTooLarge
                    | ProviderRejection::RequestCapacity,
                ) => {
                    assert!(matches!(result, Err(Error::QueueOverflow(_))), "{result:?}");
                }
                Some(ProviderRejection::Inactive) => {
                    assert!(matches!(result, Err(Error::Inactive(_))), "{result:?}");
                }
                None => assert_eq!(result, Err(Error::Remote(payload.into()))),
            }
        }
    }
}

#[test]
fn real_business_reply_wins_before_or_after_provisional_queue_overflow() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(1);
    for overflow_first in [false, true] {
        let pending = client.begin_query(b"first", Some(LIMIT)).unwrap();
        let request = service.recv_timeout(LIMIT).unwrap().unwrap();
        let overflow = || {
            request
                .reply_err(SERVICE_QUEUE_OVERFLOW)
                .encoding(SERVICE_ERROR_ENCODING)
                .wait()
                .unwrap();
        };
        if overflow_first {
            overflow();
            request.reply(KEY, b"late success").wait().unwrap();
        } else {
            request.reply(KEY, b"first success").wait().unwrap();
            overflow();
        }
        let result = pending.wait(&AtomicBool::new(false));
        if overflow_first {
            assert_eq!(result, Ok(b"late success".to_vec()));
        } else {
            assert_eq!(result, Ok(b"first success".to_vec()));
        }
    }
    assert_eq!(client.queue_stats().unwrap().received_total, 2);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
}

fn wait_for_two_services(network: &Network, first: &ZenohService, second: &ZenohService) {
    until(|| {
        let _replies = network
            .caller
            .get(KEY)
            .payload(b"discovery")
            .target(QueryTarget::All)
            .timeout(LIMIT)
            .wait()
            .unwrap();
        let first = first.recv_timeout(Duration::from_millis(200)).ok();
        let second = second.recv_timeout(Duration::from_millis(200)).ok();
        let both = first.is_some() && second.is_some();
        for request in [first, second].into_iter().flatten() {
            request.reply(Ok(b"ready".to_vec())).unwrap();
        }
        both
    });
    for service in [first, second] {
        while let Ok(request) = service.recv_timeout(Duration::ZERO) {
            request.reply(Ok(b"ready".to_vec())).unwrap();
        }
    }
}

#[test]
fn real_two_providers_wait_past_rejection_for_success_error_final_or_deadline() {
    for rejection_kind in [
        ProviderRejection::QueueOverflow,
        ProviderRejection::Inactive,
    ] {
        for persistent in [false, true] {
            for outcome in ["success", "remote", "finish", "deadline"] {
                let network = Network::open();
                let other_provider = zenoh::open(network.client_config.clone()).wait().unwrap();
                let full = ZenohService::declare(
                    &network.provider,
                    KEY.try_into().unwrap(),
                    &QoS {
                        depth: 1,
                        ..QoS::default()
                    },
                    Arc::default(),
                )
                .unwrap();
                let available = ZenohService::declare(
                    &other_provider,
                    KEY.try_into().unwrap(),
                    &QoS {
                        depth: 4,
                        ..QoS::default()
                    },
                    Arc::default(),
                )
                .unwrap();
                wait_for_two_services(&network, &full, &available);
                let initial = full.queue_stats().unwrap();
                let client = network.client(1);
                let timeout = if outcome == "deadline" {
                    Duration::from_millis(300)
                } else {
                    LIMIT
                };
                let (sent, result) = mpsc::channel();
                let mut provisional = None;
                let waiter = if persistent {
                    let pending = client.begin_query(b"wanted", Some(timeout)).unwrap();
                    provisional = Some(pending.provider_rejection.clone());
                    thread::spawn(move || {
                        let _ = sent.send(pending.wait(&AtomicBool::new(false)));
                    })
                } else {
                    let session = network.caller.clone();
                    thread::spawn(move || {
                        let _ = sent.send(call_wait(
                            &session,
                            KEY.try_into().unwrap(),
                            &Arc::default(),
                            b"wanted",
                            Some(timeout),
                            None,
                        ));
                    })
                };
                let request = available.recv_timeout(LIMIT).unwrap();
                assert_eq!(request.payload(), b"wanted");
                until(|| full.queue_stats().unwrap().received_total == initial.received_total + 1);
                // The other provider still owns its request. Reject only here,
                // either by actual KEEP_LAST eviction or an inactive reply.
                let pressure = if rejection_kind == ProviderRejection::QueueOverflow {
                    Some(
                        network
                            .caller
                            .get(KEY)
                            .payload(b"pressure")
                            .target(QueryTarget::All)
                            .timeout(LIMIT)
                            .wait()
                            .unwrap(),
                    )
                } else {
                    let inactive = full.recv_timeout(LIMIT).unwrap();
                    inactive
                        .reply(Err(Error::Inactive("inactive-reason-secret".into())))
                        .unwrap();
                    None
                };
                if pressure.is_some() {
                    until(|| {
                        full.queue_stats().unwrap().dropped_total == initial.dropped_total + 1
                    });
                }
                if let Some(provisional) = provisional {
                    until(|| provisional.current() == Some(rejection_kind));
                    assert_eq!(client.queue_stats().unwrap().queued, 0);
                }
                assert_eq!(
                    result.recv_timeout(Duration::from_millis(50)),
                    Err(mpsc::RecvTimeoutError::Timeout),
                    "one provider's rejection must not finish the query"
                );
                let completed = match outcome {
                    "success" => {
                        request.reply(Ok(b"available success".to_vec())).unwrap();
                        result.recv_timeout(LIMIT).unwrap()
                    }
                    "remote" => {
                        request
                            .reply(Err(Error::Validation("business rejected".into())))
                            .unwrap();
                        result.recv_timeout(LIMIT).unwrap()
                    }
                    "finish" => {
                        drop(request);
                        result.recv_timeout(LIMIT).unwrap()
                    }
                    "deadline" => {
                        // The second provider still owns its Query, so neither
                        // its ResponseFinal nor a business response can arrive.
                        let completed = result.recv_timeout(LIMIT).unwrap();
                        drop(request);
                        completed
                    }
                    _ => unreachable!(),
                };
                match outcome {
                    "success" => assert_eq!(completed, Ok(b"available success".to_vec())),
                    "remote" => assert!(matches!(
                        completed,
                        Err(Error::Remote(message)) if message.contains("business rejected")
                    )),
                    "finish" if rejection_kind == ProviderRejection::QueueOverflow => {
                        assert!(matches!(
                            completed,
                            Err(Error::QueueOverflow(message))
                                if message.starts_with("SERVICE_QUEUE_OVERFLOW:")
                        ))
                    }
                    "finish" => assert!(matches!(completed, Err(Error::Inactive(message))
                    if message.starts_with("SERVICE_INACTIVE:")
                        && message.contains("other providers may have received")
                        && message.contains("no automatic retry")
                        && !message.contains("inactive-reason-secret"))),
                    "deadline" => assert_eq!(completed, Err(Error::Timeout)),
                    _ => unreachable!(),
                }
                waiter.join().unwrap();
                for service in [&full, &available] {
                    if pressure.is_some() {
                        let pressure = service.recv_timeout(LIMIT).unwrap();
                        assert_eq!(pressure.payload(), b"pressure");
                        pressure.reply(Ok(b"pressure".to_vec())).unwrap();
                    }
                    assert!(matches!(
                        service.recv_timeout(Duration::ZERO),
                        Err(Error::Timeout)
                    ));
                }
            }
        }
    }
}

#[test]
fn real_early_query_completion_is_no_response_for_finite_and_get_result_waits() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client_with_no_deadline(1, true);
    for persistent in [false, true] {
        for timeout in [Some(LIMIT), None] {
            let pending = persistent.then(|| client.begin_query(b"no response", timeout).unwrap());
            let legacy = (!persistent).then(|| {
                let session = network.caller.clone();
                thread::spawn(move || {
                    call_wait(
                        &session,
                        KEY.try_into().unwrap(),
                        &Arc::default(),
                        b"no response",
                        timeout,
                        None,
                    )
                })
            });
            let request = service.recv_timeout(LIMIT).unwrap().unwrap();
            // Receiving the request does not prove whether application work
            // completed. Dropping it sends ResponseFinal, without a response.
            let started = Instant::now();
            drop(request);
            let result = if let Some(pending) = pending {
                pending.wait(&AtomicBool::new(false))
            } else {
                legacy.unwrap().join().unwrap()
            };
            assert!(matches!(
                result,
                Err(Error::NoResponse(message))
                    if message.contains("provider execution is unknown")
                        && message.contains("no automatic retry")
            ));
            assert!(started.elapsed() < Duration::from_secs(2));
        }
    }
    assert_eq!(client.queue_stats().unwrap().received_total, 0);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
}

#[test]
fn real_mixed_provider_rejections_preserve_first_observed_cause_for_both_clients() {
    for persistent in [false, true] {
        for first in [
            ProviderRejection::Inactive,
            ProviderRejection::QueueOverflow,
        ] {
            let network = Network::open();
            let other_provider = zenoh::open(network.client_config.clone()).wait().unwrap();
            let inactive = ZenohService::declare(
                &network.provider,
                KEY.try_into().unwrap(),
                &QoS::default(),
                Arc::default(),
            )
            .unwrap();
            let full = ZenohService::declare(
                &other_provider,
                KEY.try_into().unwrap(),
                &QoS {
                    depth: 1,
                    ..QoS::default()
                },
                Arc::default(),
            )
            .unwrap();
            wait_for_two_services(&network, &inactive, &full);
            let client = network.client(1);
            let shared = client.shared.clone();
            let pending = persistent.then(|| client.begin_query(b"wanted", Some(LIMIT)).unwrap());
            let legacy = (!persistent).then(|| {
                let session = network.caller.clone();
                let shared = shared.clone();
                thread::spawn(move || {
                    call_wait(
                        &session,
                        KEY.try_into().unwrap(),
                        &shared,
                        b"wanted",
                        Some(LIMIT),
                        None,
                    )
                })
            });
            let mut request = Some(inactive.recv_timeout(LIMIT).unwrap());
            until(|| full.queue_stats().unwrap().queued == 1);
            let reject_inactive = |request: &mut Option<Box<dyn RequestBackend>>| {
                request
                    .take()
                    .unwrap()
                    .reply(Err(Error::Inactive("inactive-reason-secret".into())))
                    .unwrap();
            };
            let pressure = || {
                network
                    .caller
                    .get(KEY)
                    .payload(b"pressure")
                    .target(QueryTarget::All)
                    .timeout(LIMIT)
                    .wait()
                    .unwrap()
            };
            let wait_for_rejection = |code: ProviderRejection| {
                until(|| {
                    shared.telemetry.snapshot().metrics.iter().any(|metric| {
                        metric.key.event == "zenoh.service.reply_received"
                            && metric.key.outcome == code.outcome()
                    })
                })
            };
            let pressure_query = if first == ProviderRejection::Inactive {
                reject_inactive(&mut request);
                wait_for_rejection(ProviderRejection::Inactive);
                pressure()
            } else {
                let pressure_query = pressure();
                wait_for_rejection(ProviderRejection::QueueOverflow);
                reject_inactive(&mut request);
                pressure_query
            };
            let result = match pending {
                Some(pending) => pending.wait(&AtomicBool::new(false)),
                None => legacy.unwrap().join().unwrap(),
            };
            match first {
                ProviderRejection::Inactive => {
                    assert!(matches!(result, Err(Error::Inactive(_))), "{result:?}")
                }
                ProviderRejection::QueueOverflow
                | ProviderRejection::RequestTooLarge
                | ProviderRejection::RequestCapacity => {
                    assert!(matches!(result, Err(Error::QueueOverflow(_))), "{result:?}")
                }
            }
            for service in [&inactive, &full] {
                let request = service.recv_timeout(LIMIT).unwrap();
                assert_eq!(request.payload(), b"pressure");
                request.reply(Ok(b"pressure".to_vec())).unwrap();
                assert_eq!(service.queue_stats().unwrap().queued, 0);
            }
            drop(pressure_query);
            assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
        }
    }
}

#[test]
fn real_early_completion_stays_no_response_when_consumed_after_deadline() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(1);
    let timeout = Duration::from_millis(300);
    let pending = client
        .begin_query(b"delayed consumer", Some(timeout))
        .unwrap();
    drop(service.recv_timeout(LIMIT).unwrap().unwrap());
    // Withhold consumption, while the ordinary Zenoh callback records the
    // completion. The later wait must use completion time, not dequeue time.
    thread::sleep(timeout.saturating_sub(pending.started.elapsed()) + Duration::from_millis(20));
    assert!(matches!(
        pending.wait(&AtomicBool::new(false)),
        Err(Error::NoResponse(_))
    ));
}

#[test]
fn real_concurrent_calls_preserve_correlation_and_report_every_reply_eviction() {
    const CALLS: usize = 64;
    const DEPTH: usize = 4;
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(DEPTH);
    let pending = thread::scope(|scope| {
        let workers: Vec<_> = (0..CALLS)
            .map(|number| {
                let client = &client;
                scope.spawn(move || {
                    (
                        number,
                        client
                            .begin_query(&(number as u64).to_le_bytes(), Some(LIMIT))
                            .unwrap(),
                    )
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let requests: Vec<_> = (0..CALLS)
        .map(|_| service.recv_timeout(LIMIT).unwrap().unwrap())
        .collect();
    assert_eq!(client.queue_stats().unwrap().queued, 0);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
    thread::scope(|scope| {
        for request in requests {
            scope.spawn(move || {
                let payload = request.payload().unwrap().to_bytes().into_owned();
                request.reply(KEY, payload).wait().unwrap();
            });
        }
    });
    until(|| client.queue_stats().unwrap().received_total == CALLS as u64);
    assert_eq!(client.queue_stats().unwrap().queued, DEPTH);
    assert_eq!(
        client.queue_stats().unwrap().dropped_total,
        (CALLS - DEPTH) as u64
    );
    let results = thread::scope(|scope| {
        let workers: Vec<_> = pending
            .into_iter()
            .map(|(number, pending)| {
                scope.spawn(move || (number, pending.wait(&AtomicBool::new(false))))
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut successes = 0;
    let mut overflows = 0;
    for (number, result) in results {
        match result {
            Ok(payload) => {
                successes += 1;
                assert_eq!(payload, (number as u64).to_le_bytes());
            }
            Err(Error::QueueOverflow(message)) => {
                overflows += 1;
                assert!(message.contains("provider may have executed"));
                assert!(message.contains("no automatic retry"));
            }
            other => panic!("unexpected concurrent result: {other:?}"),
        }
    }
    assert_eq!(successes, DEPTH);
    assert_eq!(overflows, CALLS - DEPTH);
    assert_eq!(client.queue_stats().unwrap().queued, 0);
    assert!(service
        .recv_timeout(Duration::from_millis(50))
        .unwrap()
        .is_none());
}

#[test]
fn real_client_correlates_out_of_order_replies_and_accepts_only_the_first() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(2);
    let first = client.begin_query(b"first", Some(LIMIT)).unwrap();
    let first_request = service.recv_timeout(LIMIT).unwrap().unwrap();
    let second = client.begin_query(b"second", Some(LIMIT)).unwrap();
    let second_request = service.recv_timeout(LIMIT).unwrap().unwrap();
    second_request.reply(KEY, b"second-first").wait().unwrap();
    second_request
        .reply(KEY, b"second-duplicate")
        .wait()
        .unwrap();
    first_request.reply(KEY, b"first").wait().unwrap();
    until(|| client.queue_stats().unwrap().received_total == 2);
    assert_eq!(first.wait(&AtomicBool::new(false)), Ok(b"first".to_vec()));
    assert_eq!(
        second.wait(&AtomicBool::new(false)),
        Ok(b"second-first".to_vec())
    );
    assert_eq!(client.queue_stats().unwrap().received_total, 2);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
}

#[test]
fn real_client_cancel_timeout_and_drop_release_reply_capacity() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(1);

    let abandoned = client.begin_query(b"drop", Some(LIMIT)).unwrap();
    let request = service.recv_timeout(LIMIT).unwrap().unwrap();
    request.reply(KEY, b"drop").wait().unwrap();
    until(|| client.queue_stats().unwrap().queued == 1);
    drop(abandoned);
    assert_eq!(client.queue_stats().unwrap().queued, 0);

    let canceled = client.begin_query(b"cancel", Some(LIMIT)).unwrap();
    let request = service.recv_timeout(LIMIT).unwrap().unwrap();
    request.reply(KEY, b"cancel").wait().unwrap();
    until(|| client.queue_stats().unwrap().queued == 1);
    assert_eq!(canceled.wait(&AtomicBool::new(true)), Err(Error::Closed));
    assert_eq!(client.queue_stats().unwrap().queued, 0);

    let timed_out = client
        .begin_query(b"timeout", Some(Duration::from_millis(100)))
        .unwrap();
    let request = service.recv_timeout(LIMIT).unwrap().unwrap();
    assert_eq!(timed_out.wait(&AtomicBool::new(false)), Err(Error::Timeout));
    // The provider may still complete business work after caller timeout.
    // Its late reply must not occupy this client's queue or be retried.
    let _ = request.reply(KEY, b"late").wait();

    let next = client.begin_query(b"next", Some(LIMIT)).unwrap();
    let request = service.recv_timeout(LIMIT).unwrap().unwrap();
    assert_eq!(request.payload().unwrap().to_bytes().as_ref(), b"next");
    request.reply(KEY, b"next").wait().unwrap();
    until(|| client.queue_stats().unwrap().queued == 1);
    assert_eq!(next.wait(&AtomicBool::new(false)), Ok(b"next".to_vec()));
    assert_eq!(client.queue_stats().unwrap().queued, 0);
    assert_eq!(client.queue_stats().unwrap().dropped_total, 0);
}

#[test]
fn real_client_close_wakes_waiter_and_discards_completed_replies() {
    let network = Network::open();
    let service = network
        .provider
        .declare_queryable(KEY)
        .complete(true)
        .wait()
        .unwrap();
    network.wait_for_service();
    let client = network.client(1);
    let ready = client.begin_query(b"ready", Some(LIMIT)).unwrap();
    let request = service.recv_timeout(LIMIT).unwrap().unwrap();
    request.reply(KEY, b"ready").wait().unwrap();
    until(|| client.queue_stats().unwrap().queued == 1);
    let pending = client.begin_query(b"pending", Some(LIMIT)).unwrap();
    let _pending_request = service.recv_timeout(LIMIT).unwrap().unwrap();
    let waiter = thread::spawn(move || pending.wait(&AtomicBool::new(false)));
    let started = Instant::now();
    client.close().unwrap();
    assert_eq!(waiter.join().unwrap(), Err(Error::Closed));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(ready.wait(&AtomicBool::new(false)), Err(Error::Closed));
    let stats = client.queue_stats().unwrap();
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.received_total, 1);
    assert_eq!(stats.dropped_total, 0);
    assert!(stats.closed);
}
