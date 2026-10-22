//! Real provider/client child processes, with a local Zenoh router in the parent.
//! A custom harness lets us assert the children's exact stdout/stderr contract.
mod support;

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{atomic::AtomicBool, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use aos_core::transport::{Endpoint, EntityKind, GraphEntity, QoS, Transport};
use aos_core::{
    ActionClient, ActionServer, ActionServerOptions, Context, EndpointOptions, Error, Interface,
    Node, NodeOptions, Telemetry,
};
use aos_zenoh::ZenohTransport;
use serde_json::{json, Value};
use zenoh::Wait;

const LIMIT: Duration = Duration::from_secs(10);
const CALLS: usize = 6;
const PAYLOAD_SECRET: &[u8] = b"telemetry-application-payload-secret";
const ERROR_SECRET: &str = "telemetry-provider-error-secret";
const ACTION_NAME: &str = "/telemetry/action";
const INACTIVE_SECRET: &str = "telemetry-inactive-reason-secret";

fn endpoint() -> Endpoint {
    Endpoint {
        name: "/telemetry/service".into(),
        type_name: "demo/srv/Telemetry".into(),
        type_hash: "a".repeat(64),
    }
}

fn transport(address: &str) -> ZenohTransport {
    let telemetry = Telemetry::default();
    telemetry.enable_log_capture(256).unwrap();
    ZenohTransport::open_with_telemetry(support::peer_config(address), 0, telemetry).unwrap()
}

fn finish(transport: &ZenohTransport) {
    transport.close().unwrap();
    let telemetry = transport.telemetry();
    let snapshot = telemetry.snapshot();
    assert!(snapshot
        .active_resources
        .values()
        .all(|active| *active == 0));
    assert_eq!(snapshot.rejected_events_total, 0);
    assert_eq!(snapshot.metric_keys_dropped_total, 0);
    assert_eq!(snapshot.log_dropped_total, 0);
    assert!(snapshot.metrics.len() < 20);
    assert!(telemetry.flush_stderr().unwrap() > 0);
    assert!(telemetry.drain_records().is_empty());
    // Only explicitly produced application JSON is allowed on stdout.
    println!("{}", json!({"ok": true, "pid": std::process::id()}));
}

fn provider(address: &str) {
    let transport = transport(address);
    let service = transport.create_service(&endpoint()).unwrap();
    let _guard = transport
        .declare_graph_entity(&GraphEntity {
            id: "telemetry_provider".into(),
            node_id: "telemetry_provider".into(),
            node_name: "telemetry_provider".into(),
            namespace: "/".into(),
            kind: EntityKind::Service,
            endpoint: Some(endpoint()),
            qos: Some(QoS::default()),
        })
        .unwrap();
    println!("{}", json!({"ready": true, "pid": std::process::id()}));
    std::io::stdout().flush().unwrap();
    for index in 0..CALLS {
        let request = service.recv_timeout(LIMIT).unwrap();
        assert_eq!(request.payload(), PAYLOAD_SECRET);
        assert!(request.request_id().is_some());
        request
            .reply(match index % 3 {
                0 => Ok(b"business-response".to_vec()),
                1 => Err(Error::Validation(ERROR_SECRET.into())),
                _ => Err(Error::Inactive(INACTIVE_SECRET.into())),
            })
            .unwrap();
    }
    finish(&transport);
}

fn caller(address: &str) {
    let transport = transport(address);
    let until = Instant::now() + LIMIT;
    while !transport
        .graph(Duration::from_millis(100))
        .unwrap()
        .iter()
        .any(|entity| entity.id == "telemetry_provider")
    {
        assert!(Instant::now() < until);
        thread::sleep(Duration::from_millis(10));
    }
    let client = transport
        .create_client_with_qos(&endpoint(), &QoS::default())
        .unwrap()
        .unwrap();
    for index in 0..CALLS {
        let result = if index < 3 {
            transport.call(&endpoint(), PAYLOAD_SECRET, LIMIT)
        } else {
            client.call_wait(PAYLOAD_SECRET, Some(LIMIT), &AtomicBool::new(false))
        };
        match index % 3 {
            0 => assert_eq!(result.unwrap(), b"business-response"),
            1 => assert!(
                matches!(result, Err(Error::Remote(message)) if message.contains(ERROR_SECRET))
            ),
            _ => assert!(matches!(result, Err(Error::Inactive(message))
                if message.starts_with("SERVICE_INACTIVE:") && !message.contains(INACTIVE_SECRET))),
        }
    }
    finish(&transport);
}

fn action_interface() -> Interface {
    Interface::from_json(json!({
        "name": "demo/action/Telemetry",
        "kind": "action",
        "goal": {
            "type": "object",
            "properties": {"mode": {"type": "string"}, "secret": {"type": "string"}},
            "required": ["mode", "secret"],
            "additionalProperties": false
        },
        "result": {
            "type": "object",
            "properties": {"done": {"type": "boolean"}},
            "required": ["done"],
            "additionalProperties": false,
            "default": {"done": false}
        },
        "feedback": {
            "type": "object",
            "properties": {"step": {"type": "integer"}},
            "required": ["step"],
            "additionalProperties": false
        }
    }))
    .unwrap()
}

fn action_context(address: &str) -> Context {
    let context = Context::new(transport(address));
    context.enable_log_capture(1024).unwrap();
    context
}

fn finish_action(context: &Context, node: &Node, goals: Value) {
    // Shutdown while application wrappers remain owned must clear logical and
    // transport gauges. Explicit Node destruction also records its final event.
    context.shutdown().unwrap();
    assert!(context
        .telemetry_snapshot()
        .active_resources
        .values()
        .all(|count| *count == 0));
    let _ = node.destroy_node();
    let snapshot = context.telemetry_snapshot();
    assert_eq!(snapshot.rejected_events_total, 0);
    assert_eq!(snapshot.metric_keys_dropped_total, 0);
    assert_eq!(snapshot.log_dropped_total, 0);
    assert!(snapshot.metrics.len() <= aos_core::MAX_TELEMETRY_METRIC_KEYS);
    assert!(context.flush_logs_to_stderr().unwrap() > 0);
    println!(
        "{}",
        json!({"ok": true, "pid": std::process::id(), "node_id":node.id(), "goals":goals})
    );
}

fn action_provider(address: &str) {
    let context = action_context(address);
    let node = Node::new(
        context.clone(),
        "action_provider",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let server = ActionServer::new(
        &node,
        &action_interface(),
        ACTION_NAME,
        ActionServerOptions::default(),
        EndpointOptions::default(),
    )
    .unwrap();
    println!("{}", json!({"ready":true,"pid":std::process::id()}));
    std::io::stdout().flush().unwrap();
    let mut ids = serde_json::Map::new();
    for mode in ["success", "rejected", "canceled", "aborted"] {
        let request = server.take_goal_request(LIMIT).unwrap();
        assert_eq!(request.request()["mode"], mode);
        assert_eq!(
            request.request()["secret"],
            std::str::from_utf8(PAYLOAD_SECRET).unwrap()
        );
        ids.insert(mode.into(), json!(request.goal_id()));
        let goal = request.decide(mode != "rejected").unwrap();
        let Some(goal) = goal else { continue };
        goal.execute().unwrap();
        match mode {
            "success" => {
                goal.succeed().unwrap();
                goal.finish(Some(&json!({"done":true}))).unwrap();
            }
            "canceled" => {
                // A denied cancel must not complete the Goal. Accept only the
                // second independent cancel request, then finish as canceled.
                let cancel = server.take_cancel_request(LIMIT).unwrap();
                assert_eq!(cancel.candidates().len(), 1);
                cancel.respond(&[]).unwrap();
                let cancel = server.take_cancel_request(LIMIT).unwrap();
                cancel.respond(&[goal.goal_id().to_owned()]).unwrap();
                assert!(goal.is_cancel_requested());
                goal.canceled().unwrap();
                goal.finish(Some(&json!({"done":false}))).unwrap();
            }
            "aborted" => {
                goal.abort().unwrap();
                goal.finish_error(ERROR_SECRET).unwrap();
            }
            _ => unreachable!(),
        }
    }
    let deadline = Instant::now() + LIMIT;
    loop {
        let sent = context
            .telemetry_snapshot()
            .metrics
            .iter()
            .filter(|metric| {
                metric.key.component == aos_core::TelemetryComponent::Action
                    && metric.key.event == "result_sent"
            })
            .map(|metric| metric.count)
            .sum::<u64>();
        if sent == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "three result replies must actually be submitted"
        );
        thread::sleep(Duration::from_millis(10));
    }
    finish_action(&context, &node, Value::Object(ids));
}

fn action_caller(address: &str) {
    let context = action_context(address);
    let node = Node::new(
        context.clone(),
        "action_caller",
        "/",
        NodeOptions::default(),
    )
    .unwrap();
    let client = ActionClient::new(
        &node,
        &action_interface(),
        ACTION_NAME,
        EndpointOptions::default(),
    )
    .unwrap();
    assert!(client.wait_for_server(Some(LIMIT)).unwrap());
    let mut ids = serde_json::Map::new();
    for mode in ["success", "rejected", "canceled", "aborted"] {
        let goal = client
            .send_goal(
                &json!({
                    "mode":mode, "secret":std::str::from_utf8(PAYLOAD_SECRET).unwrap(),
                }),
                None,
                LIMIT,
            )
            .unwrap();
        ids.insert(mode.into(), json!(goal.goal_id()));
        assert_eq!(goal.accepted(), mode != "rejected");
        match mode {
            "success" => assert_eq!(
                goal.get_result(LIMIT).unwrap(),
                json!({"status":4,"result":{"done":true}})
            ),
            "rejected" => {}
            "canceled" => {
                assert!(goal.cancel_goal(LIMIT).unwrap()["goals_canceling"]
                    .as_array()
                    .unwrap()
                    .is_empty());
                let cancel = goal.cancel_goal(LIMIT).unwrap();
                assert_eq!(cancel["goals_canceling"][0]["goal_id"], goal.goal_id());
                assert_eq!(
                    goal.get_result(LIMIT).unwrap(),
                    json!({"status":5,"result":{"done":false}})
                );
            }
            "aborted" => assert!(
                matches!(goal.get_result(LIMIT), Err(Error::Remote(message)) if message.contains(ERROR_SECRET))
            ),
            _ => unreachable!(),
        }
    }
    finish_action(&context, &node, Value::Object(ids));
}

struct OwnedChild {
    child: Child,
    lines: mpsc::Receiver<String>,
    output_reader: Option<thread::JoinHandle<()>>,
    error_reader: Option<thread::JoinHandle<String>>,
}

impl OwnedChild {
    fn spawn(role: &str, address: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([role, address])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (send, lines) = mpsc::channel();
        let output_reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if send.send(line.unwrap()).is_err() {
                    return;
                }
            }
        });
        let error_reader = thread::spawn(move || {
            let mut output = String::new();
            stderr.read_to_string(&mut output).unwrap();
            output
        });
        Self {
            child,
            lines,
            output_reader: Some(output_reader),
            error_reader: Some(error_reader),
        }
    }

    fn finish(&mut self) -> (Vec<Value>, Vec<Value>) {
        let until = Instant::now() + LIMIT;
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < until,
                "child did not exit within the bound"
            );
            thread::sleep(Duration::from_millis(10));
        };
        self.output_reader.take().unwrap().join().unwrap();
        let stderr = self.error_reader.take().unwrap().join().unwrap();
        assert!(status.success(), "child failed: {stderr}");
        assert!(!stderr.contains(std::str::from_utf8(PAYLOAD_SECRET).unwrap()));
        assert!(!stderr.contains(ERROR_SECRET));
        assert!(!stderr.contains(INACTIVE_SECRET));
        assert!(!stderr.contains("business-response"));
        let records = stderr
            .lines()
            .map(|line| serde_json::from_str(line).expect("stderr must contain only JSONL records"))
            .collect::<Vec<Value>>();
        for record in &records {
            assert_eq!(record["identity_trust"], "unauthenticated");
            assert_eq!(record["schema_version"], 1);
            assert!(record["event"].is_string());
        }
        let stdout = self
            .lines
            .try_iter()
            .map(|line| serde_json::from_str(&line).expect("stdout must contain application JSON"))
            .collect();
        (stdout, records)
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn ids(records: &[Value], event: &str) -> BTreeSet<String> {
    records
        .iter()
        .filter(|record| record["event"] == event)
        .map(|record| record["request_id"].as_str().unwrap().to_owned())
        .collect()
}

fn process_test() {
    let mut config = support::config("zenoh-router.json5");
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .unwrap();
    let router = zenoh::open(config).wait().unwrap();
    let address = router.info().locators().wait()[0].to_string();
    let mut server = OwnedChild::spawn("--telemetry-provider", &address);
    let ready: Value =
        serde_json::from_str(&server.lines.recv_timeout(LIMIT).expect("provider ready")).unwrap();
    assert_eq!(ready, json!({"ready": true, "pid": server.child.id()}));
    let mut client = OwnedChild::spawn("--telemetry-caller", &address);
    assert_ne!(server.child.id(), client.child.id());
    assert_ne!(server.child.id(), std::process::id());
    let (client_stdout, client_records) = client.finish();
    let (server_stdout, server_records) = server.finish();
    assert!(client_records
        .iter()
        .chain(&server_records)
        .all(|record| record["event"].as_str().unwrap().starts_with("zenoh.")));
    assert_eq!(
        client_stdout,
        vec![json!({"ok":true,"pid":client.child.id()})]
    );
    assert_eq!(
        server_stdout,
        vec![json!({"ok":true,"pid":server.child.id()})]
    );
    for (records, pid) in [
        (&client_records, client.child.id()),
        (&server_records, server.child.id()),
    ] {
        assert!(records.iter().all(|record| record["process_id"] == pid));
        let contexts = records
            .iter()
            .map(|record| record["context_id"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(contexts.len(), 1);
    }
    assert_ne!(
        client_records[0]["context_id"],
        server_records[0]["context_id"]
    );
    let sent = ids(&client_records, "zenoh.service.query_submitted");
    assert_eq!(sent.len(), CALLS);
    for (records, event) in [
        (&server_records, "zenoh.service.request_received"),
        (&server_records, "zenoh.service.request_taken"),
        (&server_records, "zenoh.service.reply_sent"),
        (&client_records, "zenoh.service.reply_received"),
        (&client_records, "zenoh.service.call_finished"),
    ] {
        assert_eq!(ids(records, event), sent, "{event}");
    }
    let replies = client_records
        .iter()
        .filter(|record| record["event"] == "zenoh.service.reply_received")
        .collect::<Vec<_>>();
    assert_eq!(replies.len(), CALLS);
    assert_eq!(
        replies
            .iter()
            .filter(|record| record["correlation_source"] == "reply_attachment")
            .count(),
        2
    );
    assert_eq!(
        replies
            .iter()
            .filter(|record| record["correlation_source"] == "query_context"
                && record["outcome"] == "remote")
            .count(),
        2
    );
    assert_eq!(
        replies
            .iter()
            .filter(|record| record["correlation_source"] == "query_context"
                && record["outcome"] == "inactive")
            .count(),
        2
    );
    println!("two-process telemetry correlation and stream isolation passed");
}

fn action_process_test() {
    let mut config = support::config("zenoh-router.json5");
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .unwrap();
    let router = zenoh::open(config).wait().unwrap();
    let address = router.info().locators().wait()[0].to_string();
    let mut server = OwnedChild::spawn("--telemetry-action-provider", &address);
    let ready: Value = serde_json::from_str(
        &server
            .lines
            .recv_timeout(LIMIT)
            .expect("Action provider ready"),
    )
    .unwrap();
    assert_eq!(ready, json!({"ready":true,"pid":server.child.id()}));
    let mut client = OwnedChild::spawn("--telemetry-action-caller", &address);
    let (client_stdout, client_records) = client.finish();
    let (server_stdout, server_records) = server.finish();
    assert_ne!(client.child.id(), server.child.id());
    assert_eq!(client_stdout.len(), 1);
    assert_eq!(server_stdout.len(), 1);
    for (output, records, pid) in [
        (&client_stdout[0], &client_records, client.child.id()),
        (&server_stdout[0], &server_records, server.child.id()),
    ] {
        assert_eq!(output["ok"], true);
        assert_eq!(output["pid"], pid);
        assert_eq!(output.as_object().unwrap().len(), 4);
        assert!(records.iter().all(|record| record["process_id"] == pid));
        for event in ["created", "destroyed"] {
            assert_eq!(
                records
                    .iter()
                    .filter(|record| record["component"] == "node"
                        && record["event"] == event
                        && record["node_id"] == output["node_id"])
                    .count(),
                1
            );
        }
    }
    let goals = &client_stdout[0]["goals"];
    assert_eq!(goals, &server_stdout[0]["goals"]);
    assert_eq!(goals.as_object().unwrap().len(), 4);
    let sent = ids(&client_records, "zenoh.service.query_submitted");
    // Four send_goal, two cancel_goal, and three get_result operations.
    assert_eq!(sent.len(), 9);
    for (records, event) in [
        (&server_records, "zenoh.service.request_received"),
        (&server_records, "zenoh.service.request_taken"),
        (&server_records, "zenoh.service.reply_sent"),
        (&server_records, "callback_completed"),
        (&client_records, "zenoh.service.reply_received"),
        (&client_records, "zenoh.service.call_finished"),
    ] {
        assert_eq!(ids(records, event), sent, "{event}");
    }
    let action = server_records
        .iter()
        .filter(|record| record["component"] == "action")
        .collect::<Vec<_>>();
    assert!(action
        .iter()
        .all(|record| record["node_id"] == server_stdout[0]["node_id"]
            && record["endpoint"] == ACTION_NAME));
    let wire_endpoint = |id: &str, suffix: &str| {
        assert!(client_records
            .iter()
            .any(|record| record["event"] == "zenoh.service.query_submitted"
                && record["request_id"] == id
                && record["endpoint"].as_str().unwrap().contains(suffix)));
    };
    for mode in ["success", "rejected", "canceled", "aborted"] {
        let goal_id = &goals[mode];
        let admission_name = if mode == "rejected" {
            "goal_rejected"
        } else {
            "goal_accepted"
        };
        let admission = action
            .iter()
            .filter(|record| record["event"] == admission_name && record["goal_id"] == *goal_id)
            .collect::<Vec<_>>();
        assert_eq!(admission.len(), 1);
        let goal_request_id = admission[0]["request_id"].as_str().unwrap();
        wire_endpoint(goal_request_id, "/_action/send_goal/");
        if mode == "rejected" {
            assert_eq!(admission[0]["outcome"], "rejected");
            assert_eq!(
                action
                    .iter()
                    .filter(|record| record["goal_id"] == *goal_id)
                    .count(),
                1
            );
            continue;
        }
        let terminal = match mode {
            "success" => "goal_succeeded",
            "canceled" => "goal_canceled",
            _ => "goal_aborted",
        };
        for event in ["execution_started", terminal, "result_completed"] {
            let found = action
                .iter()
                .filter(|record| record["goal_id"] == *goal_id && record["event"] == event)
                .collect::<Vec<_>>();
            assert_eq!(found.len(), 1, "{mode}: {event}");
            assert_eq!(found[0]["request_id"], goal_request_id);
        }
        let result = action
            .iter()
            .find(|record| record["goal_id"] == *goal_id && record["event"] == "result_sent")
            .unwrap();
        let result_id = result["request_id"].as_str().unwrap();
        assert_ne!(result_id, goal_request_id);
        wire_endpoint(result_id, "/_action/get_result/");
        if mode == "aborted" {
            assert_eq!(result["outcome"], "remote");
        }
        if mode == "canceled" {
            let mut cancel_ids = BTreeSet::new();
            for event in ["cancel_rejected", "cancel_accepted"] {
                let cancel = action
                    .iter()
                    .find(|record| record["goal_id"] == *goal_id && record["event"] == event)
                    .unwrap();
                let cancel_id = cancel["request_id"].as_str().unwrap();
                assert_ne!(cancel_id, goal_request_id);
                assert_ne!(cancel_id, result_id);
                wire_endpoint(cancel_id, "/_action/cancel_goal/");
                cancel_ids.insert(cancel_id);
            }
            assert_eq!(cancel_ids.len(), 2);
            let state_change = action
                .iter()
                .find(|record| {
                    record["goal_id"] == *goal_id && record["event"] == "cancel_requested"
                })
                .unwrap();
            assert_eq!(state_change["request_id"], goal_request_id);
        }
    }
    println!("two-process Action goal and request correlation passed");
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("--telemetry-provider") => provider(&args[2]),
        Some("--telemetry-caller") => caller(&args[2]),
        Some("--telemetry-action-provider") => action_provider(&args[2]),
        Some("--telemetry-action-caller") => action_caller(&args[2]),
        _ => {
            process_test();
            action_process_test();
        }
    }
}
