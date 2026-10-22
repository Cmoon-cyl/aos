//! Thin Action CLI over the shared native client. Ordinary waits never cancel.
//! SIGINT on send_goal requests cancellation, following ros2action Jazzy.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::time::Duration;

use aos_core::{ActionClient, ActionQos, Context, EndpointOptions};
use serde_json::{json, Value};

use super::{configuration::InvocationConfig, CliError, Result};
use super::{ensure_unique_provider, event, full_name, named_entities, text, unique_type};

pub fn execute(
    ctx: &Context,
    verb: &str,
    request: &Value,
    configuration: &InvocationConfig,
) -> Result<Option<Value>> {
    let seconds = request["timeout"].as_f64().unwrap_or(30.0);
    // send_goal timeout=0 skips only the result wait, never discovery or acceptance.
    let wait = Duration::from_secs_f64(if seconds == 0.0 { 30.0 } else { seconds });
    let resolution = configuration.target(text(request, "action_name"), "action")?;
    let name = &resolution.resolved_name;
    let found: Vec<_> = named_entities(ctx, "action", name, wait)?
        .into_iter()
        .filter(|entity| entity.kind == aos_core::EntityKind::ActionServer)
        .collect();
    if found.is_empty() {
        return Err(CliError::discovery(format!(
            "action server {name:?} was not discovered"
        )));
    }
    ensure_unique_provider(&found)?;
    let (type_name, type_hash) = unique_type(&found)?;
    if type_name != text(request, "action_type") {
        return Err(CliError::input(
            "action_type does not match the discovered server",
        ));
    }
    let interface =
        ctx.get_type_description(&full_name(&found[0]), &type_name, Some(&type_hash), wait)?;
    let node = configuration.create_node(ctx)?;
    let qos = ActionQos::from_json(&request["qos"])?;
    let client = Arc::new(ActionClient::new_with_qos(
        &node,
        &interface,
        name,
        EndpointOptions::default(),
        qos.clone(),
    )?);
    match verb {
        "get_result" => Ok(Some(client.get_result(text(request, "goal_id"), wait)?)),
        "cancel" => {
            let stamp = request
                .get("stamp")
                .cloned()
                .unwrap_or(json!({"sec":0,"nanosec":0}));
            Ok(Some(client.cancel(
                &json!({"goal_info":{"goal_id":text(request,"goal_id"),"stamp":stamp}}),
                wait,
            )?))
        }
        "send_goal" => {
            let interrupted = Arc::new(AtomicBool::new(false));
            let signal = interrupted.clone();
            ctrlc::set_handler(move || signal.store(true, Ordering::Release)).map_err(|error| {
                CliError {
                    code: "SIGNAL_HANDLER_ERROR",
                    message: error.to_string(),
                    exit: 1,
                }
            })?;
            let handle = client.send_goal(&request["goal"], None, wait)?;
            let goal_id = handle.goal_id().to_owned();
            super::operation_field("goal_id", json!(goal_id));
            event(
                "goal_response",
                json!({"goal_id":goal_id,"accepted":handle.accepted(),"stamp":handle.stamp(),
                "name_resolution":resolution,"qos":{
                    "goal_service":qos.goal_service.effective_value(),
                    "result_service":qos.result_service.effective_value(),
                    "cancel_service":qos.cancel_service.effective_value(),
                    "feedback":qos.feedback.effective_value(),
                    "status":qos.status.effective_value(),
                }}),
            )?;
            if !handle.accepted() || (seconds == 0.0 && !interrupted.load(Ordering::Acquire)) {
                return Ok(None);
            }
            // One GetResult request runs off the event loop, leaving feedback and
            // SIGINT responsive. Closing the client wakes this wait.
            let result_client = client.clone();
            let result_id = goal_id.clone();
            let (sender, receiver) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let _ = sender.send(result_client.get_result(&result_id, wait));
            });
            let outcome = loop {
                super::flush_logs()?;
                if interrupted.load(Ordering::Acquire) {
                    match client.cancel_goal(&goal_id, wait) {
                        Ok(response) => event("cancel_response", response)?,
                        Err(error) => event("cancel_error", json!({"message":error.to_string()}))?,
                    }
                    break Err(CliError {
                        code: "INTERRUPTED",
                        message: "SIGINT: cancellation requested; inspect the provider response"
                            .into(),
                        exit: 130,
                    });
                }
                if request["feedback"].as_bool().unwrap_or(false) {
                    match client.recv_feedback(Duration::ZERO) {
                        Ok(feedback) if feedback["goal_id"].as_str() == Some(&goal_id) => {
                            event("feedback", feedback)?;
                        }
                        Ok(_) | Err(aos_core::Error::Timeout) => {}
                        Err(error) => break Err(error.into()),
                    }
                }
                match receiver.recv_timeout(Duration::from_millis(20)) {
                    Ok(Ok(result)) => {
                        event(
                            "result",
                            json!({"goal_id":goal_id,"status":result["status"],"result":result["result"]}),
                        )?;
                        break Ok(None);
                    }
                    Ok(Err(error)) => break Err(error.into()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        break Err(CliError {
                            code: "INTERNAL_ERROR",
                            message: "result worker disconnected".into(),
                            exit: 1,
                        })
                    }
                }
            };
            let _ = client.close();
            let _ = worker.join();
            outcome
        }
        _ => unreachable!(),
    }
}
