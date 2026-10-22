//! Explicit sequential repetitions; no request is automatically retried.
use std::sync::{atomic::Ordering, mpsc, Arc};
use std::time::{Duration, Instant};

use aos_core::{resolve_qos, Context, EndpointOptions};
use serde_json::{json, Value};

use super::{
    configuration::InvocationConfig, ensure_unique_provider, event, full_name, named_entities,
    publication, text, unique_type, CliError, Result,
};

pub fn validate(request: &Value) -> Result<()> {
    super::optional_u64(request, "times")?;
    publication::seconds(request, "timeout", 30.0, false)?;
    publication::seconds(request, "interval", 0.0, true)?;
    if request.get("interval").is_some() && request.get("times").is_none() {
        return Err(CliError::input("interval requires an explicit times count"));
    }
    Ok(())
}

pub fn execute(ctx: &Context, request: &Value, config: &InvocationConfig) -> Result<Option<Value>> {
    validate(request)?;
    let timeout = publication::seconds(request, "timeout", 30.0, false)?;
    let interval = publication::seconds(request, "interval", 0.0, true)?;
    let count = super::optional_u64(request, "times")?.unwrap_or(1);
    let interrupted = publication::interrupt()?;
    let began = Instant::now();
    let mut started = 0_u64;
    let mut completed = 0_u64;
    let outcome = (|| {
        let resolution = config.target(text(request, "service_name"), "service")?;
        let name = resolution.resolved_name.clone();
        let expected_type = text(request, "service_type").to_owned();
        let interface = publication::interruptible(ctx, &interrupted, move |context| {
            let deadline = Instant::now() + timeout;
            let found = named_entities(&context, "service", &name, timeout)?;
            ensure_unique_provider(&found)?;
            let (type_name, hash) = unique_type(&found)?;
            if type_name != expected_type {
                return Err(CliError::input(
                    "service_type does not match the discovered provider",
                ));
            }
            Ok(context.get_type_description(
                &full_name(&found[0]),
                &type_name,
                Some(&hash),
                deadline.saturating_duration_since(Instant::now()),
            )?)
        })?;
        interface.validate("request", &request["values"])?;
        let node = config.create_node(ctx)?;
        let qos = resolve_qos(&request["qos"], "services_default", true)?;
        let client = Arc::new(node.create_client_with_qos(
            &interface,
            &resolution.resolved_name,
            qos.clone(),
            EndpointOptions::default(),
        )?);
        for sequence in 1..=count {
            if interrupted.load(Ordering::Acquire) {
                return Err(publication::interrupted_error());
            }
            let request_id = uuid::Uuid::new_v4().simple().to_string();
            let call_began = Instant::now();
            let caller = client.clone();
            let values = request["values"].clone();
            let (sender, receiver) = mpsc::channel();
            started += 1;
            let worker = std::thread::spawn(move || {
                let _ = sender.send(caller.call(&values, timeout));
            });
            let result = loop {
                if let Err(error) = super::flush_logs() {
                    let _ = client.close();
                    break Err(error);
                }
                if interrupted.load(Ordering::Acquire) {
                    let _ = client.close();
                    break Err(publication::interrupted_error());
                }
                match receiver.recv_timeout(Duration::from_millis(20)) {
                    Ok(result) => break result.map_err(CliError::from),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        break Err(CliError {
                            code: "INTERNAL_ERROR",
                            message: "Service response worker disconnected".into(),
                            exit: 1,
                        });
                    }
                }
            };
            let _ = worker.join();
            match result {
                Ok(response) => {
                    completed += 1;
                    event(
                        "response",
                        json!({
                            "sequence":sequence,"request_id":request_id,"request_id_scope":"CLI operation",
                            "service_name":resolution.resolved_name,"name_resolution":resolution,
                            "response":response,"qos":qos.effective_value(),
                            "elapsed_seconds":call_began.elapsed().as_secs_f64()
                        }),
                    )?;
                }
                Err(error) => {
                    event(
                        "call_error",
                        json!({
                            "sequence":sequence,"request_id":request_id,"request_id_scope":"CLI operation",
                            "code":error.code,"message":error.message,
                            "remote_execution":"unknown; local timeout or interruption does not cancel remote business",
                            "retry":false
                        }),
                    )?;
                    return Err(error);
                }
            }
            if sequence < count
                && !publication::sleep_until(Instant::now() + interval, &interrupted)
            {
                return Err(publication::interrupted_error());
            }
        }
        Ok(())
    })();
    event(
        "summary",
        json!({
            "operation":"service call","requested_count":count,"started_count":started,
            "completed_count":completed,"elapsed_seconds":began.elapsed().as_secs_f64(),
            "stop_reason":match &outcome {Ok(())=>"count",Err(e) if e.exit==130=>"interrupted",Err(_)=>"error"},
            "retry":false,"schedule":"one response or failure before the next call; interval starts after response"
        }),
    )?;
    outcome?;
    Ok(None)
}
