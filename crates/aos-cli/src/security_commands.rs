//! Bindings for the shared compiler and a bounded, TLS-only router process.
use aos_zenoh::security::{
    client_config, compile_policy, router_config, router_policy, ClientTlsProfile,
    RouterTlsProfile, SecurityPolicy,
};
use serde_json::{json, Value};

use crate::{CliError, Result};

/// main retains this owner until terminal JSONL output has finished.
#[cfg(any(unix, windows))]
pub(crate) type SignalGuard = Option<crate::process_commands::Signals>;
#[cfg(not(any(unix, windows)))]
pub(crate) type SignalGuard = Option<()>;

pub fn request_schema(verb: &str) -> Option<Value> {
    let object = json!({"type":"object"});
    let (properties, required) = match verb {
        "compile" => (json!({"policy":object}), json!(["policy"])),
        "router-config" => (
            json!({"policy":object,"router":object}),
            json!(["policy", "router"]),
        ),
        "serve" => (
            json!({
                "policy":object,"router":object,
                "startup_timeout":{"type":"number","exclusiveMinimum":0,"maximum":60},
                "shutdown_timeout":{"type":"number","exclusiveMinimum":0,"maximum":60}
            }),
            json!(["policy", "router"]),
        ),
        "client-config" => (json!({"client":object}), json!(["client"])),
        _ => return None,
    };
    Some(json!({
        "type":"object","additionalProperties":false,
        "properties":properties,"required":required,
        "description": if verb == "serve" {
            "Run one TLS-only default-deny router from the shared Rust compiler. No arbitrary native config or environment override is accepted. SIGHUP/SIGINT/SIGQUIT/SIGTERM close the session; after successful close the first signal selects exit 129/130/131/143. Windows Ctrl-C/Ctrl-Break use exit 130. Startup and shutdown each have a steady deadline."
        } else {
            "Offline, strict Rust validation. Credentials are absolute file paths, never PEM text. No network or credential files are created."
        }
    }))
}

#[cfg(not(any(unix, windows)))]
pub fn serve(_request: &Value, _signal_guard: &mut SignalGuard) -> Result<()> {
    Err(CliError {
        code: "UNSUPPORTED",
        message: "security serve supports macOS, Linux and Windows".into(),
        exit: 2,
    })
}

#[cfg(any(unix, windows))]
pub fn serve(request: &Value, signal_guard: &mut SignalGuard) -> Result<()> {
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::{Duration, Instant};
    use zenoh::Wait;

    use crate::process_commands::{
        check_stream_output, received_signal, set_stream_summary, try_stream_event, Signals,
    };
    let startup = crate::publication::seconds(request, "startup_timeout", 10.0, false)?;
    let shutdown = crate::publication::seconds(request, "shutdown_timeout", 5.0, false)?;
    let policy = SecurityPolicy::from_json(&request["policy"].to_string())?;
    let compiled = compile_policy(&policy)?;
    let profile = RouterTlsProfile::from_json(&request["router"].to_string())?;
    let config = router_config(&profile, &compiled)?;
    let compiled = router_policy(&profile, &compiled)?;
    *signal_guard = Some(Signals::install()?);
    let interrupted = |signal| CliError {
        code: "INTERRUPTED",
        message: format!("secure router stopped by signal {signal}"),
        exit: 128 + signal,
    };
    let failure = |code, message: &str| CliError {
        code,
        message: message.into(),
        exit: 1,
    };
    let summary = |closed, signal, stage, cleanup_error: Option<&str>| {
        json!({"schema_version":1,"event":"summary","data":{
            "mode":"secure_router","domain_id":compiled.domain_id(),
            "policy_digest":compiled.source_digest(),"session_closed":closed,
            "signal":signal,"stage":stage,"cleanup_error":cleanup_error
        }})
    };
    crate::phase("session_open");
    let (opened, receive) = mpsc::sync_channel(1);
    let opening = std::thread::Builder::new()
        .name("aos-secure-router-open".into())
        .spawn(move || {
            // Cancellation drops the receiver. A subsequently opened, unsent
            // session is then dropped in this worker.
            let result = zenoh::open(config)
                .wait()
                .map_err(|_| "secure router session could not be opened");
            let _ = opened.send(result);
        });
    if opening.is_err() {
        set_stream_summary(&summary(
            false,
            0,
            "opening",
            Some("router open worker could not be started; no native session was returned"),
        ))?;
        return Err(failure(
            "ROUTER_START_FAILED",
            "could not start router worker",
        ));
    }
    let deadline = Instant::now() + startup;
    let session = loop {
        let signal = received_signal();
        if signal != 0 {
            set_stream_summary(&summary(
                false,
                signal,
                "opening",
                Some("open worker was interrupted; session closure is not yet confirmed"),
            ))?;
            return Err(interrupted(signal));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            set_stream_summary(&summary(
                false,
                0,
                "opening",
                Some("open worker exceeded its deadline; CLI process exit releases OS resources"),
            ))?;
            return Err(failure(
                "ROUTER_START_TIMEOUT",
                "secure router startup deadline expired",
            ));
        }
        match receive.recv_timeout(remaining.min(Duration::from_millis(20))) {
            Ok(Ok(session)) => break session,
            Ok(Err(message)) => {
                set_stream_summary(&summary(false, 0, "opening", Some(message)))?;
                return Err(failure("ROUTER_START_FAILED", message));
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                set_stream_summary(&summary(
                    false,
                    0,
                    "opening",
                    Some("router open worker ended without returning a native session"),
                ))?;
                return Err(failure("ROUTER_START_FAILED", "router open worker ended"));
            }
        }
    };
    crate::phase("serving");
    let zid = session.zid().to_string();
    let locators: Vec<_> = session
        .info()
        .locators()
        .wait()
        .into_iter()
        .map(|locator| locator.to_string())
        .collect();
    let run = (|| {
        if locators.is_empty() || locators.iter().any(|locator| !locator.starts_with("tls/")) {
            return Err(failure(
                "ROUTER_LISTENER_MISMATCH",
                "secure router did not expose only TLS listeners",
            ));
        }
        let ready = json!({"schema_version":1,"event":"router_ready","data":{
            "zid":zid,"locators":locators,"domain_id":compiled.domain_id(),
            "policy_digest":compiled.source_digest(),
            "scope":"TLS listeners initialized. Verify client handshakes and endpoint grants separately."
        }});
        let mut queued = false;
        loop {
            let signal = received_signal();
            if signal != 0 {
                return Ok(signal);
            }
            if session.is_closed() {
                return Err(failure(
                    "ROUTER_CLOSED",
                    "router session closed unexpectedly",
                ));
            }
            if !queued {
                queued = try_stream_event(&ready)?;
            }
            check_stream_output()?;
            std::thread::sleep(Duration::from_millis(20));
        }
    })();
    crate::phase("shutdown");
    let (closed, receive) = mpsc::sync_channel(1);
    let closing = std::thread::Builder::new()
        .name("aos-secure-router-close".into())
        .spawn(move || {
            let result = session.close().wait().is_ok();
            let _ = closed.send(result);
        });
    let close_result = if closing.is_ok() {
        receive.recv_timeout(shutdown).ok()
    } else {
        None
    };
    let cleanup_error = match close_result {
        Some(true) => None,
        Some(false) => Some("native router session close returned an error"),
        None => Some("router close was not confirmed inside the shutdown deadline"),
    };
    let signal = run.as_ref().copied().unwrap_or_default();
    set_stream_summary(&summary(
        cleanup_error.is_none(),
        signal,
        "shutdown",
        cleanup_error,
    ))?;
    let signal = run?;
    if let Some(message) = cleanup_error {
        return Err(failure("ROUTER_CLOSE_FAILED", message));
    }
    if signal != 0 {
        return Err(interrupted(signal));
    }
    Ok(())
}

pub fn execute(verb: &str, request: &Value) -> Result<Value> {
    match verb {
        "compile" => {
            let policy = SecurityPolicy::from_json(&request["policy"].to_string())?;
            let compiled = compile_policy(&policy)?;
            Ok(json!({"network_opened":false,"compiled":compiled}))
        }
        "router-config" => {
            let policy = SecurityPolicy::from_json(&request["policy"].to_string())?;
            let compiled = compile_policy(&policy)?;
            let profile = RouterTlsProfile::from_json(&request["router"].to_string())?;
            let config = router_config(&profile, &compiled)?;
            let compiled = router_policy(&profile, &compiled)?;
            Ok(json!({"network_opened":false,"config":config,"compiled":compiled}))
        }
        "client-config" => {
            let profile = ClientTlsProfile::from_json(&request["client"].to_string())?;
            let config = client_config(&profile)?;
            Ok(json!({"network_opened":false,"config":config}))
        }
        _ => Err(CliError::input("unknown security command")),
    }
}
