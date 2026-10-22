//! Cross-process pub/sub probe: each process builds its own aos-core Context
//! over a ZenohTransport opened from `config/zenoh-session.json5`.
//!
//! ```text
//! probe listen <session-config> <router-endpoint> <topic> <timeout-ms>
//! probe talk   <session-config> <router-endpoint> <topic> <payload> <count>
//! ```
//!
//! Exit codes: 0 done, 2 bad arguments, 3 open/communication failure,
//! 4 wait expired. Set `RUST_LOG` for Zenoh transport diagnostics.

use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

use aos_core::{Context, Error};
use aos_zenoh::ZenohTransport;

fn main() -> ExitCode {
    zenoh::try_init_log_from_env();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["listen", config, endpoint, topic, timeout_ms] => match timeout_ms.parse() {
            Ok(ms) => listen(config, endpoint, topic, Duration::from_millis(ms)),
            Err(_) => Err(usage()),
        },
        ["talk", config, endpoint, topic, payload, count] => match count.parse() {
            Ok(count) => talk(config, endpoint, topic, payload, count),
            Err(_) => Err(usage()),
        },
        _ => Err(usage()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("probe: {message}");
            ExitCode::from(code)
        }
    }
}

type Failure = (u8, String);

fn usage() -> Failure {
    (
        2,
        "usage: probe listen CONFIG ENDPOINT TOPIC TIMEOUT_MS | probe talk CONFIG ENDPOINT TOPIC PAYLOAD COUNT".into(),
    )
}

fn failure(error: Error) -> Failure {
    let code = match error {
        Error::InvalidArgument(_)
        | Error::Validation(_)
        | Error::ResourceLimit(_)
        | Error::Unsupported(_) => 2,
        Error::Timeout => 4,
        Error::Closed | Error::Transport(_) | Error::QueueOverflow(_) | Error::NoResponse(_) => 3,
        Error::Remote(_) | Error::Inactive(_) => 1,
    };
    (code, error.to_string())
}

fn open(config: &str, endpoint: &str) -> Result<Context, Failure> {
    let mut config = zenoh::Config::from_file(config).map_err(|e| (2, e.to_string()))?;
    config
        .insert_json5("connect/endpoints", &format!(r#"["{endpoint}"]"#))
        .map_err(|e| (2, e.to_string()))?;
    let started = Instant::now();
    let transport = ZenohTransport::open(config, 0)
        .map_err(|e| (3, format!("{e} after {:?}", started.elapsed())))?;
    println!(
        "probe: pid={} zid={} routers={:?} open_ms={}",
        std::process::id(),
        transport.zid(),
        transport.router_zids(),
        started.elapsed().as_millis()
    );
    Ok(Context::new(transport))
}

fn finish(context: Context, result: Result<(), Failure>) -> Result<(), Failure> {
    // Attempt cleanup even when the exchange failed.
    let cleanup = context.shutdown().map_err(failure);
    if cleanup.is_ok() {
        println!("probe: shutdown");
    }
    result.and(cleanup)
}

fn listen(config: &str, endpoint: &str, topic: &str, timeout: Duration) -> Result<(), Failure> {
    let context = open(config, endpoint)?;
    let result = (|| {
        let subscription = context.create_subscription(topic).map_err(failure)?;
        println!("probe: ready topic={topic}");
        let payload = subscription.recv_timeout(timeout).map_err(failure)?;
        println!("probe: received {}", String::from_utf8_lossy(&payload));
        Ok(())
    })();
    finish(context, result)
}

fn talk(
    config: &str,
    endpoint: &str,
    topic: &str,
    payload: &str,
    count: u32,
) -> Result<(), Failure> {
    let context = open(config, endpoint)?;
    let result = (|| {
        let publisher = context.create_publisher(topic).map_err(failure)?;
        // Volatile samples sent before the remote subscription is known are
        // dropped, so this probe repeats; the listener reports what arrived.
        for _ in 0..count {
            publisher.publish(payload.as_bytes()).map_err(failure)?;
            thread::sleep(Duration::from_millis(100));
        }
        println!("probe: published topic={topic} count={count}");
        Ok(())
    })();
    finish(context, result)
}
