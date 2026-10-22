//! Selected-interpreter diagnostics run as an isolated, bounded child.
use std::time::Instant;

use serde_json::{json, Value};

use super::{configuration::InvocationConfig, hooks, publication, Result};

const PROBE: &str = r#"
import importlib.util, json, pathlib, runpy, sys, time
options = json.load(sys.stdin)
try:
    spec = importlib.util.find_spec("aos_sdk")
    locations = [] if spec is None else list(spec.submodule_search_locations or [])
    path = pathlib.Path(locations[0]) / "doctor.py" if locations else None
    if path is None or not path.is_file():
        report = {"status":"error","failures":[{"code":"SDK_NOT_INSTALLED","stage":"native"}],
                  "native":{"available":False},"runtime":{"executable":sys.executable}}
    else:
        module = runpy.run_path(str(path), run_name="aos_selected_doctor")
        report = module["doctor_report"](
            config=options["config"], domain_id=options["domain_id"],
            security_profile=options["security_profile"],
            timeout=max(0.001, options["timeout"]))
except BaseException as error:
    report = {"status":"error","failures":[{"code":"PYTHON_PROBE_FAILED","stage":"startup",
              "exception_type":type(error).__name__}],"runtime":{"executable":sys.executable}}
print(json.dumps(report, ensure_ascii=True, allow_nan=False))
"#;

pub fn selected_python(
    request: &Value,
    configuration: &InvocationConfig,
    deadline: Instant,
) -> Result<Value> {
    let Some(executable) = request["python"].as_str() else {
        return Ok(json!({
            "status":"not_checked",
            "reason":"Pass python as an executable path to inspect that installed SDK."
        }));
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Ok(json!({"status":"timeout","code":"DOCTOR_TIMEOUT",
            "reason":"The shared diagnostic budget expired before Python probing."}));
    }
    // The SDK doctor reserves its own cleanup time; leave an outer allowance
    // for interpreter startup and whole-group cleanup as well.
    let probe_budget = remaining
        .mul_f64(0.8)
        .min(std::time::Duration::from_secs(86400));
    let cancelled = publication::interrupt()?;
    let began = Instant::now();
    let result = hooks::run_json_command(
        executable,
        vec!["-I".into(), "-c".into(), PROBE.into()],
        &json!({
            "config":configuration.context.zenoh_config,
            "domain_id":configuration.context.domain_id,
            "security_profile":configuration.security_profile_path,
            "timeout":probe_budget.as_secs_f64(),
        }),
        &cancelled,
        deadline,
    );
    let mut report = match result {
        Ok(report) => json!({"status":report["status"],"executable":executable,"report":report}),
        Err(error) if error.kind == hooks::HookErrorKind::Interrupted => {
            return Err(publication::interrupted_error());
        }
        Err(error) => json!({
            "status":if error.kind == hooks::HookErrorKind::OverallDeadline {"timeout"} else {"error"},
            "executable":executable,"code":format!("DOCTOR_{}", error.code()),
            "reason":"The selected interpreter probe failed; no business request was sent.",
            "failure_kind":format!("{:?}",error.kind),
        }),
    };
    report["budget_seconds"] = json!(remaining.as_secs_f64());
    report["elapsed_seconds"] = json!(began.elapsed().as_secs_f64());
    Ok(report)
}

pub fn validate(request: &Value) -> Result<()> {
    if request["python"]
        .as_str()
        .is_some_and(|value| value.contains('\0'))
    {
        return Err(super::CliError::input("python must not contain a NUL byte"));
    }
    publication::seconds(request, "timeout", 30.0, false).map(|_| ())
}
