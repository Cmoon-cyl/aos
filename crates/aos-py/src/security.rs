//! Python adapters for the shared, offline Rust security compiler.
use aos_zenoh::security::{
    client_config, compile_policy, router_config, ClientTlsProfile, RouterTlsProfile,
    SecurityPolicy,
};
use pyo3::prelude::*;
use serde_json::json;

use crate::errors::to_py_err;
use crate::typed::{json_from_py, json_to_py};

#[pyfunction]
fn compile_security_policy<'py>(
    py: Python<'py>,
    policy: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let policy =
        SecurityPolicy::from_json(&json_from_py(policy)?.to_string()).map_err(to_py_err)?;
    let compiled = py.detach(|| compile_policy(&policy)).map_err(to_py_err)?;
    json_to_py(py, &json!(compiled))
}

#[pyfunction]
fn client_security_config<'py>(
    py: Python<'py>,
    profile: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let profile =
        ClientTlsProfile::from_json(&json_from_py(profile)?.to_string()).map_err(to_py_err)?;
    let config = py.detach(|| client_config(&profile)).map_err(to_py_err)?;
    json_to_py(py, &json!(config))
}

#[pyfunction]
fn router_security_config<'py>(
    py: Python<'py>,
    profile: &Bound<'_, PyAny>,
    policy: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let profile =
        RouterTlsProfile::from_json(&json_from_py(profile)?.to_string()).map_err(to_py_err)?;
    let policy =
        SecurityPolicy::from_json(&json_from_py(policy)?.to_string()).map_err(to_py_err)?;
    let config = py
        .detach(|| {
            let compiled = compile_policy(&policy)?;
            router_config(&profile, &compiled)
        })
        .map_err(to_py_err)?;
    json_to_py(py, &json!(config))
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(compile_security_policy, module)?)?;
    module.add_function(wrap_pyfunction!(client_security_config, module)?)?;
    module.add_function(wrap_pyfunction!(router_security_config, module)?)?;
    Ok(())
}
