//! QoS inputs pass unchanged to the common Rust resolver.

use pyo3::prelude::*;
use serde_json::Value;

use crate::errors::to_py_err;
use crate::typed::{json_from_py, json_to_py};

#[pyfunction(name = "_resolve_qos", signature = (value=None, *, default_preset="default", service=false))]
fn resolve_qos<'py>(
    py: Python<'py>,
    value: Option<&Bound<'_, PyAny>>,
    default_preset: &str,
    service: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let request = value.map(json_from_py).transpose()?.unwrap_or(Value::Null);
    let resolved = aos_core::resolve_qos(&request, default_preset, service).map_err(to_py_err)?;
    json_to_py(py, &resolved.effective_value())
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(resolve_qos, module)?)?;
    Ok(())
}
