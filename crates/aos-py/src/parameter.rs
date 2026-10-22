//! Bindings never store parameter state or call user validators under a lock.

use std::{path::PathBuf, sync::Mutex};

use aos_core::{
    Parameter, ParameterDescriptor, ParameterError, ParameterServer, ParameterServiceKind,
    ParameterServiceRequest, ParameterSnapshot, ParameterStore, ParameterTransaction,
    ParameterType, ParameterValue,
};
use pyo3::prelude::*;
use serde_json::Value;

use crate::errors::{parameter_error, to_py_err, InvalidArgumentError, InvalidStateError};
use crate::time::number;
use crate::typed::{duration, json_from_py, json_to_py, PyInterface};

fn parse_error(error: serde_json::Error) -> PyErr {
    parameter_error(ParameterError::InvalidArgument(error.to_string()))
}

#[pyfunction(name = "_parameter_value", signature = (value, type_=None))]
fn parameter_value<'py>(
    py: Python<'py>,
    value: &Bound<'_, PyAny>,
    type_: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let kind: Option<ParameterType> = type_
        .map(|value| serde_json::from_value(Value::String(value.into())))
        .transpose()
        .map_err(|error| parameter_error(ParameterError::InvalidArgument(error.to_string())))?;
    let value = ParameterValue::from_json_with_type(&json_from_py(value)?, kind)
        .map_err(parameter_error)?;
    json_to_py(py, &serde_json::json!(value))
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ParameterStore")]
pub(crate) struct PyParameterStore {
    pub(crate) inner: ParameterStore,
}

#[pymethods]
impl PyParameterStore {
    #[getter]
    fn version(&self, py: Python<'_>) -> PyResult<u64> {
        py.detach(|| self.inner.version()).map_err(parameter_error)
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let snapshot = py
            .detach(|| self.inner.snapshot())
            .map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(snapshot))
    }

    fn get<'py>(&self, py: Python<'py>, names: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let values = py
            .detach(|| self.inner.get_parameters(&names))
            .map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(values))
    }

    fn get_types<'py>(&self, py: Python<'py>, names: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let types = py
            .detach(|| self.inner.get_types(&names))
            .map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(types))
    }

    fn describe<'py>(&self, py: Python<'py>, names: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let records = py
            .detach(|| self.inner.describe(&names))
            .map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(records))
    }

    #[pyo3(signature = (prefixes, depth))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        prefixes: Vec<String>,
        depth: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let depth = json_from_py(depth)?.as_u64().ok_or_else(|| {
            InvalidArgumentError::new_err("parameter list depth must be a nonnegative integer")
        })?;
        let result = py
            .detach(|| self.inner.list(&prefixes, depth))
            .map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(result))
    }

    #[pyo3(signature = (name, value, descriptor, *, ignore_override=false, timeout_sec=None))]
    fn prepare_declare(
        &self,
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        descriptor: &Bound<'_, PyAny>,
        ignore_override: bool,
        timeout_sec: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyParameterTransaction> {
        let value: ParameterValue =
            serde_json::from_value(json_from_py(value)?).map_err(parse_error)?;
        let descriptor: ParameterDescriptor =
            serde_json::from_value(json_from_py(descriptor)?).map_err(parse_error)?;
        let timeout = timeout_sec
            .map(|value| duration(number(value, "parameter transaction timeout")?))
            .transpose()?
            .unwrap_or(aos_core::DEFAULT_PARAMETER_TRANSACTION_TIMEOUT);
        let inner = py
            .detach(|| {
                self.inner.prepare_declare_with_timeout(
                    name,
                    value,
                    descriptor,
                    ignore_override,
                    timeout,
                )
            })
            .map_err(parameter_error)?;
        Ok(PyParameterTransaction::new(inner))
    }

    #[pyo3(signature = (parameters, *, timeout_sec=None))]
    fn prepare_set(
        &self,
        py: Python<'_>,
        parameters: &Bound<'_, PyAny>,
        timeout_sec: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyParameterTransaction> {
        let parameters: Vec<Parameter> =
            serde_json::from_value(json_from_py(parameters)?).map_err(parse_error)?;
        let timeout = timeout_sec
            .map(|value| duration(number(value, "parameter transaction timeout")?))
            .transpose()?
            .unwrap_or(aos_core::DEFAULT_PARAMETER_TRANSACTION_TIMEOUT);
        let inner = py
            .detach(|| self.inner.prepare_set_with_timeout(parameters, timeout))
            .map_err(parameter_error)?;
        Ok(PyParameterTransaction::new(inner))
    }

    fn prepare_load(&self, py: Python<'_>, path: PathBuf) -> PyResult<PyParameterTransaction> {
        let inner = py
            .detach(|| {
                let snapshot = ParameterSnapshot::read(path)?;
                self.inner.prepare_load(&snapshot)
            })
            .map_err(parameter_error)?;
        Ok(PyParameterTransaction::new(inner))
    }

    fn dump(&self, py: Python<'_>, path: PathBuf) -> PyResult<()> {
        py.detach(|| self.inner.snapshot()?.write(path))
            .map_err(parameter_error)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ParameterTransaction")]
struct PyParameterTransaction {
    inner: Mutex<Option<ParameterTransaction>>,
}

impl PyParameterTransaction {
    fn new(inner: ParameterTransaction) -> Self {
        Self {
            inner: Mutex::new(Some(inner)),
        }
    }
}

#[pymethods]
impl PyParameterTransaction {
    #[getter]
    fn id(&self) -> PyResult<String> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|inner| inner.id().to_owned())
            .ok_or_else(|| InvalidStateError::new_err("parameter transaction is already consumed"))
    }

    #[getter]
    fn base_version(&self) -> PyResult<u64> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(ParameterTransaction::base_version)
            .ok_or_else(|| InvalidStateError::new_err("parameter transaction is already consumed"))
    }

    #[getter]
    fn candidate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let candidate = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|inner| serde_json::json!(inner.candidate()))
            .ok_or_else(|| {
                InvalidStateError::new_err("parameter transaction is already consumed")
            })?;
        json_to_py(py, &candidate)
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.lock().unwrap().take().ok_or_else(|| {
            InvalidStateError::new_err("parameter transaction is already consumed")
        })?;
        let result = py.detach(|| inner.commit()).map_err(parameter_error)?;
        json_to_py(py, &serde_json::json!(result))
    }

    fn abort(&self) {
        self.inner.lock().unwrap().take();
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ParameterServer")]
pub(crate) struct PyParameterServer {
    pub(crate) inner: ParameterServer,
}

#[pymethods]
impl PyParameterServer {
    fn recv(&self, py: Python<'_>, timeout: f64) -> PyResult<PyParameterServiceRequest> {
        let timeout = duration(timeout)?;
        let request = py
            .detach(|| self.inner.recv_timeout(timeout))
            .map_err(to_py_err)?;
        Ok(PyParameterServiceRequest {
            inner: Mutex::new(Some(request)),
        })
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

fn validate_python(
    validator: &Py<PyAny>,
    candidate: &[Parameter],
) -> aos_core::ParameterResult<()> {
    // Python::attach reacquires the GIL on this same Executor/caller thread.
    // The core invokes this between prepare and commit with no store lock.
    Python::attach(|py| {
        let reply = (|| -> PyResult<(bool, String)> {
            let value = json_to_py(py, &serde_json::json!(candidate))?;
            let reply = validator.call1(py, (value,))?;
            let value = json_from_py(reply.bind(py))?;
            let object = value.as_object().ok_or_else(|| {
                InvalidArgumentError::new_err("validator must return successful and reason")
            })?;
            let successful = object.get("successful").and_then(Value::as_bool);
            let reason = object.get("reason").and_then(Value::as_str);
            match (successful, reason) {
                (Some(successful), Some(reason)) if object.len() == 2 => {
                    Ok((successful, reason.into()))
                }
                _ => Err(InvalidArgumentError::new_err(
                    "validator must return only a bool successful and a string reason",
                )),
            }
        })();
        match reply {
            Ok((true, _)) => Ok(()),
            Ok((false, reason)) => Err(ParameterError::Validation(reason)),
            Err(error) => {
                error.print(py);
                Err(ParameterError::Validation(
                    "parameter validator raised an exception".into(),
                ))
            }
        }
    })
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ParameterServiceRequest")]
struct PyParameterServiceRequest {
    inner: Mutex<Option<ParameterServiceRequest>>,
}

#[pymethods]
impl PyParameterServiceRequest {
    #[getter]
    fn kind(&self) -> PyResult<String> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|request| request.kind().suffix().to_owned())
            .ok_or_else(|| InvalidStateError::new_err("parameter request is already consumed"))
    }

    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let value = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|request| request.request().clone())
            .ok_or_else(|| InvalidStateError::new_err("parameter request is already consumed"))?;
        json_to_py(py, &value)
    }

    fn execute(&self, py: Python<'_>, validator: Py<PyAny>) -> PyResult<()> {
        if !validator.bind(py).is_callable() {
            return Err(InvalidArgumentError::new_err("validator must be callable"));
        }
        let request =
            self.inner.lock().unwrap().take().ok_or_else(|| {
                InvalidStateError::new_err("parameter request is already consumed")
            })?;
        py.detach(|| request.execute(|candidate| validate_python(&validator, candidate)))
            .map_err(to_py_err)
    }
}

#[pyfunction]
fn parameter_service_interface(kind: &str) -> PyResult<PyInterface> {
    let kind: ParameterServiceKind = serde_json::from_value(Value::String(kind.into()))
        .map_err(|error| parameter_error(ParameterError::InvalidArgument(error.to_string())))?;
    Ok(PyInterface {
        inner: kind.interface(),
    })
}

#[pyfunction]
fn parameter_event_interface() -> PyInterface {
    PyInterface {
        inner: aos_core::parameter_event_interface(),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyParameterStore>()?;
    module.add_class::<PyParameterTransaction>()?;
    module.add_class::<PyParameterServer>()?;
    module.add_class::<PyParameterServiceRequest>()?;
    module.add_function(wrap_pyfunction!(parameter_value, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_service_interface, module)?)?;
    module.add_function(wrap_pyfunction!(parameter_event_interface, module)?)?;
    Ok(())
}
