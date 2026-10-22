//! Executor-thread callbacks around the single Rust lifecycle state machine.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use aos_core::{
    Lifecycle, LifecycleCallbackReturn, LifecycleChangeRequest, LifecycleServer,
    LifecycleServiceKind, LifecycleStateDescription, LifecycleTransition, LifecycleTransitionId,
    LifecycleTransitionReport,
};
use pyo3::prelude::*;
use serde_json::Value;

use crate::errors::{to_py_err, InvalidArgumentError, InvalidStateError};
use crate::time::number;
use crate::typed::{json_from_py, json_to_py, PyInterface};

fn transition_id(value: &Value, lifecycle: &Lifecycle) -> aos_core::Result<LifecycleTransitionId> {
    if let Some(label) = value.as_str() {
        return LifecycleTransitionId::from_label(label, lifecycle.current_state()?);
    }
    value
        .as_u64()
        .and_then(|id| u8::try_from(id).ok())
        .ok_or_else(|| {
            aos_core::Error::InvalidArgument(
                "transition must be a public lifecycle label or integer ID".into(),
            )
        })
        .and_then(LifecycleTransitionId::from_id)
}

fn callback_return(value: &Value) -> aos_core::Result<LifecycleCallbackReturn> {
    if let Some(label) = value.as_str() {
        return LifecycleCallbackReturn::from_label(label);
    }
    value
        .as_u64()
        .and_then(|id| u8::try_from(id).ok())
        .ok_or_else(|| {
            aos_core::Error::InvalidArgument(
                "lifecycle callback must return success, failure or error".into(),
            )
        })
        .and_then(LifecycleCallbackReturn::from_id)
}

fn call_python(
    callback: &Py<PyAny>,
    state: &LifecycleStateDescription,
    previous: &LifecycleStateDescription,
) -> (LifecycleCallbackReturn, &'static str) {
    // This reacquires the GIL on the same Executor thread. No core lock or
    // middleware callback thread is held while arbitrary user code executes.
    Python::attach(|py| {
        let reply = (|| -> PyResult<LifecycleCallbackReturn> {
            let state = json_to_py(py, &serde_json::json!(state))?;
            let previous = json_to_py(py, &serde_json::json!(previous))?;
            let result = callback.call1(py, (state, previous))?;
            callback_return(&json_from_py(result.bind(py))?).map_err(to_py_err)
        })();
        match reply {
            Ok(result) => (result, ""),
            Err(error) => {
                error.print(py);
                (
                    LifecycleCallbackReturn::Error,
                    "lifecycle callback raised an exception or returned an invalid result",
                )
            }
        }
    })
}

/// Both local and remote transitions use this loop. Python does not own a token
/// or duplicate the state's transition/result table.
fn execute_transition(
    mut transition: LifecycleTransition,
    callback: &Py<PyAny>,
    drain_timeout: Option<Duration>,
) -> aos_core::Result<LifecycleTransitionReport> {
    let previous = transition.previous_state();
    let mut state = transition.callback_state()?;
    loop {
        let (outcome, reason) = call_python(callback, &state, &previous);
        let progress = transition.complete_with_reason(outcome, drain_timeout, reason)?;
        if let Some(report) = progress.report {
            return Ok(report);
        }
        state = progress.callback_state.ok_or_else(|| {
            aos_core::Error::InvalidArgument("lifecycle progress has no callback or report".into())
        })?;
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Lifecycle")]
pub(crate) struct PyLifecycle {
    pub(crate) inner: Lifecycle,
}

#[pymethods]
impl PyLifecycle {
    fn current_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.current_state().map_err(to_py_err)?.description();
        json_to_py(py, &serde_json::json!(state))
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let snapshot = self.inner.snapshot().map_err(to_py_err)?;
        json_to_py(py, &serde_json::json!(snapshot))
    }

    fn available_states<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.available_states()))
    }

    fn available_transitions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transitions = self.inner.available_transitions().map_err(to_py_err)?;
        json_to_py(py, &serde_json::json!(transitions))
    }

    fn transition_graph<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.transition_graph()))
    }

    fn last_transition_report<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let report = self.inner.last_transition_report().map_err(to_py_err)?;
        json_to_py(py, &serde_json::json!(report))
    }

    fn take_transition_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.take_transition_events()))
    }

    #[pyo3(signature = (transition, callback, *, drain_timeout=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        transition: &Bound<'_, PyAny>,
        callback: Py<PyAny>,
        drain_timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !callback.bind(py).is_callable() {
            return Err(InvalidArgumentError::new_err("callback must be callable"));
        }
        let value = json_from_py(transition)?;
        let drain_timeout = drain_timeout
            .map(|value| number(value, "drain_timeout").and_then(crate::typed::duration))
            .transpose()?;
        // Reject invalid input before begin_transition or any application
        // callback. Dropping a begun token intentionally finalizes the Node.
        if drain_timeout.is_some_and(|timeout| Instant::now().checked_add(timeout).is_none()) {
            return Err(InvalidArgumentError::new_err(
                "lifecycle drain timeout is too large",
            ));
        }
        let report = py
            .detach(|| {
                let id = transition_id(&value, &self.inner)?;
                let transition = self.inner.begin_transition(id)?;
                execute_transition(transition, &callback, drain_timeout)
            })
            .map_err(to_py_err)?;
        json_to_py(py, &serde_json::json!(report))
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_LifecycleServer")]
pub(crate) struct PyLifecycleServer {
    pub(crate) inner: LifecycleServer,
}

#[pymethods]
impl PyLifecycleServer {
    fn recv(&self, py: Python<'_>, timeout: f64) -> PyResult<PyLifecycleChangeRequest> {
        let timeout = crate::typed::duration(timeout)?;
        let request = py
            .detach(|| self.inner.take_change_request(timeout))
            .map_err(to_py_err)?;
        Ok(PyLifecycleChangeRequest {
            inner: Mutex::new(Some(request)),
        })
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_LifecycleChangeRequest")]
pub(crate) struct PyLifecycleChangeRequest {
    inner: Mutex<Option<LifecycleChangeRequest>>,
}

#[pymethods]
impl PyLifecycleChangeRequest {
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let value = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|request| request.request().clone())
            .ok_or_else(|| InvalidStateError::new_err("lifecycle request is already consumed"))?;
        json_to_py(py, &value)
    }

    fn execute(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<()> {
        if !callback.bind(py).is_callable() {
            return Err(InvalidArgumentError::new_err("callback must be callable"));
        }
        let request =
            self.inner.lock().unwrap().take().ok_or_else(|| {
                InvalidStateError::new_err("lifecycle request is already consumed")
            })?;
        py.detach(|| {
            let result = request.begin_transition().and_then(|transition| {
                execute_transition(transition, &callback, request.drain_timeout())
            });
            match result {
                Ok(report) => request.reply(&report),
                Err(error) => request.reply_error(&error),
            }
        })
        .map_err(to_py_err)
    }
}

#[pyfunction]
fn lifecycle_service_interface(kind: &str) -> PyResult<PyInterface> {
    let kind: LifecycleServiceKind = serde_json::from_value(Value::String(kind.into()))
        .map_err(|error| InvalidArgumentError::new_err(error.to_string()))?;
    Ok(PyInterface {
        inner: kind.interface(),
    })
}

#[pyfunction]
fn lifecycle_event_interface() -> PyInterface {
    PyInterface {
        inner: aos_core::lifecycle_event_interface(),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyLifecycle>()?;
    module.add_class::<PyLifecycleServer>()?;
    module.add_class::<PyLifecycleChangeRequest>()?;
    module.add_function(wrap_pyfunction!(lifecycle_service_interface, module)?)?;
    module.add_function(wrap_pyfunction!(lifecycle_event_interface, module)?)?;
    Ok(())
}
