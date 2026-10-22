//! Clock state, timer arithmetic and waiting are owned by aos-core.

use std::sync::Mutex;

use aos_core::{Clock, ClockType, Rate, Timer, TimerCall};
use pyo3::prelude::*;

use crate::context::PyContext;
use crate::errors::{to_py_err, InvalidArgumentError, InvalidStateError};
use crate::typed::{interruptible_wait, json_from_py, json_to_py, PyInterface};

pub(crate) fn number(value: &Bound<'_, PyAny>, label: &str) -> PyResult<f64> {
    json_from_py(value)?
        .as_f64()
        .ok_or_else(|| InvalidArgumentError::new_err(format!("{label} must be a finite number")))
}

pub(crate) fn boolean(value: &Bound<'_, PyAny>, label: &str) -> PyResult<bool> {
    json_from_py(value)?
        .as_bool()
        .ok_or_else(|| InvalidArgumentError::new_err(format!("{label} must be a bool")))
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Clock")]
pub(crate) struct PyClock {
    pub(crate) inner: Clock,
}

#[pymethods]
impl PyClock {
    #[new]
    #[pyo3(signature = (clock_type="steady", *, context=None))]
    fn new(clock_type: &str, context: Option<&PyContext>) -> PyResult<Self> {
        let kind = clock_type.parse::<ClockType>().map_err(to_py_err)?;
        Ok(Self {
            inner: match context {
                Some(context) => context.inner.create_clock(kind).map_err(to_py_err)?,
                None => Clock::new(kind),
            },
        })
    }

    fn now<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let now = py.detach(|| self.inner.now()).map_err(to_py_err)?;
        json_to_py(py, &serde_json::json!(now))
    }

    fn set_use_sim_time(&self, py: Python<'_>, enabled: &Bound<'_, PyAny>) -> PyResult<()> {
        let enabled = boolean(enabled, "use_sim_time")?;
        py.detach(|| self.inner.set_use_sim_time(enabled))
            .map_err(to_py_err)
    }

    fn update_ros_time(&self, py: Python<'_>, nanoseconds: &Bound<'_, PyAny>) -> PyResult<()> {
        let nanoseconds = json_from_py(nanoseconds)?.as_i64().ok_or_else(|| {
            InvalidArgumentError::new_err("nanoseconds must be an integer in i64")
        })?;
        py.detach(|| self.inner.update_ros_time(nanoseconds))
            .map_err(to_py_err)
    }

    fn take_jump_events<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.take_jump_events()))
    }

    #[getter]
    fn forward_update_count(&self) -> u64 {
        self.inner.forward_update_count()
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Timer")]
pub(crate) struct PyTimer {
    pub(crate) inner: Timer,
}

#[pymethods]
impl PyTimer {
    fn take_ready(&self, py: Python<'_>) -> PyResult<Option<PyTimerCall>> {
        Ok(py
            .detach(|| self.inner.take_ready())
            .map_err(to_py_err)?
            .map(|call| PyTimerCall {
                inner: Mutex::new(Some(call)),
            }))
    }

    fn is_ready(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| self.inner.is_ready()).map_err(to_py_err)
    }

    fn is_canceled(&self) -> bool {
        self.inner.is_canceled()
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.cancel()).map_err(to_py_err)
    }

    fn reset(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.reset()).map_err(to_py_err)
    }

    fn time_until_next_call(&self, py: Python<'_>) -> PyResult<Option<u64>> {
        let remaining = py
            .detach(|| self.inner.time_until_next_call())
            .map_err(to_py_err)?;
        Ok(remaining.map(|value| value.as_nanos() as u64))
    }

    #[getter]
    fn timer_period_ns(&self) -> u64 {
        self.inner.period().as_nanos() as u64
    }

    fn clock(&self) -> PyClock {
        PyClock {
            inner: self.inner.clock(),
        }
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_TimerCall")]
struct PyTimerCall {
    inner: Mutex<Option<TimerCall>>,
}

#[pymethods]
impl PyTimerCall {
    fn is_valid(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(TimerCall::is_valid)
    }

    #[getter]
    fn info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let info = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| InvalidStateError::new_err("timer call already finished"))?
            .info();
        json_to_py(py, &serde_json::json!(info))
    }

    /// Idempotent for executor failure cleanup. Dropping the handle also frees
    /// its lease, so no Python exception can retain the timer's in-flight bit.
    fn finish(&self) {
        if let Some(call) = self.inner.lock().unwrap().take() {
            call.finish();
        }
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Rate")]
pub(crate) struct PyRate {
    pub(crate) inner: Rate,
}

#[pymethods]
impl PyRate {
    fn sleep(&self, py: Python<'_>) -> PyResult<()> {
        interruptible_wait(py, |check| self.inner.sleep_with_check(check))
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyfunction]
fn clock_interface() -> PyInterface {
    PyInterface {
        inner: aos_core::clock_interface(),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyClock>()?;
    module.add_class::<PyTimer>()?;
    module.add_class::<PyTimerCall>()?;
    module.add_class::<PyRate>()?;
    module.add_function(wrap_pyfunction!(clock_interface, module)?)?;
    Ok(())
}
