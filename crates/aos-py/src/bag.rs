//! Owned Rust bag handles. Python never owns playback state or file workers.
use std::path::PathBuf;

use aos_bag::{PlaybackOptions, RecordOptions, RecordStopReason};
use pyo3::prelude::*;
use serde::{de::DeserializeOwned, Serialize};

use crate::context::PyContext;
use crate::errors::{AOSError, InvalidArgumentError};
use crate::typed::{duration, json_from_py, json_to_py};

pyo3::create_exception!(
    aos_sdk,
    BagError,
    AOSError,
    "Bag operation failed. kind and file_offset retain the native diagnostic."
);

fn bag_error(error: aos_bag::BagError) -> PyErr {
    Python::attach(|py| {
        let exception = if error.kind == aos_bag::BagErrorKind::InvalidArgument {
            InvalidArgumentError::new_err(error.to_string())
        } else {
            BagError::new_err(error.to_string())
        };
        let kind = serde_json::to_value(error.kind).expect("BagErrorKind serializes");
        for (key, value) in [
            ("kind", kind),
            ("file_offset", serde_json::json!(error.file_offset)),
            ("report", serde_json::Value::Null),
        ] {
            if let Err(error) =
                json_to_py(py, &value).and_then(|value| exception.value(py).setattr(key, value))
            {
                return error;
            }
        }
        exception
    })
}

fn options<T: DeserializeOwned>(value: Option<&Bound<'_, PyAny>>) -> PyResult<T> {
    let value = value
        .map(json_from_py)
        .transpose()?
        .unwrap_or(serde_json::json!({}));
    serde_json::from_value(value)
        .map_err(|error| InvalidArgumentError::new_err(format!("invalid bag options: {error}")))
}

fn report<'py>(py: Python<'py>, value: impl Serialize) -> PyResult<Bound<'py, PyAny>> {
    let value = serde_json::to_value(value)
        .map_err(|error| InvalidArgumentError::new_err(error.to_string()))?;
    json_to_py(py, &value)
}

fn checked_wait<T: Send>(
    py: Python<'_>,
    run: impl FnOnce(&mut dyn FnMut() -> aos_bag::BagResult<()>) -> aos_bag::BagResult<T> + Send,
) -> PyResult<T> {
    let mut signal_error = None;
    let result = py.detach(|| {
        let mut check = || {
            Python::attach(|py| py.check_signals()).map_err(|error| {
                signal_error = Some(error);
                aos_bag::BagError::new(
                    aos_bag::BagErrorKind::Closed,
                    "Python signal interrupted the local wait",
                )
            })
        };
        run(&mut check)
    });
    match signal_error {
        Some(error) => Err(error),
        None => result.map_err(bag_error),
    }
}

#[pyfunction(name = "_inspect_bag", signature = (directory, options_value=None))]
fn inspect<'py>(
    py: Python<'py>,
    directory: PathBuf,
    options_value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let options = options(options_value)?;
    report(
        py,
        py.detach(|| aos_bag::inspect(&directory, options))
            .map_err(bag_error)?,
    )
}

#[pyfunction(name = "_recover_bag", signature = (source, destination, options_value=None))]
fn recover<'py>(
    py: Python<'py>,
    source: PathBuf,
    destination: PathBuf,
    options_value: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let options = options(options_value)?;
    report(
        py,
        py.detach(|| aos_bag::recover(&source, &destination, options))
            .map_err(bag_error)?,
    )
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_BagRecorder")]
pub(crate) struct PyRecorder {
    inner: aos_bag::Recorder,
}

#[pymethods]
impl PyRecorder {
    #[new]
    fn new(
        py: Python<'_>,
        context: &PyContext,
        options_value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let options: RecordOptions = options(Some(options_value))?;
        let context = context.inner.clone();
        let inner = py
            .detach(|| aos_bag::Recorder::start(context, options))
            .map_err(bag_error)?;
        Ok(Self { inner })
    }

    #[getter]
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        report(py, self.inner.status())
    }

    /// Request closure without waiting; the caller's Context remains owned by it.
    fn stop(&self) -> PyResult<()> {
        self.inner
            .stop(RecordStopReason::Requested)
            .map_err(bag_error)
    }

    #[pyo3(signature = (timeout=5.0))]
    fn wait<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration(timeout)?;
        report(
            py,
            checked_wait(py, |check| self.inner.wait_with_check(timeout, check))?,
        )
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_BagPlayer")]
pub(crate) struct PyPlayer {
    inner: aos_bag::Player,
}

#[pymethods]
impl PyPlayer {
    #[new]
    fn new(
        py: Python<'_>,
        context: &PyContext,
        directory: PathBuf,
        options_value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let options: PlaybackOptions = options(Some(options_value))?;
        let context = context.inner.clone();
        let inner = py
            .detach(|| aos_bag::Player::open(context, &directory, options))
            .map_err(bag_error)?;
        Ok(Self { inner })
    }

    #[getter]
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        report(py, self.inner.status())
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        report(
            py,
            checked_wait(py, |check| self.inner.start_with_check(check))?,
        )
    }

    fn pause<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        report(
            py,
            checked_wait(py, |check| self.inner.pause_with_check(check))?,
        )
    }

    fn resume<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        report(
            py,
            checked_wait(py, |check| self.inner.resume_with_check(check))?,
        )
    }

    fn seek<'py>(
        &self,
        py: Python<'py>,
        offset_ns: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Keep nanosecond offsets exact; neither bool nor 3.0 is a u64 input.
        let offset = json_from_py(offset_ns)?.as_u64().ok_or_else(|| {
            InvalidArgumentError::new_err("offset_ns must be an unsigned integer")
        })?;
        report(
            py,
            checked_wait(py, |check| self.inner.seek_with_check(offset, check))?,
        )
    }

    fn set_rate<'py>(
        &self,
        py: Python<'py>,
        rate: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rate = json_from_py(rate)?.as_f64().ok_or_else(|| {
            InvalidArgumentError::new_err("rate must be a finite positive number")
        })?;
        report(
            py,
            checked_wait(py, |check| self.inner.set_rate_with_check(rate, check))?,
        )
    }

    fn stop(&self) -> PyResult<()> {
        self.inner.stop().map_err(bag_error)
    }

    #[pyo3(signature = (timeout=5.0))]
    fn wait<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration(timeout)?;
        report(
            py,
            checked_wait(py, |check| self.inner.wait_with_check(timeout, check))?,
        )
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("BagError", module.py().get_type::<BagError>())?;
    module.add_class::<PyRecorder>()?;
    module.add_class::<PyPlayer>()?;
    module.add_function(wrap_pyfunction!(inspect, module)?)?;
    module.add_function(wrap_pyfunction!(recover, module)?)?;
    Ok(())
}
