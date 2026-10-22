//! Python handles that own process-local aos-core objects.
//!
//! Each Python object holds the Rust handle directly; there is no Python-side
//! copy of communication state and no network bridge. Every call that can wait
//! on the adapter releases the GIL, so other Python threads keep running and a
//! blocked receive can be interrupted by `destroy()` or `Context.shutdown()`
//! from another thread. No method here runs Python callbacks.

use std::path::PathBuf;
use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyType};
use serde_json::Value;

use crate::errors::{to_py_err, InvalidArgumentError, InvalidStateError};
use crate::typed::{config_environment, config_paths, json_from_py, json_to_py};

fn log_capacity(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    json_from_py(value)?
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| InvalidArgumentError::new_err("log capacity must be an integer"))
}

/// Session configuration used when the caller passes none: the checked-in
/// local-router peer profile, embedded so an installed wheel needs no file.
#[cfg(test)]
pub(crate) const DEFAULT_SESSION_CONFIG: &str = aos_zenoh::DEFAULT_SESSION_CONFIG;

/// A process-local communication context.
///
/// Explicit `shutdown()` invalidates every entity created from it and wakes
/// pending receives. Calling it again raises `InvalidStateError`; leaving a
/// `with` block shuts down without raising for an already-closed context.
#[pyclass(frozen, module = "aos_sdk", name = "Context")]
pub struct PyContext {
    pub(crate) inner: aos_core::Context,
    effective_config: Value,
}

impl PyContext {
    /// Wrap a Context whose adapter session is already open.
    #[cfg(test)]
    pub(crate) fn from_core(inner: aos_core::Context) -> Self {
        Self {
            inner,
            effective_config: Value::Null,
        }
    }
}

#[pymethods]
impl PyContext {
    /// Open this process's own Zenoh session.
    ///
    /// Program values, config files, environment, launch_config, then
    /// cli_overrides are resolved by Rust. `config` is a Zenoh session config
    /// path; config_files contain domain_id/zenoh_config deployment objects.
    /// AOS_DOMAIN_ID and AOS_ZENOH_CONFIG are the environment override inputs.
    /// AOS_PROCESS_CONFIG separately locates a strict startup document; its
    /// launch/cli sources are merged before opening and retained for all Nodes.
    /// `_environment` is reserved for the diagnostic probe's already-resolved
    /// input; ordinary callers should leave it unset.
    ///
    /// The default opens a loopback peer with gossip and connects to the local
    /// router at `tcp/localhost:7447` in the background. A missing optional
    /// router does not prevent local startup. The SDK installs no
    /// log subscriber, so Zenoh diagnostics are off and never reach stdout.
    ///
    /// An explicit security_profile, or AOS_SECURITY_PROFILE when omitted,
    /// selects strict TLS-only startup. Invalid profiles fail without fallback.
    /// The profile supplies the default domain; final overrides must agree.
    #[new]
    #[pyo3(signature = (*, domain_id=None, config=None, config_files=Vec::new(), launch_config=None, cli_overrides=None, security_profile=None, _environment=None))]
    #[allow(clippy::too_many_arguments)] // Configuration sources stay explicit in the public API.
    fn new(
        py: Python<'_>,
        domain_id: Option<&Bound<'_, PyAny>>,
        config: Option<PathBuf>,
        config_files: Vec<PathBuf>,
        launch_config: Option<&Bound<'_, PyAny>>,
        cli_overrides: Option<&Bound<'_, PyAny>>,
        security_profile: Option<PathBuf>,
        _environment: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut program = serde_json::Map::new();
        if let Some(domain_id) = domain_id {
            program.insert("domain_id".into(), json_from_py(domain_id)?);
        }
        if let Some(config) = config {
            program.insert(
                "zenoh_config".into(),
                Value::String(config_paths(&[config])?.remove(0)),
            );
        }
        let environment = match _environment {
            Some(environment) => json_from_py(environment)?,
            None => config_environment(&["AOS_DOMAIN_ID", "AOS_ZENOH_CONFIG"])?,
        };
        let request: aos_core::ContextConfigRequest = serde_json::from_value(serde_json::json!({
            "program": program,
            "config_files": config_paths(&config_files)?,
            "environment": environment,
            "launch": launch_config.map(json_from_py).transpose()?.unwrap_or(Value::Null),
            "cli": cli_overrides.map(json_from_py).transpose()?.unwrap_or(Value::Null),
        }))
        .map_err(|error| {
            InvalidArgumentError::new_err(format!("invalid Context configuration: {error}"))
        })?;
        // Python's mapping and the native environment can differ: an empty
        // value on Windows, os.putenv(), or an embedding host can update only
        // one of them. Preserve either selection and reject disagreement.
        let security_profile = match security_profile {
            Some(path) => Some(path),
            None => {
                let python_profile = py
                    .import("os")?
                    .getattr("environ")?
                    .call_method1("get", ("AOS_SECURITY_PROFILE",))?
                    .extract::<Option<PathBuf>>()
                    .map_err(|_| {
                        InvalidArgumentError::new_err(
                            "AOS_SECURITY_PROFILE must be a filesystem path",
                        )
                    })?;
                let native_profile = std::env::var_os("AOS_SECURITY_PROFILE").map(PathBuf::from);
                if let (Some(python), Some(native)) = (&python_profile, &native_profile) {
                    if python != native {
                        return Err(InvalidArgumentError::new_err(
                            "Python and native AOS_SECURITY_PROFILE values disagree",
                        ));
                    }
                }
                python_profile.or(native_profile)
            }
        };
        let inner = py
            .detach(|| {
                let profile = match security_profile.as_deref() {
                    Some(path) => aos_zenoh::security::load_client_profile(Some(path))?,
                    None => None,
                };
                match profile {
                    Some(profile) => aos_zenoh::security::open_secure_context(request, &profile),
                    None => aos_zenoh::open_context(request),
                }
            })
            .map_err(to_py_err)?;
        #[cfg(windows)]
        if inner.process_bootstrap().is_some() {
            py.import("aos_sdk._process_signals")?
                .call_method0("install_managed_console_handler")?;
        }
        let effective_config = inner
            .configuration()
            .expect("standard opener resolves config")
            .effective_value();
        Ok(Self {
            inner,
            effective_config,
        })
    }

    #[getter]
    fn effective_config<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &self.effective_config)
    }

    /// Return True until shutdown has begun.
    fn ok(&self) -> bool {
        !self.inner.is_shutdown()
    }

    /// Read this process-local Context's counters, including after shutdown.
    fn telemetry_snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.telemetry_snapshot()))
    }

    /// Capture at most capacity records in memory. This performs no log I/O.
    #[pyo3(signature = (capacity=256))]
    fn enable_log_capture(
        &self,
        #[pyo3(from_py_with = log_capacity)] capacity: usize,
    ) -> PyResult<()> {
        self.inner.enable_log_capture(capacity).map_err(to_py_err)
    }

    /// Stop new record capture without discarding already queued records.
    fn disable_log_capture(&self) {
        self.inner.disable_log_capture();
    }

    /// Drain owned metadata records without writing stdout or stderr.
    fn drain_log_records<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.drain_telemetry()))
    }

    /// Explicitly write one bounded JSONL batch to stderr on this caller thread.
    ///
    /// I/O may block. On failure, OSError is raised and loss/failure counters are
    /// updated; records are not retried. This does not imply business completion.
    fn flush_logs_to_stderr(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.inner.flush_logs_to_stderr())
            .map_err(PyErr::from)
    }

    /// Stop new work and close the session.
    ///
    /// Raises `InvalidStateError` when shutdown has already begun; that does
    /// not mean the other caller's cleanup finished. Cleanup failures raise
    /// `CommunicationError`, and the context still stays closed.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.shutdown()).map_err(to_py_err)
    }

    /// Create a publisher for an absolute topic such as `/echo/request`.
    fn create_publisher(&self, py: Python<'_>, topic: &str) -> PyResult<PyPublisher> {
        let inner = py
            .detach(|| self.inner.create_publisher(topic))
            .map_err(to_py_err)?;
        Ok(PyPublisher::from_raw(inner))
    }

    /// Create a subscription; create it before the peer publishes, because
    /// P0 delivery is volatile.
    fn create_subscription(&self, py: Python<'_>, topic: &str) -> PyResult<PySubscription> {
        let inner = py
            .detach(|| self.inner.create_subscription(topic))
            .map_err(to_py_err)?;
        Ok(PySubscription::from_raw(inner))
    }

    #[pyo3(signature = (timeout=0.2))]
    fn graph<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = crate::typed::duration(timeout)?;
        let graph = py.detach(|| self.inner.graph(timeout)).map_err(to_py_err)?;
        crate::typed::json_to_py(
            py,
            &serde_json::to_value(graph)
                .map_err(|e| InvalidArgumentError::new_err(e.to_string()))?,
        )
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyType>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        exit_result(py.detach(|| self.inner.shutdown()))
    }
}

/// A process-local publisher.
#[pyclass(frozen, module = "aos_sdk", name = "Publisher")]
pub struct PyPublisher {
    inner: PublisherHandle,
}

enum PublisherHandle {
    Raw(aos_core::Publisher),
    Typed(aos_core::TypedPublisher),
}

impl PyPublisher {
    fn from_raw(inner: aos_core::Publisher) -> Self {
        Self {
            inner: PublisherHandle::Raw(inner),
        }
    }

    pub(crate) fn from_typed(inner: aos_core::TypedPublisher) -> Self {
        Self {
            inner: PublisherHandle::Typed(inner),
        }
    }

    fn close(&self) -> aos_core::Result<()> {
        match &self.inner {
            PublisherHandle::Raw(inner) => inner.close(),
            PublisherHandle::Typed(inner) => inner.close(),
        }
    }
}

#[pymethods]
impl PyPublisher {
    /// Submit one payload. Typed Node publishers use shared Rust validation.
    fn publish(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        match &self.inner {
            PublisherHandle::Raw(inner) => {
                let data = data.cast::<PyBytes>()?.as_bytes().to_vec();
                py.detach(|| inner.publish(&data)).map_err(to_py_err)
            }
            PublisherHandle::Typed(inner) => {
                let data = crate::typed::json_from_py(data)?;
                py.detach(|| inner.publish(&data)).map_err(to_py_err)
            }
        }
    }

    /// Wait for a typed subscriber to match; this does not acknowledge delivery.
    #[pyo3(signature = (timeout=10.0))]
    fn wait_for_subscribers(&self, py: Python<'_>, timeout: f64) -> PyResult<()> {
        let timeout = crate::typed::duration(timeout)?;
        match &self.inner {
            PublisherHandle::Typed(inner) => py
                .detach(|| inner.wait_for_subscribers(timeout))
                .map_err(to_py_err),
            PublisherHandle::Raw(_) => Err(InvalidArgumentError::new_err(
                "matching waits require a typed Node publisher",
            )),
        }
    }

    /// Release this publisher; a repeated call raises `InvalidStateError`.
    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.close()).map_err(to_py_err)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyType>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        exit_result(py.detach(|| self.close()))
    }
}

/// A process-local subscription that returns owned bytes to the caller.
#[pyclass(frozen, module = "aos_sdk", name = "Subscription")]
pub struct PySubscription {
    inner: SubscriptionHandle,
}

enum SubscriptionHandle {
    Raw(aos_core::Subscription),
    Typed(aos_core::TypedSubscription),
}

impl PySubscription {
    fn from_raw(inner: aos_core::Subscription) -> Self {
        Self {
            inner: SubscriptionHandle::Raw(inner),
        }
    }

    pub(crate) fn from_typed(inner: aos_core::TypedSubscription) -> Self {
        Self {
            inner: SubscriptionHandle::Typed(inner),
        }
    }

    fn close(&self) -> aos_core::Result<()> {
        match &self.inner {
            SubscriptionHandle::Raw(inner) => inner.close(),
            SubscriptionHandle::Typed(inner) => inner.close(),
        }
    }
}

#[pymethods]
impl PySubscription {
    /// Local KEEP_LAST queue observations; None for an untyped legacy handle.
    #[getter]
    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stats = match &self.inner {
            SubscriptionHandle::Typed(inner) => inner.queue_stats(),
            SubscriptionHandle::Raw(_) => None,
        };
        json_to_py(py, &serde_json::json!(stats))
    }

    /// Samples skipped by the lifecycle gate, separate from transport drops.
    #[getter]
    fn lifecycle_skipped_total(&self) -> Option<u64> {
        match &self.inner {
            SubscriptionHandle::Typed(inner) => Some(inner.lifecycle_skipped_total()),
            SubscriptionHandle::Raw(_) => None,
        }
    }

    /// Claim a typed message until its Executor callback finishes.
    fn take_message(&self, py: Python<'_>, timeout: f64) -> PyResult<PySubscriptionCall> {
        let timeout = crate::typed::duration(timeout)?;
        match &self.inner {
            SubscriptionHandle::Typed(inner) => {
                let call = py
                    .detach(|| inner.take_message(timeout))
                    .map_err(to_py_err)?;
                Ok(PySubscriptionCall::from_core(call))
            }
            SubscriptionHandle::Raw(_) => Err(InvalidArgumentError::new_err(
                "message callback leases require a typed Node subscription",
            )),
        }
    }

    /// Wait at most `timeout` seconds for one message; 0 polls without waiting.
    ///
    /// Raises `WaitTimeoutError` when nothing arrives. The timeout only ends
    /// this local wait. `destroy()` or `Context.shutdown()` from another thread
    /// interrupts the wait with `InvalidStateError`.
    fn recv<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = crate::typed::duration(timeout)?;
        match &self.inner {
            SubscriptionHandle::Raw(inner) => {
                let data = py
                    .detach(|| inner.recv_timeout(timeout))
                    .map_err(to_py_err)?;
                Ok(PyBytes::new(py, &data).into_any())
            }
            SubscriptionHandle::Typed(inner) => {
                let data = py
                    .detach(|| inner.recv_timeout(timeout))
                    .map_err(to_py_err)?;
                crate::typed::json_to_py(py, &data)
            }
        }
    }

    /// Release this subscription and wake its pending receives.
    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.close()).map_err(to_py_err)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyType>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        exit_result(py.detach(|| self.close()))
    }
}

/// Owns the core admission lease across Executor submission and user code.
#[pyclass(frozen, module = "aos_sdk._native", name = "_SubscriptionCall")]
pub(crate) struct PySubscriptionCall {
    inner: Mutex<Option<aos_core::SubscriptionCall>>,
}

impl PySubscriptionCall {
    pub(crate) fn from_core(call: aos_core::SubscriptionCall) -> Self {
        Self {
            inner: Mutex::new(Some(call)),
        }
    }
}

#[pymethods]
impl PySubscriptionCall {
    #[getter]
    fn message<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let message = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|call| call.message().clone())
            .ok_or_else(|| InvalidStateError::new_err("subscription call is already finished"))?;
        json_to_py(py, &message)
    }

    fn is_valid(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|call| call.is_valid())
    }

    fn finish(&self) {
        self.inner.lock().unwrap().take();
    }
}

/// Leaving a `with` block tolerates an earlier explicit close, but still
/// reports cleanup failures. Returning false never suppresses the block's
/// own exception.
fn exit_result(result: aos_core::Result<()>) -> PyResult<bool> {
    match result {
        Ok(()) | Err(aos_core::Error::Closed) => Ok(false),
        Err(error) => Err(to_py_err(error)),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyContext>()?;
    module.add_class::<PyPublisher>()?;
    module.add_class::<PySubscription>()?;
    module.add_class::<PySubscriptionCall>()?;
    Ok(())
}
