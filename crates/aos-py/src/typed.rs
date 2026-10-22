//! Thin JSON/PyO3 conversion. Runtime state and validation stay in aos-core.

use std::path::PathBuf;
use std::time::Duration;

use aos_core::{
    EndpointOptions, Interface, InterfaceKind, NameKind, Node, NodeConfigRequest, NodeOptions,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use serde_json::Value;

use crate::context::{PyContext, PyPublisher, PySubscription};
use crate::errors::{to_py_err, InvalidArgumentError};
use crate::lifecycle::{PyLifecycle, PyLifecycleServer};
use crate::parameter::{PyParameterServer, PyParameterStore};
use crate::time::{boolean, number, PyClock, PyRate, PyTimer};

pub(crate) fn duration(timeout: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(timeout)
        .map_err(|_| InvalidArgumentError::new_err("timeout must be finite and non-negative"))
}

/// Reattach briefly on the same calling thread to run Python signal handlers.
/// Core calls the check outside its locks; no worker thread or RPC retry is
/// introduced. Closed is only a local escape value after a PyErr was captured.
pub(crate) fn interruptible_wait<T: Send>(
    py: Python<'_>,
    operation: impl FnOnce(&mut dyn FnMut() -> aos_core::Result<()>) -> aos_core::Result<T> + Send,
) -> PyResult<T> {
    py.detach(|| {
        let mut signal_error = None;
        let result = operation(&mut || {
            Python::attach(|py| py.check_signals()).map_err(|error| {
                signal_error = Some(error);
                aos_core::Error::Closed
            })
        });
        match signal_error {
            Some(error) => Err(error),
            None => result.map_err(to_py_err),
        }
    })
}

pub(crate) fn json_from_py(value: &Bound<'_, PyAny>) -> PyResult<Value> {
    let py = value.py();
    let kwargs = PyDict::new(py);
    kwargs.set_item("allow_nan", false)?;
    kwargs.set_item("ensure_ascii", false)?;
    let text: String = py
        .import("json")?
        .getattr("dumps")?
        .call((value,), Some(&kwargs))
        .and_then(|text| text.extract())
        .map_err(|error| InvalidArgumentError::new_err(format!("invalid JSON value: {error}")))?;
    serde_json::from_str(&text)
        .map_err(|error| InvalidArgumentError::new_err(format!("invalid JSON value: {error}")))
}

pub(crate) fn json_to_py<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    py.import("json")?
        .getattr("loads")?
        .call1((value.to_string(),))
}

pub(crate) fn config_paths(paths: &[PathBuf]) -> PyResult<Vec<String>> {
    paths
        .iter()
        .map(|path| {
            path.to_str()
                .map(str::to_owned)
                .ok_or_else(|| InvalidArgumentError::new_err("config paths must be Unicode"))
        })
        .collect()
}

pub(crate) fn config_environment(names: &[&str]) -> PyResult<Value> {
    let mut environment = serde_json::Map::new();
    for key in names {
        match std::env::var(key) {
            Ok(value) => {
                environment.insert((*key).into(), Value::String(value));
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(InvalidArgumentError::new_err(format!(
                    "{key} must contain valid Unicode"
                )));
            }
        }
    }
    Ok(Value::Object(environment))
}

#[pyclass(frozen, module = "aos_sdk", name = "Interface")]
pub struct PyInterface {
    pub(crate) inner: Interface,
}

#[pymethods]
impl PyInterface {
    /// Load a JSON interface document from a path or a Python mapping.
    #[classmethod]
    fn load(_cls: &Bound<'_, PyType>, py: Python<'_>, source: &Bound<'_, PyAny>) -> PyResult<Self> {
        let inner = if let Ok(path) = source.extract::<PathBuf>() {
            py.detach(|| Interface::load(path)).map_err(to_py_err)?
        } else {
            let value = json_from_py(source)?;
            py.detach(|| Interface::from_json(value))
                .map_err(to_py_err)?
        };
        Ok(Self { inner })
    }

    #[getter]
    fn type_name(&self) -> &str {
        self.inner.type_name()
    }

    #[getter]
    fn type_hash(&self) -> &str {
        self.inner.type_hash()
    }

    #[getter]
    fn kind(&self) -> &str {
        match self.inner.kind() {
            InterfaceKind::Message => "msg",
            InterfaceKind::Service => "srv",
            InterfaceKind::Action => "action",
        }
    }

    #[getter]
    fn definition<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, self.inner.definition())
    }

    #[pyo3(signature = (value, part="message"))]
    fn validate(&self, py: Python<'_>, value: &Bound<'_, PyAny>, part: &str) -> PyResult<()> {
        let value = json_from_py(value)?;
        py.detach(|| self.inner.validate(part, &value))
            .map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Node")]
pub(crate) struct PyNode {
    pub(crate) inner: Node,
}

#[pymethods]
impl PyNode {
    #[new]
    #[pyo3(signature = (context, name, namespace="/", *, display_name=None, summary="", tags=Vec::new(), config_files=Vec::new(), remaps=None, use_sim_time=None, launch_config=None, cli_overrides=None, parameter_overrides=None, lifecycle=false))]
    #[allow(clippy::too_many_arguments)] // Python keyword arguments expose separate config sources.
    fn new(
        py: Python<'_>,
        context: &PyContext,
        name: &str,
        namespace: &str,
        display_name: Option<String>,
        summary: &str,
        tags: Vec<String>,
        config_files: Vec<PathBuf>,
        remaps: Option<&Bound<'_, PyAny>>,
        use_sim_time: Option<&Bound<'_, PyAny>>,
        launch_config: Option<&Bound<'_, PyAny>>,
        cli_overrides: Option<&Bound<'_, PyAny>>,
        parameter_overrides: Option<&Bound<'_, PyAny>>,
        lifecycle: bool,
    ) -> PyResult<Self> {
        let options = NodeOptions {
            display_name,
            summary: summary.into(),
            tags,
        };
        let config_files = config_paths(&config_files)?;
        let environment =
            config_environment(&["AOS_NODE_NAME", "AOS_NAMESPACE", "AOS_USE_SIM_TIME"])?;
        let mut program = serde_json::Map::new();
        if let Some(value) = remaps {
            program.insert("remaps".into(), json_from_py(value)?);
        }
        if let Some(value) = use_sim_time {
            program.insert("use_sim_time".into(), json_from_py(value)?);
        }
        if let Some(value) = parameter_overrides {
            program.insert("parameters".into(), json_from_py(value)?);
        }
        let request: NodeConfigRequest = serde_json::from_value(serde_json::json!({
            "config_files": config_files,
            "program": program,
            "environment": environment,
            "launch": launch_config.map(json_from_py).transpose()?.unwrap_or(Value::Null),
            "cli": cli_overrides.map(json_from_py).transpose()?.unwrap_or(Value::Null),
        }))
        .map_err(|error| {
            InvalidArgumentError::new_err(format!("invalid Node configuration: {error}"))
        })?;
        Ok(Self {
            inner: py
                .detach(|| {
                    if lifecycle {
                        Node::new_lifecycle_with_config(
                            context.inner.clone(),
                            name,
                            namespace,
                            options,
                            request,
                        )
                    } else {
                        Node::new_with_config(
                            context.inner.clone(),
                            name,
                            namespace,
                            options,
                            request,
                        )
                    }
                })
                .map_err(to_py_err)?,
        })
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }
    #[getter]
    fn namespace(&self) -> &str {
        self.inner.namespace()
    }
    #[getter]
    fn fully_qualified_name(&self) -> String {
        self.inner.fully_qualified_name().into()
    }
    #[pyo3(signature = (name, *, kind="any"))]
    fn resolve_name(&self, name: &str, kind: &str) -> PyResult<String> {
        let kind = kind.parse::<NameKind>().map_err(to_py_err)?;
        self.inner
            .resolve_name_details(name, kind)
            .map(|details| details.resolved_name)
            .map_err(to_py_err)
    }
    #[pyo3(signature = (name, *, kind="any"))]
    fn resolve_name_details<'py>(
        &self,
        py: Python<'py>,
        name: &str,
        kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kind = kind.parse::<NameKind>().map_err(to_py_err)?;
        let details = self
            .inner
            .resolve_name_details(name, kind)
            .map_err(to_py_err)?;
        let value = serde_json::to_value(details)
            .map_err(|error| InvalidArgumentError::new_err(error.to_string()))?;
        json_to_py(py, &value)
    }
    fn effective_config<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &self.inner.effective_config())
    }
    fn node_card<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &self.inner.node_card())
    }
    fn destroy_node(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.destroy_node()).map_err(to_py_err)
    }

    fn clock(&self) -> PyClock {
        PyClock {
            inner: self.inner.clock(),
        }
    }

    fn lifecycle(&self) -> Option<PyLifecycle> {
        self.inner.lifecycle().map(|inner| PyLifecycle { inner })
    }

    fn enable_lifecycle_services(&self, py: Python<'_>) -> PyResult<PyLifecycleServer> {
        py.detach(|| self.inner.enable_lifecycle_services())
            .map(|inner| PyLifecycleServer { inner })
            .map_err(to_py_err)
    }

    fn parameters(&self) -> PyParameterStore {
        PyParameterStore {
            inner: self.inner.parameters(),
        }
    }

    fn enable_parameter_services(&self, py: Python<'_>) -> PyResult<PyParameterServer> {
        let inner = py
            .detach(|| self.inner.enable_parameter_services())
            .map_err(to_py_err)?;
        Ok(PyParameterServer { inner })
    }

    fn enable_parameter_events(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.enable_parameter_events())
            .map_err(to_py_err)
    }

    fn set_use_sim_time(&self, py: Python<'_>, enabled: &Bound<'_, PyAny>) -> PyResult<()> {
        let enabled = boolean(enabled, "use_sim_time")?;
        py.detach(|| self.inner.set_use_sim_time(enabled))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (period, *, clock=None, autostart=true))]
    fn create_timer(
        &self,
        py: Python<'_>,
        period: &Bound<'_, PyAny>,
        clock: Option<&PyClock>,
        autostart: bool,
    ) -> PyResult<PyTimer> {
        let period = duration(number(period, "timer period")?)?;
        let clock = clock.map(|clock| clock.inner.clone());
        let inner = py
            .detach(|| self.inner.create_timer(period, clock, autostart))
            .map_err(to_py_err)?;
        Ok(PyTimer { inner })
    }

    #[pyo3(signature = (frequency, *, clock=None))]
    fn create_rate(
        &self,
        py: Python<'_>,
        frequency: &Bound<'_, PyAny>,
        clock: Option<&PyClock>,
    ) -> PyResult<PyRate> {
        let frequency = number(frequency, "rate frequency")?;
        let clock = clock.map(|clock| clock.inner.clone());
        let inner = py
            .detach(|| self.inner.create_rate(frequency, clock))
            .map_err(to_py_err)?;
        Ok(PyRate { inner })
    }

    #[pyo3(signature = (interface, name, qos, *, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Python keyword arguments are the public API.
    fn create_publisher(
        &self,
        py: Python<'_>,
        interface: &PyInterface,
        name: &str,
        qos: &Bound<'_, PyAny>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<PyPublisher> {
        let qos =
            aos_core::resolve_qos(&json_from_py(qos)?, "default", false).map_err(to_py_err)?;
        let options = endpoint_options(summary, side_effects, contract_version);
        let inner = py
            .detach(|| {
                self.inner
                    .create_publisher(&interface.inner, name, qos, options)
            })
            .map_err(to_py_err)?;
        Ok(PyPublisher::from_typed(inner))
    }

    #[pyo3(signature = (interface, name, qos, *, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Python keyword arguments are the public API.
    fn create_subscription(
        &self,
        py: Python<'_>,
        interface: &PyInterface,
        name: &str,
        qos: &Bound<'_, PyAny>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<PySubscription> {
        let qos =
            aos_core::resolve_qos(&json_from_py(qos)?, "default", false).map_err(to_py_err)?;
        let options = endpoint_options(summary, side_effects, contract_version);
        let inner = py
            .detach(|| {
                self.inner
                    .create_subscription(&interface.inner, name, qos, options)
            })
            .map_err(to_py_err)?;
        Ok(PySubscription::from_typed(inner))
    }

    #[pyo3(signature = (interface, name, *, qos=None, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Separate public QoS and metadata fields.
    fn create_client(
        &self,
        py: Python<'_>,
        interface: &PyInterface,
        name: &str,
        qos: Option<&Bound<'_, PyAny>>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<PyClient> {
        let request = qos.map(json_from_py).transpose()?.unwrap_or(Value::Null);
        let qos = aos_core::resolve_qos(&request, "services_default", true).map_err(to_py_err)?;
        let options = endpoint_options(summary, side_effects, contract_version);
        let inner = py
            .detach(|| {
                self.inner
                    .create_client_with_qos(&interface.inner, name, qos, options)
            })
            .map_err(to_py_err)?;
        Ok(PyClient { inner })
    }

    #[pyo3(signature = (interface, name, *, qos=None, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Separate public QoS and metadata fields.
    fn create_service(
        &self,
        py: Python<'_>,
        interface: &PyInterface,
        name: &str,
        qos: Option<&Bound<'_, PyAny>>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<PyService> {
        let request = qos.map(json_from_py).transpose()?.unwrap_or(Value::Null);
        let qos = aos_core::resolve_qos(&request, "services_default", true).map_err(to_py_err)?;
        let options = endpoint_options(summary, side_effects, contract_version);
        let inner = py
            .detach(|| {
                self.inner
                    .create_service_with_qos(&interface.inner, name, qos, options)
            })
            .map_err(to_py_err)?;
        Ok(PyService { inner })
    }
}

fn endpoint_options(
    summary: &str,
    side_effects: Option<String>,
    contract_version: Option<String>,
) -> EndpointOptions {
    EndpointOptions {
        summary: summary.into(),
        side_effects,
        contract_version,
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Client")]
struct PyClient {
    inner: aos_core::Client,
}

#[pymethods]
impl PyClient {
    #[getter]
    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.queue_stats()))
    }

    #[pyo3(signature = (timeout_sec=None))]
    fn wait_for_service(&self, py: Python<'_>, timeout_sec: Option<f64>) -> PyResult<bool> {
        // rclpy treats zero/negative durations as a single availability check.
        let timeout = timeout_sec
            .map(|value| duration(if value < 0.0 { 0.0 } else { value }))
            .transpose()?;
        interruptible_wait(py, |check| {
            self.inner.wait_for_service_with_check(timeout, check)
        })
    }
    fn call<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, PyAny>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = json_from_py(request)?;
        let timeout = duration(timeout)?;
        let result = py
            .detach(|| self.inner.call(&request, timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &result)
    }
    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_Service")]
struct PyService {
    inner: aos_core::Service,
}

#[pymethods]
impl PyService {
    #[getter]
    fn queue_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &serde_json::json!(self.inner.queue_stats()))
    }

    fn recv(&self, py: Python<'_>, timeout: f64) -> PyResult<PyServiceRequest> {
        let timeout = duration(timeout)?;
        let inner = py
            .detach(|| self.inner.recv_timeout(timeout))
            .map_err(to_py_err)?;
        Ok(PyServiceRequest { inner })
    }
    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ServiceRequest")]
struct PyServiceRequest {
    inner: aos_core::ServiceRequest,
}

#[pymethods]
impl PyServiceRequest {
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, self.inner.request())
    }
    fn reply(&self, py: Python<'_>, response: &Bound<'_, PyAny>) -> PyResult<()> {
        let response = json_from_py(response)?;
        py.detach(|| self.inner.reply(&response)).map_err(to_py_err)
    }
    fn reply_error(&self, py: Python<'_>, message: &str) -> PyResult<()> {
        py.detach(|| self.inner.reply_error(message))
            .map_err(to_py_err)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyInterface>()?;
    module.add_class::<PyNode>()?;
    module.add_class::<PyClient>()?;
    module.add_class::<PyService>()?;
    module.add_class::<PyServiceRequest>()?;
    Ok(())
}
