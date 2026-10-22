//! Action handles owned by provider/client Rust runtimes, never by Python state.

use aos_core::{
    ActionClient, ActionQos, ActionResourceLimits, ActionServer, ActionServerOptions,
    EndpointOptions,
};
use pyo3::prelude::*;
use serde_json::Value;

use crate::errors::{to_py_err, InvalidArgumentError};
use crate::typed::{duration, interruptible_wait, json_from_py, json_to_py, PyInterface, PyNode};

#[pyclass(frozen, module = "aos_sdk._native", name = "_ActionServer")]
struct PyActionServer {
    inner: ActionServer,
}

#[pymethods]
impl PyActionServer {
    #[new]
    #[pyo3(signature = (node, interface, name, *, result_timeout=900.0, max_result_waiters=1024, resource_limits=None, qos_profiles=None, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Python keyword arguments expose the documented API.
    fn new(
        py: Python<'_>,
        node: &PyNode,
        interface: &PyInterface,
        name: &str,
        result_timeout: f64,
        max_result_waiters: usize,
        resource_limits: Option<&Bound<'_, PyAny>>,
        qos_profiles: Option<&Bound<'_, PyAny>>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<Self> {
        let options = ActionServerOptions {
            result_timeout: duration(result_timeout)?,
            max_result_waiters,
            resource_limits: ActionResourceLimits::from_json(
                &resource_limits
                    .map(json_from_py)
                    .transpose()?
                    .unwrap_or(Value::Null),
            )
            .map_err(to_py_err)?,
        };
        let metadata = EndpointOptions {
            summary: summary.into(),
            side_effects,
            contract_version,
        };
        let request = qos_profiles
            .map(json_from_py)
            .transpose()?
            .unwrap_or(Value::Null);
        let qos = ActionQos::from_json(&request).map_err(to_py_err)?;
        let inner = py
            .detach(|| {
                ActionServer::new_with_qos(
                    &node.inner,
                    &interface.inner,
                    name,
                    options,
                    metadata,
                    qos,
                )
            })
            .map_err(to_py_err)?;
        Ok(Self { inner })
    }

    fn take_goal_request(&self, py: Python<'_>, timeout: f64) -> PyResult<PyGoalRequest> {
        let timeout = duration(timeout)?;
        let inner = py
            .detach(|| self.inner.take_goal_request(timeout))
            .map_err(to_py_err)?;
        Ok(PyGoalRequest { inner })
    }

    fn take_cancel_request(&self, py: Python<'_>, timeout: f64) -> PyResult<PyCancelRequest> {
        let timeout = duration(timeout)?;
        let inner = py
            .detach(|| self.inner.take_cancel_request(timeout))
            .map_err(to_py_err)?;
        Ok(PyCancelRequest { inner })
    }

    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_GoalRequest")]
struct PyGoalRequest {
    inner: aos_core::GoalRequest,
}

#[pymethods]
impl PyGoalRequest {
    #[getter]
    fn goal_id(&self) -> &str {
        self.inner.goal_id()
    }
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, self.inner.request())
    }
    fn decide(&self, py: Python<'_>, accept: bool) -> PyResult<Option<PyServerGoalHandle>> {
        Ok(py
            .detach(|| self.inner.decide(accept))
            .map_err(to_py_err)?
            .map(|inner| PyServerGoalHandle { inner }))
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_CancelRequest")]
struct PyCancelRequest {
    inner: aos_core::CancelRequest,
}

#[pymethods]
impl PyCancelRequest {
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, self.inner.request())
    }
    fn candidates(&self) -> Vec<PyServerGoalHandle> {
        self.inner
            .candidates()
            .into_iter()
            .map(|inner| PyServerGoalHandle { inner })
            .collect()
    }
    fn respond<'py>(&self, py: Python<'py>, goal_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let response = py
            .detach(|| self.inner.respond(&goal_ids))
            .map_err(to_py_err)?;
        json_to_py(py, &response)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ServerGoalHandle")]
struct PyServerGoalHandle {
    inner: aos_core::ServerGoalHandle,
}

#[pymethods]
impl PyServerGoalHandle {
    #[getter]
    fn goal_id(&self) -> &str {
        self.inner.goal_id()
    }
    #[getter]
    fn request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, self.inner.request())
    }
    #[getter]
    fn status(&self) -> u8 {
        self.inner.status()
    }
    #[getter]
    fn is_active(&self) -> bool {
        self.inner.is_active()
    }
    #[getter]
    fn is_cancel_requested(&self) -> bool {
        self.inner.is_cancel_requested()
    }
    fn execute(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.execute()).map_err(to_py_err)
    }
    fn succeed(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.succeed()).map_err(to_py_err)
    }
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.abort()).map_err(to_py_err)
    }
    fn canceled(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.canceled()).map_err(to_py_err)
    }
    fn finish(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value = match json_from_py(value) {
            Ok(value) => value,
            Err(error) => {
                py.detach(|| {
                    self.inner
                        .finish_error("execute callback returned non-JSON data")
                })
                .map_err(to_py_err)?;
                return Err(error);
            }
        };
        py.detach(|| self.inner.finish(Some(&value)))
            .map_err(to_py_err)
    }
    fn finish_default(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.finish(None)).map_err(to_py_err)
    }
    fn publish_feedback(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value = json_from_py(value)?;
        py.detach(|| self.inner.publish_feedback(&value))
            .map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ActionClient")]
struct PyActionClient {
    inner: ActionClient,
}

#[pymethods]
impl PyActionClient {
    #[new]
    #[pyo3(signature = (node, interface, name, *, qos_profiles=None, summary="", side_effects=None, contract_version=None))]
    #[allow(clippy::too_many_arguments)] // Five channel profiles plus public metadata.
    fn new(
        py: Python<'_>,
        node: &PyNode,
        interface: &PyInterface,
        name: &str,
        qos_profiles: Option<&Bound<'_, PyAny>>,
        summary: &str,
        side_effects: Option<String>,
        contract_version: Option<String>,
    ) -> PyResult<Self> {
        let metadata = EndpointOptions {
            summary: summary.into(),
            side_effects,
            contract_version,
        };
        let request = qos_profiles
            .map(json_from_py)
            .transpose()?
            .unwrap_or(Value::Null);
        let qos = ActionQos::from_json(&request).map_err(to_py_err)?;
        let inner = py
            .detach(|| {
                ActionClient::new_with_qos(&node.inner, &interface.inner, name, metadata, qos)
            })
            .map_err(to_py_err)?;
        Ok(Self { inner })
    }
    #[pyo3(signature = (timeout_sec=None))]
    fn wait_for_server(&self, py: Python<'_>, timeout_sec: Option<f64>) -> PyResult<bool> {
        let timeout = timeout_sec
            .map(|value| duration(if value < 0.0 { 0.0 } else { value }))
            .transpose()?;
        interruptible_wait(py, |check| {
            self.inner.wait_for_server_with_check(timeout, check)
        })
    }
    #[pyo3(signature = (goal, *, goal_id=None, timeout=10.0))]
    fn send_goal(
        &self,
        py: Python<'_>,
        goal: &Bound<'_, PyAny>,
        goal_id: Option<&str>,
        timeout: f64,
    ) -> PyResult<PyClientGoalHandle> {
        let goal = json_from_py(goal)?;
        let timeout = duration(timeout)?;
        let inner = py
            .detach(|| self.inner.send_goal(&goal, goal_id, timeout))
            .map_err(to_py_err)?;
        Ok(PyClientGoalHandle { inner })
    }
    #[pyo3(signature = (goal_id, timeout=None))]
    fn get_result<'py>(
        &self,
        py: Python<'py>,
        goal_id: &str,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = timeout.map(duration).transpose()?;
        let value = py
            .detach(|| self.inner.get_result_wait(goal_id, timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
    fn cancel<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, PyAny>,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = json_from_py(request)?;
        let timeout = duration(timeout)?;
        let value = py
            .detach(|| self.inner.cancel(&request, timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
    fn recv_feedback<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration(timeout)?;
        let value = py
            .detach(|| self.inner.recv_feedback(timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
    fn take_feedback(
        &self,
        py: Python<'_>,
        timeout: f64,
    ) -> PyResult<crate::context::PySubscriptionCall> {
        let timeout = duration(timeout)?;
        py.detach(|| self.inner.take_feedback(timeout))
            .map(crate::context::PySubscriptionCall::from_core)
            .map_err(to_py_err)
    }
    fn recv_status<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration(timeout)?;
        let value = py
            .detach(|| self.inner.recv_status(timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
    fn destroy(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.close()).map_err(to_py_err)
    }
}

#[pyclass(frozen, module = "aos_sdk._native", name = "_ClientGoalHandle")]
struct PyClientGoalHandle {
    inner: aos_core::ClientGoalHandle,
}

#[pymethods]
impl PyClientGoalHandle {
    #[getter]
    fn goal_id(&self) -> &str {
        self.inner.goal_id()
    }
    #[getter]
    fn accepted(&self) -> bool {
        self.inner.accepted()
    }
    #[getter]
    fn status(&self) -> u8 {
        self.inner.status()
    }
    #[getter]
    fn stamp<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stamp = serde_json::to_value(self.inner.stamp())
            .map_err(|e| InvalidArgumentError::new_err(e.to_string()))?;
        json_to_py(py, &stamp)
    }
    #[pyo3(signature = (timeout=None))]
    fn get_result<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = timeout.map(duration).transpose()?;
        let value = py
            .detach(|| self.inner.get_result_wait(timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
    fn cancel_goal<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration(timeout)?;
        let value = py
            .detach(|| self.inner.cancel_goal(timeout))
            .map_err(to_py_err)?;
        json_to_py(py, &value)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyActionServer>()?;
    module.add_class::<PyActionClient>()?;
    module.add_class::<PyGoalRequest>()?;
    module.add_class::<PyCancelRequest>()?;
    module.add_class::<PyServerGoalHandle>()?;
    module.add_class::<PyClientGoalHandle>()?;
    Ok(())
}
