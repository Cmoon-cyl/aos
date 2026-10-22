//! Python exception classes for the error categories in Spec §13.
//!
//! The classes live in the native module so binding code can raise them
//! without importing Python modules. `aos_sdk.exceptions` re-exports them.
//! `to_py_err` keeps every aos-core error variant distinguishable in Python.

use pyo3::exceptions::PyException;
use pyo3::prelude::*;

pyo3::create_exception!(
    aos_sdk,
    ServiceError,
    AOSError,
    "The remote service handler failed; the request may have produced effects."
);
pyo3::create_exception!(
    aos_sdk,
    AOSError,
    PyException,
    "Base class for errors raised by the AOS SDK."
);
pyo3::create_exception!(
    aos_sdk,
    InvalidArgumentError,
    AOSError,
    "The request, name, or payload does not satisfy its contract or schema."
);
pyo3::create_exception!(
    aos_sdk,
    ResourceLimitError,
    InvalidArgumentError,
    "An interface definition or payload exceeded its resource admission budget."
);
pyo3::create_exception!(
    aos_sdk,
    NotFoundError,
    AOSError,
    "A named node, entity, or interface does not exist."
);
// Communication failure and wait timeout are siblings: the CLI reports them
// with different exit codes, and a timeout only ends the wait. It does not
// cancel the remote request or goal.
pyo3::create_exception!(
    aos_sdk,
    CommunicationError,
    AOSError,
    "Discovery or transport failed."
);
pyo3::create_exception!(
    aos_sdk,
    QueueOverflowError,
    CommunicationError,
    "A bounded queue evicted this request or reply; execution outcome may be unknown."
);
pyo3::create_exception!(
    aos_sdk,
    NoResponseError,
    CommunicationError,
    "The query ended without a reply; no execution outcome is implied."
);
pyo3::create_exception!(
    aos_sdk,
    WaitTimeoutError,
    AOSError,
    "A local wait ended before a response arrived; the remote work is not cancelled."
);
pyo3::create_exception!(
    aos_sdk,
    InvalidStateError,
    AOSError,
    "The operation is not allowed in the object's current state, such as after shutdown."
);
pyo3::create_exception!(
    aos_sdk,
    InactiveError,
    InvalidStateError,
    "The lifecycle gate rejected new business work while the Node is inactive."
);
pyo3::create_exception!(
    aos_sdk,
    ParameterError,
    AOSError,
    "A parameter operation failed; code and reason preserve the core result."
);

pub(crate) fn parameter_error(error: aos_core::ParameterError) -> PyErr {
    let code = error.code();
    let message = error.to_string();
    let reason = message
        .strip_prefix(&format!("{code}: "))
        .unwrap_or(&message);
    Python::attach(|py| {
        let exception = ParameterError::new_err(message.clone());
        if let Err(error) = exception.value(py).setattr("code", code) {
            return error;
        }
        if let Err(error) = exception.value(py).setattr("reason", reason) {
            return error;
        }
        exception
    })
}

/// Map a core error to its Python category, keeping the core diagnostic text.
pub(crate) fn to_py_err(error: aos_core::Error) -> PyErr {
    let message = error.to_string();
    match error {
        aos_core::Error::InvalidArgument(_)
        | aos_core::Error::Validation(_)
        | aos_core::Error::Unsupported(_) => InvalidArgumentError::new_err(message),
        aos_core::Error::ResourceLimit(_) => ResourceLimitError::new_err(message),
        aos_core::Error::Timeout => WaitTimeoutError::new_err(message),
        aos_core::Error::Closed => InvalidStateError::new_err(message),
        aos_core::Error::Inactive(_) => InactiveError::new_err(message),
        aos_core::Error::Transport(_) => CommunicationError::new_err(message),
        aos_core::Error::QueueOverflow(_) => QueueOverflowError::new_err(message),
        aos_core::Error::NoResponse(_) => NoResponseError::new_err(message),
        aos_core::Error::Remote(_) => ServiceError::new_err(message),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    module.add("AOSError", py.get_type::<AOSError>())?;
    module.add(
        "InvalidArgumentError",
        py.get_type::<InvalidArgumentError>(),
    )?;
    module.add("ResourceLimitError", py.get_type::<ResourceLimitError>())?;
    module.add("NotFoundError", py.get_type::<NotFoundError>())?;
    module.add("CommunicationError", py.get_type::<CommunicationError>())?;
    module.add("QueueOverflowError", py.get_type::<QueueOverflowError>())?;
    module.add("NoResponseError", py.get_type::<NoResponseError>())?;
    module.add("WaitTimeoutError", py.get_type::<WaitTimeoutError>())?;
    module.add("InvalidStateError", py.get_type::<InvalidStateError>())?;
    module.add("InactiveError", py.get_type::<InactiveError>())?;
    module.add("ServiceError", py.get_type::<ServiceError>())?;
    module.add("ParameterError", py.get_type::<ParameterError>())?;
    Ok(())
}
