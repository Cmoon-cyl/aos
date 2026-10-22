use pyo3::prelude::*;

mod action;
mod bag;
mod context;
mod errors;
mod lifecycle;
mod package;
mod parameter;
mod qos;
mod security;
#[cfg(test)]
mod tests;
mod time;
mod typed;

/// The Python extension and SDK run in the importing process.
// Preserve the GIL-enabled module contract until free-threaded Python is
// explicitly supported and tested.
#[pymodule(gil_used = true)]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", aos_core::VERSION)?;
    errors::register(module)?;
    context::register(module)?;
    typed::register(module)?;
    qos::register(module)?;
    action::register(module)?;
    time::register(module)?;
    parameter::register(module)?;
    lifecycle::register(module)?;
    package::register(module)?;
    bag::register(module)?;
    security::register(module)?;
    Ok(())
}
