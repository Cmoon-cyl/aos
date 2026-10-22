//! Read-only installed resource lookup, using the same index as the CLI.
use std::path::PathBuf;

use aos_launch::{resolve_package_prefixes, LaunchError, PackageIndex};
use pyo3::prelude::*;

use crate::errors::AOSError;

fn error(error: LaunchError) -> PyErr {
    Python::attach(|py| {
        let exception = AOSError::new_err(error.to_string());
        let code = serde_json::to_value(error.code)
            .expect("LaunchErrorCode serializes")
            .as_str()
            .expect("LaunchErrorCode is a string")
            .to_owned();
        for (name, value) in [("code", code), ("stage", error.stage)] {
            if let Err(failure) = exception.value(py).setattr(name, value) {
                return failure;
            }
        }
        exception
    })
}

fn json(value: impl serde::Serialize) -> PyResult<String> {
    serde_json::to_string(&value).map_err(|failure| AOSError::new_err(failure.to_string()))
}

#[pyclass(name = "_PackageIndex", frozen)]
struct PyPackageIndex {
    index: PackageIndex,
}

#[pymethods]
impl PyPackageIndex {
    #[new]
    #[pyo3(signature = (prefixes=None, installed_prefix=None))]
    fn new(
        py: Python<'_>,
        prefixes: Option<Vec<String>>,
        installed_prefix: Option<String>,
    ) -> PyResult<Self> {
        let explicit =
            prefixes.map(|paths| paths.into_iter().map(PathBuf::from).collect::<Vec<_>>());
        let installed_prefix = installed_prefix.map(PathBuf::from);
        let base =
            std::env::current_dir().map_err(|failure| AOSError::new_err(failure.to_string()))?;
        let prefixes = resolve_package_prefixes(
            explicit.as_deref(),
            std::env::var_os("AOS_PREFIX_PATH").as_deref(),
            installed_prefix.as_deref(),
            &base,
            std::env::var_os("AOS_PROCESS_CONFIG").is_some(),
        )
        .map_err(error)?;
        let index = py
            .detach(|| PackageIndex::discover(&prefixes))
            .map_err(error)?;
        Ok(Self { index })
    }

    fn packages_json(&self) -> PyResult<String> {
        json(self.index.packages())
    }

    #[pyo3(signature = (name, version=None))]
    fn package_json(&self, py: Python<'_>, name: &str, version: Option<&str>) -> PyResult<String> {
        let package = py
            .detach(|| self.index.package(name, version))
            .map_err(error)?;
        json(package)
    }

    #[pyo3(signature = (package, name, version=None))]
    fn resource_json(
        &self,
        py: Python<'_>,
        package: &str,
        name: &str,
        version: Option<&str>,
    ) -> PyResult<String> {
        let resource = py
            .detach(|| {
                self.index.package(package, version)?;
                self.index.resource(package, name)
            })
            .map_err(error)?;
        json(resource)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPackageIndex>()
}
