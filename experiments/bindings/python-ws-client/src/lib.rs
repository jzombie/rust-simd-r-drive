use pyo3::PyResult;
use pyo3::prelude::*;
use pyo3::types::PyModule;
mod ws_client_py;
use ws_client_py::DataStoreWsClient;
mod namespace_hasher_py;
use namespace_hasher_py::NamespaceHasher;

#[pymodule(name = "simd_r_drive_ws_client")]
fn python_entry(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataStoreWsClient>()?;
    m.add_class::<NamespaceHasher>()?;

    Ok(())
}
