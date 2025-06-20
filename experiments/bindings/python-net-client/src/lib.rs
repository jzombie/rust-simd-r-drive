use pyo3::PyResult;
use pyo3::prelude::*;
use pyo3::types::PyModule;
mod net_client;
use net_client::DataStoreNetClient;
mod namespace_hasher;
use namespace_hasher::NamespaceHasher;

#[pymodule(name = "simd_r_drive_net_client")]
fn python_entry(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataStoreNetClient>()?;
    m.add_class::<NamespaceHasher>()?;

    Ok(())
}
