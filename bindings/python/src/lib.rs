use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::PyResult;
mod entry_handle;
use entry_handle::EntryHandle;
mod entry_stream;
use entry_stream::EntryStream;
mod data_store;
use data_store::DataStore;
mod namespace_hasher;
use namespace_hasher::NamespaceHasher;

#[pymodule(name = "simd_r_drive")]
fn python_entry(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataStore>()?;
    m.add_class::<EntryHandle>()?;
    m.add_class::<EntryStream>()?;
    m.add_class::<NamespaceHasher>()?;

    Ok(())
}
