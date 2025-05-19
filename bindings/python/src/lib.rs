use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive::DataStore as RustDataStore;
use std::path::PathBuf;

#[pyclass]
struct DataStore {
    inner: Option<RustDataStore>,
}

#[pymethods]
impl DataStore {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let store = RustDataStore::open(&PathBuf::from(path))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(Self { inner: Some(store) })
    }

    fn write(&mut self, key: &[u8], data: &[u8]) -> PyResult<()> {
        self.inner
            .as_mut()
            .unwrap()
            .write(key, data)
            .map(|_| ()) // Discard offset
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn read<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        match self.inner.as_ref().unwrap().read(key) {
            Some(entry) => Ok(Some(PyBytes::new(py, &entry).into())),
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: &[u8]) -> PyResult<()> {
        self.inner
            .as_mut()
            .unwrap()
            .delete_entry(key)
            .map(|_| ()) // Discard offset
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn exists(&self, key: &[u8]) -> bool {
        self.inner.as_ref().unwrap().read(key).is_some()
    }

    fn close(&mut self) {
        self.inner = None; // Drops the DataStore
    }
}

#[pymodule(name = "simd_r_drive")]
fn python_entry(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataStore>()?;
    Ok(())
}
