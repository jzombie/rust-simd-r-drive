use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive::DataStore;
use std::path::PathBuf;

#[pyclass]
struct PyEngine {
    inner: DataStore,
}

#[pymethods]
impl PyEngine {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let store = DataStore::open(&PathBuf::from(path))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(Self { inner: store })
    }

    fn write(&mut self, key: &str, data: &[u8]) -> PyResult<()> {
        self.inner
            .write(key.as_bytes(), data)
            .map(|_| ()) // discard the offset
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn read<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Option<Py<PyBytes>>> {
        match self.inner.read(key.as_bytes()) {
            Some(entry) => Ok(Some(PyBytes::new(py, &entry).into())),
            None => Ok(None),
        }
    }

    fn delete(&mut self, key: &str) -> PyResult<()> {
        self.inner
            .delete_entry(key.as_bytes())
            .map(|_| ()) // discard the offset
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn exists(&self, key: &str) -> bool {
        self.inner.read(key.as_bytes()).is_some()
    }
}

#[pymodule]
fn simd_r_drive_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEngine>()?;
    Ok(())
}
