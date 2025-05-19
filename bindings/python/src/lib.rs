use memmap2::Mmap;
use pyo3::prelude::*;
// use pyo3::types::PyMemoryView;
use pyo3::types::{PyBytes, PyModule};
use pyo3::PyResult;
use simd_r_drive::DataStore as RustDataStore;
use std::path::PathBuf;
use std::sync::Arc;

/// Python wrapper around EntryHandle that exposes mmap-backed data
#[pyclass]
pub struct EntryHandle {
    data: Arc<Mmap>,
    start: usize,
    end: usize,
}

#[pymethods]
impl EntryHandle {
    /// Returns a memoryview (zero-copy) over the entry payload
    fn as_memoryview<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let slice = &slf.data[slf.start..slf.end];
        let pybytes = PyBytes::new(py, slice);
        let memoryview = PyModule::import(py, "builtins")?
            .getattr("memoryview")?
            .call1((pybytes,))?;
        Ok(memoryview.into())
    }
}

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

    fn read_entry(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<EntryHandle>>> {
        match self.inner.as_ref().unwrap().read(key) {
            Some(entry) => {
                let handle = Py::new(
                    py,
                    EntryHandle {
                        data: entry.mmap_arc().clone(),
                        start: entry.start_offset(),
                        end: entry.end_offset(),
                    },
                )?;
                Ok(Some(handle))
            }
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
    m.add_class::<EntryHandle>()?;
    Ok(())
}
