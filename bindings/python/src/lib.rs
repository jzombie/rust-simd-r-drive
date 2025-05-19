use memmap2::Mmap;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::{PyBytes, PyModule};
use pyo3::PyResult;
use simd_r_drive::{DataStore as RustDataStore, EntryStream as RustEntryStream};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Python wrapper for streaming an EntryHandle
#[pyclass]
pub struct EntryStream {
    inner: Mutex<RustEntryStream>,
}

#[pymethods]
impl EntryStream {
    fn read(&self, py: Python<'_>, size: usize) -> PyResult<Py<PyBytes>> {
        let mut buffer = vec![0u8; size];
        let n = self
            .inner
            .lock()
            .unwrap()
            .read(&mut buffer)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &buffer[..n]).into())
    }
}

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
    inner: Arc<Mutex<RustDataStore>>,
}

#[pymethods]
impl DataStore {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let store = RustDataStore::open(&PathBuf::from(path))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
        })
    }

    fn write(&self, key: &[u8], data: &[u8]) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .write(key, data)
            .map(|_| ())
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn write_stream<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        reader: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        struct PyReader<'py> {
            obj: Bound<'py, PyAny>,
            py: Python<'py>,
        }

        impl<'py> Read for PyReader<'py> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let py_bytes = self
                    .obj
                    .call_method1("read", (buf.len(),))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

                let bytes: Bound<'py, PyBytes> = py_bytes
                    .extract()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

                let b = bytes.as_bytes();
                let len = b.len().min(buf.len());
                buf[..len].copy_from_slice(&b[..len]);

                Ok(len)
            }
        }

        let mut reader = PyReader { obj: reader, py };

        self.inner
            .lock()
            .unwrap()
            .write_stream(key, &mut reader)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok(())
    }

    fn read<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        match self.inner.lock().unwrap().read(key) {
            Some(entry) => Ok(Some(PyBytes::new(py, &entry).into())),
            None => Ok(None),
        }
    }

    fn read_stream<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Py<EntryStream>>> {
        match self.inner.lock().unwrap().read(key) {
            Some(entry) => {
                let stream = EntryStream {
                    inner: Mutex::new(RustEntryStream::from(entry)),
                };
                Ok(Some(Py::new(py, stream)?))
            }
            None => Ok(None),
        }
    }

    fn read_entry(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<EntryHandle>>> {
        match self.inner.lock().unwrap().read(key) {
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

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .delete_entry(key)
            .map(|_| ())
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    }

    fn exists(&self, key: &[u8]) -> bool {
        self.inner.lock().unwrap().read(key).is_some()
    }
}

#[pymodule(name = "simd_r_drive")]
fn python_entry(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataStore>()?;
    m.add_class::<EntryHandle>()?;
    m.add_class::<EntryStream>()?;
    Ok(())
}
