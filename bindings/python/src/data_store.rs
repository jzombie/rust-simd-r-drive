use crate::EntryHandle;
use crate::EntryStream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::PyResult;
use simd_r_drive::{DataStore as BaseDataStore, EntryStream as BaseEntryStream};
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[pyclass]
pub struct DataStore {
    inner: Arc<Mutex<BaseDataStore>>,
}

#[pymethods]
impl DataStore {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let store = BaseDataStore::open(&PathBuf::from(path))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(store)),
        })
    }

    // TODO: Remove
    // fn proto(&self) -> usize {
    //     4 as usize
    // }

    // TODO: Uncomment
    // fn write(&self, key: &[u8], data: &[u8]) -> PyResult<()> {
    //     self.inner
    //         .lock()
    //         .unwrap()
    //         .write(key, data)
    //         .map(|_| ())
    //         .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
    // }

    fn batch_write(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
        let refs: Vec<(&[u8], &[u8])> = items
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        self.inner
            .lock()
            .unwrap()
            .batch_write(&refs)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
            .map(|_| ())
    }

    fn write_stream<'py>(
        &self,
        _py: Python<'py>,
        key: &[u8],
        reader: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        struct PyReader<'py> {
            obj: Bound<'py, PyAny>,
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

        let mut reader = PyReader { obj: reader };

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
                    inner: Mutex::new(BaseEntryStream::from(entry)),
                };
                Ok(Some(Py::new(py, stream)?))
            }
            None => Ok(None),
        }
    }

    fn read_entry(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<EntryHandle>>> {
        match self.inner.lock().unwrap().read(key) {
            Some(entry) => {
                let handle = Py::new(py, EntryHandle { inner: entry })?;
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

    fn __contains__(&self, key: &[u8]) -> bool {
        self.exists(key)
    }

    fn exists(&self, key: &[u8]) -> bool {
        self.inner.lock().unwrap().read(key).is_some()
    }
}
