use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::PyResult;
use simd_r_drive::EntryStream as BaseEntryStream;
use std::io::Read;
use std::sync::Mutex;

#[pyclass]
pub struct EntryStream {
    pub(crate) inner: Mutex<BaseEntryStream>,
}

#[pymethods]
impl EntryStream {
    fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> Option<Py<PyBytes>> {
        let mut buf = vec![0u8; 4096]; // or smaller if you prefer
        match self.inner.lock().unwrap().read(&mut buf) {
            Ok(0) => None,
            Ok(n) => Some(PyBytes::new(py, &buf[..n]).into()),
            Err(_) => None,
        }
    }

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
