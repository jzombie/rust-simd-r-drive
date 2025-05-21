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
    #[pyo3(name = "__iter__")]
    fn iter(slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        Ok(slf)
    }

    #[pyo3(name = "__next__")]
    fn next(slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let mut buf = vec![0u8; 4096];
        match slf.inner.lock().unwrap().read(&mut buf) {
            Ok(0) => Ok(None),
            Ok(n) => Ok(Some(PyBytes::new(py, &buf[..n]).into())),
            Err(e) => Err(pyo3::exceptions::PyIOError::new_err(e.to_string())),
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
