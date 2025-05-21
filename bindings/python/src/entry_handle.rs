use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyModule};
use simd_r_drive::EntryHandle as BaseEntryHandle;

#[pyclass(sequence)]
pub struct EntryHandle {
    pub(crate) inner: BaseEntryHandle,
}

#[pymethods]
impl EntryHandle {
    #[pyo3(name = "__len__")]
    fn len(&self) -> usize {
        self.inner.size()
    }

    #[getter]
    fn size(&self) -> usize {
        self.inner.size()
    }

    #[getter]
    fn size_with_metadata(&self) -> usize {
        self.inner.size_with_metadata()
    }

    #[getter]
    fn key_hash(&self) -> u64 {
        self.inner.key_hash()
    }

    #[getter]
    fn checksum(&self) -> u32 {
        self.inner.checksum()
    }

    fn raw_checksum(&self) -> [u8; 4] {
        self.inner.raw_checksum()
    }

    fn is_valid_checksum(&self) -> bool {
        self.inner.is_valid_checksum()
    }

    #[getter]
    fn start_offset(&self) -> usize {
        self.inner.start_offset()
    }

    #[getter]
    fn end_offset(&self) -> usize {
        self.inner.end_offset()
    }

    fn offset_range(&self) -> (usize, usize) {
        let range = self.inner.offset_range();
        (range.start, range.end)
    }

    fn address_range(&self) -> (usize, usize) {
        let range = self.inner.address_range();
        (range.start as usize, range.end as usize)
    }

    fn as_slice<'py>(&self, py: Python<'py>) -> PyResult<Py<PyBytes>> {
        let slice = self.inner.as_slice();
        Ok(PyBytes::new(py, slice).into())
    }

    fn as_memoryview<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        let pybytes = PyBytes::new(py, self.inner.as_slice());
        let memoryview = PyModule::import(py, "builtins")?
            .getattr("memoryview")?
            .call1((pybytes,))?;
        Ok(memoryview.into())
    }

    fn clone_arc(&self) -> Self {
        Self {
            inner: self.inner.clone_arc(),
        }
    }
}
