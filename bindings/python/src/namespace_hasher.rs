use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive::utils::NamespaceHasher as BaseNamespaceHasher;

#[pyclass]
pub struct NamespaceHasher {
    inner: BaseNamespaceHasher,
}

#[pymethods]
impl NamespaceHasher {
    #[no_mangle]
    #[new]
    fn new(prefix: &[u8]) -> Self {
        NamespaceHasher {
            inner: BaseNamespaceHasher::new(prefix),
        }
    }

    /// Call the `namespace` method from Rust and expose it to Python.
    #[no_mangle]
    fn namespace<'py>(&self, py: Python<'py>, key: &[u8]) -> Py<PyBytes> {
        let namespaced_key = self.inner.namespace(key); // Use existing function
        PyBytes::new(py, &namespaced_key).into() // Return as PyBytes
    }
}
