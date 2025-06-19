use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use simd_r_drive_muxio_client::{AsyncDataStoreReader, AsyncDataStoreWriter, NetClient};
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

#[pyclass]
pub struct DataStoreNetClient {
    client: Arc<NetClient>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl DataStoreNetClient {
    #[new]
    fn new(_py: Python<'_>, address: &str) -> PyResult<Self> {
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to create Tokio runtime: {}", e))
                })?,
        );

        let client = runtime.block_on(async { NetClient::new(address).await });

        Ok(Self {
            client: Arc::new(client),
            runtime,
        })
    }

    // This function remains correct from the previous step
    #[pyo3(name = "write")]
    fn py_write(&self, py: Python<'_>, key: Vec<u8>, payload: Vec<u8>) -> PyResult<PyObject> {
        let client = self.client.clone();
        let rt_handle = self.runtime.handle().clone();

        future_into_py(py, async move {
            let result = rt_handle
                .spawn(async move {
                    client
                        .write(&key, &payload)
                        .await
                        .map_err(|e| PyIOError::new_err(e.to_string()))
                })
                .await;

            match result {
                Ok(inner_py_result) => inner_py_result,
                Err(join_error) => Err(PyIOError::new_err(format!(
                    "Write task panicked or cancelled: {}",
                    join_error
                ))),
            }
        })
        .map(|bound_coroutine| bound_coroutine.into())
    }

    #[pyo3(name = "read")]
    fn py_read(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<PyObject> {
        let client = self.client.clone();
        let rt_handle = self.runtime.handle().clone();

        future_into_py(py, async move {
            let result = rt_handle
                .spawn(async move {
                    match client.read(&key).await {
                        Some(bytes) => Python::with_gil(|py| {
                            // FIX: Create a variable with an explicit type annotation
                            // to resolve the ambiguity of the `.into()` call.
                            let py_object: PyObject = PyBytes::new(py, &bytes).into();
                            Ok(Some(py_object))
                        }),
                        None => Ok(None),
                    }
                })
                .await;

            match result {
                Ok(inner_py_result) => inner_py_result,
                Err(join_error) => Err(PyIOError::new_err(format!(
                    "Read task panicked or cancelled: {}",
                    join_error
                ))),
            }
        })
        .map(|bound_coroutine| bound_coroutine.into())
    }
}
