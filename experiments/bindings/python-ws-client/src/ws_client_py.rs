use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive_ws_client::{AsyncDataStoreReader, AsyncDataStoreWriter, WsClient};
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

#[pyclass]
pub struct DataStoreWsClient {
    ws_client: Arc<WsClient>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl DataStoreWsClient {
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

        let ws_client = runtime.block_on(async { WsClient::new(address).await });

        Ok(Self {
            ws_client: Arc::new(ws_client),
            runtime,
        })
    }

    #[pyo3(name = "write")]
    fn py_write(&self, key: Vec<u8>, payload: Vec<u8>) -> PyResult<()> {
        self.runtime.block_on(async {
            self.ws_client
                .write(&key, &payload)
                .await
                .map_err(|e| PyIOError::new_err(e.to_string()))
                // Add this map call to discard the u64 success value
                // and return the unit type `()` instead.
                .map(|_bytes_written| ())
        })
    }

    #[pyo3(name = "batch_write")]
    fn py_batch_write(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
        let converted: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        self.runtime.block_on(async {
            self.ws_client
                .batch_write(&converted)
                .await
                .map_err(|e| PyIOError::new_err(e.to_string()))
                // Add this map call to discard the u64 success value
                // and return the unit type `()` instead.
                .map(|_bytes_written| ())
        })
    }

    #[pyo3(name = "read")]
    fn py_read(&self, key: Vec<u8>) -> PyResult<Option<PyObject>> {
        self.runtime.block_on(async {
            match self.ws_client.read(&key).await {
                Some(bytes) => Python::with_gil(|py| {
                    let py_bytes = PyBytes::new(py, &bytes);
                    Ok(Some(py_bytes.into()))
                }),
                None => Ok(None),
            }
        })
    }
}
