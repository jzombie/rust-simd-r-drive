use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive_muxio_client::{AsyncDataStoreReader, AsyncDataStoreWriter, NetClient};
use std::sync::Arc;
use tokio::runtime::Runtime;

#[pyclass]
pub struct DataStoreNetClient {
    client: Arc<NetClient>,
}

#[pymethods]
impl DataStoreNetClient {
    #[new]
    fn new(address: &str) -> PyResult<Self> {
        let rt = Runtime::new().unwrap();
        let client = rt.block_on(NetClient::new(address));
        Ok(Self {
            client: Arc::new(client),
        })
    }

    fn write(&self, py: Python<'_>, key: &[u8], payload: &[u8]) -> PyResult<()> {
        let client = self.client.clone();
        let key = key.to_vec();
        let payload = payload.to_vec();

        py.allow_threads(|| {
            Runtime::new().unwrap().block_on(async move {
                client
                    .write(&key, &payload)
                    .await
                    .map(|_| ())
                    .map_err(|e: std::io::Error| PyIOError::new_err(e.to_string()))
            })
        })
    }

    fn read(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
        let client = self.client.clone();
        let key = key.to_vec();

        py.allow_threads(|| {
            Runtime::new()
                .unwrap()
                .block_on(async move {
                    match client.read(&key).await {
                        Some(bytes) => {
                            Python::with_gil(|py| Ok(Some(PyBytes::new(py, &bytes).into())))
                        }
                        None => Ok(None),
                    }
                })
                .map_err(|e: std::io::Error| PyIOError::new_err(e.to_string()))
        })
    }
}
