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

    // TODO: I am *considering* renaming this to `read_prebuffered` since its operation differs from the underlying storage engine
    // TODO: Consider exposing an alternate form of `EntryHandle` here, like the Rust side.
    // The caveat is that this approach will still need to be fully read and not work with a streamer.
    // pyo3-numpy = { version = "...", features = ["tokio"] }
    // fn py_read_numpy<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<&'py PyArray<u8, numpy::Ix1>>> {
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

    #[pyo3(name = "batch_read")]
    fn py_batch_read(&self, keys: Vec<Vec<u8>>) -> PyResult<Vec<Option<PyObject>>> {
        // We must keep the key buffers alive for the duration of the async
        // call, so first park them in their own Vec.
        let key_bufs: Vec<Vec<u8>> = keys;

        // Borrow each buffer as &[u8] – this is what the WsClient expects.
        let key_slices: Vec<&[u8]> = key_bufs.iter().map(|k| k.as_slice()).collect();

        // Run the async RPC inside the Tokio runtime.
        //
        // The call is *infallible* (it returns the data directly), so we
        // don’t need `map_err` here – if the transport layer could fail,
        // the API would return a Result like the write paths.
        let results: Vec<Option<Vec<u8>>> = self
            .runtime
            .block_on(async { self.ws_client.batch_read(&key_slices).await });

        // Convert Vec<Option<Vec<u8>>> → Vec<Option<PyBytes>>
        Python::with_gil(|py| {
            let py_results: Vec<Option<PyObject>> = results
                .into_iter()
                .map(|opt| opt.map(|bytes| PyBytes::new(py, &bytes).into()))
                .collect();
            Ok(py_results)
        })
    }
}
