use crate::{ConnectionError, TimeoutError};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use simd_r_drive_ws_client::{
    AsyncDataStoreReader, AsyncDataStoreWriter, RpcTransportState, WsClient,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::time::timeout;

// TODO: Move timeout handling into inner client

// TODO: Borrow configuration props from MySQL
// connection_timeout=10,  # Timeout for the connection attempt (in seconds)
// read_timeout=30,        # Timeout for waiting for response from server (in seconds)
// write_timeout=30        # Timeout for sending data to server (in seconds)

#[pyclass(subclass)]
pub struct BaseDataStoreWsClient {
    ws_client: Arc<WsClient>,
    runtime: Arc<Runtime>,
    is_connected: Arc<AtomicBool>,
}

#[pymethods]
impl BaseDataStoreWsClient {
    #[new]
    fn new(_py: Python<'_>, host: &str, port: u16) -> PyResult<Self> {
        let runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to create Tokio runtime: {}", e))
                })?,
        );

        let ws_client = runtime
            .block_on(async { WsClient::new(host, port).await })
            .map_err(|e| PyIOError::new_err(e.to_string()))?;

        let is_connected_clone = Arc::new(AtomicBool::new(true));
        let is_connected_for_handler = is_connected_clone.clone();

        ws_client.set_state_change_handler(move |new_state: RpcTransportState| {
            tracing::info!("[Callback] Transport state changed to: {:?}", new_state);
            if new_state == RpcTransportState::Disconnected {
                is_connected_for_handler.store(false, Ordering::SeqCst);
            }
        });

        Ok(Self {
            ws_client: Arc::new(ws_client),
            runtime,
            is_connected: is_connected_clone,
        })
    }

    fn check_connection(&self) -> PyResult<()> {
        if !self.is_connected.load(Ordering::SeqCst) {
            return Err(ConnectionError::new_err("The client is disconnected."));
        }
        Ok(())
    }

    #[pyo3(name = "write")]
    fn py_write(&self, key: Vec<u8>, payload: Vec<u8>) -> PyResult<()> {
        self.check_connection()?;
        let client = self.ws_client.clone();

        self.runtime.block_on(async {
            // TODO: Don't hardcode timeout
            match timeout(Duration::from_secs(30), client.write(&key, &payload)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Write operation timed out.")),
            }
        })
    }

    #[pyo3(name = "batch_write")]
    fn py_batch_write(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> PyResult<()> {
        self.check_connection()?;
        let client = self.ws_client.clone();
        let converted: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        self.runtime.block_on(async {
            match timeout(Duration::from_secs(60), client.batch_write(&converted)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Batch write operation timed out.")),
            }
        })
    }

    // TODO: I am *considering* renaming this to `read_prebuffered` since its operation differs from the underlying storage engine
    // TODO: Consider exposing an alternate form of `EntryHandle` here, like the Rust side.
    // The caveat is that this approach will still need to be fully read and not work with a streamer.
    // pyo3-numpy = { version = "...", features = ["tokio"] }
    // fn py_read_numpy<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<&'py PyArray<u8, numpy::Ix1>>> {
    #[pyo3(name = "read")]
    fn py_read(&self, key: Vec<u8>) -> PyResult<Option<PyObject>> {
        self.check_connection()?;
        let client = self.ws_client.clone();

        let maybe_bytes = self.runtime.block_on(async {
            // TODO: Don't hardcode timeout
            match timeout(Duration::from_secs(30), client.read(&key)).await {
                Ok(Ok(entry_payload)) => Ok(entry_payload),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Read operation timed out.")),
            }
        })?;

        Python::with_gil(|py| Ok(maybe_bytes.map(|bytes| PyBytes::new(py, &bytes).into())))
    }

    #[pyo3(name = "batch_read")]
    fn py_batch_read(&self, keys: Vec<Vec<u8>>) -> PyResult<Vec<Option<PyObject>>> {
        self.check_connection()?;
        let client = self.ws_client.clone();
        let key_slices: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let results = self.runtime.block_on(async {
            // TODO: Don't hardcode timeout
            match timeout(Duration::from_secs(60), client.batch_read(&key_slices)).await {
                Ok(Ok(entries_payloads)) => Ok(entries_payloads),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Batch read operation timed out.")),
            }
        })?;

        Python::with_gil(|py| {
            Ok(results
                .into_iter()
                .map(|opt| opt.map(|bytes| PyBytes::new(py, &bytes).into()))
                .collect())
        })
    }

    #[pyo3(name = "delete")]
    fn py_delete(&self, key: Vec<u8>) -> PyResult<()> {
        self.check_connection()?;
        let client = self.ws_client.clone();

        self.runtime.block_on(async {
            // TODO: Don't hardcode timeout
            match timeout(Duration::from_secs(30), client.delete(&key)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Delete operation timed out.")),
            }
        })
    }

    // TODO: Use `u64` return type
    #[pyo3(name = "count")]
    fn py_count(&self) -> PyResult<(usize)> {
        self.check_connection()?;
        let client = self.ws_client.clone();

        self.runtime.block_on(async {
            // TODO: Don't hardcode timeout
            match timeout(Duration::from_secs(30), client.count()).await {
                Ok(Ok(total_entries)) => Ok(total_entries),
                Ok(Err(e)) => Err(PyIOError::new_err(e.to_string())),
                Err(_) => Err(TimeoutError::new_err("Count operation timed out.")),
            }
        })
    }

    // TODO: Use `u64` return type
    /// Implements the `len()` built-in for Python.
    ///
    /// This allows you to call `len(store)` to get the number of active entries.
    /// It assumes the underlying Rust client has a `len()` method.
    fn __len__(&self) -> PyResult<usize> {
        self.py_count()
    }
}
