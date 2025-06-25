use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::io;
use tracing::{Level, info, warn};
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::fmt::writer::MakeWriter;

mod base_ws_client_py;
use base_ws_client_py::*;

mod namespace_hasher_py;
use namespace_hasher_py::*;

// Define exceptions at the top level of the library file.
create_exception!(simd_r_drive_ws_client, ConnectionError, PyException);
create_exception!(simd_r_drive_ws_client, TimeoutError, PyException);

struct PythonLogger {
    log_callback: PyObject,
}

impl io::Write for PythonLogger {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // TODO: Debug
        // --- DEBUG PRINT ADDED ---
        // This prints directly to stderr and helps us know if this code is even running.
        eprintln!("[Rust eprint]: PythonLogger::write was called.");

        let log_message = match std::str::from_utf8(buf) {
            Ok(s) => s.trim_end(),
            Err(_) => "Could not convert log message to UTF-8",
        };
        if log_message.is_empty() {
            return Ok(buf.len());
        }
        Python::with_gil(|py| {
            if let Err(e) = self.log_callback.call1(py, (log_message,)) {
                eprintln!("[Rust eprint]: FAILED to call Python callback: {:?}", e);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct MakePythonWriter {
    log_callback: PyObject,
}

impl<'a> MakeWriter<'a> for MakePythonWriter {
    type Writer = PythonLogger;
    fn make_writer(&'a self) -> Self::Writer {
        let log_callback = Python::with_gil(|py| self.log_callback.clone_ref(py));
        PythonLogger { log_callback }
    }
}

/// Sets up the logging system to forward logs to the provided Python callback.
#[pyfunction]
fn setup_logging(callback: PyObject) -> PyResult<()> {
    Python::with_gil(|py| {
        if !callback.bind(py).is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "log_callback must be a callable function",
            ));
        }
        Ok(())
    })?;
    let writer = MakePythonWriter {
        log_callback: callback,
    };
    let subscriber = SubscriberBuilder::default()
        .with_max_level(Level::INFO)
        .with_writer(writer)
        .without_time()
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to set logger: {}", e))
    })?;
    info!("Rust logging configured successfully.");
    Ok(())
}

// TODO: Continue to debug; `eprintln!` works, though
/// Generates one INFO and one WARN log message for testing purposes.
#[pyfunction]
fn test_rust_logging() {
    info!("This is an info log from Rust.");
    warn!("This is a warning log from Rust.");
}

// Main PyO3 module entry point
#[pymodule]
fn simd_r_drive_ws_client(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(setup_logging, m)?)?;
    m.add_function(wrap_pyfunction!(test_rust_logging, m)?)?;
    m.add_class::<BaseDataStoreWsClient>()?;
    m.add_class::<NamespaceHasher>()?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;
    Ok(())
}
