use crate::storage_engine::EntryHandle;
use std::io::{self, Read};

/// `EntryStream` provides a **streaming interface** over an `EntryHandle`.
///
/// This struct allows **reading large entries in chunks** instead of loading
/// the entire entry into memory. It is useful when working with entries larger
/// than available RAM.
///
/// # ⚠️ **Non Zero-Copy Warning**
/// Unlike `EntryHandle`, this implementation **performs memory copies**.
/// Each call to `read()` copies a portion of the entry into a user-provided buffer.
///
/// **For zero-copy access**, use `EntryHandle::as_slice()` instead.
///
/// # Example Usage
/// ```rust
/// use simd_r_drive::storage_engine::{DataStore, EntryHandle, EntryStream, traits::{DataStoreReader, DataStoreWriter}};
/// use std::io::Read;
/// use std::path::PathBuf;
///
/// let data_store = DataStore::from(PathBuf::from("test_storage.bin"));
///
/// // Write some test data
/// data_store.write(b"test_key", b"test_data");
/// let entry_handle = data_store.read(b"test_key").unwrap();
///
/// // Assume `entry_handle` is obtained from storage
/// let mut stream = EntryStream::from(entry_handle);
///
/// let mut buffer = vec![0; 4096]; // Read in 4KB chunks
/// while let Ok(bytes_read) = stream.read(&mut buffer) {
///     if bytes_read == 0 {
///         break; // EOF
///     }
///     // Replace this with actual processing logic
///     println!("Read {} bytes", bytes_read);
/// }
/// ```
pub struct EntryStream {
    entry_handle: EntryHandle,
    position: usize, // Tracks how much has been read
}

impl From<EntryHandle> for EntryStream {
    /// Converts an `EntryHandle` into an `EntryStream`.
    ///
    /// This allows the entry's data to be read **incrementally** instead of accessing
    /// the full slice in memory at once.
    ///
    /// # ⚠️ **Non Zero-Copy Warning**
    /// - **Streaming reads require memory copies.**
    /// - If you need direct access to the full entry **without copying**, use `EntryHandle::as_slice()`.
    fn from(entry_handle: EntryHandle) -> Self {
        Self {
            position: entry_handle.range.start,
            entry_handle,
        }
    }
}

impl Read for EntryStream {
    // Reads a chunk of the entry into the provided buffer.
    ///
    /// - Returns `Ok(0)` on EOF (no more data).
    /// - Reads up to `buf.len()` bytes from the entry.
    /// - Moves the read position forward after each call.
    ///
    /// # ⚠️ **Non Zero-Copy Warning**
    /// - This method **copies** data from the memory-mapped file into the buffer.
    /// - **Use `EntryHandle::as_slice()` for zero-copy access.**
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let slice = self.entry_handle.as_slice(); // Get zero-copy reference

        let remaining = self.entry_handle.range.end - self.position;
        if remaining == 0 {
            return Ok(0); // EOF
        }

        let bytes_to_read = remaining.min(buf.len());
        let start_idx = self.position - self.entry_handle.range.start;

        buf[..bytes_to_read].copy_from_slice(&slice[start_idx..start_idx + bytes_to_read]);

        self.position += bytes_to_read;
        Ok(bytes_to_read)
    }
}
