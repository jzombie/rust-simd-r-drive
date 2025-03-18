#[cfg(doctest)]
doc_comment::doctest!("../README.md");

use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use simd_r_drive::DataStore;
use std::io::{self, ErrorKind};

/// Special marker for explicitly storing `None` values in binary storage.
/// This ensures that `None` is distinguishable from an empty or default value.
const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE];

#[cfg(any(test, debug_assertions))]
pub const TEST_OPTION_TOMBSTONE_MARKER: [u8; 2] = OPTION_TOMBSTONE_MARKER;

/// # Storage Utilities for Handling `Option<T>`
///
/// This trait provides methods to store and retrieve `Option<T>` values
/// in a `DataStore`, ensuring that `None` values are explicitly handled.
///
/// ## Purpose
/// - **Prevents ambiguity**: Ensures `None` is stored and retrieved correctly.
/// - **Efficient storage**: Uses a compact representation.
/// - **Binary-safe**: Avoids unintended interpretation of missing values.
///
/// ## Implementation Details
/// - **`Some(value)`**: Serialized using `bincode`.
/// - **`None`**: Stored using a predefined method.
///
/// ## Example Usage
///
/// ```rust
/// use simd_r_drive::DataStore;
/// use simd_r_drive_extensions::StorageOptionExt;
/// use std::path::PathBuf;
///
/// let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();
///
/// // Store `Some(value)`
/// storage.write_option(b"key1", Some(&42)).unwrap();
///
/// // Store `None` (tombstone)
/// storage.write_option::<i32>(b"key2", None).unwrap();
///
/// // Read values
/// assert_eq!(storage.read_option::<i32>(b"key1").unwrap(), Some(42));
/// assert_eq!(storage.read_option::<i32>(b"key2").unwrap(), None);
/// ```
pub trait StorageOptionExt {
    /// Writes an `Option<T>` into the `DataStore`, ensuring `None` values are preserved.
    ///
    /// - `Some(value)`: Serialized using `bincode`.
    /// - `None`: Stored in a way that allows correct retrieval.
    ///
    /// # Arguments
    /// - `key`: The binary key under which the value is stored.
    /// - `value`: An optional reference to `T`, where `None` is handled appropriately.
    ///
    /// # Returns
    /// - `Ok(offset)`: The **file offset** where the data was written.
    /// - `Err(std::io::Error)`: If the write operation fails.
    ///
    /// # Example
    /// ```rust
    /// use simd_r_drive::DataStore;
    /// use simd_r_drive_extensions::StorageOptionExt;
    /// use std::path::PathBuf;
    ///
    /// let storage = DataStore::open(&PathBuf::from("store.bin")).unwrap();
    ///
    /// // Write `Some(value)`
    /// storage.write_option(b"key", Some(&123)).unwrap();
    ///
    /// // Write `None` (tombstone)
    /// storage.write_option::<i32>(b"deleted_key", None).unwrap();
    /// ```
    fn write_option<T: Serialize>(&self, key: &[u8], value: Option<&T>) -> std::io::Result<u64>;

    /// Reads an `Option<T>` from storage.
    ///
    /// - **Returns `None`** if the stored value represents a missing entry.
    /// - **Attempts deserialization** of `T` otherwise.
    /// - **Returns `Ok(None)`** if the key does not exist.
    ///
    /// # Arguments
    /// - `key`: The binary key to retrieve.
    ///
    /// # Returns
    /// - `Ok(Some(T))`: If deserialization succeeds.
    /// - `Ok(None)`: If the key does not exist or represents `None`.
    /// - `Err(std::io::Error)`: If deserialization fails.
    ///
    /// # Example
    /// ```rust
    /// use simd_r_drive::DataStore;
    /// use simd_r_drive_extensions::StorageOptionExt;
    /// use std::path::PathBuf;
    ///
    /// let storage = DataStore::open(&PathBuf::from("store.bin")).unwrap();
    ///
    /// storage.write_option(b"some_key", Some(&789)).unwrap();
    /// storage.write_option::<i32>(b"deleted_key", None).unwrap();
    ///
    /// assert_eq!(storage.read_option::<i32>(b"some_key").unwrap(), Some(789));
    /// assert_eq!(storage.read_option::<i32>(b"deleted_key").unwrap(), None);
    /// ```
    fn read_option<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, std::io::Error>;
}

/// Implements `StorageOptionExt` for `DataStore`
impl StorageOptionExt for DataStore {
    fn write_option<T: Serialize>(&self, key: &[u8], value: Option<&T>) -> std::io::Result<u64> {
        let serialized = match value {
            Some(v) => bincode::serialize(v).unwrap_or_else(|_| OPTION_TOMBSTONE_MARKER.to_vec()),
            None => OPTION_TOMBSTONE_MARKER.to_vec(),
        };

        self.write(key, &serialized)
    }

    /// Reads an `Option<T>` from storage.
    ///
    /// # ⚠️ **Non Zero-Copy Warning**
    /// - **Not zero-copy**: Requires deserialization.
    /// - **Returns `None`** if key does not exist or is a tombstone marker.
    /// - **Errors on invalid deserialization.**
    ///
    /// # Safety
    /// - This function **allocates memory** for deserialization.
    fn read_option<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, io::Error> {
        match self.read(key) {
            Some(entry) => {
                let data = entry.as_slice();
                if data == OPTION_TOMBSTONE_MARKER {
                    return Ok(None);
                }
                bincode::deserialize::<T>(data)
                    .map(Some)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            }
            None => Ok(None),
        }
    }
}
