#[cfg(doctest)]
doc_comment::doctest!("../README.md");

use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use simd_r_drive::DataStore;

pub const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE]; // Distinct from bincode None (0x00)

/// # Storage Utilities for Handling `Option<T>`
///
/// This trait provides convenience methods for storing and retrieving
/// `Option<T>` values in a `DataStore`. It ensures that `None` values are
/// explicitly marked using a **tombstone marker** (`OPTION_TOMBSTONE_MARKER`)
/// instead of serializing to an empty value or `bincode`'s default representation.
///
/// ## Use Cases
/// - Efficiently handling `Option<T>` in storage.
/// - Preventing accidental overwrites with default `bincode` representations.
/// - Explicitly distinguishing between `Some(value)` and `None` in a **binary format**.
///
/// ## Tombstone Marker
/// - `None` values are stored as `OPTION_TOMBSTONE_MARKER: [0xFF, 0xFE]` to
///   distinguish them from serialized `None` values (`0x00` in `bincode`).
///
/// ## Example Usage
///
/// ```rust
/// use simd_r_drive::{DataStore};
/// use simd_r_drive_extensions::StorageOptionExt;
/// use std::path::PathBuf;
///
/// let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();
///
/// // Writing `Some(value)`
/// storage.write_option(b"key1", Some(&42)).unwrap();
///
/// // Writing `None` (tombstone)
/// storage.write_option::<i32>(b"key2", None).unwrap();
///
/// // Reading values back
/// let value: Option<i32> = storage.read_option(b"key1").unwrap();
/// assert_eq!(value, Some(42));
///
/// let none_value: Option<i32> = storage.read_option(b"key2").unwrap();
/// assert_eq!(none_value, None);
/// ```
pub trait StorageOptionExt {
    /// Writes an `Option<T>` to storage, storing `None` as a tombstone marker.
    ///
    /// This function:
    /// - Serializes `Some(value)` using `bincode`.
    /// - Stores `None` as a **tombstone marker** (`OPTION_TOMBSTONE_MARKER`).
    ///
    /// # Parameters
    /// - `key`: The binary key under which the value will be stored.
    /// - `value`: An `Option<&T>`, where `Some(value)` is stored normally, and
    ///   `None` is replaced by a tombstone marker.
    ///
    /// # Returns
    /// - `Ok(offset)`: The **file offset** where the entry was written.
    /// - `Err(std::io::Error)`: If the write operation fails.
    ///
    /// # Example
    /// ```rust
    /// use simd_r_drive::{DataStore};
    /// use simd_r_drive_extensions::StorageOptionExt;
    /// use std::path::PathBuf;
    ///
    /// let storage = DataStore::open(&PathBuf::from("example_store.bin")).unwrap();
    ///
    /// // Store an integer
    /// storage.write_option(b"some_key", Some(&123)).unwrap();
    ///
    /// // Store None (creates a tombstone)
    /// storage.write_option::<i32>(b"deleted_key", None).unwrap();
    /// ```
    fn write_option<T: Serialize>(&self, key: &[u8], value: Option<&T>) -> std::io::Result<u64>;

    /// Reads an `Option<T>` from storage, returning `None` if a tombstone is found.
    ///
    /// This function:
    /// - Reads the stored data.
    /// - If the data matches `OPTION_TOMBSTONE_MARKER`, it returns `None`.
    /// - Otherwise, it attempts to deserialize `T` using `bincode`.
    ///
    /// # Parameters
    /// - `key`: The binary key to retrieve.
    ///
    /// # Returns
    /// - `Some(Some(T))`: If the key is found and contains valid serialized data.
    /// - `Some(None)`: If the key exists but was stored as a tombstone (`None`).
    /// - `None`: If the key does not exist in the storage.
    ///
    /// # Example
    /// ```rust
    /// use simd_r_drive::{DataStore};
    /// use simd_r_drive_extensions::StorageOptionExt;
    /// use std::path::PathBuf;
    ///
    /// let storage = DataStore::open(&PathBuf::from("example_store.bin")).unwrap();
    ///
    /// storage.write_option(b"some_key", Some(&789)).unwrap();
    /// storage.write_option::<i32>(b"deleted_key", None).unwrap();
    ///
    /// let value: Option<i32> = storage.read_option(b"some_key").unwrap();
    /// assert_eq!(value, Some(789));
    ///
    /// let deleted_value: Option<i32> = storage.read_option(b"deleted_key").unwrap();
    /// assert_eq!(deleted_value, None);
    /// ```
    fn read_option<T: DeserializeOwned>(&self, key: &[u8]) -> Option<Option<T>>;
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

    // TODO: Document how this is not zero-copy
    fn read_option<T: DeserializeOwned>(&self, key: &[u8]) -> Option<Option<T>> {
        let entry = self.read(key)?;
        let data = entry.as_slice();

        if data == OPTION_TOMBSTONE_MARKER {
            return Some(None);
        }

        bincode::deserialize::<T>(&data).ok().map(Some)
    }
}
