use crate::constants::TTL_PREFIX;
use serde::Serialize;
use serde::de::DeserializeOwned;
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
    utils::NamespaceHasher,
};
use std::io::{self, ErrorKind};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

static TTL_NAMESPACE_HASHER: OnceLock<Arc<NamespaceHasher>> = OnceLock::new();

#[cfg(any(test, debug_assertions))]
pub const TEST_TTL_PREFIX: &[u8] = TTL_PREFIX;

/// # Storage Utilities for Handling Auto-Evicting TTL Entries
///
/// Stores a timestamp (in seconds) before the actual value.
///
/// Note: Option types are *safely* handled by this without additional serialization
/// as they are stored with the TTL value as well.
pub trait StorageCacheExt {
    /// Writes a value with a TTL (Time-To-Live).
    ///
    /// - Stores the expiration timestamp as a **binary prefix** before the actual data.
    /// - If the key exists, it will be **overwritten**.
    ///
    /// ## Arguments
    /// - `key`: The binary key to store.
    /// - `value`: The value to be stored.
    /// - `ttl_secs`: The TTL in **seconds** (relative to current time).
    ///
    /// ## Returns
    /// - `Ok(offset)`: The **file offset** where the data was written.
    /// - `Err(std::io::Error)`: If the write operation fails.
    fn write_with_ttl<T: Serialize>(&self, key: &[u8], value: &T, ttl_secs: u64)
    -> io::Result<u64>;

    /// Reads a value, checking TTL expiration.
    ///
    /// - **⚠️ Non Zero-Copy Warning**: Requires deserialization.
    /// - If the TTL has expired, the key is **automatically evicted**, and `None` is returned.
    /// - If the key does not exist, returns `Err(ErrorKind::NotFound)`.
    /// - If deserialization fails, returns `Err(ErrorKind::InvalidData)`.
    ///
    /// ## Returns
    /// - `Ok(Some(T))`: If the TTL is still valid and the value is readable.
    /// - `Ok(None)`: If the TTL has expired and the entry has been evicted.
    /// - `Err(std::io::Error)`: If the key is missing or deserialization fails.
    fn read_with_ttl<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, io::Error>;
}

/// Implements TTL-based caching for `DataStore`
impl StorageCacheExt for DataStore {
    fn write_with_ttl<T: Serialize>(
        &self,
        key: &[u8],
        value: &T,
        ttl_secs: u64,
    ) -> io::Result<u64> {
        let namespace_hasher =
            TTL_NAMESPACE_HASHER.get_or_init(|| Arc::new(NamespaceHasher::new(TTL_PREFIX)));
        let namespaced_key = namespace_hasher.namespace(key);

        let expiration_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
            .saturating_add(ttl_secs); // Avoid overflow

        let mut data = expiration_timestamp.to_le_bytes().to_vec();
        let serialized_value = bincode::serialize(value)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Serialization failed"))?;
        data.extend_from_slice(&serialized_value);

        self.write(&namespaced_key, &data)
    }

    fn read_with_ttl<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>, io::Error> {
        let namespace_hasher =
            TTL_NAMESPACE_HASHER.get_or_init(|| Arc::new(NamespaceHasher::new(TTL_PREFIX)));
        let namespaced_key = namespace_hasher.namespace(key);

        match self.read(&namespaced_key) {
            Some(entry) => {
                let data = entry.as_slice();

                if data.len() < 8 {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Data too short to contain TTL",
                    ));
                }

                let expiration_timestamp = u64::from_le_bytes(data[..8].try_into().unwrap());
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs();

                if now >= expiration_timestamp {
                    self.delete_entry(&namespaced_key).ok(); // Remove expired entry
                    return Ok(None);
                }

                bincode::deserialize::<T>(&data[8..])
                    .map(Some)
                    .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Deserialization failed"))
            }
            None => Err(io::Error::new(ErrorKind::NotFound, "Key not found")),
        }
    }
}
