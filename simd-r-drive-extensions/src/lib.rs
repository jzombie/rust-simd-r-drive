use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use simd_r_drive::DataStore;

pub const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE]; // Distinct from bincode None (0x00)

/// **Storage Utilities for Handling `Option<T>`**
pub trait StorageOptionExt {
    /// Writes an `Option<T>` to storage, storing `None` as a tombstone marker.
    fn write_option<T: Serialize>(&self, key: &[u8], value: Option<&T>) -> std::io::Result<u64>;

    /// Reads an `Option<T>` from storage, returning `None` if a tombstone is found.
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

    fn read_option<T: DeserializeOwned>(&self, key: &[u8]) -> Option<Option<T>> {
        let entry = self.read(key)?;
        let data = entry.as_slice();

        if data == OPTION_TOMBSTONE_MARKER {
            return Some(None);
        }

        bincode::deserialize::<T>(&data).ok().map(Some)
    }
}
