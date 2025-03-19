use bincode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{self, ErrorKind};

/// Special marker for explicitly storing `None` values in binary storage.
const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE];

/// Serializes an `Option<T>` into a binary representation.
///
/// - If `Some(value)`, serializes the value using `bincode`.
/// - If `None`, returns a tombstone marker (`[0xFF, 0xFE]`).
///
/// ## Returns
/// - `Vec<u8>` containing the serialized value or tombstone marker.
/// - `Err(io::Error)`: If serialization fails.
///
/// ## Example
/// ```
/// use simd_r_drive_extensions::utils::option_serializer::serialize_option;
///
/// let some_value = serialize_option(Some(&42)).unwrap();
/// let none_value = serialize_option::<i32>(None).unwrap();
///
/// assert_ne!(some_value, none_value);
/// assert_eq!(none_value, vec![0xFF, 0xFE]); // Tombstone marker for `None`
/// ```
pub fn serialize_option<T: Serialize>(value: Option<&T>) -> io::Result<Vec<u8>> {
    match value {
        Some(v) => bincode::serialize(v)
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Failed to serialize Option<T>")),
        None => Ok(OPTION_TOMBSTONE_MARKER.to_vec()),
    }
}

/// Deserializes an `Option<T>` from binary storage.
///
/// - **⚠️ Non Zero-Copy Warning**: Requires deserialization.
/// - If the data matches the tombstone marker, returns `Ok(None)`.
/// - Otherwise, attempts to deserialize the stored value.
///
/// ## Returns
/// - `Ok(Some(T))` if the data is valid.
/// - `Ok(None)` if the tombstone marker is found.
/// - `Err(io::Error)`: If deserialization fails.
///
/// ## Example
/// ```
/// use simd_r_drive_extensions::utils::option_serializer::{serialize_option, deserialize_option};
///
/// let some_value = serialize_option(Some(&42)).unwrap();
/// let none_value = serialize_option::<i32>(None).unwrap();
///
/// let deserialized_some: Option<i32> = deserialize_option(&some_value).unwrap();
/// let deserialized_none: Option<i32> = deserialize_option(&none_value).unwrap();
///
/// assert_eq!(deserialized_some, Some(42));
/// assert_eq!(deserialized_none, None);
/// ```
pub fn deserialize_option<T: DeserializeOwned>(data: &[u8]) -> Result<Option<T>, io::Error> {
    if data == OPTION_TOMBSTONE_MARKER {
        return Ok(None);
    }

    bincode::deserialize::<T>(data)
        .map(Some)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "Failed to deserialize Option<T>"))
}
