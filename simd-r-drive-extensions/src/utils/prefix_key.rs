/// Utility function to **prefix a binary key** with a given prefix.
///
/// - Ensures the prefixed key remains valid for storage.
/// - Prevents key collisions by ensuring distinct namespaces.
///
/// ## Arguments
/// - `prefix`: The binary prefix to prepend.
/// - `key`: The original binary key.
///
/// ## Returns
/// - A new `Vec<u8>` containing the prefixed key.
///
/// ## Example
/// ```rust
/// use simd_r_drive_extensions::utils::prefix_key;
///
/// let key = b"my_key";
/// let prefixed = prefix_key(b"cache_", key);
///
/// assert_eq!(prefixed, b"cache_my_key".to_vec());
/// ```
pub fn prefix_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut prefixed_key = Vec::with_capacity(prefix.len() + key.len());
    prefixed_key.extend_from_slice(prefix);
    prefixed_key.extend_from_slice(key);
    prefixed_key
}
