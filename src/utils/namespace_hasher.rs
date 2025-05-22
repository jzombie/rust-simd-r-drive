use crate::storage_engine::digest::compute_hash;

/// A utility struct for namespacing keys using XXH3 hashing.
///
/// This ensures that keys are uniquely identified within a given namespace,
/// even if they share the same suffix. By hashing both the namespace and key
/// separately before combining them, it prevents unintended collisions.
///
/// # Example:
/// ```
/// use simd_r_drive_extensions::utils::NamespaceHasher;
///
/// let hasher = NamespaceHasher::new(b"opt");
/// let namespaced_key = hasher.namespace(b"my_key");
/// assert_eq!(namespaced_key.len(), 16, "Namespaced key should be exactly 16 bytes");
/// ```
pub struct NamespaceHasher {
    prefix: u64,
}

impl NamespaceHasher {
    /// Creates a new `NamespaceHasher` with a given prefix.
    ///
    /// The prefix itself is hashed using XXH3 to ensure a unique namespace identifier.
    /// This avoids collisions between different namespaces while keeping the hashing fast.
    ///
    /// # Arguments
    /// - `prefix`: A byte slice representing the namespace prefix.
    ///
    /// # Returns
    /// - A `NamespaceHasher` instance with a precomputed prefix hash.
    #[inline]
    pub fn new(prefix: &[u8]) -> Self {
        Self {
            prefix: compute_hash(prefix),
        }
    }

    /// Computes a namespaced key, returning it as a **16-byte vector**.
    ///
    /// The final namespaced key is derived by:
    /// 1. Hashing the key separately to ensure uniqueness.
    /// 2. Combining it with the precomputed namespace hash.
    /// 3. Returning the **concatenation of both hashes** as a **16-byte key**.
    ///
    /// This ensures that:
    /// - **Different namespaces** do not generate overlapping keys.
    /// - **Keys within a namespace** remain **uniquely identifiable**.
    ///
    /// # Arguments
    /// - `key`: A byte slice representing the key to be namespaced.
    ///
    /// # Returns
    /// - A `Vec<u8>` containing the **16-byte** namespaced key (`8-byte prefix hash + 8-byte key hash`).
    #[inline]
    pub fn namespace(&self, key: &[u8]) -> Vec<u8> {
        let key_hash = compute_hash(key);

        // Combine both hashes into a 16-byte buffer
        let mut buffer = Vec::with_capacity(16);
        buffer.extend_from_slice(&self.prefix.to_le_bytes()); // Prefix hash (8 bytes)
        buffer.extend_from_slice(&key_hash.to_le_bytes()); // Key hash (8 bytes)

        buffer
    }
}
