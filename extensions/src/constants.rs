/// Special marker for explicitly storing `None` values in binary storage.
pub(crate) const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE];

/// # Namespaced Prefixes for Storage Features
///
/// These prefixes are **not** used to differentiate values themselves;  
/// SIMD R Drive already handles type and structure differentiation.  
///
/// Instead, these prefixes are used to distinguish **storage features** such as:
/// - **Option handling** (explicit tombstones for `None` values)
/// - **TTL-based auto-eviction** (keys prefixed with expiration timestamps)
///
/// By applying **feature-based** prefixes, we ensure that:
/// - Different feature extensions do not naturally conflict.
/// - Per-extensions read/write operations apply the correct logic.
/// - Keys remain distinct even if their raw values are identical.
///
/// This ensures relatively safe, efficient, and collision-free feature separation  
/// without interfering with the actual stored values.
macro_rules! namespace_prefix {
    ($name:expr) => {{
        const PREFIX: &[u8] = &{
            const LEN: usize = $name.len();
            let mut arr = [0u8; LEN + 2]; // Boundary + Name + Boundary

            arr[0] = 0xF7; // Start Boundary: Non-standard, forbidden high UTF-8 range
            arr[LEN + 1] = 0xFD; // End Boundary: Another high, rarely used byte

            let mut i = 0;
            while i < LEN {
                arr[i + 1] = $name[i];
                i += 1;
            }
            arr
        };
        PREFIX
    }};
}

/// Namespaced extension prefixes
pub(crate) const OPTION_PREFIX: &[u8] = namespace_prefix!(b"option");
pub(crate) const TTL_PREFIX: &[u8] = namespace_prefix!(b"ttl");
