/// Special marker for explicitly storing `None` values in binary storage.
pub(crate) const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE];

/// Generates a namespaced prefix dynamically for arbitrary-length names
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
