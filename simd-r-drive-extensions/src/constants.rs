/// Special marker for explicitly storing `None` values in binary storage.
pub(crate) const OPTION_TOMBSTONE_MARKER: [u8; 2] = [0xFF, 0xFE];

pub(crate) const OPTION_PREFIX: &[u8] = b"--extension-option--";
pub(crate) const TTL_PREFIX: &[u8] = b"--extension-ttl--";
