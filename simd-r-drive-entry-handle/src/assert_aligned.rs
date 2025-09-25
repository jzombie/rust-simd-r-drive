#[cfg(any(test, debug_assertions))]
#[inline]
pub fn assert_aligned(ptr: *const u8, align: usize) {
    debug_assert!(align.is_power_of_two());
    debug_assert!(
        (ptr as usize & (align - 1)) == 0,
        "buffer base is not {}-byte aligned",
        align
    );
}

#[cfg(any(test, debug_assertions))]
#[inline]
pub fn assert_aligned_offset(off: u64) {
    use crate::constants::PAYLOAD_ALIGNMENT;

    debug_assert!(
        PAYLOAD_ALIGNMENT.is_power_of_two(),
        "PAYLOAD_ALIGNMENT must be a power of two"
    );
    debug_assert!(
        off % PAYLOAD_ALIGNMENT == 0,
        "derived payload start not {}-byte aligned (got {})",
        PAYLOAD_ALIGNMENT,
        off
    );
}
