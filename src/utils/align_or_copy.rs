use std::{borrow::Cow, mem};

/// Attempts to reinterpret a byte slice as a slice of `T`, falling back to copying if alignment fails.
///
/// This function enables efficient, zero-copy parsing of binary data into typed slices (e.g., `[f32]`)
/// by using `slice::align_to::<T>()`, which reinterprets `&[u8]` as `&[T]` if alignment and size match.
///
/// If the memory is not correctly aligned for `T`, or if the total byte length is not a multiple of
/// `size_of::<T>()`, the function falls back to an owned `Vec<T>` by manually parsing each fixed-size chunk.
///
/// # Type Parameters
/// - `T`: Target type to reinterpret or decode from little-endian bytes (e.g., `f32`, `u32`)
/// - `N`: Fixed byte size of each element, must be equal to `size_of::<T>()`
///
/// # Arguments
/// - `bytes`: Raw input byte buffer
/// - `from_le_bytes`: Conversion function for `[u8; N]` into `T` (e.g., `f32::from_le_bytes`)
///
/// # Returns
/// A [`Cow<[T]>`] that:
/// - Borrows the original memory if alignment and size are compatible
/// - Allocates a new vector if fallback decoding is required
///
/// # Panics
/// - If `mem::size_of::<T>() != N`
/// - If fallback path is triggered but the input length is not a multiple of `N`
///
/// # Safety
/// The `align_to::<T>()` call is marked `unsafe` because it performs a type cast from `u8` to `T`.
/// Rust requires that:
/// - The starting address must be aligned to `align_of::<T>()`
/// - The total size of the aligned region must be a multiple of `size_of::<T>()`
///
/// We guard this by checking that `prefix` and `suffix` are empty before returning the borrowed slice.
/// If those checks fail, we instead decode manually and safely.
///
/// # Example
/// ```rust
/// use std::borrow::Cow;
/// let raw = &[0x00, 0x00, 0x80, 0x3f]; // f32 value = 1.0
/// let result: Cow<[f32]> = simd_r_drive::utils::align_or_copy::<f32, 4>(raw, f32::from_le_bytes);
/// assert_eq!(result[0], 1.0);
/// ```
pub fn align_or_copy<T, const N: usize>(
    bytes: &[u8],
    from_le_bytes: fn([u8; N]) -> T,
) -> Cow<'_, [T]>
where
    T: Copy,
{
    assert_eq!(mem::size_of::<T>(), N, "Mismatched size for target type");

    // SAFETY: `align_to::<T>()` requires that we only use the aligned region if:
    // - the prefix is empty (i.e., the starting address is aligned for `T`)
    // - the suffix is empty (i.e., length is a multiple of `size_of::<T>`)
    // We enforce both conditions below before returning a borrowed slice.
    let (prefix, aligned, suffix) = unsafe { bytes.align_to::<T>() };
    if prefix.is_empty() && suffix.is_empty() {
        Cow::Borrowed(aligned)
    } else {
        assert!(
            bytes.len().is_multiple_of(N),
            "Input length must be a multiple of element size"
        );

        Cow::Owned(
            bytes
                .chunks_exact(N)
                .map(|chunk| from_le_bytes(chunk.try_into().unwrap()))
                .collect(),
        )
    }
}
