#[cfg(test)]
mod tests {
    use simd_r_drive::utils::align_or_copy;
    use std::borrow::Cow;

    #[test]
    fn test_aligned_f32_zero_copy() {
        let raw: &[u8] = &[0x00, 0x00, 0x80, 0x3f]; // f32 = 1.0
        let result = align_or_copy::<f32, 4>(raw, f32::from_le_bytes);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result[0], 1.0);
    }

    #[test]
    fn test_aligned_u32_values() {
        let raw = 0x12345678u32.to_le_bytes();
        let result = align_or_copy::<u32, 4>(&raw, u32::from_le_bytes);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result[0], 0x12345678);
    }

    #[test]
    fn test_fallback_copy_f32_misaligned() {
        let mut raw = vec![0x00]; // 1-byte prefix = misaligned
        raw.extend_from_slice(&1.0f32.to_le_bytes());
        let result = align_or_copy::<f32, 4>(&raw[1..], f32::from_le_bytes);
        assert!(matches!(result, Cow::Owned(_)));
        assert_eq!(result[0], 1.0);
    }

    #[test]
    fn test_fallback_copy_trailing_bytes() {
        let mut raw = 1.0f32.to_le_bytes().to_vec();
        raw.push(0xFF); // 5 bytes: not a multiple of 4
        let result = std::panic::catch_unwind(|| align_or_copy::<f32, 4>(&raw, f32::from_le_bytes));
        assert!(result.is_err(), "Should panic on non-multiple of size");
    }

    #[test]
    fn test_aligned_u8_trivial() {
        let raw = [1u8, 2, 3, 4];
        let result = align_or_copy::<u8, 1>(&raw, u8::from_le_bytes);
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result.as_ref(), &raw);
    }

    #[test]
    fn test_multiple_f32_values() {
        let values = [1.0f32, 2.0, 3.5];
        let bytes: Vec<u8> = values.iter().flat_map(|f| f.to_le_bytes()).collect();
        let result = align_or_copy::<f32, 4>(&bytes, f32::from_le_bytes);
        assert_eq!(result.as_ref(), &values[..]);
    }
}
