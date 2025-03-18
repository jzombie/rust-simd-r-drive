/// Parses a buffer size string with optional unit suffixes into a `usize` value.
///
/// This function accepts numeric values with optional unit suffixes (e.g., `"K"`, `"MB"`, `"GB"`)
/// and converts them into their corresponding byte values. The parsing is case-insensitive,
/// and spaces around the input string are trimmed.
///
/// # Supported Units
///
/// - No unit: Assumes bytes (e.g., `"1024"` → `1024` bytes)
/// - `K` or `KB`: Kilobytes (e.g., `"2K"` or `"2KB"` → `2048` bytes)
/// - `M` or `MB`: Megabytes (e.g., `"1M"` or `"1MB"` → `1048576` bytes)
/// - `G` or `GB`: Gigabytes (e.g., `"1G"` or `"1GB"` → `1073741824` bytes)
///
/// # Arguments
///
/// * `size_str` - A string slice representing the buffer size with an optional unit.
///
/// # Returns
///
/// Returns `Ok(usize)` if parsing succeeds, or an `Err(String)` if the format is invalid.
///
/// # Examples
///
/// ```
/// use simd_r_drive::utils::parse_buffer_size;
///
/// assert_eq!(parse_buffer_size("1024").unwrap(), 1024);
/// assert_eq!(parse_buffer_size("2K").unwrap(), 2048);
/// assert_eq!(parse_buffer_size("1MB").unwrap(), 1_048_576);
/// assert_eq!(parse_buffer_size("1G").unwrap(), 1_073_741_824);
///
/// assert!(parse_buffer_size("abc").is_err()); // Invalid input
/// assert!(parse_buffer_size("10XZ").is_err()); // Invalid unit
/// ```
pub fn parse_buffer_size(size_str: &str) -> Result<usize, String> {
    let size_str = size_str.trim().to_lowercase();

    // Find the position where the numeric part ends
    let num_end = size_str
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(size_str.len());

    let (num_part, unit_part) = size_str.split_at(num_end);

    let multiplier = match unit_part {
        "" => 1, // No unit -> assume bytes
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return Err(format!("Invalid buffer size unit: {}", unit_part)),
    };

    num_part
        .parse::<usize>()
        .map(|n| n * multiplier)
        .map_err(|_| format!("Failed to parse buffer size: {}", size_str))
}
