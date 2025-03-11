/// Converts a file size in bytes into a human-readable format.
///
/// This function formats byte sizes dynamically into **KB, MB, or GB** for readability.
///
/// # Conversion Logic:
/// - **1,024 bytes → KB**
/// - **1,024 KB → MB**
/// - **1,024 MB → GB**
///
/// # Formatting:
/// - Uses **two decimal places** for precision (e.g., `"10.43 MB"`).
/// - If size is below 1 KB, it is displayed in **raw bytes** (e.g., `"512 bytes"`).
///
/// # Parameters:
/// - `bytes`: The size in bytes to format.
///
/// # Returns:
/// - A `String` representing the human-readable file size.
///
/// # Examples
/// ```
/// use simd_r_drive::format_bytes;
///
/// assert_eq!(format_bytes(500), "500 bytes");
/// assert_eq!(format_bytes(2048), "2.00 KB");
/// assert_eq!(format_bytes(5_242_880), "5.00 MB");
/// assert_eq!(format_bytes(8_796_093_440), "8.19 GB");
/// ```
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    match bytes {
        b if b >= GB => format!("{:.2} GB", b as f64 / GB as f64),
        b if b >= MB => format!("{:.2} MB", b as f64 / MB as f64),
        b if b >= KB => format!("{:.2} KB", b as f64 / KB as f64),
        _ => format!("{} bytes", bytes),
    }
}
