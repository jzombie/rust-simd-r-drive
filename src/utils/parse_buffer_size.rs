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
