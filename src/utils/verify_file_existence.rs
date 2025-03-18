use std::path::Path;

/// Checks if the storage file exists and is a valid file before initializing.
///
/// # Parameters:
/// - `path`: The path to the storage file.
///
/// # Returns:
/// - `Ok(())` if the file exists and is a regular file.
/// - `Err(std::io::Error)` if the file does not exist or is not a regular file.
pub fn verify_file_existence(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Storage file '{}' does not exist.", path.display()),
        ));
    }

    if !path.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("'{}' is not a valid file.", path.display()),
        ));
    }

    Ok(())
}
