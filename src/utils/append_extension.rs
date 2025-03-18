use std::path::PathBuf;

/// Appends an additional extension to the existing extension of a file path.
///
/// This function preserves the existing extension and appends the new extension
/// to it, instead of replacing it. If the path does not have an existing
/// extension, the new extension is simply added.
///
/// # Arguments
///
/// * `path` - A reference to a `PathBuf` representing the file path.
/// * `ext` - The additional extension to append.
///
/// # Returns
///
/// A new `PathBuf` with the appended extension.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use simd_r_drive::utils::append_extension;
///
/// let path = PathBuf::from("example.txt");
/// let modified = append_extension(&path, "bk");
/// assert_eq!(modified, PathBuf::from("example.txt.bk"));
///
/// let path_no_ext = PathBuf::from("example");
/// let modified_no_ext = append_extension(&path_no_ext, "bk");
/// assert_eq!(modified_no_ext, PathBuf::from("example.bk"));
///
/// let path_multi_ext = PathBuf::from("archive.tar.gz");
/// let modified_multi_ext = append_extension(&path_multi_ext, "bk");
/// assert_eq!(modified_multi_ext, PathBuf::from("archive.tar.gz.bk"));
/// ```
pub fn append_extension(path: &PathBuf, ext: &str) -> PathBuf {
    let mut new_path = path.clone();
    if let Some(original_ext) = new_path.extension() {
        let new_ext = format!("{}.{}", original_ext.to_string_lossy(), ext);
        new_path.set_extension(new_ext);
    } else {
        new_path.set_extension(ext);
    }
    new_path
}
