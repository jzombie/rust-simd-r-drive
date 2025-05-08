use crate::NamespaceHasher;
use simd_r_drive::DataStore;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// Recursively walks a directory and imports files into the DataStore.
/// Keys are the relative Unix-style paths from `base_dir`.
pub trait StorageFileImportExt {
    /// Adds all regular files under `base_dir` into the DataStore.
    ///
    /// # Arguments
    /// - `base_dir`: Root directory to walk.
    /// - `namespace`: Optional namespace prefix for stored keys.
    ///
    /// # Returns
    /// - A list of `(key, offset)` for stored files.
    fn import_dir_recursively<P: AsRef<Path>>(
        &self,
        base_dir: P,
        namespace: Option<&[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, u64)>>;
}

impl StorageFileImportExt for DataStore {
    fn import_dir_recursively<P: AsRef<Path>>(
        &self,
        base_dir: P,
        namespace: Option<&[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let mut results = Vec::new();
        let base = base_dir.as_ref();
        let hasher = namespace.map(|ns| NamespaceHasher::new(ns));

        for entry in walkdir::WalkDir::new(base)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let rel_path = path.strip_prefix(base).unwrap();
            let unix_key = rel_path
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/"); // Force Unix-style path separation

            let key_bytes = unix_key.as_bytes();
            let namespaced_key = hasher
                .as_ref()
                .map(|h| h.namespace(key_bytes))
                .unwrap_or_else(|| key_bytes.to_vec());

            let mut file = File::open(path)?;
            let mut contents = Vec::new();
            file.read_to_end(&mut contents)?;

            let offset = self.write(&namespaced_key, &contents)?;
            results.push((namespaced_key, offset));
        }

        Ok(results)
    }
}
