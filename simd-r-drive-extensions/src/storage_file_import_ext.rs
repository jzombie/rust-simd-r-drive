use crate::NamespaceHasher;
use simd_r_drive::{DataStore, EntryHandle, EntryStream};
use std::fs::File;
use std::io::{self};
use std::path::Path;

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

    /// Retrieves a file entry from storage given its relative path and optional namespace.
    ///
    /// # Arguments
    /// - `rel_path`: Relative file path using OS-native separators.
    /// - `namespace`: Optional namespace prefix used during import.
    ///
    /// # Returns
    /// - `Some(EntryHandle)`: If the file exists.
    /// - `None`: If the key is missing or expired.
    fn read_file_entry<P: AsRef<Path>>(
        &self,
        rel_path: P,
        namespace: Option<&[u8]>,
    ) -> Option<EntryHandle>;

    /// Retrieves a streamed entry for a given relative file path.
    ///
    /// # Arguments
    /// - `rel_path`: Path relative to the import base.
    /// - `namespace`: Optional namespace used during import.
    ///
    /// # Returns
    /// - `Some(EntryStream)`: If the entry exists.
    /// - `None`: If the key is not found or deleted.
    fn open_file_stream<P: AsRef<Path>>(
        &self,
        rel_path: P,
        namespace: Option<&[u8]>,
    ) -> Option<EntryStream>;
}

impl StorageFileImportExt for DataStore {
    fn import_dir_recursively<P: AsRef<Path>>(
        &self,
        base_dir: P,
        namespace: Option<&[u8]>,
    ) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let mut results = Vec::new();
        let base = base_dir.as_ref();

        for entry in walkdir::WalkDir::new(base)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let rel_path = path.strip_prefix(base).unwrap();

            let namespaced_key = to_namespaced_key(rel_path, namespace);

            let mut file = File::open(path)?;
            let offset = self.write_stream(&namespaced_key, &mut file)?;
            results.push((namespaced_key, offset));
        }

        Ok(results)
    }

    fn read_file_entry<P: AsRef<Path>>(
        &self,
        rel_path: P,
        namespace: Option<&[u8]>,
    ) -> Option<EntryHandle> {
        let namespaced_key = to_namespaced_key(rel_path, namespace);
        self.read(&namespaced_key)
    }

    fn open_file_stream<P: AsRef<Path>>(
        &self,
        rel_path: P,
        namespace: Option<&[u8]>,
    ) -> Option<EntryStream> {
        let namespaced_key = to_namespaced_key(rel_path, namespace);
        self.read(&namespaced_key).map(EntryStream::from)
    }
}

fn to_namespaced_key<P: AsRef<Path>>(rel_path: P, namespace: Option<&[u8]>) -> Vec<u8> {
    let unix_key = rel_path
        .as_ref()
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
        .into_bytes();

    match namespace {
        Some(ns) => NamespaceHasher::new(ns).namespace(&unix_key),
        None => unix_key,
    }
}
