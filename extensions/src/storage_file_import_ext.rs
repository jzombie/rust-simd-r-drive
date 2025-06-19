use simd_r_drive::{
    DataStore, EntryHandle, EntryStream,
    traits::{DataStoreReader, DataStoreWriter},
    utils::NamespaceHasher,
};
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

    /// Retrieves a file entry **stored in the DataStore**, using a relative path
    /// and optional namespace.
    ///
    /// This does **not** read from the actual filesystem. Instead, it accesses
    /// a previously imported file based on its logical key.
    ///
    /// # Arguments
    /// - `rel_path`: Relative path to the file, using OS-native separators.
    /// - `namespace`: Optional namespace used during import (if any).
    ///
    /// # Returns
    /// - `Some(EntryHandle)`: If the file exists in storage.
    /// - `None`: If the key is missing or marked deleted.
    fn read_file_entry<P: AsRef<Path>>(
        &self,
        rel_path: P,
        namespace: Option<&[u8]>,
    ) -> Option<EntryHandle>;

    /// Opens a **streaming reader** for a file stored in the DataStore,
    /// identified by its relative path and optional namespace.
    ///
    /// This reads from the internal append-only store — not the filesystem.
    /// Paths must match the relative structure used during import.
    ///
    /// # Arguments
    /// - `rel_path`: Relative path used during import (OS-native separators allowed).
    /// - `namespace`: Optional namespace prefix applied during import.
    ///
    /// # Returns
    /// - `Some(EntryStream)`: If the file exists in storage.
    /// - `None`: If no entry is found or it has been evicted.
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

        if !base.exists() || !base.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Directory does not exist: {}", base.display()),
            ));
        }

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
