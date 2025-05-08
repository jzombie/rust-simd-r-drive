use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageFileImportExt;
use std::io::Read;
use std::path::PathBuf;

fn main() {
    let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();

    storage
        .import_dir_recursively("../assets", None)
        .expect("Failed to import directory");

    // Read a previously imported file by relative path
    let mut stream = storage
        .open_file_stream("storage-layout.png", None)
        .expect("File not found in storage");
}
