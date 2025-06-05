use std::{
    fs::{self, File},
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::Command,
};

use simd_r_drive::DataStore;
use tempfile::NamedTempFile;

// PROTOTYPE ONLY
// Note: This originally started off as a shellcode loader but if I can contain binaries and run them this way, that will be good enough

/// Load a binary payload from the data store by key and execute it.
pub fn exec_from_store(store: &DataStore, key: &[u8]) {
    let entry = store.read(key).expect("no payload for given key");
    let bytes = entry.as_slice();

    let mut tmp = NamedTempFile::new().expect("failed to create temp file");
    tmp.write_all(bytes).expect("failed to write payload");
    let tmp_path = tmp.into_temp_path();

    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755))
        .expect("failed to set permissions");

    let status = Command::new(&tmp_path)
        .current_dir(".")
        .spawn()
        .expect("failed to spawn binary")
        .wait()
        .expect("failed to wait on process");

    println!("Exited with: {status}");
}

fn main() {
    let store = DataStore::open_existing(Path::new("../data.bin")).unwrap();
    exec_from_store(&store, b"hello");
}
