use std::{fs, io::copy, os::unix::fs::PermissionsExt, path::Path, process::Command};

use simd_r_drive::DataStore;
use simd_r_drive::storage_engine::{EntryHandle, EntryStream};
use std::io::Result;
use tempfile::NamedTempFile;

/// Executes a binary payload stored in `DataStore` by key.
///
/// Streams the entry to a temporary file, marks it executable,
/// and spawns it as a subprocess with CWD set to `.`.
pub fn exec_from_store(store: &DataStore, key: &[u8], args: &[&str]) -> Result<()> {
    let handle: EntryHandle = store.read(key).expect("no such key");
    let mut stream = EntryStream::from(handle);

    // Persist and close the temp file
    let tmp_path = NamedTempFile::new()?.into_temp_path().keep()?;

    {
        let mut file = fs::OpenOptions::new().write(true).open(&tmp_path)?;
        copy(&mut stream, &mut file)?;
    }

    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o755))?;

    let status = Command::new(&tmp_path)
        .args(args)
        .current_dir(".")
        .spawn()?
        .wait()?;

    println!("Exited with: {}", status);
    Ok(())
}

fn main() {
    let store = DataStore::open_existing(Path::new("../data.bin")).unwrap();
    exec_from_store(&store, b"sys", &[]).unwrap();
}
