use std::{
    fs::{self, OpenOptions},
    io::{Result, copy},
    path::{Path, PathBuf},
    process::Command,
};

use simd_r_drive::DataStore;
use simd_r_drive::storage_engine::{EntryHandle, EntryStream};
use std::io;
use tempfile::NamedTempFile;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// PROTOTYPE only... This started off as an experiment to execute shellcode, but that was
// rather complex and limiting, and I think this might wind up being something useful.

/// Executes a binary payload stored in `DataStore` by key.
///
/// Streams the entry to a temporary file, marks it executable,
/// and spawns it as a subprocess with CWD set to `.`.
pub fn exec_from_store(store: &DataStore, key: &[u8], args: &[&str]) -> Result<i32> {
    let handle: EntryHandle = store.read(key).expect("no such key");
    let mut stream = EntryStream::from(handle);

    let tmp_file = NamedTempFile::new()?;

    #[cfg(windows)]
    let exec_path: PathBuf = {
        let new_path = tmp_file.path().with_extension("exe");
        tmp_file.persist(&new_path)?;
        new_path
    };

    #[cfg(unix)]
    let exec_path: PathBuf = {
        let new_path = tmp_file.path().to_path_buf();
        tmp_file.persist(&new_path)?;
        new_path
    };

    {
        let mut file = OpenOptions::new().write(true).open(&exec_path)?;
        copy(&mut stream, &mut file)?;
    }

    #[cfg(unix)]
    fs::set_permissions(&exec_path, fs::Permissions::from_mode(0o755))?;

    let mut child = Command::new(&exec_path)
        .args(args)
        .current_dir(".")
        .spawn()?;

    // Wait for child after deletion
    let status = child.wait()?;

    // Delete the temp file after process exit (should already be handled by `NamedTempFile`)
    fs::remove_file(&exec_path)?;

    let code = status.code().unwrap_or(-1);

    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Process exited with code {:?}", code),
        ));
    }

    Ok(code)
}

fn main() {
    let store = DataStore::open_existing(Path::new("../data.bin")).unwrap();
    exec_from_store(&store, b"code", &[]).unwrap();
}
