use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Cursor, Read, Result, copy},
    path::Path,
    process::Command,
};

use bytes::Buf;
use simd_r_drive::{DataStore, storage_engine::EntryStream};
use tempfile::NamedTempFile;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use flate2::read::GzDecoder;
use reqwest::blocking::get;
use tar::Archive;

/// Downloads and caches Nushell release for the current platform into `DataStore`.
pub fn download_and_store_nushell(store: &mut DataStore, key: &[u8]) -> Result<()> {
    // TODO: Don't harecode paths
    let (url, binary_path_in_tar) = match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64") => (
            "https://github.com/nushell/nushell/releases/download/0.94.0/nu-0.94.0-aarch64-apple-darwin.tar.gz",
            "nu",
        ),
        ("macos", "x86_64") => (
            "https://github.com/nushell/nushell/releases/download/0.94.0/nu-0.94.0-x86_64-apple-darwin.tar.gz",
            "nu",
        ),
        ("linux", "x86_64") => (
            "https://github.com/nushell/nushell/releases/download/0.94.0/nu-0.94.0-x86_64-unknown-linux-gnu.tar.gz",
            "nu",
        ),
        ("windows", "x86_64") => (
            "https://github.com/nushell/nushell/releases/download/0.94.0/nu-0.94.0-x86_64-pc-windows-msvc.zip",
            "nu.exe",
        ),
        _ => return Err(io::Error::new(io::ErrorKind::Other, "Unsupported platform")),
    };

    let resp = get(url)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Request error: {e}")))?;

    let mut archive = vec![];
    let content = resp
        .error_for_status()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("HTTP error: {e}")))?
        .bytes()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Read error: {e}")))?
        .reader();

    copy(&mut content.take(50_000_000), &mut archive)?;

    let mut extracted_bin = vec![];

    if url.ends_with(".tar.gz") {
        let tar = GzDecoder::new(Cursor::new(&archive));
        let mut archive = Archive::new(tar);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?;
            if path.ends_with(binary_path_in_tar) {
                entry.read_to_end(&mut extracted_bin)?;
                break;
            }
        }
    } else if url.ends_with(".zip") {
        let reader = Cursor::new(&archive);
        let mut zip = zip::ZipArchive::new(reader)?;
        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            if file.name().ends_with(binary_path_in_tar) {
                file.read_to_end(&mut extracted_bin)?;
                break;
            }
        }
    }

    store.write_stream(key, &mut Cursor::new(extracted_bin))?;
    Ok(())
}

pub fn exec_from_store(store: &DataStore, key: &[u8], args: &[&str]) -> Result<i32> {
    let handle = store.read(key).expect("no such key");
    let mut stream = EntryStream::from(handle);

    // Create an isolated temp sandbox directory
    let sandbox_root = tempfile::tempdir()?;

    let tmp_file = NamedTempFile::new()?;

    #[cfg(windows)]
    let exec_path = {
        let new_path = tmp_file.path().with_extension("exe");
        tmp_file.persist(&new_path)?;
        new_path
    };

    #[cfg(unix)]
    let exec_path = {
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

    // TODO: Replace direct `Command::new` with this
    // exec_in_sandbox(binary, args, {
    //     fs_root: temp_dir(),
    //     env: minimal(),
    //     network: loopback_only(),
    //     config_path: fs_root.join("config"),
    //     data_path: fs_root.join("data"),
    //     stdout: capture(),
    // });

    let mut child = Command::new(&exec_path)
        .args(args)
        // .current_dir(sandbox_root.path())
        .current_dir(".")
        // .env_clear()
        // .env("PATH", sandbox_root.path().join("bin"))
        .env("HOME", sandbox_root.path())
        .env("XDG_CONFIG_HOME", sandbox_root.path().join("config"))
        .env("XDG_DATA_HOME", sandbox_root.path().join("data"))
        .env("XDG_CACHE_HOME", sandbox_root.path().join("cache"))
        .env("TMPDIR", sandbox_root.path().join("tmp"))
        .env("TEMP", sandbox_root.path().join("tmp"))
        .env("TMP", sandbox_root.path().join("tmp"))
        .spawn()?;

    let status = child.wait()?;
    fs::remove_file(&exec_path)?;

    let code = status.code().unwrap_or(-1);
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Exited with {:?}", code),
        ));
    }

    Ok(code)
}

fn main() -> Result<()> {
    let mut store = DataStore::open_existing(Path::new("../data.bin"))?;
    let key = b"nushell";

    if store.read(key).is_none() {
        println!("Downloading Nushell...");
        download_and_store_nushell(&mut store, key)?;
        println!("Stored Nushell binary.");
    }

    let code = exec_from_store(&store, key, &[])?;
    println!("Exit code: {}", code);
    Ok(())
}
