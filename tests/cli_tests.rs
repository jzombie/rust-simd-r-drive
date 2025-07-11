use serial_test::serial;
use std::fs;
use std::io::Write;
use std::process::Command;

const TEST_STORAGE: &str = "test_storage.bin";
const TARGET_STORAGE: &str = "target_storage.bin";

#[test]
#[serial]
fn test_write_and_read() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Write a value to the storage
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "write",
            "test_key",
            "hello",
        ])
        .output()
        .expect("Failed to execute process");

    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Read the value back
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "read", "test_key"])
        .output()
        .expect("Failed to execute process");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim(), "hello", "Unexpected read output: {stdout:?}",);

    // Cleanup
    fs::remove_file(TEST_STORAGE).ok();
}

#[test]
#[serial]
fn test_write_without_value() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Try writing without a value (should fail)
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "write", "test_key"])
        .env("FORCE_NO_TTY", "1") // Set env variable to override is_terminal()
        .stdin(std::process::Stdio::null()) // Explicitly set no stdin
        .output()
        .expect("Failed to execute process");

    assert!(
        !output.status.success(),
        "Expected failure on missing value"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No value provided"),
        "Unexpected error message: {stderr:?}",
    );

    fs::remove_file(TEST_STORAGE).ok(); // Cleanup
}

#[test]
#[serial]
fn test_read_nonexistent_key() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Ensure the storage file exists
    let mut file = fs::File::create(TEST_STORAGE).expect("Failed to create storage file");
    file.write_all(b"")
        .expect("Failed to initialize storage file");

    // Attempt to read a nonexistent key (should fail)
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "read",
            "nonexistent_key",
        ])
        .stderr(std::process::Stdio::piped()) // Capture stderr
        .output()
        .expect("Failed to execute process");

    assert!(
        !output.status.success(),
        "Expected failure for nonexistent key, but command succeeded."
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stderr.trim().contains("Key 'nonexistent_key' not found")
            || stderr.trim().contains("Failed to open storage"),
        "Unexpected error output: {stderr:?}",
    );

    fs::remove_file(TEST_STORAGE).ok();
}

#[test]
#[serial]
fn test_read_with_buffer_size() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    let large_value = "A".repeat(128 * 1024); // 128KB of data

    // Write the large value to the storage using stdin
    let mut child = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "write", "large_key"])
        .stdin(std::process::Stdio::piped()) // Open a pipe to send data
        .spawn()
        .expect("Failed to execute process");

    // Send data through stdin
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(large_value.as_bytes())
            .expect("Failed to write to stdin");
    }

    let output = child
        .wait_with_output()
        .expect("Failed to wait on child process");

    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Read the value back with a 64KB buffer size
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "read",
            "large_key",
            "--buffer-size",
            "64K",
        ])
        .output()
        .expect("Failed to execute process");

    assert!(output.status.success(), "Read command failed: {output:?}",);

    let stdout = output.stdout;
    assert_eq!(
        stdout.len(),
        large_value.len(),
        "Output length does not match expected value length"
    );

    // Ensure output is chunked correctly
    assert!(
        stdout.chunks(65536).all(|chunk| chunk.len() <= 65536),
        "Read output was not properly chunked according to buffer size"
    );

    fs::remove_file(TEST_STORAGE).ok(); // Cleanup
}

#[test]
#[serial]
fn test_copy_key() {
    fs::remove_file(TEST_STORAGE).ok();
    fs::remove_file(TARGET_STORAGE).ok();

    // Write a value to the storage
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "write",
            "copy_key",
            "copy_test",
        ])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Copy the key to target storage
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "copy",
            "copy_key",
            TARGET_STORAGE,
        ])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Copy command failed: {output:?}",);

    // Read from target storage
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TARGET_STORAGE, "read", "copy_key"])
        .output()
        .expect("Failed to execute process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.trim(),
        "copy_test",
        "Unexpected read output: {stdout:?}",
    );

    fs::remove_file(TEST_STORAGE).ok();
    fs::remove_file(TARGET_STORAGE).ok();
}

#[test]
#[serial]
fn test_rename_key() {
    fs::remove_file(TEST_STORAGE).ok();

    // Write a value
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "write",
            "old_key",
            "rename_test",
        ])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Rename the key
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "rename",
            "old_key",
            "new_key",
        ])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Rename command failed: {output:?}",);

    // Ensure old key doesn't exist
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "read", "old_key"])
        .output()
        .expect("Failed to execute process");
    assert!(!output.status.success(), "Old key should not exist");

    // Ensure new key exists
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "read", "new_key"])
        .output()
        .expect("Failed to execute process");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.trim(),
        "rename_test",
        "Unexpected read output: {stdout:?}",
    );

    fs::remove_file(TEST_STORAGE).ok();
}

#[test]
#[serial]
fn test_delete_key() {
    fs::remove_file(TEST_STORAGE).ok();

    // Write a value
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "write",
            "delete_key",
            "delete_test",
        ])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Delete the key
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "delete", "delete_key"])
        .output()
        .expect("Failed to execute process");
    assert!(output.status.success(), "Delete command failed: {output:?}",);

    // Ensure key doesn't exist
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "read", "delete_key"])
        .output()
        .expect("Failed to execute process");
    assert!(!output.status.success(), "Deleted key should not exist");

    fs::remove_file(TEST_STORAGE).ok();
}

#[test]
#[serial]
fn test_metadata() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Write a test value
    let output = Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--",
            TEST_STORAGE,
            "write",
            "test_key",
            "hello",
        ])
        .output()
        .expect("Failed to execute process");

    assert!(output.status.success(), "Write command failed: {output:?}",);

    // Retrieve metadata for the key
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "metadata", "test_key"])
        .output()
        .expect("Failed to execute process");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("METADATA SUMMARY"),
        "Metadata output invalid: {stdout:?}",
    );
    assert!(
        stdout.contains("ENTRY FOR:"),
        "Metadata missing ENTRY FOR: {stdout:?}",
    );
    assert!(
        stdout.contains("test_key"),
        "Metadata does not contain key: {stdout:?}",
    );
    assert!(
        stdout.contains("PAYLOAD SIZE:"),
        "Metadata missing payload size: {stdout:?}",
    );
    assert!(
        stdout.contains("TOTAL SIZE (W/ METADATA):"),
        "Metadata missing total size: {stdout:?}",
    );
    assert!(
        stdout.contains("OFFSET RANGE:"),
        "Metadata missing offset range: {stdout:?}",
    );
    assert!(
        stdout.contains("MEMORY ADDRESS:"),
        "Metadata missing memory address: {stdout:?}",
    );
    assert!(
        stdout.contains("KEY HASH:"),
        "Metadata missing key hash: {stdout:?}",
    );
    assert!(
        stdout.contains("CHECKSUM:"),
        "Metadata missing checksum: {stdout:?}",
    );
    assert!(
        stdout.contains("CHECKSUM VALIDITY:"),
        "Metadata missing checksum validity: {stdout:?}",
    );
    assert!(
        stdout.contains("STORED METADATA:"),
        "Metadata missing stored metadata: {stdout:?}",
    );

    fs::remove_file(TEST_STORAGE).ok(); // Cleanup
}

#[test]
#[serial]
fn test_info() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Initialize an empty storage file
    let _ = fs::File::create(TEST_STORAGE).expect("Failed to create storage file");

    // Retrieve storage info
    let output = Command::new("cargo")
        .args(["run", "--quiet", "--", TEST_STORAGE, "info"])
        .output()
        .expect("Failed to execute process");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("STORAGE INFO"),
        "Info output invalid: {stdout:?}",
    );
    assert!(
        stdout.contains("STORAGE FILE:"),
        "Info missing storage file: {stdout:?}",
    );
    assert!(
        stdout.contains("TOTAL SIZE:"),
        "Info missing total size: {stdout:?}",
    );
    assert!(
        stdout.contains("ACTIVE ENTRIES:"),
        "Info missing active entries: {stdout:?}",
    );
    assert!(
        stdout.contains("COMPACTION SAVINGS:"),
        "Info missing compaction savings: {stdout:?}",
    );

    fs::remove_file(TEST_STORAGE).ok(); // Cleanup
}
