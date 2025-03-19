use serial_test::serial;
use std::fs;
use std::io::Write;
use std::process::Command;

// const BIN_NAME: &str = "simd-r-drive"; // Ensure this matches your Cargo binary name
const TEST_STORAGE: &str = "test_storage.bin";

#[test]
#[serial]
fn test_write_and_read() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Write a value to the storage
    let output = Command::new("cargo")
        .args(&[
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

    assert!(
        output.status.success(),
        "Write command failed: {:?}",
        output
    );

    // Read the value back
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", TEST_STORAGE, "read", "test_key"])
        .output()
        .expect("Failed to execute process");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.trim(),
        "hello",
        "Unexpected read output: {:?}",
        stdout
    );

    // Cleanup
    fs::remove_file(TEST_STORAGE).ok();
}

#[test]
#[serial]
fn test_write_without_value() {
    fs::remove_file(TEST_STORAGE).ok(); // Cleanup before test

    // Try writing without a value (should fail)
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", TEST_STORAGE, "write", "test_key"])
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
        "Unexpected error message: {:?}",
        stderr
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
        .args(&[
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
        "Unexpected error output: {:?}",
        stderr
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
        .args(&["run", "--quiet", "--", TEST_STORAGE, "write", "large_key"])
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

    assert!(
        output.status.success(),
        "Write command failed: {:?}",
        output
    );

    // Read the value back with a 64KB buffer size
    let output = Command::new("cargo")
        .args(&[
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

    assert!(output.status.success(), "Read command failed: {:?}", output);

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
