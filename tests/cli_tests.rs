use std::fs;
use std::path::Path;
use std::process::Command;

const BIN_NAME: &str = "simd-r-drive"; // Ensure this matches your Cargo binary name
const TEST_STORAGE: &str = "test_storage.bin";

#[test]
fn test_write_and_read() {
    let _ = fs::remove_file(TEST_STORAGE); // Cleanup before test

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
    let _ = fs::remove_file(TEST_STORAGE);
}

#[test]
fn test_write_without_value() {
    let _ = fs::remove_file(TEST_STORAGE);

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

    let _ = fs::remove_file(TEST_STORAGE);
}

// TODO: Fix
// #[test]
// fn test_read_nonexistent_key() {
//     let _ = fs::remove_file(TEST_STORAGE);

//     // Read a nonexistent key (should fail)
//     let output = Command::new("cargo")
//         .args(&[
//             "run",
//             "--quiet",
//             "--",
//             TEST_STORAGE,
//             "read",
//             "nonexistent_key",
//         ])
//         .output()
//         .expect("Failed to execute process");

//     assert!(
//         !output.status.success(),
//         "Expected failure for nonexistent key"
//     );
//     let stderr = String::from_utf8_lossy(&output.stderr);
//     assert!(
//         stderr.contains("Key 'nonexistent_key' not found"),
//         "Unexpected error: {:?}",
//         stderr
//     );

//     let _ = fs::remove_file(TEST_STORAGE);
// }
