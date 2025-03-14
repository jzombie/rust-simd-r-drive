use serial_test::serial;
use simd_r_drive::DataStore;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Notify;
use tokio::task;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_slow_streamed_write_test() {
    /// Simulated slow reader (delays are applied in the reading loop)
    struct SlowReader<R: Read> {
        inner: R,
        delay: Duration,
    }

    impl<R: Read> SlowReader<R> {
        fn new(inner: R, delay: Duration) -> Self {
            Self { inner, delay }
        }
    }

    impl<R: Read> Read for SlowReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            // Simulate network or disk latency
            std::thread::sleep(self.delay);
            self.inner.read(buf)
        }
    }

    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(DataStore::open(&path).unwrap());

    let test_cases = vec![
        (b"stream_key_A", dir.path().join("test_stream_a.bin"), b'A'),
        (b"stream_key_B", dir.path().join("test_stream_b.bin"), b'B'),
    ];

    let payload_size = 1 * 1024 * 1024; // 1 MB
    let mut tasks = Vec::new();

    // Generate test files
    for (_, file_path, byte) in &test_cases {
        let test_data = vec![*byte; payload_size];
        File::create(file_path)
            .unwrap()
            .write_all(&test_data)
            .unwrap();
    }

    for (i, (key, file_path, _)) in test_cases.iter().enumerate() {
        let storage_clone = Arc::clone(&storage);
        let file_path = file_path.clone();
        let key = *key;

        tasks.push(task::spawn(async move {
            let file = File::open(&file_path).unwrap();
            let reader = BufReader::new(file);
            let mut slow_reader = SlowReader::new(reader, Duration::from_millis(100));

            // Call write_stream only once with the full slow reader
            let bytes_written = storage_clone
                .write_stream(key, &mut slow_reader)
                .expect("Failed to write stream!");

            eprintln!(
                "[Task {}] Finished writing stream {:?} ({} bytes written)",
                i, key, bytes_written
            );
        }));
    }

    // Wait for all tasks to finish
    for task in tasks {
        task.await.unwrap();
    }

    // Validate all writes
    for (key, _, expected_byte) in test_cases {
        let expected_data = vec![expected_byte; payload_size];
        let retrieved = storage.read(key).unwrap();

        let all_values_match = retrieved.as_slice() == expected_data.as_slice();
        let length_match = retrieved.len() == expected_data.len();

        // Note: assert_eq! can work but if it fails it console spams making it
        // really difficult to figure out the error, hence the `all_values_match`
        assert!(
            all_values_match,
            "Stream {:?} data mismatch: contents do not match",
            key
        );

        assert!(
            length_match,
            "Stream {:?} length mismatch: expected {} but got {}",
            key,
            expected_data.len(),
            retrieved.len()
        );
    }

    eprintln!("[Main] All streams written successfully and validated.");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn concurrent_write_test() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(DataStore::open(&path).unwrap());

    let num_writes = 10;
    let thread_count = 16;
    let mut tasks = Vec::new();

    for thread_id in 0..thread_count {
        let storage_clone = Arc::clone(&storage);
        tasks.push(tokio::spawn(async move {
            for i in 0..num_writes {
                let key = format!("thread{}_key{}", thread_id, i).into_bytes();
                let value = format!("thread{}_value{}", thread_id, i).into_bytes();

                // Directly call the method without attempting to mutate the Arc
                storage_clone.write(&key, &value).unwrap();

                eprintln!("[Thread {}] Wrote: {:?} -> {:?}", thread_id, key, value);
                tokio::time::sleep(Duration::from_millis(5)).await; // Simulate delays
            }
        }));
    }

    // Wait for all threads to finish
    for task in tasks {
        task.await.unwrap();
    }

    // Final Check: Ensure all written keys exist
    for thread_id in 0..thread_count {
        for i in 0..num_writes {
            let key = format!("thread{}_key{}", thread_id, i).into_bytes();
            let value = format!("thread{}_value{}", thread_id, i).into_bytes();

            let stored_value = storage.read(&key);
            eprintln!(
                "[Main] Verifying {} -> {:?} (Found: {:?})",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value),
                stored_value.as_deref().map(String::from_utf8_lossy)
            );

            assert_eq!(stored_value.as_deref(), Some(value.as_ref()));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn interleaved_read_write_test() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(DataStore::open(&path).unwrap());
    let notify_a = Arc::new(Notify::new());
    let notify_b = Arc::new(Notify::new());

    // Spawn Thread A (Writer → Reader)
    let storage_clone_a = Arc::clone(&storage);
    let notify_a_clone = Arc::clone(&notify_a);
    let notify_b_clone = Arc::clone(&notify_b);
    let thread_a = task::spawn(async move {
        let key = b"shared_key";

        // Step 1: Write initial data
        let value_a1 = b"value_from_A1";
        storage_clone_a.write(key, value_a1).unwrap();
        eprintln!("[Thread A] Wrote: {:?}", value_a1);

        // Step 2: Notify Thread B that it can read now
        notify_a_clone.notify_waiters();

        // Step 5: Wait for Thread B to write before reading the updated value
        notify_b_clone.notified().await;

        let result = storage_clone_a.read(key);
        eprintln!("[Thread A] Read updated value: {:?}", result.as_slice());
        assert_eq!(result.as_deref(), Some(b"value_from_B".as_ref()));
    });

    // Spawn Thread B (Reader → Writer)
    let storage_clone_b = Arc::clone(&storage);
    let notify_a_clone = Arc::clone(&notify_a);
    let notify_b_clone = Arc::clone(&notify_b);
    let thread_b = task::spawn(async move {
        let key = b"shared_key";

        // Step 3: Wait for Thread A to write before reading
        notify_a_clone.notified().await;

        let result = storage_clone_b.read(key);
        eprintln!("[Thread B] Read initial value: {:?}", result);
        assert_eq!(result.as_deref(), Some(b"value_from_A1".as_ref()));

        // Step 4: Write new data
        let value_b = b"value_from_B";
        storage_clone_b.write(key, value_b).unwrap();
        eprintln!("[Thread B] Wrote: {:?}", value_b);

        // Step 6: Notify Thread A that it can now read the updated value
        notify_b_clone.notify_waiters();
    });

    // Ensure both threads run concurrently
    let (res_a, res_b) = tokio::join!(thread_a, thread_b);

    res_a.unwrap();
    res_b.unwrap();

    // Final Check: Ensure storage contains the latest value
    let final_value = storage.read(b"shared_key");
    eprintln!("[Main] FINAL VALUE: {:?}", final_value);
    assert_eq!(final_value.as_deref(), Some(b"value_from_B".as_ref()));
}
