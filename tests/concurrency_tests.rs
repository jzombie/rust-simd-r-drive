use serial_test::serial;
use simd_r_drive::DataStore;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Notify;
use tokio::task;
use tokio::time::Duration;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_slow_streamed_write_test() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(DataStore::open(&path).unwrap());

    let key_a = b"stream_key_A";
    let key_b = b"stream_key_B";

    // 1. Prepare two different large test files for streaming
    let file_a_path = dir.path().join("test_stream_a.bin");
    let file_b_path = dir.path().join("test_stream_b.bin");

    // TODO: Use larger payload size after finishing debugging
    // let payload_size = 2 * 1024 * 1024; // 2MB per stream
    let payload_size = 4096 * 4;

    let test_data_a = vec![b'A'; payload_size];
    let test_data_b = vec![b'B'; payload_size];

    File::create(&file_a_path)
        .unwrap()
        .write_all(&test_data_a)
        .unwrap();
    File::create(&file_b_path)
        .unwrap()
        .write_all(&test_data_b)
        .unwrap();

    let notify = Arc::new(Notify::new());

    let storage_clone_a = Arc::clone(&storage);
    let notify_clone_a = Arc::clone(&notify);
    let task_a = task::spawn(async move {
        let file_a = File::open(&file_a_path).unwrap();
        let reader_a = BufReader::new(file_a);
        let mut slow_reader_a = SlowReader::new(reader_a, Duration::from_millis(10));

        let mut buffer = vec![0; 4096];
        let mut total_written = 0;

        while let Ok(bytes_read) = slow_reader_a.inner.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            // Introduce artificial delay between reads
            tokio::time::sleep(slow_reader_a.delay).await;

            storage_clone_a
                .write_stream(key_a, &mut &buffer[..bytes_read])
                .expect("Stream A failed to write!");

            total_written += bytes_read;
        }

        eprintln!(
            "[Task A] Finished writing stream A ({} bytes written)",
            total_written
        );
        notify_clone_a.notify_waiters();
    });

    let storage_clone_b = Arc::clone(&storage);
    let notify_clone_b = Arc::clone(&notify);
    let task_b = task::spawn(async move {
        // Introduce a slight delay before starting task B
        tokio::time::sleep(Duration::from_millis(50)).await;

        let file_b = File::open(&file_b_path).unwrap();
        let reader_b = BufReader::new(file_b);
        let mut slow_reader_b = SlowReader::new(reader_b, Duration::from_millis(10));

        let mut buffer = vec![0; 4096];
        let mut total_written = 0;

        while let Ok(bytes_read) = slow_reader_b.inner.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            // Introduce artificial delay between reads
            tokio::time::sleep(slow_reader_b.delay).await;

            storage_clone_b
                .write_stream(key_b, &mut &buffer[..bytes_read])
                .expect("Stream B failed to write!");

            total_written += bytes_read;
        }

        eprintln!(
            "[Task B] Finished writing stream B ({} bytes written)",
            total_written
        );
        notify_clone_b.notify_waiters();
    });

    // Ensure both tasks complete
    let (res_a, res_b) = tokio::join!(task_a, task_b);
    res_a.unwrap();
    res_b.unwrap();

    // 4. Validate that both keys were written correctly
    let retrieved_a = storage.read(key_a).unwrap();
    let retrieved_b = storage.read(key_b).unwrap();

    assert_eq!(
        retrieved_a.as_slice(),
        test_data_a.as_slice(),
        "Data mismatch for Stream A"
    );
    assert_eq!(
        retrieved_b.as_slice(),
        test_data_b.as_slice(),
        "Data mismatch for Stream B"
    );

    eprintln!("[Main] Both streams written successfully and validated.");
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
