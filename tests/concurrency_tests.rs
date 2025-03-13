use serial_test::serial;
use simd_r_drive::DataStore;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Notify;
use tokio::task;
use tokio::time::Duration;

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
                storage_clone.append_entry(&key, &value).unwrap();

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

            let stored_value = storage.get_entry_by_key(&key);
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
        storage_clone_a.append_entry(key, value_a1).unwrap();
        eprintln!("[Thread A] Wrote: {:?}", value_a1);

        // Step 2: Notify Thread B that it can read now
        notify_a_clone.notify_waiters();

        // Step 5: Wait for Thread B to write before reading the updated value
        notify_b_clone.notified().await;

        let result = storage_clone_a.get_entry_by_key(key);
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

        let result = storage_clone_b.get_entry_by_key(key);
        eprintln!("[Thread B] Read initial value: {:?}", result);
        assert_eq!(result.as_deref(), Some(b"value_from_A1".as_ref()));

        // Step 4: Write new data
        let value_b = b"value_from_B";
        storage_clone_b.append_entry(key, value_b).unwrap();
        eprintln!("[Thread B] Wrote: {:?}", value_b);

        // Step 6: Notify Thread A that it can now read the updated value
        notify_b_clone.notify_waiters();
    });

    // Ensure both threads run concurrently
    let (res_a, res_b) = tokio::join!(thread_a, thread_b);

    res_a.unwrap();
    res_b.unwrap();

    // Final Check: Ensure storage contains the latest value
    let final_value = storage.get_entry_by_key(b"shared_key");
    eprintln!("[Main] FINAL VALUE: {:?}", final_value);
    assert_eq!(final_value.as_deref(), Some(b"value_from_B".as_ref()));
}
