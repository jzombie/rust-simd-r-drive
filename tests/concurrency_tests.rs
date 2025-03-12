use simd_r_drive::AppendStorage;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tempfile::tempdir;
use tokio::sync::{Barrier, Mutex, Notify, RwLock};
use tokio::task;
use tokio::time::{sleep, Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_write_test() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(RwLock::new(AppendStorage::open(&path).unwrap()));

    let notify = Arc::new(Notify::new());

    // Spawn Writer Task
    let storage_clone = storage.clone();
    let notify_clone = notify.clone();
    let writer = task::spawn(async move {
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            storage_clone
                .write()
                .await
                .append_entry(&key, &value)
                .unwrap();
            sleep(Duration::from_millis(10)).await; // Simulate delays
        }
        notify_clone.notify_waiters(); // Signal readers that writes are done
    });

    // Spawn Multiple Reader Tasks
    let mut readers = Vec::new();
    for _ in 0..4 {
        eprintln!("I am a reader");

        let storage_clone = storage.clone();
        let notify_clone = notify.clone();
        readers.push(task::spawn(async move {
            notify_clone.notified().await; // Wait until writer finishes

            // FIX: Hold the read lock in a variable to extend its lifetime
            let read_guard = storage_clone.read().await;
            let result = read_guard.get_entry_by_key(b"key5");

            assert_eq!(result.as_deref(), Some(b"value5".as_ref())); // Ensure correctness
        }));
    }

    writer.await.unwrap();
    for reader in readers {
        reader.await.unwrap();
    }

    let read_guard = storage.read().await;
    assert!(read_guard.get_entry_by_key(b"key9").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_read_write_test() {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = Arc::new(Mutex::new(AppendStorage::open(&path).unwrap()));
    let notify_a = Arc::new(Notify::new());
    let notify_b = Arc::new(Notify::new());

    // Spawn Thread A (Writer → Reader)
    let storage_clone_a = storage.clone();
    let notify_a_clone = notify_a.clone();
    let notify_b_clone = notify_b.clone();
    let thread_a = task::spawn(async move {
        let key = b"shared_key";

        // Step 1: Write initial data
        let value_a1 = b"value_from_A1";
        {
            let mut storage = storage_clone_a.lock().await;
            storage.append_entry(key, value_a1).unwrap();
        }
        eprintln!("[Thread A] Wrote: {:?}", value_a1);

        // Step 2: Notify Thread B that it can read now
        notify_a_clone.notify_waiters();

        // Step 5: Wait for Thread B to write before reading the updated value
        notify_b_clone.notified().await;

        {
            let storage = storage_clone_a.lock().await;
            let result = storage.get_entry_by_key(key);
            eprintln!("[Thread A] Read updated value: {:?}", result.as_slice());
            assert_eq!(result.as_deref(), Some(b"value_from_B".as_ref()));
        }
    });

    // Spawn Thread B (Reader → Writer)
    let storage_clone_b = storage.clone();
    let notify_a_clone = notify_a.clone();
    let notify_b_clone = notify_b.clone();
    let thread_b = task::spawn(async move {
        let key = b"shared_key";

        // Step 3: Wait for Thread A to write before reading
        notify_a_clone.notified().await;

        {
            let storage = storage_clone_b.lock().await;
            let result = storage.get_entry_by_key(key);
            eprintln!("[Thread B] Read initial value: {:?}", result);
            assert_eq!(result.as_deref(), Some(b"value_from_A1".as_ref()));
        }

        // Step 4: Write new data
        let value_b = b"value_from_B";
        {
            let mut storage = storage_clone_b.lock().await;
            storage.append_entry(key, value_b).unwrap();
        }
        eprintln!("[Thread B] Wrote: {:?}", value_b);

        // Step 6: Notify Thread A that it can now read the updated value
        notify_b_clone.notify_waiters();
    });

    // Ensure both threads run concurrently
    let (res_a, res_b) = tokio::join!(thread_a, thread_b);

    res_a.unwrap();
    res_b.unwrap();

    // Final Check: Ensure storage contains the latest value
    {
        let storage = storage.lock().await;
        let final_value = storage.get_entry_by_key(b"shared_key");
        eprintln!("[Main] FINAL VALUE: {:?}", final_value);
        assert_eq!(final_value.as_deref(), Some(b"value_from_B".as_ref()));
    }
}
