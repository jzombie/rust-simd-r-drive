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

    // ✅ Spawn Writer Task
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

            eprintln!("HELLO123");
        }
        notify_clone.notify_waiters(); // ✅ Signal readers that writes are done
    });

    eprintln!("HELLO");

    // ✅ Spawn Multiple Reader Tasks
    let mut readers = Vec::new();
    for _ in 0..4 {
        let storage_clone = storage.clone();
        let notify_clone = notify.clone();
        readers.push(task::spawn(async move {
            notify_clone.notified().await; // ✅ Wait until writer finishes

            // ✅ FIX: Hold the read lock in a variable to extend its lifetime
            let read_guard = storage_clone.read().await;
            let result = read_guard.get_entry_by_key(b"key5");

            assert_eq!(result, Some(b"value5".as_ref())); // Ensure correctness
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

    let storage_clone_a = storage.clone();
    let thread_a = task::spawn(async move {
        let key = b"shared_key";

        // ✅ Step 1: Write initial data
        let value_a1 = b"value_from_A1";
        {
            let mut storage = storage_clone_a.lock().await; // ✅ Hold lock in a variable
            storage.append_entry(key, value_a1).unwrap();
        }
        eprintln!("[Thread A] Wrote: {:?}", value_a1);

        // ✅ Step 2: Simulate delay before next operation
        sleep(Duration::from_millis(100)).await;

        // ✅ Step 5: Read the new value from Thread B
        {
            let storage = storage_clone_a.lock().await; // ✅ Hold lock in a variable
            let result = storage.get_entry_by_key(key);
            eprintln!("[Thread A] Read updated value: {:?}", result);
            assert_eq!(result, Some(b"value_from_B".as_ref()));
        }
    });

    let storage_clone_b = storage.clone();
    let thread_b = task::spawn(async move {
        let key = b"shared_key";

        // ✅ Step 3: Read the first value
        sleep(Duration::from_millis(50)).await; // Ensure Thread A writes first
        {
            let storage = storage_clone_b.lock().await; // ✅ Hold lock in a variable
            let result = storage.get_entry_by_key(key);
            eprintln!("[Thread B] Read initial value: {:?}", result);
            assert_eq!(result, Some(b"value_from_A1".as_ref()));
        }

        // ✅ Step 4: Write new data
        let value_b = b"value_from_B";
        {
            let mut storage = storage_clone_b.lock().await; // ✅ Hold lock in a variable
            storage.append_entry(key, value_b).unwrap();
        }
        eprintln!("[Thread B] Wrote: {:?}", value_b);
    });

    // ✅ Ensure both threads run concurrently
    let (res_a, res_b) = tokio::join!(thread_a, thread_b);

    res_a.unwrap();
    res_b.unwrap();

    // ✅ Final Check: Ensure storage contains the latest value
    {
        let storage = storage.lock().await; // ✅ Hold lock in a variable
        let final_value = storage.get_entry_by_key(b"shared_key");
        eprintln!("[Main] FINAL VALUE: {:?}", final_value);
        assert_eq!(final_value, Some(b"value_from_B".as_ref()));
    }
}
