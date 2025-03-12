use simd_r_drive::AppendStorage;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock}; // ✅ Use RwLock
use tokio::task;
use tokio::time::{sleep, Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_read_write_test() {
    let storage = Arc::new(RwLock::new(
        AppendStorage::open(&PathBuf::from("test.db")).unwrap(),
    ));

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
        }
        notify_clone.notify_waiters(); // ✅ Signal readers that writes are done
    });

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
