//! tests/buffered_write.rs
//! -----------------------
//! Unit-tests for DataStore::buf_write / buf_write_flush and WriteBuffer.

use simd_r_drive::{
    DataStore,
    traits::{DataStoreBufWriter, DataStoreReader},
};
use tempfile::tempdir;

/// Helper function to create a temporary file for testing
fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = DataStore::open(&path).expect("Failed to open storage");
    (dir, storage)
}

/// A buffered write must not be visible until we flush.
#[test]
fn buf_write_requires_explicit_flush() {
    let (_dir, store) = create_temp_storage(); // keep _dir alive!

    let key = b"key-a";
    let payload = b"value-a";

    let auto = store.buf_write(key, payload).expect("buf_write");
    assert!(!auto, "single tiny record should not auto-flush");

    assert!(
        store.read(key).is_none(),
        "value leaked before explicit flush"
    );

    store.buf_write_flush().expect("flush");

    let got = store.read(key).expect("post-flush read");
    assert_eq!(got.as_slice(), payload);
}

/// Auto-flush kicks in when the soft limit is exceeded.
#[test]
fn buf_write_auto_flush() {
    let (_dir, store) = create_temp_storage();

    let payload = [0u8; 1024]; // 1 KiB
    let target = 4 * 1024 * 1024; // 4 MiB soft limit

    let mut written = 0usize;
    for i in 0.. {
        let key = format!("key-{i}").into_bytes();
        written += payload.len();
        let did_flush = store.buf_write(&key, &payload).expect("buf_write");

        if did_flush {
            // Everything up to (and including) this key must now be visible.
            for j in 0..=i {
                let k = format!("key-{j}");
                let val = store.read(k.as_bytes()).expect("visible after auto-flush");
                assert_eq!(val.as_slice(), &payload);
            }
            assert!(
                written >= target,
                "auto-flush happened before soft limit was crossed"
            );
            return; // success
        }
    }
}

/// Flushing an empty buffer must be a no-op.
#[test]
fn buf_write_flush_noop() {
    let (_dir, store) = create_temp_storage();

    let before = store.get_storage_size().expect("size before");

    store.buf_write_flush().expect("flush #1");
    let after1 = store.get_storage_size().expect("size after #1");

    store.buf_write_flush().expect("flush #2");
    let after2 = store.get_storage_size().expect("size after #2");

    assert_eq!(before, after1, "size changed after first no-op flush");
    assert_eq!(before, after2, "size changed after second no-op flush");
}
