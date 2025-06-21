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

/// `buf_write()` must eventually return `true` once the buffer crosses its
/// soft-limit, but **no entry is persisted until we call `buf_write_flush()`**.
#[test]
fn buf_write_needs_flush_flag() {
    let (_dir, store) = create_temp_storage(); // keep temp-dir alive

    const PAYLOAD: [u8; 1_024] = [0; 1_024]; // 1 KiB per record
    let mut keys_written = Vec::new();
    let mut needs_flush = false;

    // ── 1. Stream records until the flag flips to `true` ───────────────────
    for i in 0usize..1_000_000 {
        // hard upper-bound
        let key = format!("key-{i}").into_bytes();
        let flag = store.buf_write(&key, &PAYLOAD).expect("buf_write");
        keys_written.push(key);

        if flag {
            // first time the soft-limit is exceeded
            needs_flush = true;
            break;
        }
    }
    assert!(needs_flush, "soft-limit never reached during test");

    // ── 2. *Before* flushing nothing must be visible ───────────────────────
    for key in &keys_written {
        assert!(
            store.read(key).is_none(),
            "data became visible before explicit flush"
        );
    }

    // ── 3. Flush and verify persistence of every buffered record ────────────
    store.buf_write_flush().expect("explicit flush");

    for key in &keys_written {
        let val = store.read(key).expect("value after flush");
        assert_eq!(val.as_slice(), &PAYLOAD);
    }

    // ── 4. A second flush with an empty buffer is a no-op ───────────────────
    let size_before = store.get_storage_size().unwrap();
    store.buf_write_flush().expect("no-op flush");
    let size_after = store.get_storage_size().unwrap();
    assert_eq!(size_before, size_after, "file size changed on no-op flush");
}

#[test]
fn buf_write_double_flush_idempotent() {
    let (_dir, store) = create_temp_storage();

    // 1 KiB payload -- small enough to stay well under the soft-limit.
    let key = b"key-idem";
    let payload = [7u8; 1024];

    // ── 1. Stage one record and flush it ───────────────────────────────────
    let auto = store.buf_write(key, &payload).expect("buf_write");
    assert!(!auto, "single record must not auto-flush");

    store.buf_write_flush().expect("first flush");

    // File length *after* first flush
    let size_after_first = store.get_storage_size().expect("size after #1");

    // The record must be readable and correct
    let val = store.read(key).expect("value after #1");
    assert_eq!(val.as_slice(), &payload);

    // ── 2. Immediately flush again (buffer is empty) ───────────────────────
    store.buf_write_flush().expect("second flush");

    // Size and data must be unchanged
    let size_after_second = store.get_storage_size().expect("size after #2");
    assert_eq!(
        size_after_first, size_after_second,
        "file size changed on redundant flush"
    );

    let val2 = store.read(key).expect("value after #2");
    assert_eq!(
        val2.as_slice(),
        &payload,
        "payload mutated on redundant flush"
    );
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
