//! Verify fixed payload alignment across writes, deletes, overwrites.
//! Start with unaligned strings, then overwrite with aligned payloads.
//! Prove zero-copy typed views via bytemuck and do SIMD loads on
//! x86_64/aarch64.

use std::mem::{align_of, size_of};

use tempfile::tempdir;

use simd_r_drive::{
    DataStore,
    storage_engine::constants::PAYLOAD_ALIGNMENT,
    traits::{DataStoreReader, DataStoreWriter},
};

use bytemuck::try_cast_slice;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{__m128i, _mm_load_si128};

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::{uint8x16_t, vld1q_u8};

fn assert_payload_addr_aligned(bytes: &[u8]) {
    let ptr = bytes.as_ptr() as usize;
    let a = PAYLOAD_ALIGNMENT as usize;
    assert!(
        ptr.is_multiple_of(a),
        "payload start address is not {}-byte aligned",
        a
    );
}

/// Purely safe pointer math: prove a &[T] view would be legal.
fn assert_can_view_as<T: Copy>(bytes: &[u8]) {
    let a_t = align_of::<T>();
    assert!(
        a_t <= PAYLOAD_ALIGNMENT as usize,
        "type align {} exceeds PAYLOAD_ALIGNMENT {}",
        a_t,
        PAYLOAD_ALIGNMENT
    );
    let ptr = bytes.as_ptr() as usize;
    assert!(
        ptr.is_multiple_of(a_t),
        "payload addr {} is not aligned to T (align {})",
        ptr,
        a_t
    );
    assert!(
        bytes.len().is_multiple_of(size_of::<T>()),
        "payload length {} is not a multiple of {}",
        bytes.len(),
        size_of::<T>()
    );
}

/// bytemuck zero-copy proof: get a typed view or fail.
fn assert_bytemuck_view_u32(bytes: &[u8]) {
    let _: &[u32] = try_cast_slice(bytes).expect("cast &[u8]->&[u32] failed");
}
fn assert_bytemuck_view_u64(bytes: &[u8]) {
    let _: &[u64] = try_cast_slice(bytes).expect("cast &[u8]->&[u64] failed");
}
fn assert_bytemuck_view_u128(bytes: &[u8]) {
    let _: &[u128] = try_cast_slice(bytes).expect("cast &[u8]->&[u128] failed");
}

#[cfg(target_arch = "x86_64")]
fn assert_simd_16_byte_loadable(bytes: &[u8]) {
    assert!(
        (bytes.as_ptr() as usize) % 16 == 0,
        "SIMD pointer must be 16-byte aligned"
    );
    let lanes = bytes.len() / 16;
    unsafe {
        for i in 0..lanes {
            let p = bytes.as_ptr().add(i * 16) as *const __m128i;
            let v = _mm_load_si128(p);
            core::hint::black_box(v);
        }
    }
}

#[cfg(target_arch = "aarch64")]
fn assert_simd_16_byte_loadable(bytes: &[u8]) {
    assert!(
        (bytes.as_ptr() as usize).is_multiple_of(16),
        "SIMD pointer must be 16-byte aligned"
    );
    let lanes = bytes.len() / 16;
    unsafe {
        for i in 0..lanes {
            let p = bytes.as_ptr().add(i * 16);
            let v0 = vld1q_u8(p);
            core::hint::black_box(v0);
            let p_vec = p as *const uint8x16_t;
            let v1: uint8x16_t = core::ptr::read(p_vec);
            core::hint::black_box(v1);
        }
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn assert_simd_16_byte_loadable(bytes: &[u8]) {
    // Portable fallback: re-assert address and u128 view conditions.
    assert_payload_addr_aligned(bytes);
    if bytes.len() >= 16 && bytes.len() % 16 == 0 {
        assert_can_view_as::<u128>(bytes);
        assert_bytemuck_view_u128(bytes);
    }
}

#[test]
fn byte_alignment_unaligned_then_overwrite_and_simd() {
    let dir = tempdir().expect("create tempdir");
    let path = dir.path().join("store.bin");
    let store = DataStore::open(&path).expect("open datastore");

    // Phase 1: write unaligned strings first (3,5,7,9 bytes).
    let s1 = b"abc"; // 3
    let s2 = b"abcde"; // 5
    let s3 = b"abcdefg"; // 7
    let s4 = b"abcdefghi"; // 9

    store.write(b"k_s1", s1).unwrap();
    store.write(b"k_s2", s2).unwrap();
    store.write(b"k_s3", s3).unwrap();
    store.write(b"k_s4", s4).unwrap();

    // Mix in numeric payloads to stress alignment interactions.
    let v_u32 = vec![0xEEu8; 5 * 4]; // 20 bytes
    let v_u64 = vec![0x33u8; 9 * 8]; // 72 bytes
    let v_u128 = vec![0x77u8; 4 * 16]; // 64 bytes
    store.write(b"k_u32", &v_u32).unwrap();
    store.write(b"k_u64", &v_u64).unwrap();
    store.write(b"k_u128", &v_u128).unwrap();

    // Assert alignment after initial writes.
    let e_s1 = store.read(b"k_s1").unwrap().expect("k_s1 missing");
    let e_s2 = store.read(b"k_s2").unwrap().expect("k_s2 missing");
    let e_s3 = store.read(b"k_s3").unwrap().expect("k_s3 missing");
    let e_s4 = store.read(b"k_s4").unwrap().expect("k_s4 missing");
    let e_u32 = store.read(b"k_u32").unwrap().expect("k_u32 missing");
    let e_u64 = store.read(b"k_u64").unwrap().expect("k_u64 missing");
    let e_u128 = store.read(b"k_u128").unwrap().expect("k_u128 missing");

    for bytes in [
        e_s1.as_slice(),
        e_s2.as_slice(),
        e_s3.as_slice(),
        e_s4.as_slice(),
        e_u32.as_slice(),
        e_u64.as_slice(),
        e_u128.as_slice(),
    ] {
        assert_payload_addr_aligned(bytes);
    }

    // Phase 2: delete one string (tombstone, no pre-pad).
    store.delete(b"k_s2").unwrap();

    // Phase 3: overwrite with 16B-multiple payloads.
    let s1_aligned = vec![0xA5u8; 2 * 16]; // 32 bytes
    let s3_aligned = vec![0xB6u8; 3 * 16]; // 48 bytes
    let u32_aligned = vec![0xCCu8; 16 * 4]; // 64 bytes

    store.write(b"k_s1", &s1_aligned).unwrap();
    store.write(b"k_s3", &s3_aligned).unwrap();
    store.write(b"k_u32", &u32_aligned).unwrap();

    // Fetch survivors and assert alignment again.
    let e_s1_new = store.read(b"k_s1").unwrap().expect("k_s1 missing");
    let e_s3_new = store.read(b"k_s3").unwrap().expect("k_s3 missing");
    let e_s4_new = store.read(b"k_s4").unwrap().expect("k_s4 missing");
    let e_u32_new = store.read(b"k_u32").unwrap().expect("k_u32 missing");
    let e_u64_new = store.read(b"k_u64").unwrap().expect("k_u64 missing");
    let e_u128_new = store.read(b"k_u128").unwrap().expect("k_u128 missing");
    let e_s2_gone = store.read(b"k_s2").unwrap();
    assert!(e_s2_gone.is_none(), "deleted key k_s2 should be absent");

    for bytes in [
        e_s1_new.as_slice(),
        e_s3_new.as_slice(),
        e_s4_new.as_slice(),
        e_u32_new.as_slice(),
        e_u64_new.as_slice(),
        e_u128_new.as_slice(),
    ] {
        assert_payload_addr_aligned(bytes);
    }

    // Prove typed views would be zero-copy by math and bytemuck.
    assert_can_view_as::<u32>(e_u32_new.as_slice());
    assert_can_view_as::<u64>(e_u64_new.as_slice());
    assert_can_view_as::<u128>(e_u128_new.as_slice());
    assert_bytemuck_view_u32(e_u32_new.as_slice());
    assert_bytemuck_view_u64(e_u64_new.as_slice());
    assert_bytemuck_view_u128(e_u128_new.as_slice());

    // SIMD loads or portable fallback.
    for bytes in [
        e_s1_new.as_slice(),
        e_s3_new.as_slice(),
        e_u32_new.as_slice(),
        e_u64_new.as_slice(),
        e_u128_new.as_slice(),
    ] {
        if bytes.len() >= 16 {
            assert_simd_16_byte_loadable(bytes);
        }
    }

    // Iterator must yield aligned, non-tombstone payloads.
    for entry in store.iter_entries() {
        let bytes = entry.as_slice();
        assert_payload_addr_aligned(bytes);
        if bytes.len() >= 16 {
            assert_simd_16_byte_loadable(bytes);
        }
    }

    // tempdir cleans up automatically.
}
