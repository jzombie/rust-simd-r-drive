# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/) and this project adheres to
(or is loosely based on) Semantic Versioning.

## [0.17.0-alpha] - 2026-08-21

### Changed
- Bumped `arrow` from `59.0.0` to `59.1.0`.
- Bumped `bytemuck` from `1.25.0` to `1.25.1`.
- Bumped `xxhash-rust` from `0.8.15` to `0.8.16`.
- Bumped `muxio`-family crates from `0.10.2-alpha` to `0.15.0-alpha` (**breaking wire format change**, see Migration below; the base `muxio` crate is superseded by `muxio-core`; pulls in `tungstenite`/`tokio-tungstenite` `0.30` alongside the existing `0.29`).
- Refreshed `Cargo.lock` to reflect updated transitive dependencies (e.g., `rand` `0.10.2`, `typenum` `1.20.1`).
- Added `.DS_Store` and `.codegraph` to `.gitignore`.
- Replaced `chunks_exact(N)` with `as_chunks::<N>()` in `align_or_copy` to satisfy current clippy lints (no functional change).

### Fixed
- Clarified the crate-level "Safety Notes" (#99): appending via the engine API is always safe (the store re-maps the file internally after each write); the "do not resize while in use" warning refers to resizing the storage file out-of-band while the store is open.

### Migration

**Note**: The following migration is only necessary if using the wire protocol handled by [Muxio](https://crates.io/crates/muxio).

- `muxio >= 0.15.0-alpha` is **not wire-compatible** with peers built against `<= 0.14.0-alpha`: `muxio-core` removed the `u64 timestamp_micros` field from frame headers (`FRAME_HEADER_SIZE` reduced from `21` to `13` bytes), so old peers sending 21-byte headers are rejected as `CorruptFrame`. Note that request/stream IDs additionally gained a high-bit direction marker (client `0x0000_0000`, server `0x8000_0000`) in `0.14.0-alpha`. Upgrade all muxio-based clients and servers together; mixed-version deployments will fail to communicate.
- Stores written by `simd-r-drive` itself are unaffected — the change only impacts live RPC transports (WS/IPC), not the on-disk format.

## [0.16.3-alpha] - 2026-07-11

### Added
- Hash stability tests (`tests/hash_stability_tests.rs`) that enforce hardcoded xxh3 hash values to guard against silent regressions if the `xxhash-rust` dependency is updated or replaced.

### Changed
- Bumped `arrow` from `58.3.0` to `59.0.0`.
- Bumped `dashmap` from `6.1.0` to `6.2.1`.
- Bumped `serde_json` from `1.0.149` to `1.0.150`.
- Bumped `serial_test` from `3.4.0` to `3.5.0`.
- Bumped `crossbeam-epoch` from `0.9.18` to `0.9.20` (fixes RUSTSEC-2026-0204).

### Deprecated
### Removed
### Fixed
### Security
### Migration

## [0.16.2-alpha] - 2026-05-11

### Added
### Changed
- Bumped `arrow` from `58.1.0` to `58.3.0`.
- Bumped `clap` from `4.6.0` to `4.6.1`.
- Bumped `muxio`-family crates from `0.10.1-alpha` to `0.10.2-alpha`.
- Bumped `rayon` from `1.11.0` to `1.12.0`.
- Bumped `tokio` from `1.51.1` to `1.52.3`.
- Refreshed `Cargo.lock` to reflect updated transitive dependencies (`hashbrown` `0.17.1`, consolidated `tokio-tungstenite`/`tungstenite` versions).
### Deprecated
### Removed
### Fixed
### Security
### Migration

## [0.16.1-alpha] - 2026-04-21

### Added
### Changed
- Bumped workspace dependency versions: `rand` from `0.9.2` to `0.10.1`, and `tokio` from `1.51.0` to `1.51.1`.
### Deprecated
### Removed
### Fixed
- Updated benchmark imports to use `rand::RngExt` so benchmark targets compile cleanly with `rand` `0.10.x`.
### Security
### Migration
- If there are breaking changes, put a short, actionable checklist here.


## [0.16.0-alpha] - 2026-03-23
### Changed
- Replaced `bincode` with `bitcode` for serialization in extension utilities and related tests.
- Updated dependency versions across workspace crates and refreshed lockfiles to current compatible releases.

---


## [0.15.5-alpha] - 2025-10-27
### Changed
- Bumped Apache Arrow dependency to 57.0.0. (No other functional changes.)

---

## [0.15.0-alpha] - 2025-09-25
### Breaking
- Default payload alignment increased from 16 bytes to 64 bytes to ensure
  SIMD- and cacheline-safe zero-copy access across SSE/AVX/AVX-512 code
  paths. Readers/writers compiled with `<= 0.14.x-alpha` that assume
  16-byte alignment will not be able to parse 0.15.x stores correctly.

### Added
- Debug/test-only assertions (`assert_aligned`, `assert_aligned_offset`)
  to validate both pointer- and offset-level alignment invariants.

### Changed
- Updated documentation and examples to reflect the new 64-byte default
  `PAYLOAD_ALIGNMENT` (still configurable in
  `src/storage_engine/constants.rs`).
- `EntryHandle::as_arrow_buffer` and `into_arrow_buffer` now check both
  pointer and offset alignment when compiled in test or debug mode.

### Migration
- Stores created with 0.15.x are not backward-compatible with
  0.14.x readers/writers due to the alignment change.
- To migrate:
  1. Read entries with your existing 0.14.x binary.
  2. Rewrite into a fresh 0.15.x store (which will apply 64-byte
     alignment).
  3. Deploy upgraded readers before upgrading writers in multi-service
     environments.

---

## [0.14.0-alpha] - 2025-09-08
### Breaking
- Files written by 0.14.0-alpha use padded payload starts for fixed alignment.
  Older readers (<= 0.13.x-alpha) may misinterpret pre-pad bytes as part of the
  payload. Upgrade all readers/writers before mixing file versions.

### Added
- Fixed payload alignment for zero-copy typed views. Payloads now begin
  at an address that is a multiple of `PAYLOAD_ALIGNMENT`, configured in
  `src/storage_engine/constants.rs` via:
  - `PAYLOAD_ALIGN_LOG2`
  - `PAYLOAD_ALIGNMENT = 1 << PAYLOAD_ALIGN_LOG2`
- Experimental `arrow` feature which exposes `as_arrow_buffer` and `into_arrow_buffer`
  methods in `EntryHandle`.

### Changed
- Internal on-disk layout: each non-tombstone payload may be preceded by
  a small zero pre-pad (0..A-1 bytes) to satisfy alignment (A is the
  configured alignment). Public API is unchanged.

### Migration
- Regenerate stores with the new version:
  1) Open the old store with the matching old binary and read entries.
  2) Write each entry into a new 0.14.0-alpha store.
  3) Replace the old file after verification.
- If you maintain separate services, deploy reader upgrades before
  writer upgrades to avoid mixed-version reads.
