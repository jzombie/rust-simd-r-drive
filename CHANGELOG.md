# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project adheres to
(or is loosely based on) Semantic Versioning.

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
### Migration
- If there are breaking changes, put a short, actionable checklist here.

## [0.14.0-alpha] - 2024-09-08
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
