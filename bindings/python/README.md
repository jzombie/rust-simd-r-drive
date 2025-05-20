# SIMD R Drive (Python Bindings)

**Experimental Python bindings for [SIMD R Drive](https://crates.io/crates/simd-r-drive)** — a high-performance, schema-less storage engine using a single-file storage container optimized for zero-copy binary access, written in [Rust](https://www.rust-lang.org/).

This library provides access to core functionality of `simd-r-drive` from Python, including high-performance key/value storage, zero-copy reads via `memoryview`, and support for streaming writes and reads.

> ⚠ **Threaded streaming writes from Python are not supported.** See [Thread Safety](#thread-safety) for important limitations.

## Features

* 🔑 Append-only key/value storage
* ⚡ Zero-copy reads via `memoryview` and `mmap`
* 📆 Single-file binary container (no schema or serialization required)
* ↺ Streaming interface for writing and reading large entries
* 🐍 Native Rust extension module for Python (via [PyO3](https://github.com/PyO3/pyo3))

## Supported Environments

The `simd_r_drive_py` Python bindings are built as native extension modules and require environments that support both Python and Rust toolchains.

### ✅ Platforms

* **Linux (x86\_64, aarch64)**
* **macOS (x86\_64, arm64/M1/M2)**

> Wheels are built using [cibuildwheel](https://github.com/pypa/cibuildwheel) and tested on GitHub Actions.

### ✅ Supported Python Versions

* **Python 3.10 – 3.13**

Older versions (≤3.9) are explicitly skipped during wheel builds.

### ❌ Not Supported

* **Windows (x86\_64, AMD64, ARM64)** Python bindings are not officially supported on Windows due to platform-specific filesystem and memory-mapping inconsistencies in the Python runtime.
    > The underlying Rust library works on Windows and is tested continuously, but the Python bindings fail some unit tests in CI. Manual builds (including `AMD64` and `ARM64`) have succeeded locally but are not considered production-stable.
* **Python < 3.10**
* **32-bit Python**
* **musl-based Linux environments** (e.g., Alpine Linux)
* **PyPy** or other alternative Python interpreters

> If you need support for other environments or interpreters, consider compiling from source with `maturin develop` inside a compatible environment.

## Installation

```sh
pip install -i simd-r-drive-py
```

## Usage

### Regular Writes and Reads

```python
from simd_r_drive import DataStore

# Create or open a datastore
store = DataStore("mydata.bin")

# Write a key/value pair
store.write(b"username", b"jdoe")

# Read the value
value = store.read(b"username")
print(value)  # b'jdoe'

# Check existence
assert store.exists(b"username")

# Delete the key
store.delete(b"username")
assert store.read(b"username") is None
```

### Batch Writes

```python
from simd_r_drive import DataStore

store = DataStore("batch.bin")

# Prepare entries as a list of (key, value) byte tuples
entries = [
    (b"user:1", b"alice"),
    (b"user:2", b"bob"),
    (b"user:3", b"charlie"),
]

# Write all entries in a single batch
store.batch_write(entries)

# Verify that all entries were written correctly
for key, value in entries:
    assert store.read(key) == value
```

### Streamed Writes and Reads (Large Payloads)

```python
from simd_r_drive import DataStore
import io

store = DataStore("streamed.bin")

# Simulated payload — in practice, this could be any file-like stream,
# including one that does not fit entirely into memory.
payload = b"x" * (10 * 1024 * 1024)  # Example: 10 MB of dummy data
stream = io.BytesIO(payload)

store.write_stream(b"large-file", stream)

# Read the payload back in chunks
read_stream = store.read_stream(b"large-file")
result = bytearray()

while chunk := read_stream.read(4096):
    result.extend(chunk)

assert result == payload
```

## API

### `DataStore(path: str)`

Opens (or creates) a file-backed storage container at the given path.

### `.write(key: bytes, value: bytes) -> None`

Atomically appends a new key-value entry. Overwrites any previous version of the key.

### `.batch_write(items: List[Tuple[bytes, bytes]]) -> None`

Writes multiple key-value pairs in a single operation. Each item must be a tuple of (key, value) where both are bytes.

### `.write_stream(key: bytes, reader: IO[bytes]) -> None`

Streams from a Python file-like object (`.read(n)` interface). Not thread-safe.

### `.read(key: bytes) -> Optional[bytes]`

Returns the full value for a key, or `None` if the key does not exist.

### `.read_entry(key: bytes) -> Optional[EntryHandle]`

Returns a memory-mapped handle, exposing `.as_memoryview()` for zero-copy access.

### `.read_stream(key: bytes) -> Optional[EntryStream]`

Returns a streaming reader exposing `.read(n)`.

### `.delete(key: bytes) -> None`

Marks an entry as deleted. The file remains append-only; use Rust-side compaction if needed.

### `.exists(key: bytes) -> bool`

Returns whether a key is currently valid in the index.

## Thread Safety

This Python binding **is not thread-safe**.

Due to Python’s Global Interpreter Lock (GIL) and the limitations of `PyO3`, concurrent streaming writes or reads from multiple threads are **not supported**, and doing so may cause hangs or inconsistent behavior.

* ⚠ **Use only from a single thread.**
* ❌ Do not call methods like `write_stream` or `read_stream` from multiple threads.
* ❌ Do not share a `DataStore` instance across threads.
* ✅ For concurrent, high-performance use — especially with streaming — use the native Rust version directly.

> This design avoids working around the GIL or spawning internal locks for artificial concurrency. If you need reliable multithreading, call into the Rust API instead.

## Limitations

* Python bindings currently **lack async support**.
* `write_stream` is blocking and not safe for concurrent use.
* Compaction is not yet exposed via Python.
* This is **not a drop-in database** — you're expected to manage your own data formats.

## Development

To develop and test the Python bindings:

### Requirements

- Python 3.10 or above
- Rust toolchain (with `cargo`)

```sh
pip install -r requirements.txt -r requirements-dev.txt
```

### Test Changes

```sh
maturin develop # Builds the Rust library
pytest # Tests the Python integration
```

### Build a Release Wheel

```bash
maturin build --release
pip install dist/simd_r_drive_py-*.whl
```

## License

Licensed under the [Apache-2.0 License](LICENSE).
