# SIMD R Drive (Python Bindings)

**Experimental Python bindings for [SIMD R Drive](https://crates.io/crates/simd-r-drive)** — an append-only, zero-copy storage engine built in Rust.

This library provides access to core functionality of `simd-r-drive` from Python, including high-performance key/value storage, zero-copy reads via `memoryview`, and support for streaming writes and reads.

> ⚠ **Threaded streaming writes from Python are not supported.** See [Thread Safety](#thread-safety) for important limitations.

## Features

* 🔑 Append-only key/value storage
* ⚡ Zero-copy reads via `memoryview` and `mmap`
* 🧵 Thread-safe reads and writes from Python (with restrictions)
* 📆 Single-file binary container (no schema or serialization required)
* ↺ Streaming interface for writing and reading large entries
* 🐍 Fully native Python interface (no C extension required)

## Installation

```sh
pip install -i simd-r-drive-py
```

## Usage

TODO: Separate examples into 3 categories
    - regular writes
    - batch writes
    - stream writes

```python
from simd_r_drive import DataStore
import io

# Open or create a storage file
store = DataStore("mydata.bin")

# Write a key/value pair
store.write(b"mykey", b"myvalue")

# Read as bytes
assert store.read(b"mykey") == b"myvalue"

# Read as zero-copy memoryview
entry = store.read_entry(b"mykey")
view = entry.as_memoryview()
assert bytes(view) == b"myvalue"

# Streaming write
payload = b"x" * 1024 * 1024
store.write_stream(b"large", io.BytesIO(payload))

# Streaming read
stream = store.read_stream(b"large")
data = bytearray()
while chunk := stream.read(4096):
    data.extend(chunk)

assert data == payload
```

## API

### `DataStore(path: str)`

Opens (or creates) a file-backed storage container at the given path.

### `.write(key: bytes, value: bytes) -> None`

Atomically appends a new key-value entry. Overwrites any previous version of the key.

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
