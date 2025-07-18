import tempfile
import os
from simd_r_drive import DataStore
import numpy as np
import gc

def test_write_and_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"hello"
        value = b"world"

        engine.write(key, value)
        result = engine.read(key)

        assert result == value
        assert engine.exists(key)
        assert key in engine # Dunder method

        # Explicitly close the engine to ensure the file is released on Windows
        #
        # PyO3 does not guarantee deterministic destruction of Rust-backed objects.
        # Especially on Windows, mmap or file handles may remain open until GC finalizes
        # the Python object. This can cause file deletion or cleanup to fail.
        #
        # Manually calling `engine.close()` ensures internal Rust resources are dropped.
        del engine
        
def test_batch_write():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        # Create a batch of key-value pairs
        entries = [
            (b"user:1", b"alice"),
            (b"user:2", b"bob"),
            (b"user:3", b"charlie"),
        ]

        engine.batch_write(entries)

        # Validate all entries were correctly stored
        for key, value in entries:
            assert engine.exists(key)
            assert engine.read(key) == value

        # Cleanup
        del engine
        gc.collect()


def test_read_entry_returns_memoryview():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"abc"
        value = b"xyz123"

        engine.write(key, value)

        entry = engine.read_entry(key)
        assert entry is not None

        mv = entry.as_memoryview()
        assert isinstance(mv, memoryview)

        # Confirm contents are correct
        assert bytes(mv) == value

        # Convert to NumPy and validate zero-copy
        arr = np.frombuffer(mv, dtype=np.uint8)
        assert arr.tobytes() == value

        # Windows workaround: Explicitly drop strong references
        del mv
        del entry
        del engine

        # Windows workaround: Force garbage collection to release mmap handle
        gc.collect()

def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        engine.write(b"to_delete", b"data")
        assert engine.exists(b"to_delete")

        engine.delete(b"to_delete")
        assert not engine.exists(b"to_delete")
        assert engine.read(b"to_delete") is None

        del engine

def test_read_missing_key_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        assert not engine.exists(b"nonexistent")
        assert engine.read(b"nonexistent") is None

        del engine

def test_write_stream_and_read_stream():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"stream_key"
        value = os.urandom(1024 * 256)  # 256 KB of random data

        # Simulate a streaming reader using BytesIO
        import io
        stream = io.BytesIO(value)
        engine.write_stream(key, stream)

        # Retrieve streaming handle
        reader = engine.read_stream(key)
        assert reader is not None

        chunks = []
        for chunk in reader:
            chunks.append(chunk)

        result = b"".join(chunks)
        assert result == value

        # Cleanup
        del reader
        del engine
        gc.collect()

def test_write_and_read_many_numpy_arrays():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        shape = (16, 16)
        dtype = np.float32
        arrays = {}

        # Write 100 arrays
        for i in range(100):
            key = f"array_{i}".encode()
            arr = (np.random.rand(*shape) * 100).astype(dtype)
            engine.write(key, arr.tobytes())
            arrays[key] = arr

        # Read and verify each
        for key, original in arrays.items():
            entry = engine.read_entry(key)
            assert entry is not None
            mv = entry.as_memoryview()
            recovered = np.frombuffer(mv, dtype=dtype).reshape(shape)
            assert np.allclose(recovered, original)

        # Cleanup
        del mv
        del engine
        gc.collect()

def test_write_and_read_numpy_matrix():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"matrix"
        original = (np.random.rand(32, 32) * 255).astype(np.uint8)

        engine.write(key, original.tobytes())

        entry = engine.read_entry(key)
        assert entry is not None

        mv = entry.as_memoryview()
        assert isinstance(mv, memoryview)
        assert len(mv) == original.size

        # Reconstruct from buffer and reshape
        recovered = np.frombuffer(mv, dtype=np.uint8).reshape(original.shape)
        assert np.array_equal(recovered, original)

        # Cleanup
        del mv
        del entry
        del engine
        gc.collect()

def test_write_and_read_mixed_dtypes():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        test_cases = {
            b"float32": np.random.rand(32).astype(np.float32),
            b"int64": np.random.randint(0, 1_000_000, size=32).astype(np.int64),
            b"uint8": np.random.randint(0, 256, size=128).astype(np.uint8),
            b"bool": np.random.rand(64) > 0.5,
            b"float64": np.random.rand(16).astype(np.float64),
        }

        for key, array in test_cases.items():
            engine.write(key, array.tobytes())

        for key, original in test_cases.items():
            entry = engine.read_entry(key)
            assert entry is not None
            mv = entry.as_memoryview()
            recovered = np.frombuffer(mv, dtype=original.dtype)
            assert np.array_equal(recovered, original)

        # Cleanup
        del mv
        del engine
        gc.collect()

def test_entry_accessors():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"meta"
        value = b"test_payload_123456"
        engine.write(key, value)

        entry = engine.read_entry(key)
        assert entry is not None

        # Access basic metadata
        assert isinstance(entry.size, int)
        assert entry.size == len(value)
        assert len(entry) == len(value) # Dunder test

        assert entry.file_size > entry.size

        # Check offset logic
        start = entry.start_offset
        end = entry.end_offset
        assert end - start == entry.size
        offset_range = entry.offset_range()
        assert offset_range == (start, end)

        # Check address range returns usable values
        addr_start, addr_end = entry.address_range()
        assert isinstance(addr_start, int)
        assert addr_end > addr_start

        # Check checksum
        assert isinstance(entry.checksum, int)
        raw = entry.raw_checksum()
        assert isinstance(raw, bytes) or isinstance(raw, bytearray) or isinstance(raw, (list, tuple))
        assert len(raw) == 4

        # Validate checksum correctness
        assert entry.is_valid_checksum()

        # Confirm memoryview still works
        mv = entry.as_memoryview()
        assert isinstance(mv, memoryview)
        assert bytes(mv) == value

        # Cleanup
        del mv
        del entry
        del engine
        gc.collect()
