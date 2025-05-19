import tempfile
import os
import pytest
from simd_r_drive import DataStore
import numpy as np


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

        # Explicitly close the engine to ensure the file is released on Windows
        #
        # PyO3 does not guarantee deterministic destruction of Rust-backed objects.
        # Especially on Windows, mmap or file handles may remain open until GC finalizes
        # the Python object. This can cause file deletion or cleanup to fail.
        #
        # Manually calling `engine.close()` ensures internal Rust resources are dropped.
        engine.close()

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

        engine.close()

def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        engine.write(b"to_delete", b"data")
        assert engine.exists(b"to_delete")

        engine.delete(b"to_delete")
        assert not engine.exists(b"to_delete")
        assert engine.read(b"to_delete") is None

        engine.close()

def test_read_missing_key_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        assert not engine.exists(b"nonexistent")
        assert engine.read(b"nonexistent") is None

        engine.close()
