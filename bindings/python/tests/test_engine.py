import tempfile
import os
import pytest
from simd_r_drive_py import PyEngine


def test_write_and_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = PyEngine(filepath)

        key = "hello"
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

def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = PyEngine(filepath)

        engine.write("to_delete", b"data")
        assert engine.exists("to_delete")

        engine.delete("to_delete")
        assert not engine.exists("to_delete")
        assert engine.read("to_delete") is None

        engine.close()

def test_read_missing_key_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = PyEngine(filepath)

        assert not engine.exists("nonexistent")
        assert engine.read("nonexistent") is None

        engine.close()
