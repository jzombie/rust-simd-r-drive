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


def test_delete():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = PyEngine(filepath)

        engine.write("to_delete", b"data")
        assert engine.exists("to_delete")

        engine.delete("to_delete")
        assert not engine.exists("to_delete")
        assert engine.read("to_delete") is None


def test_read_missing_key_returns_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = PyEngine(filepath)

        assert engine.read("nonexistent") is None
