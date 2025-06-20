import os
import numpy as np
import tempfile
import pytest
from simd_r_drive import DataStore

ENTRY_SIZE = 8
WRITE_BATCH_SIZE = 1024
NUM_ENTRIES = 100_000  # Reduce to make CI tolerable
NUM_RANDOM_CHECKS = 100_000

@pytest.fixture(scope="module")
def store(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("simd_bench")
    store = DataStore(str(tmp_path / "store.bin"))
    for i in range(NUM_ENTRIES):
        key = f"bench-key-{i}".encode()
        value = np.uint64(i).tobytes()
        store.write(key, value)
    return store

@pytest.mark.benchmark
def test_benchmark_append_entries(benchmark):
    def setup():
        tmpdir = tempfile.TemporaryDirectory()
        path = os.path.join(tmpdir.name, "store.bin")
        store = DataStore(path)
        return store, tmpdir

    def write_batch():
        store, tmpdir = setup()
        batch = []
        for i in range(NUM_ENTRIES):
            key = f"bench-key-{i}".encode()
            value = np.uint64(i).tobytes()
            batch.append((key, value))

            if len(batch) >= WRITE_BATCH_SIZE:
                store.batch_write(batch)
                batch.clear()

        if batch:
            store.batch_write(batch)

        tmpdir.cleanup()

    benchmark(write_batch)

@pytest.mark.benchmark
def test_benchmark_sequential_reads(benchmark, store):
    def sequential_reads():
        for i in reversed(range(NUM_ENTRIES)):
            key = f"bench-key-{i}".encode()
            result = store.read(key)
            assert result is not None
            value = int.from_bytes(result, "little")
            assert value == i

    benchmark(sequential_reads)

@pytest.mark.benchmark
def test_benchmark_random_reads(benchmark, store):
    indices = np.random.default_rng(42).integers(0, NUM_ENTRIES, NUM_RANDOM_CHECKS)

    def random_reads():
        for i in indices:
            key = f"bench-key-{i}".encode()
            result = store.read(key)
            assert result is not None
            value = int.from_bytes(result, "little")
            assert value == i

    benchmark(random_reads)
