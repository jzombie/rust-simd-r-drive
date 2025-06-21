import os
import threading
import secrets
import time

import pytest
from simd_r_drive_ws_client import DataStoreWsClient

SERVER_ADDR = os.environ.get("TEST_SERVER_ADDR", "127.0.0.1:34129")


@pytest.fixture(scope="module")
def main_client():
    """
    One client kept alive for the final global flush/reads.
    """
    # brief grace-period in case the server comes up just before the tests
    time.sleep(2)
    return DataStoreWsClient(SERVER_ADDR)


def test_stage_write_multithreaded(main_client):
    """
    * Stage-writes happen in parallel from N threads.
    * No key is visible until we explicit-flush once at the end.
    """

    THREADS = 8
    WRITES_PER_THREAD = 250
    KEY_PREFIX = b"stage-thread"
    shared = {}  # key -> value
    lock = threading.Lock()
    workers = []
    errors = []

    def worker(tid: int):
        try:
            cli = DataStoreWsClient(SERVER_ADDR)  # own connection per thread
            for i in range(WRITES_PER_THREAD):
                key = b"%s-%d-%d" % (KEY_PREFIX, tid, i)
                val = secrets.token_bytes(64)  # 64-byte random payload

                # ---- stage (buffer) but DO NOT flush --------------------
                needs_flush = cli.stage_write(key, val)
                assert (
                    not needs_flush
                ), "stage_write should never auto-flush (soft-limit disabled)"

                # record for later verification
                with lock:
                    shared[key] = val
        except Exception as exc:
            errors.append(exc)

    # ---- spawn threads ----------------------------------------------------
    for t in range(THREADS):
        th = threading.Thread(target=worker, args=(t,))
        th.start()
        workers.append(th)

    for th in workers:
        th.join()

    assert not errors, f"worker thread raised: {errors}"

    # ---- 1st pass: data MUST NOT be visible yet ---------------------------
    for key in list(shared.keys()):
        assert main_client.read(key) is None, f"key {key!r} leaked before flush"

    # ---- global flush -----------------------------------------------------
    main_client.stage_write_flush()

    # ---- 2nd pass: everything must now be durable -------------------------
    for key, expected in shared.items():
        got = main_client.read(key)
        assert got == expected, f"data mismatch for key {key!r}"
