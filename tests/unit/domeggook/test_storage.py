import os
import time
import subprocess
import sys

import pytest

from domeggook_API.persistence.storage import (
    FileLock,
    atomic_write_json,
    chunked,
    load_state,
)


def test_chunked_splits_detail_batches_by_100():
    product_ids = [str(value) for value in range(205)]

    chunks = chunked(product_ids, 100)

    assert [len(chunk) for chunk in chunks] == [100, 100, 5]
    assert chunks[0][0] == "0"
    assert chunks[-1][-1] == "204"


def test_atomic_write_json_replaces_valid_file(tmp_path):
    path = tmp_path / "state.json"

    atomic_write_json(path, {"1": {"productId": "1"}})
    atomic_write_json(path, {"2": {"productId": "2"}})

    assert load_state(path) == {"2": {"productId": "2"}}
    assert not list(tmp_path.glob("*.tmp"))


def test_file_lock_rejects_live_owner(tmp_path):
    path = tmp_path / "collector.lock"
    with FileLock(path):
        old_time = time.time() - 120
        os.utime(path, (old_time, old_time))
        with pytest.raises(RuntimeError, match="another domeggook_API collection"):
            with FileLock(path, stale_after_seconds=60):
                pass

    assert path.exists()


def test_file_lock_reuses_leftover_file(tmp_path):
    path = tmp_path / "collector.lock"
    path.write_text("12345", encoding="ascii")
    with FileLock(path, stale_after_seconds=60):
        assert path.exists()

    with FileLock(path):
        assert path.exists()


def test_file_lock_releases_after_process_is_killed(tmp_path):
    path = tmp_path / "collector.lock"
    process = subprocess.Popen(
        [sys.executable, "-u", "-c",
         "from pathlib import Path; import sys, time; "
         "from domeggook_API.persistence.storage import FileLock; "
         "lock = FileLock(Path(sys.argv[1])); lock.__enter__(); "
         "print('locked', flush=True); time.sleep(60)", str(path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(RuntimeError):
            with FileLock(path):
                pass
        process.kill()
        process.wait(timeout=10)
        with FileLock(path):
            assert path.exists()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()
