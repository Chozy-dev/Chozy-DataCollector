from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class FileLock:
    """Process-owned lock; a leftover file never blocks a restarted collector."""

    def __init__(self, path: Path, stale_after_seconds: float = 12 * 60 * 60) -> None:
        self.path = path
        # Retain the argument for callers; lock ownership no longer expires by age.
        self.stale_after_seconds = stale_after_seconds
        self._file = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self._file.seek(0)
                if not self._file.read(1):
                    self._file.write(b"0")
                    self._file.flush()
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise RuntimeError(f"another domeggook_API collection appears to be running: {self.path}") from exc
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        # Keep the inode: unlinking it could let two processes lock different files.


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(tmp_path, path)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig") as fp:
        payload = json.load(fp)
    return payload if isinstance(payload, dict) else {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_json(path, state)


def clear_state(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def chunked(values: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("size must be greater than zero")
    return [values[index : index + size] for index in range(0, len(values), size)]

