from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def find_project_root(start: Path) -> Path:
    current = start.resolve()
    candidates = [current, *current.parents]
    for candidate in candidates:
        if (candidate / "coupang_API").is_dir() and (
            (candidate / ".env").exists()
            or (candidate / ".env.example").exists()
            or (candidate / "requirements.txt").exists()
        ):
            return candidate
    for candidate in candidates:
        if (candidate / "coupang_API").is_dir():
            return candidate
    return Path(__file__).resolve().parents[2]


def load_credentials(project_root: Path) -> tuple[str, str]:
    load_dotenv(project_root / ".env", override=False, encoding="utf-8-sig")
    access_key = os.getenv("COUPANG_ACCESS_KEY")
    secret_key = os.getenv("COUPANG_SECRET_KEY")
    missing = [
        name
        for name, value in (
            ("COUPANG_ACCESS_KEY", access_key),
            ("COUPANG_SECRET_KEY", secret_key),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"missing required environment variables: {', '.join(missing)}")
    return access_key or "", secret_key or ""
