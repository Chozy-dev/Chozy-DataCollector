from __future__ import annotations

import hashlib
import json
from typing import Any


def item_list_hash(items: list[str]) -> str:
    payload = json.dumps(items, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resume_index(items: list[str], state: dict[str, Any], list_hash: str) -> int:
    if state.get("trackedListHash") == list_hash:
        try:
            return min(max(int(state.get("nextIndex", 0)), 0), len(items))
        except (TypeError, ValueError):
            return 0
    # Without a matching hash, new IDs may precede the last completed ID.
    # Replaying is safe; trusting that old boundary could lose the new prefix.
    return 0


def next_page_cursor(page_info: dict[str, Any], after: str | None, seen: set[str]) -> str | None:
    """Only an explicit terminal page is evidence of completion."""
    if not isinstance(page_info.get("hasNextPage"), bool):
        raise ValueError("Missing or invalid hasNextPage in API response")
    if not page_info["hasNextPage"]:
        return None
    cursor = page_info.get("endCursor")
    if not cursor:
        raise ValueError("Missing cursor on a non-terminal API page")
    cursor = str(cursor)
    if cursor == after or cursor in seen:
        raise ValueError(f"Repeated pagination cursor: {cursor}")
    return cursor


def collection_outcome(*, failed: bool = False, pause_reason: str | None = None) -> dict[str, str]:
    return {
        "status": "failed" if failed else "paused" if pause_reason else "completed",
        "reason": "failure" if failed else pause_reason or "range_finished",
    }


def validate_limits(**limits: int | None) -> None:
    for name, value in limits.items():
        if value is not None and value < 1:
            raise ValueError(f"{name} must be positive")
