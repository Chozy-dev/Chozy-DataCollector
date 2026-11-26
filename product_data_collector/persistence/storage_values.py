"""Shared value conversions for PostgreSQL payloads."""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def parse_datetime(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def text_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)

def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: json_safe(child) for key, child in value.items()}
    if isinstance(value, list):
        return [json_safe(child) for child in value]
    return value
