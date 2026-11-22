from __future__ import annotations


class RunBudget:
    def __init__(self, max_api_calls: int | None) -> None:
        self.max_api_calls = max_api_calls if max_api_calls is not None else None
        self.used_api_calls = 0

    def can_call(self) -> bool:
        return self.max_api_calls is None or self.used_api_calls < self.max_api_calls

    def record_call(self) -> None:
        self.used_api_calls += 1

    def exhausted(self) -> bool:
        return self.max_api_calls is not None and self.used_api_calls >= self.max_api_calls


class PersistentCallBudget:
    """Local account/day operational allowance, shared across jobs and restarts.

    Not a claim about provider quota. A shared service can implement reserve_call
    later without changing transports. Multi-host account coordination is pending.
    """
    def __init__(self, path, max_api_calls):
        if max_api_calls < 1:
            raise ValueError("Call allowance must be positive")
        self.path = path
        self.maximum = max_api_calls

    def reserve_call(self):
        from datetime import datetime, timezone
        from ..persistence.storage import FileLock, load_state, save_state
        from ..api.client import CallBudgetExhausted
        with FileLock(self.path.with_suffix(".lock")):
            day = datetime.now(timezone.utc).date().isoformat()
            state = load_state(self.path)
            used = state.get("used", 0) if state.get("day") == day else 0
            if used >= self.maximum:
                raise CallBudgetExhausted("Local daily call allowance exhausted")
            save_state(self.path, {"day": day, "used": used + 1})
