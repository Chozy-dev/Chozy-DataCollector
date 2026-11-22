from dataclasses import dataclass
from pathlib import Path

import yaml

from coupang_API.services.best import positive_int


@dataclass(frozen=True)
class BestConfig:
    categories: tuple[tuple[int, str], ...]
    max_calls: int
    requests_per_minute: int
    limit: int = 100

    def __post_init__(self):
        if not self.categories or len({c[0] for c in self.categories}) != len(self.categories):
            raise ValueError("Explicit nonempty unique categories required")
        for category_id, name in self.categories:
            positive_int(category_id)
            if not isinstance(name, str) or not name.strip():
                raise ValueError("Category name required")
        for value in (self.max_calls, self.requests_per_minute, self.limit):
            positive_int(value)
        if self.limit > 100:
            raise ValueError("limit must be <=100")

    def scope(self):
        return {"categories": [list(c) for c in sorted(self.categories)], "limit": self.limit}


def load_best_config(path: Path):
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return BestConfig(tuple((positive_int(c["id"]), c["name"]) for c in payload["categories"]),
                      positive_int(payload["max_calls"]), positive_int(payload["requests_per_minute"]),
                      positive_int(payload.get("limit", 100)))
