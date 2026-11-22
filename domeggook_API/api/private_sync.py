"""Official documentation contract, NOT a measured successful API contract.

https://openapi.domeggook.com/ko/articles/상품정보-동기화-6b5f900f
No provider RPM is assumed. Callers supply an operational limiter and budget.
"""
import time
import requests

from .client import (DomeggookApiError, DomeggookHttpError, DomeggookResponseError,
                     DomeggookTimeoutError, consume_budget, _retry_after_seconds)

ENDPOINT = "https://product.domeggook.com/api/v1/product/_search"


class PrivateSyncClient:
    def __init__(self, api_key, *, rate_limiter, request_budget, session=None,
                 timeout=20, max_retries=3):
        self._key = api_key
        self.limiter = rate_limiter
        self.budget = request_budget
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries

    def search(self, start, end, *, after=None, size=100):
        if not 1 <= size <= 100:
            raise ValueError("Private search size must be 1..100")
        body = {"size": size, "track_total_hits": True,
                "query": {"bool": {"filter": [
                    {"term": {"channel.supply": True}},
                    {"range": {"dateModify": {"gte": start, "lte": end}}}]}},
                "sort": [{"no": {"order": "desc"}}]}
        if after is not None:
            body["search_after"] = after
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            consume_budget(self.budget)  # failures and retries both consume account budget
            try:
                response = self.session.post(ENDPOINT, json=body,
                    headers={"Authorization": "Bearer " + self._key}, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt == self.max_retries:
                    error_type = DomeggookTimeoutError if isinstance(exc, requests.Timeout) else DomeggookApiError
                    raise error_type(type(exc).__name__) from None
                time.sleep(min(2 ** attempt, 30))
                continue
            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                delay = _retry_after_seconds(response.headers.get("Retry-After"))
                # Long provider waits pause the job rather than ignore Retry-After.
                if delay is not None and delay > 60:
                    raise DomeggookHttpError(response.status_code, "provider requested deferred retry")
                time.sleep(delay if delay is not None else min(2 ** attempt, 30))
                continue
            if response.status_code >= 400:
                raise DomeggookHttpError(response.status_code, "Private search rejected")
            try:
                payload = response.json()
            except ValueError:
                raise DomeggookApiError("Private search invalid JSON") from None
            parse_search_page(payload)  # reject malformed/error responses before progress
            return payload


def parse_search_page(payload):
    if not isinstance(payload, dict) or payload.get("result") is not True:
        raise DomeggookResponseError(None, "Private search result is not true")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("list"), list) or "searchAfter" not in data:
        raise ValueError("Private search missing documented page fields")
    rows, after = data["list"], data["searchAfter"]
    if after is not None and (not isinstance(after, list) or not after):
        raise ValueError("Invalid searchAfter")
    if any(not isinstance(r, dict) or not isinstance(r.get("no"), str) or not r["no"].strip() for r in rows):
        raise ValueError("Private search missing product no")
    if not rows and after is not None:
        raise ValueError("Empty nonterminal Private search page")
    return rows, after
