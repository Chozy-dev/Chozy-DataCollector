from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from product_data_collector.operations.budget import SupplierSession

from ..api.auth import generate_authorization
from ..api.rate_limiter import RateLimiter


BASE_URL = "https://api-gateway.coupang.com"
BEST_ENDPOINT = "/v2/providers/affiliate_open_api/apis/openapi/products/bestcategories"


class CoupangApiError(Exception):
    pass


class CoupangHttpError(CoupangApiError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code


class CoupangResponseError(CoupangApiError):
    def __init__(self, r_code: str | None, r_message: str | None) -> None:
        super().__init__(f"Coupang API rCode={r_code!r}, rMessage={r_message!r}")
        self.r_code = r_code
        self.r_message = r_message


@dataclass(frozen=True)
class BestCategoryRequest:
    category_id: int
    limit: int = 100


def build_best_uri(request: BestCategoryRequest) -> str:
    if type(request.category_id) is not int or request.category_id <= 0:
        raise ValueError("category_id must be a positive integer")
    if type(request.limit) is not int or not 1 <= request.limit <= 100:
        raise ValueError("best limit must be between 1 and 100")
    return f"{BEST_ENDPOINT}/{request.category_id}?limit={request.limit}"


class CoupangPartnersClient:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        rate_limiter: RateLimiter | None = None,
        timeout: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self._access_key = access_key
        self._secret_key = secret_key
        self._rate_limiter = rate_limiter
        self._timeout = timeout
        self._session = session or SupplierSession()

    def best_products(self, request: BestCategoryRequest) -> dict[str, Any]:
        # One HTTP attempt: the daily workflow reserves every call durably.
        if self._rate_limiter is None:
            raise ValueError("Best API requires an explicitly configured rate limiter")
        uri = build_best_uri(request)
        self._rate_limiter.wait()
        headers = {
            "Authorization": generate_authorization("GET", uri, self._access_key, self._secret_key)
        }
        try:
            response = self._session.get(f"{BASE_URL}{uri}", headers=headers, timeout=self._timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            raise CoupangApiError(f"network error: {exc.__class__.__name__}") from exc

        if response.status_code >= 400:
            raise CoupangHttpError(response.status_code, _safe_response_message(response))

        try:
            payload = response.json()
        except ValueError as exc:
            raise CoupangApiError("Invalid JSON response") from exc
        if not isinstance(payload, dict):
            raise CoupangApiError("Invalid response envelope")
        r_code = payload.get("rCode")
        if str(r_code) != "0":
            raise CoupangResponseError(None if r_code is None else str(r_code), payload.get("rMessage"))
        return payload


def _safe_response_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.reason or "request failed"
    r_message = payload.get("rMessage")
    return str(r_message) if r_message else (response.reason or "request failed")
