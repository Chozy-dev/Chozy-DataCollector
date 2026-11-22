"""Nine-HTTP-cap API/session crossover. Offline unless --execute is supplied."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import logging
import time

import requests

from scripts.probes.ownerclan_query_comparison import (
    OUT, ROOT, AUTH_ENDPOINTS, API_ENDPOINTS, ITEM_FIELDS, JwtProvider,
    MeasuredSession, OwnerclanClient, RateLimiter, StopProbe, assess,
    item_query, items_by_keys_query, load_credentials,
)
from scripts.probes.ownerclan_missing_items_probe import describe_response

CASES = (
    ("WFSQRWB", "item", "A", "new"),
    ("WFSQRWB", "itemsByKeys", "B", "new"),
    ("WFSQRWB", "itemsByKeys", "A", "reused"),
    ("WFSQRWB", "item", "B", "reused"),
    ("WFM25VZ", "itemsByKeys", "C", "new"),
    ("WFM25VZ", "item", "D", "new"),
    ("WFM25VZ", "item", "C", "reused"),
    ("WFM25VZ", "itemsByKeys", "D", "reused"),
)


class WireBudget:
    """One guard and one start clock across auth and every connection pool."""

    def __init__(self):
        self.auth = self.reads = 0
        self.stopped = False
        # Windows monotonic may have 15.625ms resolution; use the high-resolution
        # counter so a rounded-up tick cannot shorten the actual start interval.
        self.limiter = RateLimiter(3, clock=time.perf_counter)
        self.started = time.monotonic()

    def reserve(self, url):
        if self.stopped or time.monotonic() - self.started > 480:
            raise StopProbe("stopped_or_time_cap")
        if url == AUTH_ENDPOINTS["production"]:
            if self.auth >= 1:
                raise StopProbe("auth_cap")
            self.auth += 1
        elif url == API_ENDPOINTS["production"]:
            if self.reads >= 8:
                raise StopProbe("read_cap")
            self.reads += 1
        else:
            raise StopProbe("unexpected_endpoint")
        self.limiter.wait()


class GuardedAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, budget, result):
        super().__init__(max_retries=0)
        self.budget, self.result = budget, result

    def send(self, request, **kwargs):
        if request.method != "POST" or kwargs.get("timeout") != (5, 45):
            raise StopProbe("unexpected_transport")
        self.budget.reserve(request.url)
        record = self.result["calls"][-1]
        record.update(transport_started_at=datetime.now(timezone.utc).isoformat(),
                      transport_offset_seconds=time.monotonic() - self.budget.started,
                      transport_attempted=True)
        transport_started = time.monotonic()
        try:
            response = super().send(request, **kwargs)
            record["transport_to_headers_seconds"] = time.monotonic() - transport_started
            record["request_id_headers"] = {k: v for k, v in response.headers.items()
                                             if k.lower() in {"x-request-id", "x-correlation-id", "traceparent"}}
            return response
        except Exception:
            record["transport_failure_seconds"] = time.monotonic() - transport_started
            self.budget.stopped = True
            raise


class FrozenToken:
    def __init__(self, value):
        self.value = value

    def token(self):
        return self.value

    def refresh(self):
        raise StopProbe("additional_auth_forbidden")


def run(result, save, credentials=load_credentials):
    budget = WireBudget()
    started = time.monotonic()
    graph_started = None
    result.update(status="running", calls=[], cases=[], started_at=datetime.now(timezone.utc).isoformat(),
                  fields_sha256=hashlib.sha256(ITEM_FIELDS.encode()).hexdigest(),
                  limits=dict(auth=1, reads=8, http=9, start_interval_seconds=3,
                              timeout=[5, 45], workers=1, retries=0, redirects=False))
    save()

    def session(stack, name):
        value = stack.enter_context(MeasuredSession(result, save, max_reads=8, max_seconds=480))
        value.started = budget.started
        value.limiter = RateLimiter(0)  # The adapter owns cross-session pacing.
        value.mount("https://", GuardedAdapter(budget, result))
        value.context = dict(api="auth", session=name)
        value.response_observer = describe_response
        return value

    try:
        with ExitStack() as stack:
            username, password = credentials(ROOT)
            auth_session = session(stack, "auth")
            token = JwtProvider(username, password, "production", 45, session=auth_session).token()
            frozen = FrozenToken(token)
            sessions = {}
            graph_started = time.monotonic()
            for key, api, name, usage in CASES:
                if name not in sessions:
                    sessions[name] = session(stack, name)
                current = sessions[name]
                query = item_query(key) if api == "item" else items_by_keys_query([key])
                current.context = dict(api=api, session=name, usage=usage, key=key,
                                       query_sha256=hashlib.sha256(query.encode()).hexdigest())
                case = dict(current.context, status="running")
                result["cases"].append(case)
                case_started = time.monotonic()
                try:
                    client = OwnerclanClient(frozen, "production", RateLimiter(0), 45, 0, 300, session=current)
                    data = client.graphql(query)
                    value = data.get(api)
                    case["operation_present"] = api in data
                    case["response_type"] = type(value).__name__
                    nodes = [value] if api == "item" and isinstance(value, dict) else value
                    case["assessment"] = assess([key], nodes)
                    if (not case["operation_present"] or (api == "item" and not isinstance(value, dict))
                            or not case["assessment"]["complete"]):
                        raise StopProbe("incomplete_or_invalid_products")
                    case["status"] = "complete"
                finally:
                    case["wall_seconds"] = time.monotonic() - case_started
                    save()
            result["status"] = "complete"
    except Exception as exc:
        budget.stopped = True
        result.update(status="stopped", stop_reason=str(exc) if isinstance(exc, StopProbe) else type(exc).__name__)
        if result["cases"] and result["cases"][-1]["status"] == "running":
            result["cases"][-1]["status"] = "stopped"
    finally:
        elapsed = time.monotonic() - started
        result.update(finished_at=datetime.now(timezone.utc).isoformat(), wall_seconds=elapsed,
                      counts=dict(auth=budget.auth, reads=budget.reads, total=budget.auth + budget.reads),
                      completion_seconds=elapsed if result["status"] == "complete" else None,
                      query_wall_seconds=time.monotonic()-graph_started if graph_started is not None else None)
        save()
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        print(json.dumps(dict(execute=False, auth=1, reads=8, http_cap=9, cases=CASES)))
        return 0
    path = OUT / "session-measurement.json"
    if path.exists():
        parser.error("session-measurement.json exists; refuse accidental rerun")
    logging.disable(logging.CRITICAL)
    result = dict(authorization="Current user authorized <=8 reads + <=1 auth; report unverifiable account use",
                  preflight=dict(local_collectors="none observed; app idle", budget_relations="absent",
                                 account_wide_usage="unverifiable", provider_remaining_budget="unverifiable"))

    def save():
        pending = path.with_suffix(".pending")
        pending.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        pending.replace(path)

    run(result, save)
    print(json.dumps(dict(status=result["status"], counts=result["counts"],
                         stop_reason=result.get("stop_reason"))), flush=True)
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
