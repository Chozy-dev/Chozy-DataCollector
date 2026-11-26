"""Bounded, read-only Ownerclan RPM and detail comparison; offline unless --execute."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time

import requests
from product_data_collector.operations.budget import SupplierSession

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ownerclan_API.api.auth import AUTH_ENDPOINTS, extract_token_response
from ownerclan_API.api.client import API_ENDPOINTS
from ownerclan_API.api.queries import ITEM_FIELDS
from ownerclan_API.config.settings import load_credentials

OUTPUT = ROOT / "artifacts/experiments/ownerclan-api-policy/measurement.json"
LIGHT_QUERY = ('query { allItems(first: 1, sortBy: registerDateDesc) { '
               'pageInfo { hasNextPage endCursor } edges { node { key } } } }')
LIMIT_TERMS = ("too many", "rate limit", "quota", "호출 제한", "요청 제한")


class StopProbe(Exception):
    pass


def assess(keys, nodes):
    if not isinstance(nodes, list):
        raise StopProbe("invalid_detail_array")
    returned = [n.get("key") for n in nodes if isinstance(n, dict)]
    if any(not isinstance(key, str) for key in returned):
        raise StopProbe("invalid_returned_key")
    counts = Counter(returned)
    return {
        "requested": len(keys), "rows": len(nodes),
        "returned": len(set(returned) & set(keys)),
        "missing_keys": sorted(set(keys) - set(returned)),
        "unexpected_keys": sorted(set(returned) - set(keys)),
        "duplicate_keys": sorted(key for key, count in counts.items() if count > 1),
        "null_rows": sum(node is None for node in nodes),
    }


def summarize_rate(calls, elapsed):
    offsets = [call["offset_seconds"] for call in calls]
    # Count real starts in half-open rolling minute windows, not configured RPM.
    peak = max((sum(start <= other < start + 60 for other in offsets)
                for start in offsets), default=0)
    latencies = [call["http_seconds"] for call in calls]
    return {
        "attempts": len(calls), "ok": sum(call.get("ok", False) for call in calls),
        "elapsed_seconds": round(elapsed, 4),
        "observed_rpm": round(len(calls) * 60 / max(elapsed, .001), 3),
        "peak_starts_in_60_seconds": peak,
        "latency_mean_seconds": statistics.mean(latencies) if latencies else None,
        "latency_max_seconds": max(latencies, default=None),
        "start_span_seconds": offsets[-1] - offsets[0] if len(offsets) > 1 else 0,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--rates", default="60,120,150")
    parser.add_argument("--keys-source", type=Path, help="Existing JSON containing keys; skips API seed lookup")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--seconds-per-rate", type=float, default=65)
    parser.add_argument("--max-calls", type=int, default=400)
    parser.add_argument("--max-seconds", type=float, default=720)
    args = parser.parse_args(argv)
    try:
        rates = [] if args.rates == "skip" else [float(value) for value in args.rates.split(",")]
    except ValueError:
        parser.error("--rates must contain numbers")
    if (any(not 0 < rate <= 150 for rate in rates)
            or not 0 < args.seconds_per_rate <= 120 or not 1 <= args.max_calls <= 500
            or not 60 <= args.max_seconds <= 900):
        parser.error("limits: RPM <=150, seconds/rate <=120, calls <=500, wall seconds 60..900")
    output = args.output.resolve()
    if output.parent != OUTPUT.parent.resolve() or output.suffix != ".json":
        parser.error("--output must be a JSON file under artifacts/experiments/ownerclan-api-policy")
    keys = None
    if args.keys_source:
        source = args.keys_source.resolve()
        keys = json.loads(source.read_text(encoding="utf-8"))["keys"][:101]
        if (len(keys) != 101 or len(set(keys)) != 101
                or any(not isinstance(key, str) or not key.strip() for key in keys)):
            parser.error("--keys-source must contain at least 101 unique non-empty keys")
    result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "environment": "production", "execute": args.execute,
        "limits": {"target_rpm": rates, "seconds_per_rate": args.seconds_per_rate,
                   "max_calls_including_auth": args.max_calls,
                   "max_wall_seconds": args.max_seconds, "timeout_connect_read": [5, 45],
                   "workers": 1, "automatic_retries": 0, "stop_after_network_error": 1},
        "scope": "one Windows host, one account, external account usage unverified; no DB or collector writes",
        "calls": [], "rate_cases": [], "detail_cases": [],
        "fields_sha256": hashlib.sha256(ITEM_FIELDS.encode()).hexdigest(),
        "complete": False,
    }
    if args.keys_source:
        result["keys_source"] = str(source)
        result["keys_source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    if not args.execute:
        print(json.dumps({"plan": result, "output": str(output)}, ensure_ascii=False, indent=2))
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    last_start = None

    def save():
        result["elapsed_seconds"] = round(time.monotonic() - started, 4)
        pending = output.with_suffix(".pending")
        pending.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        pending.replace(output)

    def guard():
        if len(result["calls"]) >= args.max_calls:
            raise StopProbe("call_budget")
        # Leave room for the bounded connect/read request, including slow reads.
        if time.monotonic() - started >= args.max_seconds - 55:
            raise StopProbe("wall_budget")

    def send(session, label, query=None, *, interval=1.0, deadline=None, token=None):
        nonlocal last_start
        guard()
        now = time.monotonic()
        due = max(now, (last_start + interval) if last_start is not None else now)
        if deadline is not None and due >= deadline:
            return None
        if due > now:
            time.sleep(due - now)
        guard()
        sent = time.monotonic()
        last_start = sent
        record = {"label": label, "offset_seconds": round(sent - started, 6),
                  "method": "GET" if query and label.startswith("rate-") else "POST"}
        if query:
            record["query_sha256"] = hashlib.sha256(query.encode()).hexdigest()
        result["calls"].append(record)
        save()
        try:
            if query is None:
                username, password = load_credentials(ROOT)
                response = session.post(AUTH_ENDPOINTS["production"], json={
                    "service": "ownerclan", "userType": "seller", "username": username,
                    "password": password}, timeout=(5, 45), allow_redirects=False)
            else:
                values = {"query": query}
                headers = {"Authorization": f"Bearer {token}"}
                kwargs = {"params" if record["method"] == "GET" else "json": values}
                response = session.request(record["method"], API_ENDPOINTS["production"],
                    headers=headers, timeout=(5, 45), allow_redirects=False, **kwargs)
            record.update(status=response.status_code, response_bytes=len(response.content),
                limit_headers={key: value for key, value in response.headers.items()
                               if key.lower().startswith(("ratelimit", "x-ratelimit", "retry-after"))})
            if query is None:
                if response.status_code != 200:
                    raise StopProbe("auth_rejected")
                auth_token = extract_token_response(response)
                if not auth_token:
                    raise StopProbe("invalid_auth_token")
                record["ok"] = True
                return auth_token
            if response.status_code in (401, 403, 429):
                raise StopProbe(f"http_{response.status_code}")
            if not 200 <= response.status_code < 300:
                raise StopProbe(f"http_{response.status_code}")
            try:
                payload = json.loads(response.content.decode("utf-8-sig"))
            except (ValueError, UnicodeError):
                raise StopProbe("invalid_json")
            if not isinstance(payload, dict):
                raise StopProbe("invalid_payload")
            errors = payload.get("errors")
            if errors:
                record["graphql_errors"] = errors
                text = json.dumps(errors, ensure_ascii=False).lower()
                if any(term in text for term in LIMIT_TERMS):
                    raise StopProbe("graphql_rate_limit")
                if label == "keys-cap-101" and "100" in text:
                    record["expected_cap_rejection"] = True
                    return payload
                raise StopProbe("graphql_error")
            if not isinstance(payload.get("data"), dict):
                raise StopProbe("missing_data")
            record["ok"] = True
            if label == "keys-cap-101":
                record["assessment"] = assess(keys, payload["data"].get("itemsByKeys"))
            return payload["data"]
        except (requests.Timeout, requests.ConnectionError) as exc:
            record["error"] = type(exc).__name__
            raise StopProbe("network_error")
        except StopProbe as exc:
            record["stop"] = str(exc)
            raise
        finally:
            record["http_seconds"] = round(time.monotonic() - sent, 6)
            save()

    try:
        with SupplierSession() as session:
            token = send(session, "auth")
            for rate in rates:
                label = f"rate-{rate:g}"
                phase_start = time.monotonic()
                deadline = phase_start + args.seconds_per_rate
                phase = {"target_rpm": rate, "query": LIGHT_QUERY, "complete": False}
                result["rate_cases"].append(phase)
                index = len(result["calls"])
                try:
                    while time.monotonic() < deadline:
                        if send(session, label, LIGHT_QUERY, interval=60/rate,
                                deadline=deadline, token=token) is None:
                            remaining = deadline-time.monotonic()
                            if remaining > 0:
                                time.sleep(remaining)
                            break
                    phase["complete"] = True
                finally:
                    phase.update(summarize_rate(result["calls"][index:], time.monotonic()-phase_start))
                    save()
                    print(json.dumps({"event": "rate_summary", **phase}, ensure_ascii=False), flush=True)

            if keys is None:
                seed_query = ('query { allItems(first: 101, sortBy: registerDateDesc) { '
                              'pageInfo { hasNextPage endCursor } edges { node { key } } } }')
                data = send(session, "seed-101", seed_query, token=token)
                keys = [edge["node"]["key"] for edge in data["allItems"]["edges"]]
                if len(keys) != 101 or len(set(keys)) != 101:
                    raise StopProbe("invalid_seed_keys")
            result["sample_keys"] = keys
            result["sample_sha256"] = hashlib.sha256(json.dumps(keys).encode()).hexdigest()
            query = 'query { itemsByKeys(keys: '+json.dumps(keys)+') { key } }'
            send(session, "keys-cap-101", query, token=token)
            for size in (50, 100, 100, 50):
                phase = {"batch_size": size, "batches": [], "complete": False}
                result["detail_cases"].append(phase)
                phase_start = time.monotonic()
                index = len(result["calls"])
                try:
                    for offset in range(0, 100, size):
                        batch = keys[offset:offset+size]
                        query = ('query { itemsByKeys(keys: '+json.dumps(batch)+') { '+ITEM_FIELDS+' } }')
                        data = send(session, f"detail-{len(result['detail_cases'])}-{offset}", query, token=token)
                        assessment = assess(batch, data.get("itemsByKeys"))
                        phase["batches"].append(assessment)
                        if assessment["unexpected_keys"] or assessment["duplicate_keys"]:
                            raise StopProbe("key_mismatch")
                    phase["complete"] = not any(batch["missing_keys"] for batch in phase["batches"])
                finally:
                    phase["elapsed_seconds"] = round(time.monotonic()-phase_start, 6)
                    phase["http_calls"] = len(result["calls"])-index
                    phase["returned"] = sum(batch["returned"] for batch in phase["batches"])
                    save()
                    print(json.dumps({"event": "detail_summary", **phase}, ensure_ascii=False), flush=True)
            result["complete"] = True
    except StopProbe as exc:
        result["stop_reason"] = str(exc)
    finally:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
        print(json.dumps({"event": "finished", "complete": result["complete"],
                          "stop_reason": result.get("stop_reason"), "calls": len(result["calls"]),
                          "elapsed_seconds": result["elapsed_seconds"]}, ensure_ascii=False), flush=True)
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
