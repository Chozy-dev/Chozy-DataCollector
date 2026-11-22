"""Bounded read-only small or 100-product coverage comparison; offline by default."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
from pathlib import Path
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ownerclan_API.api.auth import AUTH_ENDPOINTS, JwtProvider
from ownerclan_API.api.client import API_ENDPOINTS, OwnerclanClient
from ownerclan_API.api.queries import ITEM_FIELDS, item_query, items_by_keys_query
from ownerclan_API.api.rate_limiter import RateLimiter
from ownerclan_API.config.settings import load_credentials

OUT = ROOT / "artifacts/experiments/ownerclan-query-comparison"


class StopProbe(Exception):
    pass


def assess(keys, nodes):
    rows = nodes if isinstance(nodes, list) else []
    counts = Counter(n.get("key") for n in rows
                     if isinstance(n, dict) and isinstance(n.get("key"), str))
    expected = set(keys)
    valid = set()
    for node in rows:
        if not isinstance(node, dict):
            continue
        price = node.get("price")
        if (isinstance(node.get("key"), str)
                and node["key"] in expected and counts[node["key"]] == 1
                and isinstance(price, (float, int)) and not isinstance(price, bool)
                and math.isfinite(price) and price >= 0
                and isinstance(node.get("status"), str)
                and node["status"] in {"available", "unavailable", "soldout", "discontinued"}):
            valid.add(node["key"])
    return dict(requested=len(keys), rows=len(rows), response_type=type(nodes).__name__,
                returned=len(expected & counts.keys()),
                normal_products=len(valid), missing=len(expected-counts.keys()),
                duplicate_keys=sum(n > 1 for n in counts.values()),
                unexpected_keys=len(counts.keys()-expected),
                invalid_or_duplicate_rows=len(rows)-len(valid),
                complete=isinstance(nodes, list) and len(rows) == len(keys)
                and valid == expected)


class MeasuredSession(requests.Session):
    def __init__(self, result, save, *, max_reads=8, max_seconds=245):
        super().__init__()
        self.result, self.save = result, save
        self.limiter = RateLimiter(3)
        self.started = time.monotonic()
        self.auth_calls = 0
        self.read_calls = 0
        self.max_reads = max_reads
        self.max_seconds = max_seconds
        self.context = {"api": "auth"}

    def request(self, method, url, **kwargs):
        if time.monotonic()-self.started > self.max_seconds:
            raise StopProbe("local_call_or_time_cap")
        auth = url == AUTH_ENDPOINTS["production"]
        if url not in (AUTH_ENDPOINTS["production"], API_ENDPOINTS["production"]):
            raise StopProbe("unexpected_endpoint")
        if auth:
            if self.auth_calls:
                raise StopProbe("automatic_auth_retry_blocked")
            self.auth_calls += 1
        else:
            if self.read_calls >= self.max_reads:
                raise StopProbe("local_read_cap")
            self.read_calls += 1
            if method.upper() == "GET":
                kwargs["json"] = kwargs.pop("params")
        self.limiter.wait()
        if time.monotonic()-self.started > self.max_seconds:
            raise StopProbe("local_time_cap")
        started = time.monotonic()
        record = dict(self.context, sequence=len(self.result["calls"])+1,
                      started_at=datetime.now(timezone.utc).isoformat(),
                      offset_seconds=started-self.started, method="POST", status=None,
                      graphql_errors=0, response_bytes=None, timed_out=False,
                      connection_seconds=None, first_byte_seconds=None,
                      headers_elapsed_seconds=None, post_headers_seconds=None)
        self.result["calls"].append(record)
        self.save()
        kwargs.update(timeout=(5, 45), allow_redirects=False, stream=True)
        response = None
        phase = "awaiting_headers"
        try:
            response = super().request("POST", url, **kwargs)
            record.update(status=response.status_code,
                          headers_elapsed_seconds=response.elapsed.total_seconds(),
                          limit_headers={k: v for k, v in response.headers.items()
                                         if k.lower().startswith(("retry-after", "ratelimit", "x-ratelimit"))})
            phase = "reading_body"
            body_started = time.monotonic()
            try:
                record["response_bytes"] = len(response.content)
            finally:
                record["body_read_seconds"] = time.monotonic() - body_started
            phase = "response_received"
            observer = getattr(self,"response_observer",None)
            if observer is not None and not auth:
                record["response_diagnostic"] = observer(response)
            if not 200 <= response.status_code < 300:
                raise StopProbe(f"http_{response.status_code}")
            if not auth:
                try:
                    payload = json.loads(response.content.decode("utf-8-sig"))
                except (ValueError, UnicodeError):
                    raise StopProbe("invalid_json") from None
                errors = payload.get("errors") if isinstance(payload, dict) else None
                if errors:
                    record["graphql_errors"] = len(errors) if isinstance(errors, list) else 1
                    text = json.dumps(errors, ensure_ascii=False).lower()
                    limited = any(t in text for t in ("too many", "quota", "rate limit", "호출 제한", "요청 제한"))
                    record["graphql_category"] = "rate_limit" if limited else "other"
                    raise StopProbe("graphql_rate_limit" if limited else "graphql_error")
            return response
        except requests.RequestException as exc:
            # requests may wrap a body ReadTimeout in ConnectionError.
            record["timed_out"] = isinstance(exc, requests.Timeout) or "read timed out" in str(exc).lower()
            record["error"] = type(exc).__name__
            record["failure_phase"] = phase
            raise StopProbe("timeout" if record["timed_out"] else "network_error") from None
        finally:
            if response is not None:
                response.close()
            record["http_seconds"] = time.monotonic()-started
            if record["headers_elapsed_seconds"] is not None:
                remainder = record["http_seconds"]-record["headers_elapsed_seconds"]
                record["post_headers_seconds"] = remainder if remainder >= 0 else None
            self.save()


def coverage_summary(case, calls):
    """Keep partial runs distinct; never extrapolate an incomplete completion."""
    attempts = len(calls)
    # A local persistence failure can leave a reserved record before transport.
    # Do not mask that original exception with a timing KeyError in finally.
    times = [c['http_seconds'] for c in calls if c.get('http_seconds') is not None]
    batches = case["batches"]
    case.update(
        actual_calls=attempts,
        http_mean_seconds=sum(times)/len(times) if times else None,
        http_max_seconds=max(times) if times else None,
        normal_unique_products=sum(b["normal_products"] for b in batches),
        response_missing_products=sum(b["missing"] for b in batches),
        duplicate_keys=sum(b["duplicate_keys"] for b in batches),
        unexpected_keys=sum(b["unexpected_keys"] for b in batches),
        incomplete_responses=sum(not b["complete"] for b in batches),
        timeouts=sum(c["timed_out"] for c in calls),
        other_errors=sum(c.get("error") is not None or c["status"] != 200
                         or c["graphql_errors"] > 0 for c in calls if not c["timed_out"]),
        response_bytes=sum(c["response_bytes"] or 0 for c in calls),
        unrequested_products=100-attempts*case["batch_size"],
    )
    case["not_obtained_products"] = 100-case["normal_unique_products"]
    case["complete"] = (attempts == case["planned_calls"]
                        and case["normal_unique_products"] == 100
                        and all(b["complete"] for b in batches)
                        and not case["timeouts"] and not case["other_errors"])
    wall = case.get("wall_seconds")
    case["completion_seconds"] = wall if case["complete"] else None
    case["observed_rpm"] = attempts*60/wall if wall else None
    case["products_per_minute"] = case["normal_unique_products"]*60/wall if wall else None


def coverage_run(plan, keys, execute):
    order = (20,50,100,5,10)
    if not execute:
        print(json.dumps(dict(execute=False,mode="coverage",order=order,
                              reads=38,auth=1,interval_seconds=3,target_products_per_case=100)))
        return 0
    logging.disable(logging.CRITICAL)
    result = dict(started_at=datetime.now(timezone.utc).isoformat(),status="running",
                  mode="coverage",account_usage_verified=False,official_daily_limit_verified=False,
                  authorization="User approved at most 38 product reads plus one authentication for this experiment",
                  workers=1,min_start_interval_seconds=3,automatic_retries=0,
                  timeout_connect_read=[5,45],calls=[],
                  sample_sha256=plan["sample"]["ordered_keys_sha256"],
                  fields_sha256=hashlib.sha256(ITEM_FIELDS.encode()).hexdigest(),
                  order=order, cases=[dict(batch_size=b,planned_calls=100//b,
                    status="not_started",complete=False,completion_seconds=None,batches=[])
                    for b in order],
                  wall_definition="Contiguous cases, including pacing before each case's first request; authentication excluded. No interleaving or reuse of old-run responses.")
    def save():
        pending = OUT/"batch-measurement.pending"
        pending.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        pending.replace(OUT/"batch-measurement.json")
    save()
    try:
        username,password = load_credentials(ROOT)
        with MeasuredSession(result,save,max_reads=38,max_seconds=1750) as session:
            provider = JwtProvider(username,password,"production",45,session=session)
            client = OwnerclanClient(provider,"production",RateLimiter(0),45,0,300,session=session)
            provider.token()
            for case in result["cases"]:
                b = case["batch_size"]
                case.update(status="running",started_at=datetime.now(timezone.utc).isoformat())
                started = time.monotonic()
                index = len(result["calls"])
                try:
                    for offset in range(0,100,b):
                        batch = keys[offset:offset+b]
                        query = items_by_keys_query(batch)
                        session.context = dict(api="itemsByKeys",batch_size=b,fields="full",
                            sample_slice=[offset,offset+b],query_sha256=hashlib.sha256(query.encode()).hexdigest())
                        data = client.graphql(query)
                        assessment = assess(batch,data.get("itemsByKeys"))
                        case["batches"].append(assessment)
                        save()
                        if not assessment["complete"]:
                            raise StopProbe("incomplete_or_invalid_products")
                    case["status"] = "finished"
                finally:
                    case.update(wall_seconds=time.monotonic()-started,
                                finished_at=datetime.now(timezone.utc).isoformat())
                    coverage_summary(case,result["calls"][index:])
                    if not case["complete"]:
                        case["status"] = "partial"
                    save()
                    print(json.dumps({k:v for k,v in case.items() if k!='batches'}),flush=True)
            result["status"] = "complete"
    except Exception as exc:
        result.update(status="stopped",stop_reason=str(exc) if isinstance(exc,StopProbe) else type(exc).__name__)
    finally:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        calls = result["calls"]
        result["counts"] = dict(auth=sum(c["api"]=="auth" for c in calls),
                                graphql=sum(c["api"]!="auth" for c in calls))
        if calls:
            elapsed = calls[-1]["offset_seconds"]+calls[-1]["http_seconds"]
            result.update(http_observation_seconds=elapsed,
                          observed_rpm_including_auth=len(calls)*60/elapsed,
                          peak_starts_in_60_seconds=max(sum(a["offset_seconds"] <= b["offset_seconds"] < a["offset_seconds"]+60 for b in calls) for a in calls))
        save()
        print(json.dumps({k:result[k] for k in ("status","counts","finished_at")}),flush=True)
    return 0 if result["status"] == "complete" else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mode", choices=("small","coverage"),default="small")
    args = parser.parse_args(argv)
    plan = json.loads((OUT/"plan.json").read_text(encoding="utf-8"))
    source = ROOT / plan["sample"]["source"]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == plan["sample"]["source_sha256"]
    keys = json.loads(source.read_text(encoding="utf-8"))["sample_keys"][:100]
    assert len(keys) == len(set(keys)) == 100
    assert hashlib.sha256(json.dumps(keys,separators=(',',':')).encode()).hexdigest() == plan["sample"]["ordered_keys_sha256"]
    if args.mode == "coverage":
        return coverage_run(plan,keys,args.execute)
    cases = plan["phases"][0]["requests"]
    assert len(cases) == 8 and plan["transport"]["min_start_interval_seconds"] == 3
    if not args.execute:
        print(json.dumps(dict(execute=False,reads=8,auth=1,interval_seconds=3)))
        return 0
    logging.disable(logging.CRITICAL)
    result = dict(started_at=datetime.now(timezone.utc).isoformat(), status="running",
                  authorization="User explicitly waived account-budget verification for this bounded run",
                  account_usage_verified=False, workers=1, min_start_interval_seconds=3,
                  automatic_retries=0, timeout_connect_read=[5,45], calls=[], cases=[],
                  sample_sha256=plan["sample"]["ordered_keys_sha256"],
                  fields_sha256=hashlib.sha256(ITEM_FIELDS.encode()).hexdigest(),
                  timing_note="response.elapsed includes connection/server/header time; post_headers is an approximate remainder, not isolated body time; bytes are decoded content")
    def save():
        pending = OUT/"measurement.pending"
        pending.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
        pending.replace(OUT/"measurement.json")
    save()
    try:
        username, password = load_credentials(ROOT)
        with MeasuredSession(result, save) as session:
            provider = JwtProvider(username,password,"production",45,session=session)
            client = OwnerclanClient(provider,"production",RateLimiter(0),45,0,300,session=session)
            provider.token()
            for spec in cases:
                batch = keys[slice(*spec["sample_slice"])]
                query = item_query(batch[0]) if spec["api"] == "item" else items_by_keys_query(batch)
                if spec["fields"] == "minimal":
                    query = query.replace(ITEM_FIELDS,"key price status")
                assert hashlib.sha256(query.encode()).hexdigest() == spec["query_sha256"]
                session.context = {k:spec[k] for k in ("api","batch_size","fields","query_sha256")}
                case = dict(session.context, complete=False, assessment=None)
                result["cases"].append(case)
                started = time.monotonic()
                try:
                    data = client.graphql(query)
                    nodes = data.get(spec["api"])
                    if spec["api"] == "item":
                        nodes = [nodes] if isinstance(nodes,dict) else []
                    case["assessment"] = assess(batch,nodes)
                    case["complete"] = case["assessment"]["complete"]
                    if not case["complete"]:
                        raise StopProbe("incomplete_or_invalid_products")
                finally:
                    case["wall_seconds"] = time.monotonic()-started
                    normal = (case["assessment"] or {}).get("normal_products",0)
                    case["products_per_minute"] = normal*60/case["wall_seconds"]
                    case["observed_rpm"] = 60/case["wall_seconds"]
                    case["all_requested_obtained_seconds"] = case["wall_seconds"] if case["complete"] else None
                    save()
                    print(json.dumps(case),flush=True)
            result["status"] = "complete"
    except Exception as exc:
        result["status"] = "stopped"
        result["stop_reason"] = str(exc) if isinstance(exc,StopProbe) else type(exc).__name__
    finally:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        calls = result["calls"]
        result["counts"] = dict(auth=sum(c["api"]=="auth" for c in calls),
                                graphql=sum(c["api"]!="auth" for c in calls))
        if calls:
            elapsed = calls[-1]["offset_seconds"]+calls[-1]["http_seconds"]
            result["http_observation_seconds"] = elapsed
            result["observed_rpm_including_auth"] = len(calls)*60/elapsed
            result["peak_starts_in_60_seconds"] = max(sum(a["offset_seconds"] <= b["offset_seconds"] < a["offset_seconds"]+60 for b in calls) for a in calls)
        save()
        print(json.dumps({k:result[k] for k in ("status","counts","finished_at")}),flush=True)
    return 0 if result["status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
