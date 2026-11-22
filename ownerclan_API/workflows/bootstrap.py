"""Resumable full catalogue observation plus independently durable history catch-up."""
from ..api.factory import make_client
from ..api.queries import all_items_query
from ..api.client import OwnerclanGraphQLError
from ..services.normalization import extract_connection_items, normalize_item
from ..persistence.storage import CheckpointLock, load_state, save_state
from product_data_collector.common.collection_state import next_page_cursor, collection_outcome, validate_limits
from product_data_collector.persistence.postgres_storage import save_products_with_raw_samples_if_enabled, require_product_save
from .history_sync import collect_history, clock, failure_reason


def bootstrap(project_root, config, *, client=None, page_limit=None, dry_run=False, mode="bootstrap"):
    """allItems is a live traversal, not a provider-certified atomic snapshot.

    Conservative run-start observation protects newer detail writes. Capture
    histories before the scan and after EVERY page (also on resumed runs). This
    prevents long scans from waiting until retention has expired to start capture.
    End-of-scan reconciliation replays the whole overlapping scan interval.
    Discovery/recovery start a new traversal after completion; bootstrap stays done.
    """
    validate_limits(page_limit=page_limit)
    if mode not in {"bootstrap", "discovery", "recovery"}:
        raise ValueError("Invalid catalogue mode")
    path = config.output.state_dir / f"{mode}-state.json"
    client = client or make_client(project_root, config)
    with CheckpointLock(path.with_suffix(".lock")):
        state = load_state(path)
        if state.get("completed") and mode == "bootstrap":
            return {"status": "completed", "failureCount": 0, "snapshotComplete": True}
        if not state or state.get("completed"):
            state = {"startedAt": clock(), "after": None, "processedCount": 0,
                     "snapshotComplete": False, "completed": False}
        def checkpoint():
            if not dry_run:
                save_state(path, state)
        checkpoint()
        pages = 0
        seen = set(state.get("seenCursors", []))
        try:
            # Independent daily checkpoint can continue even while snapshot is paused.
            history = collect_history(project_root, config, client=client, dry_run=dry_run)
            if history["status"] != "completed":
                raise RuntimeError("Initial history capture incomplete")
            while not state["snapshotComplete"]:
                if len(seen) >= 100000:
                    raise ValueError("Catalogue pagination safety ceiling reached")
                try:
                    data = client.graphql(all_items_query(first=config.incremental.page_size, after=state["after"]))
                except OwnerclanGraphQLError as exc:
                    if not exc.looks_like_unknown_field():
                        raise
                    data = client.graphql(all_items_query(first=config.incremental.page_size, after=state["after"], minimal=True))
                nodes, info = extract_connection_items(data, "allItems")
                cursor = next_page_cursor(info, state["after"], seen)
                if cursor and not nodes:
                    raise ValueError("Empty non-terminal catalogue page")
                if any(not node.get("key") for node in nodes):
                    raise ValueError("Catalogue item missing key")
                products = [normalize_item(node, state["startedAt"]) for node in nodes]
                if not dry_run:
                    receipt = save_products_with_raw_samples_if_enabled(project_root=project_root,
                        platform="ownerclan", collected_at=state["startedAt"], products=products,
                        raw_sample_limit=config.output.raw_sample_limit)
                    require_product_save(receipt, products)
                state.update(after=cursor, processedCount=state["processedCount"] + len(products), snapshotComplete=cursor is None)
                if cursor:
                    seen.add(cursor)
                state["seenCursors"] = sorted(seen)
                pages += 1
                checkpoint()
                history = collect_history(project_root, config, client=client, dry_run=dry_run)
                if history["status"] != "completed":
                    raise RuntimeError("History capture incomplete during catalogue scan")
                if cursor and page_limit and pages >= page_limit:
                    return {**collection_outcome(pause_reason="page_limit_reached"), "failureCount": 0,
                            "snapshotComplete": False, "processedCount": state["processedCount"]}
            state.setdefault("reconcileTo", clock())
            checkpoint()
            # If the scan has lasted longer than provider retention, do not claim
            # a full replay. Daily captures remain durable; explicit backfill gap
            # evidence is required rather than fabricating completeness.
            result = collect_history(project_root, config, client=client, mode="bootstrap-catchup",
                date_from=state["startedAt"], date_to=state["reconcileTo"], dry_run=dry_run)
            state["completed"] = result["eventCollectionComplete"]
            state["currentStateFreshness"] = "refresh_queue_pending"
            checkpoint()
            return {**collection_outcome(failed=not state["completed"]), "failureCount": int(not state["completed"]),
                    "snapshotComplete": True, "eventCollectionComplete": state["completed"], "detailRefreshComplete": False,
                    "processedCount": state["processedCount"]}
        except Exception as exc:
            state["lastFailure"] = {"type": type(exc).__name__, "reason": failure_reason(exc), "after": state["after"]}
            checkpoint()
            return {"status": "failed", "failureCount": 1, "snapshotComplete": state["snapshotComplete"]}
