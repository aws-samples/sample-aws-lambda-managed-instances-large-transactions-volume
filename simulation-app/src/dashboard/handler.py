"""
Metrics Dashboard Lambda
========================
Queries the SimMetrics DynamoDB table for a given run_id and returns
an aggregated performance summary including:
  - Total requests, success rate, error breakdown
  - p50 / p95 / p99 latency across all waves
  - Estimated peak TPS derived from wave timestamps
  - Per-wave time series data for plotting

GET /dashboard
  ?run_id=<uuid>                  (required)
  &last_n_waves=<int>             (optional; default: fetch all waves)
  &event_id=<string>              (required when include_ground_truth or
                                   include_ee_stats is set)
  &include_ground_truth=true      (optional; adds ~5-15s: API Gateway Count
                                   from CloudWatch + parallel DDB Scan of
                                   the ticketing bookings table)
  &include_ee_stats=true          (optional; adds ~5-15s: Log Insights on
                                   each ticketing handler's log group +
                                   per-handler Lambda CloudWatch metrics)

When both include_ground_truth and include_ee_stats are set, the two
enrichment paths run in parallel so total wall time is roughly the
larger of the two, not their sum.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION"))
_metrics_table = _dynamodb.Table(os.environ.get("METRICS_TABLE", "sim-metrics"))
_cloudwatch = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION"))
_logs = boto3.client("logs", region_name=os.environ.get("AWS_REGION"))
_bookings_table_name = os.environ.get("BOOKINGS_TABLE_NAME", "ticketing-lmi-bookings")
_ticketing_api_name = os.environ.get("TICKETING_API_NAME", "ticketing-lmi-api")
_ticketing_api_stage = os.environ.get("TICKETING_API_STAGE", "v1")
# Ticketing handler log groups — queried for post-burst EE analysis
_TICKETING_HANDLERS = ["queue-position", "inventory-lock", "booking-confirm"]
_TICKETING_STACK_PREFIX = os.environ.get("TICKETING_STACK_PREFIX", "ticketing-lmi")
# Cross-stack bookings scan feature flag. "true"/"false" from the template
# parameter EnableGroundTruthScan. When "false" (default), the handler
# short-circuits ?include_ground_truth=true with 403, matching the IAM
# posture: the SimulationGroundTruthPolicy is not attached either, so a
# scan attempt would also fail with AccessDenied on principle. Keeping
# both controls together (feature flag + IAM) means operators can inspect
# intent at either layer.
_GROUND_TRUTH_ENABLED = os.environ.get("ENABLE_GROUND_TRUTH_SCAN", "false").lower() == "true"


def _query_waves(run_id: str, limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch ALL waves for a run, paginating through DDB Query results.
    limit=None means no cap (used by default so aggregates are complete);
    pass an integer to short-circuit for huge runs."""
    items: list[dict[str, Any]] = []
    last_key: dict[str, Any] | None = None
    while True:
        kwargs: dict[str, Any] = {
            "KeyConditionExpression": Key("run_id").eq(run_id),
            "ScanIndexForward": False,
        }
        if last_key is not None:
            kwargs["ExclusiveStartKey"] = last_key
        response = _metrics_table.query(**kwargs)
        items.extend(response.get("Items", []))
        if limit is not None and len(items) >= limit:
            items = items[:limit]
            break
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
    # Return in chronological order
    return sorted(items, key=lambda x: int(x.get("ts_ms", 0)))


def _aggregate(waves: list[dict[str, Any]]) -> dict[str, Any]:
    if not waves:
        return {"error": "no data found for this run_id"}

    total_success = sum(int(w.get("success_count", 0)) for w in waves)
    total_errors = sum(int(w.get("error_count", 0)) for w in waves)
    total_requests = total_success + total_errors
    # `success_count` above is HTTP 200s across all 3 sequential steps
    # (queue-position, inventory-lock, booking-confirm). The true count of
    # durable bookings written to DynamoDB is the confirm-only success total.
    # Older wave records won't have `confirm_success_count` — fall back to 0.
    total_bookings_confirmed = sum(
        int(w.get("confirm_success_count", 0)) for w in waves
    )

    p99_values = [float(w["p99_ms"]) for w in waves if "p99_ms" in w]
    p95_values = [float(w["p95_ms"]) for w in waves if "p95_ms" in w]
    p50_values = [float(w["p50_ms"]) for w in waves if "p50_ms" in w]

    # Estimate peak TPS: find the 1-second window with the most requests
    # Use a simple sliding window over wave timestamps
    peak_tps = 0
    if len(waves) >= 2:
        ts_first = int(waves[0]["ts_ms"])
        ts_last = int(waves[-1]["ts_ms"])
        duration_sec = max((ts_last - ts_first) / 1000, 1)
        peak_tps = int(total_requests / duration_sec)

    time_series = [
        {
            "wave": int(w.get("wave", 0)),
            "ts_ms": int(w.get("ts_ms", 0)),
            "success_count": int(w.get("success_count", 0)),
            "error_count": int(w.get("error_count", 0)),
            "p50_ms": float(w.get("p50_ms", 0)),
            "p95_ms": float(w.get("p95_ms", 0)),
            "p99_ms": float(w.get("p99_ms", 0)),
        }
        for w in waves
    ]

    return {
        "total_requests": total_requests,
        "total_success": total_success,
        "total_errors": total_errors,
        "total_bookings_confirmed": total_bookings_confirmed,
        "success_rate_pct": round(total_success / max(total_requests, 1) * 100, 2),
        "error_rate_pct": round(total_errors / max(total_requests, 1) * 100, 2),
        "estimated_peak_tps": peak_tps,
        "latency": {
            "p50_ms": round(statistics.median(p50_values), 2) if p50_values else None,
            "p95_ms": round(statistics.median(p95_values), 2) if p95_values else None,
            "p99_ms": round(statistics.median(p99_values), 2) if p99_values else None,
            "max_p99_ms": round(max(p99_values), 2) if p99_values else None,
        },
        "waves_sampled": len(waves),
        "time_series": time_series,
    }


def _fetch_apigw_count(start_ts_ms: int, end_ts_ms: int) -> dict[str, Any]:
    """Query CloudWatch for API Gateway `Count` metric across the burst window.
    Returns the number of requests that reached the ticketing API Gateway,
    which is the AWS-side ground truth for HTTP throughput.

    Notes:
    - Query uses (ApiName, Stage) dimensions rather than ApiName alone.
      The ApiName-only aggregate metric has late-arriving data — recent
      minute-buckets frequently show as low as 1/3 of the (ApiName, Stage)
      count until CloudWatch fully reconciles (up to several minutes).
    - CloudWatch buckets at 1-minute granularity. We round the window out
      to whole minutes on both sides, then extend the end by 3 minutes to
      pick up the standard 60-90s ingestion lag.
    """
    start_dt = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc)
    start_dt = start_dt.replace(second=0, microsecond=0)
    end_dt = end_dt.replace(second=0, microsecond=0) + timedelta(minutes=3)

    try:
        resp = _cloudwatch.get_metric_statistics(
            Namespace="AWS/ApiGateway",
            MetricName="Count",
            Dimensions=[
                {"Name": "ApiName", "Value": _ticketing_api_name},
                {"Name": "Stage", "Value": _ticketing_api_stage},
            ],
            StartTime=start_dt,
            EndTime=end_dt,
            Period=60,
            Statistics=["Sum"],
        )
        points = resp.get("Datapoints", [])
        total = int(sum(p["Sum"] for p in points))
        return {
            "count": total,
            "buckets_returned": len(points),
            "window_start": start_dt.isoformat(),
            "window_end": end_dt.isoformat(),
            "api_name": _ticketing_api_name,
            "stage": _ticketing_api_stage,
        }
    except Exception as exc:  # noqa: BLE001
        # Log the exception detail server-side; the client sees a generic
        # error code only. Operators correlate via CloudWatch Logs.
        logger.warning("API Gateway ground truth fetch failed", exc_info=True)
        return {"error": "ground_truth_fetch_failed", "api_name": _ticketing_api_name}


def _scan_segment(
    segment: int,
    total_segments: int,
    event_id: str,
    deadline: float,
) -> tuple[int, int, bool]:
    """Scan a single segment of the bookings table with an event_id filter.
    Runs until it exhausts the segment or hits `deadline` (wall-clock).
    Returns (matched_count, scanned_count, truncated_flag).
    """
    matched = 0
    scanned = 0
    last_key: dict[str, Any] | None = None
    truncated = False

    while True:
        if time.time() > deadline:
            truncated = True
            break
        kwargs: dict[str, Any] = {
            "TableName": _bookings_table_name,
            "FilterExpression": Attr("event_id").eq(event_id),
            "Select": "COUNT",
            "Segment": segment,
            "TotalSegments": total_segments,
            "Limit": 5000,
        }
        if last_key is not None:
            kwargs["ExclusiveStartKey"] = last_key
        resp = _dynamodb.meta.client.scan(**kwargs)
        matched += int(resp.get("Count", 0))
        scanned += int(resp.get("ScannedCount", 0))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return matched, scanned, truncated


def _scan_bookings_count(event_id: str, timeout_s: float = 20.0) -> dict[str, Any]:
    """Parallel-scan the ticketing bookings table with a filter on event_id
    and return the count of matching rows — the true count of committed
    bookings for this burst.

    We split the table into 10 segments and scan them concurrently via
    ThreadPoolExecutor. On a ~1M-row table this cuts wall-clock from
    ~30s (single-threaded, which exceeds API Gateway's 29s hard timeout)
    to ~3-5s.

    Timeout budget is deliberately conservative (20s) to leave headroom
    for the CloudWatch call and API Gateway response serialisation while
    still fitting inside the 29s API GW limit.
    """
    if not event_id:
        return {"error": "event_id required for bookings ground truth"}

    total_segments = 10
    started = time.time()
    deadline = started + timeout_s
    total_matched = 0
    total_scanned = 0
    truncated_any = False

    try:
        with ThreadPoolExecutor(max_workers=total_segments) as pool:
            futures = [
                pool.submit(_scan_segment, seg, total_segments, event_id, deadline)
                for seg in range(total_segments)
            ]
            for f in futures:
                m, s, t = f.result()
                total_matched += m
                total_scanned += s
                truncated_any = truncated_any or t
    except Exception as exc:  # noqa: BLE001
        # Scan failures are expected when the ground-truth IAM policy is
        # not attached. Log the detail server-side; client sees a generic
        # error code.
        logger.warning("Parallel bookings scan failed", exc_info=True)
        return {"error": "bookings_scan_failed", "table": _bookings_table_name}

    result = {
        "count": total_matched,
        "scanned_rows": total_scanned,
        "elapsed_s": round(time.time() - started, 2),
        "table": _bookings_table_name,
        "event_id": event_id,
        "segments": total_segments,
    }
    if truncated_any:
        result["truncated"] = True
        result["note"] = f"One or more segments hit the {timeout_s}s deadline; count is a lower bound"
    return result


def _run_log_query(log_group: str, start_s: int, end_s: int, query: str, deadline: float) -> list[dict[str, str]]:
    """Start a Logs Insights query and poll until complete (or deadline).
    Returns rows as [{field: value, ...}, ...]."""
    r = _logs.start_query(
        logGroupName=log_group, startTime=start_s, endTime=end_s, queryString=query
    )
    qid = r["queryId"]
    while time.time() < deadline:
        s = _logs.get_query_results(queryId=qid)
        if s["status"] in ("Complete", "Failed", "Cancelled"):
            return [{c["field"]: c["value"] for c in row} for row in s.get("results", [])]
        # CloudWatch Logs Insights has no completion callback; polling is
        # the documented pattern. 400 ms balances responsiveness against
        # get_query_results API cost across the dashboard's parallel queries.
        time.sleep(0.4)  # nosemgrep: arbitrary-sleep
    logger.warning("Log query timed out for %s", log_group)
    return []


def _lambda_metrics_for_handler(handler: str, start_s: int, end_s: int) -> dict[str, Any]:
    """Fetch per-handler Lambda-side operational metrics from CloudWatch:
    - Throttles (Sum over window) — count of invocations rejected by Lambda
      for capacity reasons; near-zero at steady state means the pre-warmed
      pool absorbed the burst without hitting LMI's concurrency ceiling.
    - Duration percentiles (p50/p95/p99 across 1-min buckets, max taken to
      capture peak) — how long handler code ran, not counting API GW hops.
    Distinct from the aggregate latency computed in _aggregate() which is
    the median across wave records from the sim.
    """
    fn = f"{_TICKETING_STACK_PREFIX}-{handler}"
    start_dt = datetime.fromtimestamp(start_s, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_s, tz=timezone.utc)
    result: dict[str, Any] = {}
    try:
        thr = _cloudwatch.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Throttles",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start_dt, EndTime=end_dt, Period=60, Statistics=["Sum"],
        )
        result["throttles"] = int(sum(p["Sum"] for p in thr.get("Datapoints", [])))
    except Exception:  # noqa: BLE001
        # Generic error flag only. The exception detail stays in CloudWatch
        # Logs for operator correlation.
        logger.warning("Throttles metric fetch failed", extra={"function": fn}, exc_info=True)
        result["throttles_error"] = "metric_fetch_failed"

    try:
        dur = _cloudwatch.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start_dt, EndTime=end_dt, Period=60,
            ExtendedStatistics=["p50", "p95", "p99"],
        )
        points = dur.get("Datapoints", [])
        # Peak of 1-min-bucket percentiles across the burst window
        p50 = max((float(p.get("ExtendedStatistics", {}).get("p50", 0)) for p in points), default=0)
        p95 = max((float(p.get("ExtendedStatistics", {}).get("p95", 0)) for p in points), default=0)
        p99 = max((float(p.get("ExtendedStatistics", {}).get("p99", 0)) for p in points), default=0)
        result["duration_p50_ms"] = round(p50, 2)
        result["duration_p95_ms"] = round(p95, 2)
        result["duration_p99_ms"] = round(p99, 2)
    except Exception:  # noqa: BLE001
        logger.warning("Duration metric fetch failed", extra={"function": fn}, exc_info=True)
        result["duration_error"] = "metric_fetch_failed"

    return result


def _ee_stats_for_handler(handler: str, start_s: int, end_s: int, event_id: str, deadline: float) -> dict[str, Any]:
    """Derive execution-environment count for one handler over a time window.
    Each log stream = one EE lifetime, so `count_distinct(@logStream)` on
    log records tagged with this burst's event_id gives the number of EEs
    that actually served requests for the burst — unaffected by any
    concurrent traffic in the same window.

    Falls back to `@type = "REPORT"` (any invocation) when event_id is
    empty — for bursts fired before structured logging shipped, or when
    the caller can't supply event_id."""
    log_group = f"/aws/lambda/{_TICKETING_STACK_PREFIX}-{handler}"

    if event_id:
        query = f'filter event_id = "{event_id}" | stats count_distinct(@logStream) as ees'
    else:
        query = 'filter @type = "REPORT" | stats count_distinct(@logStream) as ees'

    try:
        rows = _run_log_query(log_group, start_s, end_s, query, deadline)
    except Exception as exc:  # noqa: BLE001
        # Log the exception detail server-side; the client sees a generic
        # error code only. Operators correlate via CloudWatch Logs.
        logger.warning("EE stats query failed", extra={"handler": handler}, exc_info=True)
        return {"handler": handler, "error": "ee_stats_query_failed"}

    distinct_ees = int(rows[0]["ees"]) if rows else 0

    return {
        "handler": handler,
        "distinct_ees_used": distinct_ees,
        "log_group": log_group,
        # Lambda-side CloudWatch metrics over the same window: Throttles
        # (Sum) + Duration percentiles. Small CW call (~200ms) relative to
        # the Log Insights query above, so no need to parallelise.
        "lambda_metrics": _lambda_metrics_for_handler(handler, start_s, end_s),
    }


def _fetch_ee_stats(waves: list[dict[str, Any]], event_id: str = "") -> dict[str, Any]:
    """Post-burst EE analysis. Uses the burst's wave timestamps to bound
    the log-insights query window, and — when event_id is provided —
    additionally filters invocation counts to only records logged for
    that specific burst.
    """
    if not waves:
        return {"error": "no waves; cannot determine burst time window"}

    ts_values = [int(w.get("ts_ms", 0)) for w in waves]
    start_ms = min(ts_values) - 60_000    # 60s pre-burst pad
    end_ms = max(ts_values) + 180_000     # 180s post-burst pad (catch late init events)
    start_s = start_ms // 1000
    end_s = end_ms // 1000

    deadline = time.time() + 15.0   # 15s budget for all log queries
    handlers = []
    with ThreadPoolExecutor(max_workers=len(_TICKETING_HANDLERS)) as pool:
        futures = [
            pool.submit(_ee_stats_for_handler, h, start_s, end_s, event_id, deadline)
            for h in _TICKETING_HANDLERS
        ]
        for f in futures:
            handlers.append(f.result())

    return {
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "event_id_filter": event_id or None,
        "handlers": handlers,
    }


def _fetch_ground_truth(
    waves: list[dict[str, Any]],
    event_id: str,
) -> dict[str, Any]:
    """Compute AWS-side ground truth: API Gateway Count + bookings-table Scan.
    Uses the wave records to determine the burst time window.
    """
    if not waves:
        return {"error": "no waves; cannot determine burst time window"}

    ts_values = [int(w.get("ts_ms", 0)) for w in waves]
    start_ts = min(ts_values)
    end_ts = max(ts_values)

    return {
        "api_gateway": _fetch_apigw_count(start_ts, end_ts),
        "bookings": _scan_bookings_count(event_id),
    }


def _build_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    """Build an API Gateway proxy response.

    CORS is enforced at the API Gateway stage (SAM Cors property) with the
    AllowedOrigin parameter, so no Access-Control-Allow-Origin header is
    emitted here. Keeping CORS in one place (the gateway) prevents a
    handler and the gateway from disagreeing on allowed origins.
    """
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    try:
        params = event.get("queryStringParameters") or {}
        run_id: str = params.get("run_id", "").strip()
        # Default None = fetch ALL waves for the run (paginated). Callers may
        # pass ?last_n_waves=N to short-circuit for extremely large runs.
        last_n_raw = params.get("last_n_waves")
        last_n: int | None = int(last_n_raw) if last_n_raw else None

        # Ground-truth flag: also fetch API Gateway Count (from CloudWatch)
        # and the durable bookings count (from a scan of the ticketing
        # bookings table). This path takes 5-15s on large tables and is
        # opt-in via `?include_ground_truth=true`. The cross-stack bookings
        # scan is additionally gated by the ENABLE_GROUND_TRUTH_SCAN feature
        # flag (template parameter EnableGroundTruthScan); when the flag is
        # off we return 403 immediately rather than attempting the scan
        # with an unattached IAM policy.
        include_gt = str(params.get("include_ground_truth", "")).lower() in ("1", "true", "yes")
        if include_gt and not _GROUND_TRUTH_ENABLED:
            request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
            logger.warning(
                "Ground-truth path requested but disabled by operator",
                extra={"request_id": request_id, "status_code": 403},
            )
            return _build_response(
                403,
                {
                    "error": "ground_truth_disabled",
                    "detail": "Set EnableGroundTruthScan=true at deploy to enable the cross-stack bookings-table scan.",
                    "request_id": request_id,
                },
            )
        event_id: str = (params.get("event_id") or "").strip()

        # EE-stats flag: also derive per-handler execution-environment stats
        # from CloudWatch Logs Insights (one log stream = one EE lifetime).
        # Adds 5-15s to the response depending on log volume; opt-in via
        # `?include_ee_stats=true`.
        include_ee = str(params.get("include_ee_stats", "")).lower() in ("1", "true", "yes")

        if not run_id:
            return _build_response(400, {"error": "run_id query parameter is required"})

        waves = _query_waves(run_id, limit=last_n)
        summary = _aggregate(waves)
        summary["run_id"] = run_id

        # Ground truth (CloudWatch + DDB Scan) and EE stats (Log Insights +
        # CloudWatch) are I/O-bound and independent — run them in parallel
        # when both are requested so the /dashboard call finishes in
        # roughly max(gt, ee) instead of gt + ee. The UI always sets both
        # flags, so this halves user-visible latency on "View results".
        if include_gt and include_ee:
            with ThreadPoolExecutor(max_workers=2) as pool:
                gt_future = pool.submit(_fetch_ground_truth, waves, event_id)
                ee_future = pool.submit(_fetch_ee_stats, waves, event_id)
                summary["ground_truth"] = gt_future.result()
                summary["ee_stats"] = ee_future.result()
        elif include_gt:
            summary["ground_truth"] = _fetch_ground_truth(waves, event_id)
        elif include_ee:
            summary["ee_stats"] = _fetch_ee_stats(waves, event_id)

        return _build_response(200, summary)

    except Exception as exc:  # noqa: BLE001
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error("Dashboard error", extra={"request_id": request_id, "error": str(exc)}, exc_info=True)
        # Generic response: internal detail stays in CloudWatch, correlatable
        # by request_id.
        return _build_response(500, {"error": "internal_server_error", "request_id": request_id})
