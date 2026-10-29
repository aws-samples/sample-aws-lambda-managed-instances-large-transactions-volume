"""
Load Worker Lambda
==================
Each invocation fires `num_requests` concurrent HTTP calls against the
Ticketing API, cycling through:
  1. POST /queue/position   — get a queue spot
  2. POST /inventory/lock   — lock a seat
  3. POST /booking/confirm  — confirm the booking

The worker uses ThreadPoolExecutor for true concurrency inside the
execution environment (I/O-bound HTTP calls).  This is safe in LMI's
multi-concurrent model because all threads share the same process.

After completing all requests the worker writes aggregate latency and
error metrics to DynamoDB for the dashboard to consume.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import boto3
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION"))
_metrics_table = _dynamodb.Table(os.environ.get("METRICS_TABLE", "sim-metrics"))

# Reuse the connection pool across requests in the same execution environment
_http = urllib3.PoolManager(
    num_pools=50,
    maxsize=50,
    retries=urllib3.Retry(total=2, backoff_factor=0.1),
    timeout=urllib3.Timeout(connect=2.0, read=5.0),
)

# SigV4 signer for the ticketing API. The ticketing API is deployed with
# AuthorizationType: AWS_IAM so every POST we make must carry a SigV4
# signature. SigV4Auth is thread-safe for independent calls because its
# internal state is only the credentials reference (botocore refreshes
# STS session tokens on the shared credentials object as needed).
_boto_session = boto3.Session()
_sigv4_credentials = _boto_session.get_credentials()
_SIGV4_REGION = _boto_session.region_name or os.environ.get("AWS_REGION") or "us-east-1"
_sigv4 = SigV4Auth(_sigv4_credentials, "execute-api", _SIGV4_REGION) if _sigv4_credentials else None
if _sigv4_credentials is None:
    logger.warning(
        "No AWS credentials for load_worker SigV4 signer; "
        "ticketing API calls will return 403 until the Lambda role is configured."
    )

# Default to 7-day TTL for metrics records
_METRICS_TTL_DAYS = 7


@dataclass
class RequestResult:
    endpoint: str
    status_code: int
    duration_ms: float
    error: str | None = None


@dataclass
class BurstStats:
    results: list[RequestResult] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.status_code < 400 and r.error is None)

    @property
    def error_count(self) -> int:
        return len(self.results) - self.success_count

    @property
    def confirm_success_count(self) -> int:
        """Count of successful POST /booking/confirm responses.
        Represents durable bookings that exist in DynamoDB after the wave —
        distinct from `success_count`, which counts HTTP 2xx responses across
        all three sequential steps (queue-position, inventory-lock,
        booking-confirm) that a single user session may fire.
        Accepts any 2xx: 201 = fresh write, 200 = idempotent replay
        (the row was already present)."""
        return sum(
            1
            for r in self.results
            if r.endpoint == "confirm"
            and 200 <= r.status_code < 300
            and r.error is None
        )

    @property
    def p50(self) -> float:
        return self._percentile(50)

    @property
    def p95(self) -> float:
        return self._percentile(95)

    @property
    def p99(self) -> float:
        return self._percentile(99)

    def _percentile(self, pct: int) -> float:
        durations = sorted(r.duration_ms for r in self.results)
        if not durations:
            return 0.0
        idx = int(len(durations) * pct / 100)
        return durations[min(idx, len(durations) - 1)]


def _post(url: str, payload: dict[str, Any]) -> RequestResult:
    """SigV4-sign a POST to the ticketing API and return timing/status.

    The ticketing API's AuthorizationType is AWS_IAM, so every request
    carries SigV4 signatures (Authorization, X-Amz-Date, X-Amz-Security-Token
    for STS sessions, X-Amz-Content-SHA256). We build the request with
    botocore.AWSRequest + SigV4Auth, then transfer the signed headers
    verbatim to a urllib3 call so the hot path still uses the pre-warmed
    connection pool. Content-Length is omitted from the handed-off header
    set because urllib3 computes it from the body bytes we pass in.
    """
    start = time.monotonic_ns()
    endpoint = url.split("/")[-1]
    body = json.dumps(payload).encode()
    try:
        if _sigv4 is None:
            raise RuntimeError("SigV4 signer unavailable")
        aws_req = AWSRequest(
            method="POST",
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        _sigv4.add_auth(aws_req)
        signed_headers = {k: v for k, v in aws_req.headers.items() if k.lower() != "content-length"}
        resp = _http.request(
            "POST",
            url,
            body=body,
            headers=signed_headers,
        )
        duration_ms = (time.monotonic_ns() - start) / 1_000_000
        return RequestResult(
            endpoint=endpoint,
            status_code=resp.status,
            duration_ms=duration_ms,
        )
    except Exception as exc:  # noqa: BLE001
        duration_ms = (time.monotonic_ns() - start) / 1_000_000
        return RequestResult(
            endpoint=endpoint,
            status_code=0,
            duration_ms=duration_ms,
            error=str(exc),
        )


def _simulate_full_flow(
    base_url: str,
    user_id: str,
    event_id: str,
    seat_id: str,
) -> list[RequestResult]:
    """
    Execute the full ticket purchase flow for one simulated user.
    Returns results for all three steps.
    """
    results: list[RequestResult] = []

    # Step 1: queue position
    qp_result = _post(
        f"{base_url}/queue/position",
        {"event_id": event_id, "user_id": user_id, "request_id": str(uuid.uuid4())},
    )
    results.append(qp_result)

    # Step 2: inventory lock (only attempt if queue step succeeded)
    if qp_result.status_code == 200:
        lock_result = _post(
            f"{base_url}/inventory/lock",
            {"event_id": event_id, "seat_id": seat_id, "user_id": user_id},
        )
        results.append(lock_result)

        # Step 3: confirm (only attempt if lock step succeeded)
        if lock_result.status_code == 200:
            booking_id = str(uuid.uuid4())
            confirm_result = _post(
                f"{base_url}/booking/confirm",
                {
                    "event_id": event_id,
                    "seat_id": seat_id,
                    "user_id": user_id,
                    "booking_id": booking_id,
                },
            )
            results.append(confirm_result)

    return results


def _write_metrics(
    run_id: str,
    wave: int,
    stats: BurstStats,
) -> None:
    ts_ms = int(time.time() * 1000)
    ttl = int(time.time()) + _METRICS_TTL_DAYS * 86400
    # DynamoDB's resource interface rejects Python float — convert to Decimal.
    # Wrapping in str() first avoids float-to-Decimal precision artefacts.
    _metrics_table.put_item(
        Item={
            "run_id": run_id,
            "ts_ms": ts_ms,
            "wave": wave,
            "success_count": stats.success_count,
            "error_count": stats.error_count,
            "confirm_success_count": stats.confirm_success_count,
            # DynamoDB (via boto3 resource) rejects native floats; use Decimal
            "p50_ms": Decimal(str(round(stats.p50, 2))),
            "p95_ms": Decimal(str(round(stats.p95, 2))),
            "p99_ms": Decimal(str(round(stats.p99, 2))),
            "ttl": ttl,
        }
    )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    run_id: str = event.get("run_id", "unknown")
    wave: int = int(event.get("wave", 0))
    event_id: str = event.get("event_id", "EVT-001")
    seat_offset: int = int(event.get("seat_offset", 0))
    num_requests: int = int(event.get("num_requests", 50))
    base_url: str = event.get("ticketing_endpoint", os.environ.get("TICKETING_API_ENDPOINT", ""))
    # Seat pool size must match SimulatedSeatsPerEvent in the simulation template;
    # otherwise workers cycle through a smaller ID space and hit lock contention
    # long before the configured pool is exhausted.
    seats_per_event: int = int(
        event.get("seats_per_event")
        or os.environ.get("SIMULATED_SEATS_PER_EVENT", "2000000")
    )

    if not base_url:
        logger.error("ticketing_endpoint is not set")
        return {"statusCode": 500, "error": "ticketing_endpoint not set"}

    stats = BurstStats()

    # Spawn num_requests concurrent flows. At 1024 MB memory Lambda gets
    # ~0.5 vCPU, enough for 128 I/O-bound threads (each blocks on urllib3
    # network I/O ~90% of the time). Combined with MAX_TOTAL_WORKERS=2000
    # in the orchestrator, this puts the deliverable-TPS ceiling at ~65K.
    with ThreadPoolExecutor(max_workers=min(num_requests, 128)) as pool:
        futures = [
            pool.submit(
                _simulate_full_flow,
                base_url,
                f"user-{run_id[:8]}-{i}",
                event_id,
                f"seat-{(seat_offset + i) % seats_per_event:07d}",
            )
            for i in range(num_requests)
        ]
        for fut in as_completed(futures):
            try:
                results = fut.result()
                stats.results.extend(results)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Worker thread error: %s", exc)
                stats.results.append(
                    RequestResult(
                        endpoint="unknown",
                        status_code=0,
                        duration_ms=0,
                        error=str(exc),
                    )
                )

    # Persist metrics
    try:
        _write_metrics(run_id, wave, stats)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to write metrics: %s", exc)

    logger.info(
        "Wave complete",
        extra={
            "run_id": run_id,
            "wave": wave,
            "success": stats.success_count,
            "errors": stats.error_count,
            "p99_ms": stats.p99,
        },
    )

    return {
        "statusCode": 200,
        "run_id": run_id,
        "wave": wave,
        "success_count": stats.success_count,
        "error_count": stats.error_count,
        "p99_ms": round(stats.p99, 2),
    }
