"""
Simulation Orchestrator Lambda
==============================
Top-level controller for the concert ticket on-sale load simulation.

Fan-out architecture (scales with target TPS):
  Orchestrator (1)
    └─► Coordinator Lambda × NUM_COORDINATORS         (async invocation)
          └─► Load Worker Lambda × WORKERS_PER_COORD  (async invocation)
                └─► HTTP burst × BURST_SIZE requests

Validated end-to-end at 40K TPS. See docs/test-results.md for the full
progression from 5K → 40K and the calibration story behind the constants
below.

TPS math
--------
Each load worker fires BURST_SIZE concurrent HTTP requests per invocation.

AVG_DURATION_S is a calibration constant, not a raw HTTP round-trip time.
It's the "seconds of wall-clock effort per HTTP request" that makes the
formula below produce enough workers to actually deliver target_tps at
API Gateway. Per-worker overhead includes Lambda init, boto3 warmup,
ThreadPoolExecutor scheduling, 3 sequential HTTP calls per session,
urllib3 retries on the rare failure, and a wave-end DDB metric write.

With BURST_SIZE=100 and AVG_DURATION_S=0.100, one worker delivers:
    100 requests / 0.100 s ≈ 1,000 req/s while running

The orchestrator fires a new wave every TICK_INTERVAL_S seconds. Workers
needed per tick to sustain TARGET_TPS:
    workers_per_tick = ceil(TARGET_TPS / (BURST_SIZE / AVG_DURATION_S))

Example for 5,000 TPS:
    workers_per_tick = ceil(5_000 / (100 / 0.100)) = ceil(5) = 5

Example for 40,000 TPS:
    workers_per_tick = ceil(40_000 / (100 / 0.100)) = 40

Scaling guidance (at AVG_DURATION_S = 0.100 s, MAX_WORKERS_PER_COORD = 100)
--------------------------------------------------------------------------
  TARGET_TPS   | workers_per_tick | coordinators | workers/coord
  -------------|------------------|--------------|---------------
       5,000   |        5         |      1       |       5
      40,000   |       40         |      1       |      40
     100,000   |      100         |      1       |     100
     500,000   |      500         |      5       |     100
   1,000,000   |    1,000         |     10       |     100
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_lambda_client = boto3.client("lambda", region_name=os.environ.get("AWS_REGION"))

# Resolve sibling function names from this function's own name.
# Convention: <stack>-orchestrator, <stack>-coordinator, <stack>-load-worker
_SELF_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
_STACK_PREFIX = _SELF_NAME.rsplit("-orchestrator", 1)[0]
_COORDINATOR_FUNCTION = os.environ.get(
    "COORDINATOR_FUNCTION_NAME", f"{_STACK_PREFIX}-coordinator"
)

# Simulation defaults. These are read-only after module import and are
# used only when the caller does not supply a value. The actual values
# used for a given run live in per-invocation locals inside lambda_handler
# and _run_simulation — never module state — so one caller's parameters
# cannot bleed into the next caller's run in a warm EE.
_DEFAULT_TARGET_TPS = int(os.environ.get("TARGET_TPS", "1000"))
_DEFAULT_RAMP_SECONDS = int(os.environ.get("RAMP_DURATION_SECONDS", "30"))
_DEFAULT_SUSTAIN_SECONDS = int(os.environ.get("SUSTAIN_DURATION_SECONDS", "120"))

# Hard ceilings sourced from CloudFormation parameters. Cost is rate * duration;
# capping only rate (via MAX_TOTAL_WORKERS) left duration unbounded, so a caller
# could request a multi-hour sustain and run the fleet continuously. These
# clamps bound both dimensions server-side and the handler rejects over-cap
# requests with 400 before any coordinator is invoked.
_MAX_TARGET_TPS = int(os.environ.get("MAX_TARGET_TPS", "40000"))
_MAX_RAMP_SECONDS = int(os.environ.get("MAX_RAMP_SECONDS", "60"))
_MAX_SUSTAIN_SECONDS = int(os.environ.get("MAX_SUSTAIN_SECONDS", "300"))

_SIMULATED_EVENTS = os.environ.get("SIMULATED_EVENTS", "EVT-001").split(",")
_SEATS_PER_EVENT = int(os.environ.get("SIMULATED_SEATS_PER_EVENT", "2000000"))
_TICKETING_ENDPOINT = os.environ.get("TICKETING_API_ENDPOINT", "")

# Worker burst size — each worker fires this many concurrent HTTP requests
BURST_SIZE = 100
# Observed per-worker throughput factor (seconds per request equivalent).
# This is not a raw HTTP round-trip time — it's a calibration constant that
# makes the formula `workers = target_tps / (BURST_SIZE / AVG_DURATION_S)`
# produce enough workers to actually deliver target_tps at the API Gateway.
#
# Real-world overhead per worker Lambda invocation includes:
#   - Lambda init + boto3 warmup (~50-100ms)
#   - ThreadPoolExecutor scheduling of 128 concurrent sessions
#   - Per-session 3 sequential HTTP calls (~30-70ms each) with TCP handshake
#   - urllib3 retry backoff on the rare failure
#   - Wave-end DDB metric write (~20-50ms)
#
# Empirical calibration:
#   - At 0.070, formula said 36 workers for 50K target → delivered ~34K
#   - Per-worker actual delivery: 944 rps (66% of theoretical 1428)
#   - Adjusted: 0.070 / 0.66 = 0.106 ≈ 0.10 gives target ≈ delivered
#
# Setting 0.100 spawns ~50 workers for 50K target, ~40 for 40K, ~10 for 10K.
# The MAX_TOTAL_WORKERS=2000 hard cap remains the ultimate ceiling.
AVG_DURATION_S = 0.100
# How often the orchestrator fires a new wave (seconds)
TICK_INTERVAL_S = 0.1   # 10 ticks/sec
# Maximum workers per coordinator invocation (keeps coordinator payload small)
MAX_WORKERS_PER_COORD = 100
# Hard cap on total concurrent worker Lambdas
# Bumped from 1000 → 2000 to raise the load-gen ceiling. Combined with the
# worker's ThreadPoolExecutor(max_workers=128) — up from 64 — the theoretical
# delivered-TPS ceiling roughly doubles from ~34K to ~65K. Higher values
# start to hit Lambda's account-level async invoke throttle.
MAX_TOTAL_WORKERS = 2000


def _workers_needed_per_tick(target_tps: int) -> int:
    """
    How many worker Lambdas to invoke per tick to sustain target_tps.

    Each worker delivers approx BURST_SIZE / AVG_DURATION_S req/s while running.
    We fire a new wave every TICK_INTERVAL_S, and each worker runs for
    ~AVG_DURATION_S seconds, so at any point in time the number of
    *concurrently running* workers equals workers_per_tick.
    """
    req_per_worker_per_s = BURST_SIZE / AVG_DURATION_S  # 100 / 0.100 = 1,000
    workers = math.ceil(target_tps / req_per_worker_per_s)
    return max(1, min(workers, MAX_TOTAL_WORKERS))


def _coordinators_and_workers(total_workers: int) -> tuple[int, int]:
    """
    Split total_workers across coordinators so each coordinator handles
    at most MAX_WORKERS_PER_COORD workers.
    Returns (num_coordinators, workers_per_coordinator).
    """
    num_coords = math.ceil(total_workers / MAX_WORKERS_PER_COORD)
    workers_per_coord = math.ceil(total_workers / num_coords)
    return num_coords, workers_per_coord


def _invoke_coordinator(
    run_id: str,
    wave: int,
    workers_per_coord: int,
    seat_offset: int,
    simulated_events: list[str],
) -> None:
    """Asynchronously invoke a coordinator Lambda."""
    payload = {
        "run_id": run_id,
        "wave": wave,
        "workers_to_spawn": workers_per_coord,
        "seat_offset": seat_offset,
        "ticketing_endpoint": _TICKETING_ENDPOINT,
        "simulated_events": simulated_events,
        "seats_per_event": _SEATS_PER_EVENT,
    }
    _lambda_client.invoke(
        FunctionName=_COORDINATOR_FUNCTION,
        InvocationType="Event",   # fire-and-forget
        Payload=json.dumps(payload).encode(),
    )


def _run_simulation(
    run_id: str,
    target_tps: int,
    ramp_seconds: int,
    sustain_seconds: int,
) -> dict[str, Any]:
    """Run one burst with per-invocation parameters.

    Parameters are passed in (not read from module state) so two concurrent
    invocations in the same EE cannot clobber each other's run shape. All
    three values are already clamped against the MAX_* ceilings in
    lambda_handler before this function is called.
    """
    peak_workers = _workers_needed_per_tick(target_tps)
    num_coords, workers_per_coord = _coordinators_and_workers(peak_workers)

    # Give every burst a fresh (event_id, seat_id) namespace so seat contention
    # resets across runs. Without this, once a run fills the SIMULATED_SEATS_PER_EVENT
    # space with HELD/CONFIRMED rows for a given event_id, all subsequent bursts
    # see ~100% conditional-write failures at inventory-lock and zero bookings
    # reach the confirm step. Deriving event_ids from run_id keeps prior state
    # in DynamoDB (for auditability) without polluting the new run's seat pool.
    run_events = [f"RUN-{run_id[:8]}-{i:02d}" for i in range(len(_SIMULATED_EVENTS))]

    logger.info(
        "Simulation plan",
        extra={
            "run_id": run_id,
            "target_tps": target_tps,
            "peak_workers_per_tick": peak_workers,
            "num_coordinators": num_coords,
            "workers_per_coordinator": workers_per_coord,
            "burst_size_per_worker": BURST_SIZE,
            "theoretical_peak_tps": peak_workers * BURST_SIZE / AVG_DURATION_S,
            "ramp_seconds": ramp_seconds,
            "sustain_seconds": sustain_seconds,
            "run_events": run_events,
        },
    )

    wave = 0
    seat_counter = 0
    total_requests_estimate = 0
    sim_start = time.time()

    # ── Ramp phase ─────────────────────────────────────────────────────────
    ramp_end = sim_start + ramp_seconds
    while time.time() < ramp_end:
        elapsed = time.time() - sim_start
        fraction = elapsed / max(ramp_seconds, 1)
        current_tps = max(1, int(fraction * target_tps))

        active_workers = _workers_needed_per_tick(current_tps)
        active_coords, w_per_coord = _coordinators_and_workers(active_workers)

        for _ in range(active_coords):
            # Advance by (workers_per_coord * BURST_SIZE) so each coord dispatch
            # gets a fresh non-overlapping seat range. Coordinator hands seats
            # to its workers as (seat_offset + i * BURST_SIZE), covering exactly
            # workers_per_coord * BURST_SIZE seats — no wave-to-wave overlap.
            seat_offset = (seat_counter * BURST_SIZE * w_per_coord) % _SEATS_PER_EVENT
            _invoke_coordinator(run_id, wave, w_per_coord, seat_offset, run_events)
            seat_counter += 1

        total_requests_estimate += active_workers * BURST_SIZE
        wave += 1
        # Load-generation heartbeat: paces coordinator dispatches at
        # TICK_INTERVAL_S (0.1 s = 10 Hz) so worker Lambdas stack cleanly
        # across the ramp instead of firing all at once.
        time.sleep(TICK_INTERVAL_S)  # nosemgrep: arbitrary-sleep

    # ── Sustain phase ───────────────────────────────────────────────────────
    sustain_end = time.time() + sustain_seconds
    while time.time() < sustain_end:
        for _ in range(num_coords):
            seat_offset = (seat_counter * BURST_SIZE * workers_per_coord) % _SEATS_PER_EVENT
            _invoke_coordinator(run_id, wave, workers_per_coord, seat_offset, run_events)
            seat_counter += 1

        total_requests_estimate += peak_workers * BURST_SIZE
        wave += 1
        # Same 10 Hz tick loop, holding at peak_workers per tick during sustain.
        time.sleep(TICK_INTERVAL_S)  # nosemgrep: arbitrary-sleep

    elapsed_total = time.time() - sim_start
    return {
        "run_id": run_id,
        "target_tps": target_tps,
        "peak_workers_per_tick": peak_workers,
        "num_coordinators": num_coords,
        "workers_per_coordinator": workers_per_coord,
        "theoretical_peak_tps": int(peak_workers * BURST_SIZE / AVG_DURATION_S),
        "total_waves": wave,
        "estimated_total_requests": total_requests_estimate,
        "duration_seconds": round(elapsed_total, 1),
        "status": "COMPLETED",
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
        "body": json.dumps(body),
    }


def _clamp_run_params(
    target_tps_raw: Any,
    ramp_raw: Any,
    sustain_raw: Any,
) -> tuple[int, int, int, str]:
    """Validate caller-supplied run parameters against the server-side maxima.

    Returns (target_tps, ramp_seconds, sustain_seconds, error). If error is
    non-empty, the handler returns 400 with that message and does not launch
    a run. Values default to the module defaults when the caller omits them.
    """
    try:
        target_tps = int(target_tps_raw) if target_tps_raw is not None else _DEFAULT_TARGET_TPS
        ramp = int(ramp_raw) if ramp_raw is not None else _DEFAULT_RAMP_SECONDS
        sustain = int(sustain_raw) if sustain_raw is not None else _DEFAULT_SUSTAIN_SECONDS
    except (TypeError, ValueError):
        return 0, 0, 0, "target_tps, ramp_duration_seconds, and sustain_duration_seconds must be integers"

    if target_tps < 1:
        return 0, 0, 0, "target_tps must be >= 1"
    if ramp < 0:
        return 0, 0, 0, "ramp_duration_seconds must be >= 0"
    if sustain < 1:
        return 0, 0, 0, "sustain_duration_seconds must be >= 1"

    if target_tps > _MAX_TARGET_TPS:
        return 0, 0, 0, f"target_tps {target_tps} exceeds MAX_TARGET_TPS {_MAX_TARGET_TPS}"
    if ramp > _MAX_RAMP_SECONDS:
        return 0, 0, 0, f"ramp_duration_seconds {ramp} exceeds MAX_RAMP_SECONDS {_MAX_RAMP_SECONDS}"
    if sustain > _MAX_SUSTAIN_SECONDS:
        return 0, 0, 0, f"sustain_duration_seconds {sustain} exceeds MAX_SUSTAIN_SECONDS {_MAX_SUSTAIN_SECONDS}"

    return target_tps, ramp, sustain, ""


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    # If called directly (async invocation from self), run the simulation.
    # Parameters are already clamped by the API-Gateway-entry call that
    # self-invoked us, but we re-clamp defensively because the orchestrator
    # function can also be invoked directly via the Lambda API (bypassing
    # API Gateway) by any IAM principal with lambda:InvokeFunction.
    if "run_id" in event and "body" not in event:
        target_tps, ramp, sustain, err = _clamp_run_params(
            event.get("target_tps"),
            event.get("ramp_duration_seconds"),
            event.get("sustain_duration_seconds"),
        )
        if err:
            logger.warning("Rejected direct-invoke run parameters", extra={"error": err})
            return {"status": "REJECTED", "error": err}
        summary = _run_simulation(event["run_id"], target_tps, ramp, sustain)
        logger.info("Simulation finished", extra=summary)
        return summary

    # If called via API Gateway, invoke self async and return immediately
    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
        run_id: str = body.get("run_id") or str(uuid.uuid4())

        target_tps, ramp, sustain, err = _clamp_run_params(
            body.get("target_tps"),
            body.get("ramp_duration_seconds"),
            body.get("sustain_duration_seconds"),
        )
        if err:
            logger.warning("Rejected API-Gateway run parameters", extra={"run_id": run_id, "error": err})
            return _build_response(400, {"error": "invalid_parameters", "detail": err})

        if not _TICKETING_ENDPOINT:
            return _build_response(
                400, {"error": "TICKETING_API_ENDPOINT environment variable is not set"}
            )

        # Async self-invocation — runs the simulation without blocking the API response
        _lambda_client.invoke(
            FunctionName=os.environ.get("AWS_LAMBDA_FUNCTION_NAME", _SELF_NAME),
            InvocationType="Event",
            Payload=json.dumps({
                "run_id": run_id,
                "target_tps": target_tps,
                "ramp_duration_seconds": ramp,
                "sustain_duration_seconds": sustain,
            }).encode(),
        )

        return _build_response(
            202,
            {
                "run_id": run_id,
                "status": "STARTED",
                "target_tps": target_tps,
                "ramp_duration_seconds": ramp,
                "sustain_duration_seconds": sustain,
                "limits": {
                    "max_target_tps": _MAX_TARGET_TPS,
                    "max_ramp_seconds": _MAX_RAMP_SECONDS,
                    "max_sustain_seconds": _MAX_SUSTAIN_SECONDS,
                },
                "message": "Simulation started asynchronously. Query /dashboard?run_id={} for results.".format(run_id),
            },
        )

    except Exception as exc:  # noqa: BLE001
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error("Orchestrator error", extra={"request_id": request_id, "error": str(exc)}, exc_info=True)
        # Generic response: internal detail stays in CloudWatch, correlatable
        # by request_id.
        return _build_response(500, {"error": "internal_server_error", "request_id": request_id})
