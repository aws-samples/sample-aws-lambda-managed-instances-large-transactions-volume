"""
Simulation Coordinator Lambda
==============================
Middle tier in the fan-out hierarchy.

  Orchestrator  ──async──►  Coordinator (this)  ──async──►  Load Worker × N

Receives a "spawn N workers" instruction from the orchestrator and
asynchronously invokes N load-worker Lambdas in parallel using threads.
Using threads here (rather than sequential invocation) ensures all N
workers start at nearly the same instant, giving the tightest possible
TPS burst shape at the ticketing API.

The coordinator itself is lightweight — it issues N Lambda invoke API
calls and returns.  Each invoke call is async (InvocationType=Event) so
the coordinator does not wait for workers to finish.
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_lambda_client = boto3.client("lambda", region_name=os.environ.get("AWS_REGION"))

_SELF_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
_STACK_PREFIX = _SELF_NAME.rsplit("-coordinator", 1)[0]
_WORKER_FUNCTION = os.environ.get(
    "WORKER_FUNCTION_NAME", f"{_STACK_PREFIX}-load-worker"
)
# BURST_SIZE is passed to workers in the payload AND used to compute
# per-worker seat offsets. Reading it once at module scope keeps the two
# uses in sync — a divergence here would produce overlapping seat ranges
# whenever BurstSizePerWorker is set to a non-default value at deploy time.
_BURST_SIZE = int(os.environ.get("BURST_SIZE", "100"))


def _invoke_worker(
    run_id: str,
    wave: int,
    event_id: str,
    seat_offset: int,
    ticketing_endpoint: str,
    seats_per_event: int,
) -> None:
    payload = {
        "run_id": run_id,
        "wave": wave,
        "event_id": event_id,
        "seat_offset": seat_offset,
        "ticketing_endpoint": ticketing_endpoint,
        "num_requests": _BURST_SIZE,
        "seats_per_event": seats_per_event,
    }
    _lambda_client.invoke(
        FunctionName=_WORKER_FUNCTION,
        InvocationType="Event",   # fire-and-forget
        Payload=json.dumps(payload).encode(),
    )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    run_id: str = event.get("run_id", "unknown")
    wave: int = int(event.get("wave", 0))
    workers_to_spawn: int = int(event.get("workers_to_spawn", 1))
    seat_offset: int = int(event.get("seat_offset", 0))
    ticketing_endpoint: str = event.get("ticketing_endpoint", "")
    simulated_events: list[str] = event.get("simulated_events", ["EVT-001"])
    seats_per_event: int = int(event.get("seats_per_event", 2000000))

    if not ticketing_endpoint:
        logger.error("ticketing_endpoint not provided in event payload")
        return {"statusCode": 400, "error": "ticketing_endpoint missing"}

    invoke_errors = 0

    # Invoke all workers concurrently — fire-and-forget, so each thread
    # just issues the async invoke API call and returns immediately.
    with ThreadPoolExecutor(max_workers=min(workers_to_spawn, 100)) as pool:
        futures = [
            pool.submit(
                _invoke_worker,
                run_id,
                wave,
                simulated_events[i % len(simulated_events)],
                (seat_offset + i * _BURST_SIZE) % seats_per_event,
                ticketing_endpoint,
                seats_per_event,
            )
            for i in range(workers_to_spawn)
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                invoke_errors += 1
                logger.warning("Worker invoke failed: %s", exc)

    logger.info(
        "Coordinator wave dispatched",
        extra={
            "run_id": run_id,
            "wave": wave,
            "workers_spawned": workers_to_spawn - invoke_errors,
            "invoke_errors": invoke_errors,
        },
    )

    return {
        "statusCode": 200,
        "workers_spawned": workers_to_spawn - invoke_errors,
        "invoke_errors": invoke_errors,
    }
