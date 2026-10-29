"""
Inventory Lock Lambda — Concert Ticketing App
=============================================
Performs an optimistic-locking seat reservation in DynamoDB.

A seat can be in one of three states:
  AVAILABLE  — free to lock
  HELD       — locked by a user, expires at hold_expires_at (TTL)
  CONFIRMED  — permanently booked

The DynamoDB condition expression ensures only one winner per seat, even
at extremely high concurrency.  Losing requests receive a 409 Conflict.

Target latency: 40–80 ms at p99.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import zlib
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Format/length constraints for request-body identifiers.
# event_id and seat_id become part of DynamoDB partition keys; constraining
# them here keeps oversized or oddly-shaped values from reaching DDB
# (which would reject them with a less-friendly error) and from being
# written into log lines.
_MAX_IDENTIFIER_LEN = 128
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def _validate_identifier(name: str, value: str) -> str:
    """Return "" if value is a valid identifier, else an error message."""
    if not value:
        return f"{name} is required"
    if len(value) > _MAX_IDENTIFIER_LEN:
        return f"{name} exceeds {_MAX_IDENTIFIER_LEN} characters"
    if not _IDENTIFIER_PATTERN.match(value):
        return f"{name} must be 1-{_MAX_IDENTIFIER_LEN} chars of [A-Za-z0-9_-]"
    return ""

# Connection pool sized to match PerExecutionEnvironmentMaxConcurrency.
# Default is 10 — under 16-concurrent-per-EE load, 6 invocations block
# waiting for a pool connection, causing 5s HTTP client timeouts.
_boto_config = Config(max_pool_connections=64, retries={"mode": "standard", "max_attempts": 3})

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ.get("AWS_REGION"),
    config=_boto_config,
)
_inventory_table = _dynamodb.Table(os.environ["INVENTORY_TABLE"])
_HOLD_TTL = int(os.environ.get("HOLD_TTL_SECONDS", "180"))


def _authenticated_user_id(event: dict[str, Any]) -> str:
    """Return the authenticated caller's identifier.

    With API Gateway AuthorizationType: AWS_IAM, every request is signed
    with SigV4 and API Gateway populates requestContext.identity with
    the verified principal. We prefer userArn (stable, globally unique)
    and fall back to caller/user for older event shapes. An empty return
    means no authenticated principal is present and the handler must
    reject with 401 — never trust a body-supplied user_id.
    """
    identity = (event.get("requestContext") or {}).get("identity") or {}
    for key in ("userArn", "caller", "user"):
        v = str(identity.get(key) or "").strip()
        if v:
            return v
    return ""

# Write-sharding: the inventory table's partition key `pk` is a deterministic
# hash of (event_id, seat_id) so writes distribute across many DDB partitions
# instead of all landing on event_id. With SHARD_COUNT=500, at 40K TPS each
# shard sees ~80 WCU/sec on average — a 5x safety margin below the ~1K
# WCU/sec per-partition cap that keeps per-shard variance from heating any
# single physical partition. crc32 is used because it is fast (native C),
# deterministic across processes, and gives good distribution for
# high-cardinality inputs like `seat-0000042`.
_SHARD_COUNT = int(os.environ.get("INVENTORY_SHARD_COUNT", "500"))


def _compute_pk(event_id: str, seat_id: str) -> str:
    """Deterministic sharded partition key for the inventory table."""
    shard = zlib.crc32(seat_id.encode()) % _SHARD_COUNT
    return f"{event_id}#{shard:03d}"


def _lock_seat(event_id: str, seat_id: str, user_id: str, hold_expires_at: int) -> None:
    """
    Attempt to transition a seat from AVAILABLE → HELD.
    Raises ClientError with ConditionalCheckFailedException if the seat
    is already held or confirmed.
    """
    # `event_id` is stored as a regular attribute even though it also lives
    # in the `pk` composite. Keeping the raw attribute lets downstream
    # consumers (analytics, reporting) read it without parsing `pk`.
    _inventory_table.update_item(
        Key={"pk": _compute_pk(event_id, seat_id), "seat_id": seat_id},
        UpdateExpression=(
            "SET seat_status = :held, "
            "event_id = :evt, "
            "held_by = :uid, "
            "hold_expires_at = :exp, "
            "updated_at = :now"
        ),
        ConditionExpression=(
            "attribute_not_exists(seat_status) OR seat_status = :available"
        ),
        ExpressionAttributeValues={
            ":held": "HELD",
            ":available": "AVAILABLE",
            ":evt": event_id,
            ":uid": user_id,
            ":exp": hold_expires_at,
            ":now": int(time.time()),
        },
    )


def _build_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Service": "ticketing-inventory-lock",
        },
        "body": json.dumps(body),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    start = time.monotonic_ns()
    # Extracted outside the try so error-path logs can include the caller's
    # correlation context (event_id, seat_id, user_id). All log lines in
    # this handler include these fields so a single Logs Insights query
    # can filter every log record for a given event_id / burst.
    event_id: str = ""
    seat_id: str = ""
    user_id: str = ""

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
        event_id = body.get("event_id", "").strip()
        seat_id = body.get("seat_id", "").strip()
        # user_id is derived from the AWS_IAM-authenticated principal, never
        # from the request body. A body field named user_id is ignored.
        user_id = _authenticated_user_id(event)

        if not user_id:
            logger.warning(
                "Missing authenticated principal on /inventory/lock",
                extra={"event_id": event_id, "seat_id": seat_id, "status_code": 401},
            )
            return _build_response(401, {"error": "unauthorized"})

        for field_name, field_value in (("event_id", event_id), ("seat_id", seat_id)):
            err = _validate_identifier(field_name, field_value)
            if err:
                logger.warning(
                    "Invalid request on /inventory/lock",
                    extra={"event_id": event_id, "seat_id": seat_id, "user_id": user_id, "status_code": 400, "detail": err},
                )
                return _build_response(400, {"error": err})

        hold_expires_at = int(time.time()) + _HOLD_TTL

        try:
            _lock_seat(event_id, seat_id, user_id, hold_expires_at)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
                # 409 conflict is an EXPECTED outcome under seat contention,
                # not a system fault. Logged at warning (not error) so it
                # doesn't pollute error-rate dashboards.
                logger.warning(
                    "Seat lock conflict — already held or confirmed",
                    extra={
                        "event_id": event_id,
                        "seat_id": seat_id,
                        "user_id": user_id,
                        "status_code": 409,
                        "elapsed_ms": round(elapsed_ms, 2),
                    },
                )
                return _build_response(
                    409,
                    {
                        "error": "seat_unavailable",
                        "detail": "Seat is already held or confirmed by another user.",
                    },
                )
            raise

        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        logger.info(
            "Seat locked",
            extra={
                "event_id": event_id,
                "seat_id": seat_id,
                "user_id": user_id,
                "hold_expires_at": hold_expires_at,
                "elapsed_ms": round(elapsed_ms, 2),
                "status_code": 200,
            },
        )

        return _build_response(
            200,
            {
                "event_id": event_id,
                "seat_id": seat_id,
                "user_id": user_id,
                "status": "HELD",
                "hold_expires_at": hold_expires_at,
                "hold_ttl_seconds": _HOLD_TTL,
            },
        )

    except ClientError as exc:
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error(
            "DynamoDB error on /inventory/lock",
            extra={"event_id": event_id, "seat_id": seat_id, "user_id": user_id, "status_code": 503, "request_id": request_id, "error": str(exc)},
            exc_info=True,
        )
        # Generic response: no internal detail. Correlate via request_id in
        # CloudWatch Logs.
        return _build_response(503, {"error": "service_unavailable", "request_id": request_id})

    except Exception as exc:  # noqa: BLE001
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error(
            "Unexpected error on /inventory/lock",
            extra={"event_id": event_id, "seat_id": seat_id, "user_id": user_id, "status_code": 500, "request_id": request_id, "error": str(exc)},
            exc_info=True,
        )
        return _build_response(500, {"error": "internal_server_error", "request_id": request_id})
