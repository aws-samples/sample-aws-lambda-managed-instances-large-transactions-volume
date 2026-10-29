"""
Booking Confirmation Lambda — Concert Ticketing App
====================================================
Finalises a held seat reservation and writes a permanent booking record.

Flow:
  1. Verify the seat is in HELD state and held by the requesting user.
  2. Transition the seat from HELD → CONFIRMED (conditional write).
  3. Write a new booking record to the Bookings table (idempotent via
     booking_id uniqueness check).

Both writes happen in a DynamoDB TransactWrite so they are atomic.

Target latency: 50–100 ms at p99.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
import zlib
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Format/length constraints for request-body identifiers.
# event_id and seat_id become part of DynamoDB partition keys; booking_id is
# the primary key of the BookingsTable. Constraining the character set keeps
# oversized or oddly-shaped values from reaching DDB and log lines, and makes
# idempotency keys deterministic for operator audit.
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

_dynamodb = boto3.client(
    "dynamodb",
    region_name=os.environ.get("AWS_REGION"),
    config=_boto_config,
)
_BOOKINGS_TABLE = os.environ["BOOKINGS_TABLE"]
_INVENTORY_TABLE = os.environ["INVENTORY_TABLE"]

# Write-sharding: must match the derivation used in inventory_lock so both
# handlers target the same physical row. See _compute_pk in that module.
_SHARD_COUNT = int(os.environ.get("INVENTORY_SHARD_COUNT", "500"))


def _compute_pk(event_id: str, seat_id: str) -> str:
    """Deterministic sharded partition key for the inventory table."""
    shard = zlib.crc32(seat_id.encode()) % _SHARD_COUNT
    return f"{event_id}#{shard:03d}"


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


def _confirm_booking(
    event_id: str,
    seat_id: str,
    user_id: str,
    booking_id: str,
    now_ts: int,
) -> None:
    """
    Atomically confirms the booking using a DynamoDB TransactWrite:
      - Inventory: HELD → CONFIRMED (conditioned on held_by = user_id)
      - Bookings: Insert new record (conditioned on booking_id not existing)
    """
    _dynamodb.transact_write_items(
        TransactItems=[
            {
                "Update": {
                    "TableName": _INVENTORY_TABLE,
                    "Key": {
                        "pk": {"S": _compute_pk(event_id, seat_id)},
                        "seat_id": {"S": seat_id},
                    },
                    "UpdateExpression": (
                        "SET seat_status = :confirmed, "
                        "booking_id = :bid, "
                        "confirmed_at = :now "
                        "REMOVE hold_expires_at, held_by"
                    ),
                    "ConditionExpression": (
                        "seat_status = :held AND held_by = :uid"
                    ),
                    "ExpressionAttributeValues": {
                        ":confirmed": {"S": "CONFIRMED"},
                        ":held": {"S": "HELD"},
                        ":uid": {"S": user_id},
                        ":bid": {"S": booking_id},
                        ":now": {"N": str(now_ts)},
                    },
                }
            },
            {
                "Put": {
                    "TableName": _BOOKINGS_TABLE,
                    "Item": {
                        "booking_id": {"S": booking_id},
                        "event_id": {"S": event_id},
                        "seat_id": {"S": seat_id},
                        "user_id": {"S": user_id},
                        "status": {"S": "CONFIRMED"},
                        "created_at": {"N": str(now_ts)},
                    },
                    # Idempotency guard — reject if booking_id already exists
                    "ConditionExpression": "attribute_not_exists(booking_id)",
                }
            },
        ]
    )


def _build_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Service": "ticketing-booking-confirm",
        },
        "body": json.dumps(body),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    start = time.monotonic_ns()
    # Extracted outside the try so error-path logs can include the caller's
    # correlation context. All log lines in this handler include event_id,
    # seat_id, user_id, and booking_id where available so a single Logs
    # Insights query can filter every log record for a given event_id / burst.
    event_id: str = ""
    seat_id: str = ""
    user_id: str = ""
    booking_id: str = ""

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
        event_id = body.get("event_id", "").strip()
        seat_id = body.get("seat_id", "").strip()
        # user_id is derived from the AWS_IAM-authenticated principal, never
        # from the request body. A body field named user_id is ignored. The
        # same principal must have called /inventory/lock for the condition
        # held_by = :uid to match in the TransactWriteItems below.
        user_id = _authenticated_user_id(event)
        # Allow caller to supply a booking_id for idempotent retries
        booking_id = body.get("booking_id") or str(uuid.uuid4())

        if not user_id:
            logger.warning(
                "Missing authenticated principal on /booking/confirm",
                extra={
                    "event_id": event_id, "seat_id": seat_id,
                    "booking_id": booking_id, "status_code": 401,
                },
            )
            return _build_response(401, {"error": "unauthorized"})

        for field_name, field_value in (("event_id", event_id), ("seat_id", seat_id), ("booking_id", booking_id)):
            err = _validate_identifier(field_name, field_value)
            if err:
                logger.warning(
                    "Invalid request on /booking/confirm",
                    extra={
                        "event_id": event_id, "seat_id": seat_id, "user_id": user_id,
                        "booking_id": booking_id, "status_code": 400, "detail": err,
                    },
                )
                return _build_response(400, {"error": err})

        now_ts = int(time.time())

        try:
            _confirm_booking(event_id, seat_id, user_id, booking_id, now_ts)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "TransactionCanceledException":
                reasons = exc.response.get("CancellationReasons", [])
                # First item is the inventory update condition failure
                if reasons and reasons[0].get("Code") == "ConditionalCheckFailed":
                    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
                    logger.warning(
                        "Booking confirmation failed — hold missing or expired",
                        extra={
                            "event_id": event_id, "seat_id": seat_id, "user_id": user_id,
                            "booking_id": booking_id, "status_code": 409,
                            "elapsed_ms": round(elapsed_ms, 2),
                        },
                    )
                    return _build_response(
                        409,
                        {
                            "error": "confirmation_failed",
                            "detail": (
                                "Seat is not held by you, or the hold has expired."
                            ),
                        },
                    )
                # Second item is the booking duplicate guard
                if len(reasons) > 1 and reasons[1].get("Code") == "ConditionalCheckFailed":
                    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
                    logger.info(
                        "Booking idempotent replay — row already exists",
                        extra={
                            "event_id": event_id, "seat_id": seat_id, "user_id": user_id,
                            "booking_id": booking_id, "status_code": 200, "idempotent": True,
                            "elapsed_ms": round(elapsed_ms, 2),
                        },
                    )
                    return _build_response(
                        200,
                        {
                            "event_id": event_id,
                            "seat_id": seat_id,
                            "user_id": user_id,
                            "booking_id": booking_id,
                            "status": "CONFIRMED",
                            "idempotent": True,
                        },
                    )
            raise

        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        logger.info(
            "Booking confirmed",
            extra={
                "event_id": event_id,
                "seat_id": seat_id,
                "user_id": user_id,
                "booking_id": booking_id,
                "elapsed_ms": round(elapsed_ms, 2),
                "status_code": 201,
            },
        )

        return _build_response(
            201,
            {
                "event_id": event_id,
                "seat_id": seat_id,
                "user_id": user_id,
                "booking_id": booking_id,
                "status": "CONFIRMED",
                "confirmed_at": now_ts,
            },
        )

    except ClientError as exc:
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error(
            "DynamoDB error on /booking/confirm",
            extra={
                "event_id": event_id, "seat_id": seat_id, "user_id": user_id,
                "booking_id": booking_id, "status_code": 503, "request_id": request_id,
                "error": str(exc),
            },
            exc_info=True,
        )
        # Generic response: no internal detail. Correlate via request_id in
        # CloudWatch Logs.
        return _build_response(503, {"error": "service_unavailable", "request_id": request_id})

    except Exception as exc:  # noqa: BLE001
        request_id = (event.get("requestContext", {}).get("requestId", "") or "").strip()
        logger.error(
            "Unexpected error on /booking/confirm",
            extra={
                "event_id": event_id, "seat_id": seat_id, "user_id": user_id,
                "booking_id": booking_id, "status_code": 500, "request_id": request_id,
                "error": str(exc),
            },
            exc_info=True,
        )
        return _build_response(500, {"error": "internal_server_error", "request_id": request_id})
