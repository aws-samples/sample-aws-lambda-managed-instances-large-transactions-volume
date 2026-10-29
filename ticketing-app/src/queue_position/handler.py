"""
Queue Position Lambda — Concert Ticketing App
=============================================
Assigns a virtual queue position to each incoming ticket request.

Design notes:
- Multi-concurrent: this handler is safe to run concurrently inside a single
  execution environment because it holds no mutable module-level state.
- Primary counter uses Redis (ElastiCache Serverless / Valkey) via atomic
  INCR — the industry-standard approach for high-throughput ticketing.
  Redis handles 1M+ atomic increments per second on a single key.
- Fallback path uses DynamoDB `ADD` if Redis is unavailable — keeps the
  handler functional during Redis provisioning or outages. Note: DDB single-
  row atomic increments cap at ~1-3K ops/sec due to partition throughput.
- Target latency: 5-20 ms at p99 on the Redis path.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Format/length constraints for request-body identifiers.
# event_id and seat_id become part of DynamoDB partition keys and
# CloudWatch log filters; constraining them here keeps oversized or
# oddly-shaped values from reaching those systems. The character set
# allows the UUID / dash-prefixed shapes used by the simulator
# (RUN-abc12345-00, seat-0000042) and the demo defaults (EVT-001).
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

# ---------------------------------------------------------------------------
# Module-level clients — initialized once per execution environment and
# shared across concurrent invocations under LMI's
# PerExecutionEnvironmentMaxConcurrency (8 by default).
#
# boto3 clients and resources are documented thread-safe for the API-call
# surface we use here (one shared resource, no re-authentication, no
# mutation of config after init). redis-py is thread-safe for command
# dispatch once the client is constructed. The one race LMI's
# multi-concurrent model would otherwise expose — two first-invocations
# both constructing a Redis client under the lazy-init `global` — is
# serialized by _redis_client_lock below (double-checked locking).
# ---------------------------------------------------------------------------

# Connection pool sized to match PerExecutionEnvironmentMaxConcurrency.
# Default is 10 — under 16-concurrent-per-EE load, 6 invocations block
# waiting for a pool connection, causing 5s HTTP client timeouts.
_boto_config = Config(max_pool_connections=64, retries={"mode": "standard", "max_attempts": 3})

# DynamoDB — used as fallback if Redis is unavailable
_dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ.get("AWS_REGION"),
    config=_boto_config,
)
_inventory_table = _dynamodb.Table(os.environ["INVENTORY_TABLE"])
_QUEUE_COUNTER_KEY = "__queue_counter__"
# The DDB fallback stores the queue counter in a single row per event.
# We put it in a dedicated shard suffix ("cnt") so it never collides with
# hashed seat rows. This fallback is expected to be cold — Redis is the
# primary path — so single-partition throughput is not a concern here.
_QUEUE_COUNTER_SHARD = "cnt"

# Redis — primary path for atomic counter
# ElastiCache Serverless requires TLS on port 6379. We verify the Redis
# endpoint's certificate against the system CA bundle: ElastiCache
# Serverless uses certificates signed by Amazon Trust Services, which is
# in the default CA store on the Lambda Python runtime, so no explicit
# ssl_ca_certs is needed. Setting ssl_cert_reqs="required" closes the
# T-4 finding in docs/threat-model.md (the earlier config ssl_cert_reqs=None
# would accept any cert and did not authenticate the Redis endpoint).
# Connection pool is initialized lazily on first use so a missing/unreachable
# Redis doesn't prevent the handler from starting up.
_REDIS_ENDPOINT = os.environ.get("REDIS_ENDPOINT", "")
_REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
_redis_client: Any = None  # lazy-initialized on first use
# Serializes the lazy-init path so two concurrent first-invocations
# inside the same EE cannot both construct a Redis client.
_redis_client_lock = threading.Lock()


def _get_redis():
    """Lazily initialize the Redis client. Returns None if config is missing.

    Thread safety: the common path (client already constructed) is a
    lock-free read of the module-level reference. The construction path
    is serialized with _redis_client_lock using the double-checked
    locking pattern so only one invocation in a given EE performs the
    initial redis.Redis() call even when LMI runs multiple invocations
    concurrently in that EE.
    """
    global _redis_client
    # Fast path: client already constructed in this EE.
    if _redis_client is not None:
        return _redis_client
    with _redis_client_lock:
        # Re-check under the lock — another thread may have raced ahead
        # between the fast-path read and acquiring the lock.
        if _redis_client is not None:
            return _redis_client
        if not _REDIS_ENDPOINT:
            return None
        try:
            import redis  # local import so cold-import doesn't fail if package missing

            _redis_client = redis.Redis(
                host=_REDIS_ENDPOINT,
                port=_REDIS_PORT,
                ssl=True,
                # Verify the ElastiCache endpoint's certificate against the
                # system CA bundle. Amazon Trust Services roots are present
                # by default on the Lambda Python runtime.
                ssl_cert_reqs="required",
                socket_timeout=1.0,
                socket_connect_timeout=1.0,
                decode_responses=True,
                # Retry once on TimeoutError; short backoff. redis-py handles this
                # inside the client so callers don't need explicit retry logic.
                retry_on_timeout=True,
            )
            return _redis_client
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis client init failed, falling back to DynamoDB: %s", exc)
            return None


def _authenticated_user_id(event: dict[str, Any]) -> str:
    """Return the authenticated caller's identifier.

    With API Gateway AuthorizationType: AWS_IAM, every request is signed
    with SigV4 and API Gateway populates requestContext.identity with the
    verified principal. We prefer userArn (stable, globally unique) and
    fall back to caller/user for older event shapes. An empty return means
    no authenticated principal is present and the handler must reject
    with 401 — never trust a body-supplied user_id.
    """
    identity = (event.get("requestContext") or {}).get("identity") or {}
    for key in ("userArn", "caller", "user"):
        v = str(identity.get(key) or "").strip()
        if v:
            return v
    return ""


def _next_position_redis(event_id: str) -> int:
    """Atomic INCR on Redis. Returns new counter value."""
    client = _get_redis()
    if client is None:
        raise RuntimeError("Redis client unavailable")
    return int(client.incr(f"queue:{event_id}"))


def _next_position_ddb(event_id: str) -> int:
    """DynamoDB fallback — single-row atomic ADD."""
    response = _inventory_table.update_item(
        Key={
            "pk": f"{event_id}#{_QUEUE_COUNTER_SHARD}",
            "seat_id": _QUEUE_COUNTER_KEY,
        },
        UpdateExpression="ADD queue_position :inc",
        ExpressionAttributeValues={":inc": 1},
        ReturnValues="UPDATED_NEW",
    )
    return int(response["Attributes"]["queue_position"])


def _get_next_position(event_id: str) -> tuple[int, str]:
    """Try Redis first, fall back to DDB. Returns (position, backend_used)."""
    try:
        return _next_position_redis(event_id), "redis"
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis INCR failed, falling back to DDB: %s", exc)
        return _next_position_ddb(event_id), "ddb"


def _build_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "X-Service": "ticketing-queue-position",
        },
        "body": json.dumps(body),
    }


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:  # noqa: ANN401
    start = time.monotonic_ns()
    # Extracted outside the try so error-path logs can include the caller's
    # correlation context (event_id, user_id). All log lines in this handler
    # include these fields so a single Logs Insights query can filter every
    # log record for a given event_id / burst.
    event_id: str = ""
    user_id: str = ""

    try:
        body: dict[str, Any] = json.loads(event.get("body") or "{}")
        event_id = body.get("event_id", "").strip()
        # user_id is derived from the AWS_IAM-authenticated principal in
        # requestContext.identity, never from the request body. A body
        # field named user_id is ignored. If auth is missing (unlikely
        # once SAM applies DefaultAuthorizer: AWS_IAM, but handled
        # defensively) we fail with 401.
        user_id = _authenticated_user_id(event)
        request_id: str = (
            event.get("requestContext", {}).get("requestId", "") or ""
        ).strip()

        if not user_id:
            logger.warning(
                "Missing authenticated principal on /queue/position",
                extra={"event_id": event_id, "status_code": 401},
            )
            return _build_response(401, {"error": "unauthorized"})

        err = _validate_identifier("event_id", event_id)
        if err:
            logger.warning(
                "Invalid request on /queue/position",
                extra={"event_id": event_id, "user_id": user_id, "status_code": 400, "detail": err},
            )
            return _build_response(400, {"error": err})

        position, backend = _get_next_position(event_id)

        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        logger.info(
            "Queue position assigned",
            extra={
                "event_id": event_id,
                "user_id": user_id,
                "position": position,
                "backend": backend,
                "request_id": request_id,
                "elapsed_ms": round(elapsed_ms, 2),
                "status_code": 200,
            },
        )

        return _build_response(
            200,
            {
                "event_id": event_id,
                "user_id": user_id,
                "queue_position": position,
                "backend": backend,
                "request_id": request_id,
                "timestamp_ms": int(time.time() * 1000),
            },
        )

    except ClientError as exc:
        logger.error(
            "DynamoDB error on /queue/position",
            extra={"event_id": event_id, "user_id": user_id, "status_code": 503, "request_id": request_id, "error": str(exc)},
            exc_info=True,
        )
        # Generic response: client sees only the error code and the API Gateway
        # request_id for correlation. The exception detail stays in CloudWatch
        # Logs (via the logger.error above with exc_info=True) and is correlatable
        # by request_id, so operators can look it up without exposing table
        # names, ARNs, or internal structure to callers.
        return _build_response(503, {"error": "service_unavailable", "request_id": request_id})

    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Unexpected error on /queue/position",
            extra={"event_id": event_id, "user_id": user_id, "status_code": 500, "request_id": request_id, "error": str(exc)},
            exc_info=True,
        )
        return _build_response(500, {"error": "internal_server_error", "request_id": request_id})
