# Booking flow — request sequence

Three-phase booking flow across the ticketing app. Each successful booking
calls all three endpoints in order: queue → lock → confirm. Values match
the shipped defaults in `ticketing-app/template.yaml`.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as API Gateway
    participant Q as queue-position<br/>(LMI)
    participant R as Redis<br/>(ElastiCache Serverless)
    participant L as inventory-lock<br/>(LMI)
    participant B as booking-confirm<br/>(LMI)
    participant D as DynamoDB<br/>(inventory + bookings)

    Note over C,D: Phase 1 — Queue admission (Redis atomic counter, measured p99 ~5 ms)
    C->>API: POST /queue/position<br/>{event_id, user_id}
    API->>Q: invoke
    Q->>R: INCR queue:{event_id}
    R-->>Q: position
    Q-->>API: 200
    API-->>C: {queue_position, backend: "redis"}

    Note over C,D: Phase 2 — Inventory hold (TTL 180 s, measured p99 ~16 ms)
    C->>API: POST /inventory/lock<br/>{event_id, seat_id, user_id}
    API->>L: invoke
    L->>D: UpdateItem inventory<br/>SET seat_status = HELD<br/>ConditionExpression: seat_status = AVAILABLE<br/>pk = event_id#(crc32(seat_id) mod 500)
    D-->>L: OK
    L-->>API: 200
    API-->>C: {status: "HELD", hold_expires_at, hold_ttl_seconds: 180}

    Note over C,D: Phase 3 — Booking confirm (atomic TransactWriteItems, measured p99 ~40 ms)
    C->>API: POST /booking/confirm<br/>{event_id, seat_id, user_id, booking_id?}
    API->>B: invoke
    B->>D: TransactWriteItems (atomic):<br/>1. Update inventory: HELD → CONFIRMED<br/>   (ConditionExpression: held_by = user_id)<br/>2. Put bookings: new booking record<br/>   (ConditionExpression: booking_id absent)
    D-->>B: OK
    B-->>API: 201
    API-->>C: {booking_id, status: "CONFIRMED", confirmed_at}
```

## Notes on the flow

- **Sharded partition key.** The `inventory` table uses
  `pk = event_id#(crc32(seat_id) mod 500)` to spread writes across 500
  logical partitions. At 40 K TPS this keeps each shard around 80 WCU/s,
  well below DynamoDB's per-partition ceiling.
- **Idempotency.** The client may supply its own `booking_id` on
  `POST /booking/confirm` to make retries safe. A duplicate `booking_id`
  short-circuits to a `200 { idempotent: true }` instead of re-writing.
- **Optimistic locking.** Phase 2 conditions on `seat_status = AVAILABLE`,
  so at most one caller wins each seat. Losers get `409 seat_unavailable`.
- **TransactWriteItems atomicity.** Both writes in Phase 3 succeed or fail
  together. If the inventory condition fails (hold expired, or held by a
  different user), the bookings row is not written either.
- **Not shown here (happy path only).** The queue-position handler has a
  DynamoDB fallback if Redis is unavailable; both handlers return `409` on
  conflicts (already held, hold expired) and `503` on DDB errors.

## Handler-to-endpoint map

| Endpoint | Handler | Data plane |
|---|---|---|
| `POST /queue/position` | `queue_position/handler.py` | Redis (primary), DynamoDB (fallback) |
| `POST /inventory/lock` | `inventory_lock/handler.py` | DynamoDB `inventory` |
| `POST /booking/confirm` | `booking_confirm/handler.py` | DynamoDB `inventory` + `bookings` (atomic) |
