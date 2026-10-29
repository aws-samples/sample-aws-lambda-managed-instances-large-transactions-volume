# Architecture — LMI Ticketing Reference

A reference architecture for concert-ticketing-style workloads on AWS Lambda Managed Instances (LMI), DynamoDB with write-sharded partition keys, and ElastiCache Serverless (Valkey) for hot atomic counters. The stack sustained **40,000 delivered requests per second for 120 seconds** with a p99 end-to-end latency of **121 ms** and zero Lambda-side errors or throttles. See [`test-results.md`](./test-results.md) for the full measurement report.

## Contents

1. [At a glance](#1-at-a-glance)
2. [Request lifecycle](#2-request-lifecycle)
3. [Deployment topology](#3-deployment-topology)
4. [Component details](#4-component-details)
5. [Load generator](#5-load-generator)
6. [Concurrency safety](#6-concurrency-safety)

---

## 1. At a glance

![AWS architecture](images/architecture.png)

The request path enters through API Gateway and traverses three sequential Lambda handlers running on shared LMI capacity. Each handler is backed by the data store that suits its access pattern:

- `queue-position` → ElastiCache Serverless (Valkey) — atomic `INCR` on a hot counter key
- `inventory-lock` → DynamoDB inventory table — conditional `UpdateItem` for optimistic seat holding
- `booking-confirm` → DynamoDB `TransactWriteItems` across the **inventory** and **bookings** tables in a single atomic write

The Simulation App on the left drives load into the Ticketing App on the right through the three-tier fan-out (Orchestrator → Coordinator × N → Worker × M). Every worker fires HTTP flows against the Ticketing API while recording per-wave metrics.

**Diagram source:** [`images/architecture.drawio`](./images/architecture.drawio). Edit in [drawio.com](https://app.diagrams.net) or via the VS Code drawio extension.

### Key numbers (from the 40 K TPS × 120 s headline run)

| Property | Value |
|---|---|
| Peak delivered throughput | 40,035 req/s (100.1 % of target) |
| Burst duration | 120 s |
| Total HTTP requests | 4,804,200 |
| Bookings landed (durable in DynamoDB) | 1,750,886 |
| End-to-end p50 / p99 latency | 44 ms / 121 ms |
| Lambda errors / Lambda throttles / DDB throttles | 0 / 0 / 0 |
| Instance type | c7i.4xlarge (16 vCPU, hosts 4 EEs each) |
| LMI fleet at baseline Min EEs | 300 EEs → 75 instances |
| DynamoDB shard count | 500 |
| Region | AWS `us-east-1` |

---

## 2. Request lifecycle

The reference implementation follows a **queue plus hold-and-confirm** booking pattern that is standard in inventory-constrained e-commerce. Each successful booking calls three endpoints in order:

- **`POST /queue/position`** — `queue-position` handler; Redis `INCR queue:{event_id}` for admission control. 
- **`POST /inventory/lock`** — `inventory-lock` handler; conditional `UpdateItem` transitions the seat `AVAILABLE` → `HELD` with a 180 s TTL. 
- **`POST /booking/confirm`** — `booking-confirm` handler; atomic `TransactWriteItems` transitions the seat `HELD` → `CONFIRMED` **and** writes a durable booking record. Either both writes succeed or neither does — no over-selling, no orphaned records. 

**Handler state and thread safety.** All *business* state (seat status, queue counters, booking records) lives in Redis and DynamoDB — the handlers hold no request-specific state across invocations. Each handler does cache a small amount of connection-level state at module scope: boto3 clients, boto3 resources, a `boto3.dynamodb.ServiceResource.Table()` reference, and (in `queue-position`) the ElastiCache Redis client. The specific thread-safety contract the handlers rely on:

- **boto3 low-level clients** (`boto3.client(...)`) are explicitly documented thread-safe for the API-call surface. The sample uses this pattern directly in `booking-confirm/handler.py` with `_dynamodb = boto3.client("dynamodb", ...)` and `_dynamodb.transact_write_items(...)`. See the AWS SDK for Python guidance: [Resources and sessions - Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html).
- **boto3 high-level resources** (`boto3.resource(...)` and `resource.Table(...)`) are **not** blanket thread-safe; the official SDK guidance recommends not sharing a resource across threads. In practice, the operations our handlers perform — `Table.update_item(...)` with request-scoped keyword arguments — delegate to the thread-safe underlying client without mutating the resource object. `queue-position` and `inventory-lock` use this pattern (`_inventory_table = _dynamodb.Table(os.environ["INVENTORY_TABLE"])` once at import, then concurrent `.update_item(...)` calls). This pattern is routinely used in Lambda handlers under multi-concurrent execution and is correct for the specific no-mutation call sites we have; it is called out here so that future contributors who add a resource-mutating call know they must guard it or switch that handler to the lower-level client.
- **redis-py `Redis` client** is thread-safe for command dispatch once constructed. The one race LMI's multi-concurrent model would otherwise expose — two first-invocations both constructing a Redis client under the lazy-init `global` — is serialized with a `threading.Lock` in `queue-position/handler.py` (double-checked locking).

Under LMI's multi-concurrent model (`PerExecutionEnvironmentMaxConcurrency: 8`) handler code must be thread-safe. The sample's pattern is "no mutable request-level state, atomic writes to externalize all shared business state, serialized lazy-init for connection caches, call-site operations that only read module state."

See [`booking-flow.md`](./booking-flow.md) for the full sequence diagram, request/response payloads, and error paths.

---

## 3. Deployment topology

The reference is provisioned by two CloudFormation stacks in a single region:

- **`ticketing-lmi`** — the production data plane. API Gateway → LMI capacity provider (in a customer VPC) → Redis + two DynamoDB tables.
- **`ticketing-simulation`** — the control plane and load generator. API Gateway → four Lambdas that fan out load into the ticketing API and record results.

See [`deployment-topology.md`](./deployment-topology.md) for the full deployment diagram, IAM roles, VPC + security-group setup, and observability wiring.

---

## 4. Component details

### 4.1 Compute layer: AWS Lambda Managed Instances

LMI runs your Lambda functions on pre-provisioned EC2 instances in your account. Each instance hosts one or more *execution environments* (EEs) — think of an EE as a warm Lambda container held on the fleet, ready to run without a cold start. Every function has two knobs:

- **`MinExecutionEnvironments`** — how many EEs stay warm 24/7. This is the idle baseline.
- **`MaxExecutionEnvironments`** — the ceiling LMI can burst to. New EEs come online in 1–2 seconds each.

Multiple invocations run inside the same EE concurrently (`PerExecutionEnvironmentMaxConcurrency`, default 8) as long as handlers are thread-safe. You pay for the underlying EC2 instances plus a 15 % management fee — EC2 Savings Plans and Reserved Instances apply, making pre-provisioned capacity substantially cheaper than per-request Lambda at sustained high volume.

The three ticketing handlers share one LMI capacity provider. Each handler has its own scaling config applied at deploy time via `put-function-scaling-config`:

| Handler | Min EEs | Max EEs | PerEE Max Concurrency | Concurrent slots at Min |
|---|---:|---:|---:|---:|
| queue-position | 50 | 100 | 8 | 400 |
| inventory-lock | 100 | 500 | 8 | 800 |
| booking-confirm | 150 | 500 | 8 | 1,200 |

**Instance packing.** Each 8 GiB EE receives 4 vCPUs by Lambda's memory-to-vCPU proportion (`ExecutionEnvironmentMemoryGiBPerVCpu = 2.0`). A c7i.4xlarge (16 vCPU) hosts exactly 4 EEs, so 300 EEs at baseline = **75 c7i.4xlarge instances**.

### 4.2 Data layer: sharded DynamoDB

**Inventory table** — one row per seat, with a synthetic sharded partition key:

```
pk = f"{event_id}#{crc32(seat_id) % 500:03d}"
seat_id  = <seat identifier>
```

At 500 shards and 40 K TPS, each shard receives ~80 WCU/sec on average — an order of magnitude below the ~1,000 WCU/sec per-physical-partition ceiling even under load skew. Both `inventory-lock` and `booking-confirm` derive the same `pk` from `(event_id, seat_id)` deterministically, so they target the same physical row without coordination.

**Pre-provisioned capacity.** Both tables use `WarmThroughput` to pre-provision physical partitions before the burst:

- inventory table: 25,000 RCU / 40,000 WCU (higher write budget because inventory sees two writes per booking — the hold in Phase 2 and the transition in Phase 3)
- bookings table: 25,000 RCU / 25,000 WCU

### 4.3 Hot-key layer: ElastiCache Serverless (Valkey)

The queue counter is a **canonical hot-key pattern** — every request increments the same key. DynamoDB single-row atomic increments cap at ~1,000 ops/s because the row lives on one physical partition; Redis `INCR` on a single key handles over a million ops/sec because the counter lives in memory. This throughput asymmetry drives the two-store architecture:

- Redis for hot atomic counters (queue positions)
- DynamoDB for durable per-seat state (holds, bookings)

---

## 5. Load generator

The simulation stack drives synthetic traffic into the ticketing API through a three-tier fan-out that scales from smoke-test volumes into the hundreds of thousands of TPS:

```
Orchestrator (1)
  └─► Coordinator × C  (async, each tick)
        └─► Load Worker × W  (async, spawned in parallel threads)
              └─► BURST_SIZE concurrent HTTP requests
```

Each load worker fires `BURST_SIZE = 100` concurrent HTTP requests. At the calibrated per-request cost of ~100 ms (`AVG_DURATION_S` in `simulation-app/src/orchestrator/handler.py`) that is ~1,000 req/s per worker while it is running. Workers needed to sustain a target TPS:

```
workers = ceil(target_tps / (BURST_SIZE / avg_duration_s))
        = ceil(1,000,000 / (100 / 0.100))
        = ceil(1,000) = 1,000
```

| Target TPS | Workers/tick | Coordinators | Workers/coord | Concurrent Lambda functions |
|---:|---:|---:|---:|---:|
| 5,000 | 5 | 1 | 5 | 7 |
| 50,000 | 50 | 1 | 50 | 52 |
| 500,000 | 500 | 5 | 100 | 506 |
| **1,000,000** | **1,000** | **10** | **100** | **1,011** |

At 40 K TPS (~40 workers/tick) the simulation fits inside the default account concurrency limit of 1,000. Targets that push the concurrent-Lambda count above ~1,000 require a Lambda concurrency limit increase via AWS Support. The coordinator uses `ThreadPoolExecutor` to invoke all workers simultaneously, so every worker in a tick starts at the same instant rather than staggered — keeping the burst shape tight.

**Single-account ceiling.** A single-account load generator saturates around 65 K HTTP TPS at the current settings (`MAX_TOTAL_WORKERS = 2000`, `ThreadPoolExecutor(max_workers=128)` per worker Lambda). Use Distributed Load Testing on AWS or similar tooling for larger targets.

---

## 6. Concurrency safety

LMI's multi-concurrent execution model runs multiple invocations *inside the same execution environment*. Handler code must be safe under Python's threading model, which the three ticketing handlers achieve by:

- **No mutable module-level state** that could be clobbered between concurrent calls
- **All shared state** (seat status, queue counters) lives in DynamoDB or Redis with conditional writes and atomic operations
- **ThreadPoolExecutor** in the load worker is used only for I/O-bound HTTP fan-out — safe under Python's threading semantics for that use case
- **Idempotent endpoints** — retrying `POST /booking/confirm` with the same `booking_id` returns the existing booking record rather than creating a duplicate
