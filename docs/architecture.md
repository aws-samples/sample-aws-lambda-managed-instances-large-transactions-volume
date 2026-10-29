# Architecture — LMI Ticketing Reference

A reference architecture for concert-ticketing-style workloads on AWS Lambda Managed Instances (LMI), Amazon DynamoDB, and Amazon ElastiCache Serverless. This document describes the services in the deployed topology and how they interact. For the request/response schema and error paths see [`booking-flow.md`](./booking-flow.md).

## Contents

1. [Overview](#1-overview)
2. [Architecture diagram](#2-architecture-diagram)
3. [Ticketing data plane](#3-ticketing-data-plane)
4. [Simulation control plane](#4-simulation-control-plane)
5. [How the components interact](#5-how-the-components-interact)
6. [Deployment topology](#6-deployment-topology)

---

## 1. Overview

The reference implements a **queue plus hold-and-confirm** booking flow — the standard pattern for inventory-constrained, high-concurrency e-commerce. A caller takes a queue position, places a time-limited hold on a specific seat, and then atomically converts the hold into a durable booking.

The system is split into two independent CloudFormation stacks:

- **Ticketing data plane** — the system under test. Serves the booking flow.
- **Simulation control plane** — a separate stack that drives synthetic load into the ticketing API for burst testing. Not required to run the ticketing API.

Both stacks deploy into a single region and share nothing at the data layer; the simulation stack reaches the ticketing stack only as a signed HTTP client.

---

## 2. Architecture diagram

![AWS architecture](images/architecture.png)

The diagram shows the Simulation App on the left, driving load into the Ticketing App on the right. The ticketing data plane sits in a customer VPC with scoped egress to AWS service endpoints; the simulation stack sits outside the VPC and calls the ticketing API over the public API Gateway endpoint using SigV4-signed requests.

---

## 3. Ticketing data plane

The data plane is three Lambda handlers behind one API Gateway, backed by one ElastiCache Serverless cache and two DynamoDB tables.

### Amazon API Gateway

Regional REST API that terminates TLS and performs SigV4 authentication against AWS_IAM. Every route requires a signed caller — anonymous requests are rejected before any handler runs. CORS is restricted to a configured operator origin. The gateway front-ends three routes, one per handler.

### AWS Lambda — Managed Instances (LMI)

Three Lambda functions, one per booking phase, hosted on a shared LMI capacity provider:

- `queue-position` — handles queue admission.
- `inventory-lock` — places a time-limited hold on a seat.
- `booking-confirm` — converts a hold into a durable booking.

LMI runs these functions on pre-provisioned, Lambda-managed capacity inside the customer VPC. Each function keeps a warm baseline of execution environments and bursts to a per-function ceiling under load. Multiple invocations run concurrently inside the same execution environment, so handler code must be written to be thread-safe.

### Amazon ElastiCache Serverless

In-memory data store for the hot queue-counter key. Chosen for atomic increment on a single key at high throughput — the access pattern an in-memory key-value store is purpose-built for. Reachable only from the LMI security group over TLS; no public endpoint.

### Amazon DynamoDB — inventory table

Durable per-seat state. One row per seat, keyed by event and seat identifier. The handlers use DynamoDB conditional writes to enforce "one winner per seat" without a distributed lock — the condition is the lock. Encrypted at rest with a customer-managed KMS key.

### Amazon DynamoDB — bookings table

Durable per-booking record. One row per confirmed booking. Written as part of the same atomic `TransactWriteItems` operation that transitions the seat row from HELD to CONFIRMED, so inventory and bookings never diverge. Also CMK-encrypted.

### Amazon CloudWatch, AWS X-Ray

Standard observability wiring — handler logs, API Gateway access logs, X-Ray traces, and CloudWatch metrics. Deployed by both stacks.

---

## 4. Simulation control plane

The simulation stack drives test load into the ticketing API. It is deliberately separate from the data plane so that a test run cannot destabilise production inventory and so operators can delete it independently.

### Amazon API Gateway (simulation)

Separate REST API for test control. Routes:

- `POST /simulation/start` — begin a burst run.
- `GET /dashboard` — read aggregated results for a completed run.

AWS_IAM-authenticated like the ticketing API.

### AWS Lambda — simulation functions

Four Lambda functions handle the test lifecycle:

- **Orchestrator** — entry point. Validates run parameters against server-side clamps, assigns a run ID, and starts the fan-out.
- **Coordinator** — middle tier that spawns workers in parallel.
- **Load worker** — issues SigV4-signed HTTP requests to the ticketing API.
- **Metrics dashboard** — aggregates results per run from CloudWatch, DynamoDB, and the ticketing-side logs.

Reserved concurrency on the orchestrator caps how many simulation runs can be in flight simultaneously.

### Amazon DynamoDB (simulation)

A results table that stores per-run aggregates for the dashboard to read.

### Amazon CloudWatch, AWS X-Ray

Simulation-side telemetry. Metrics and traces are tagged with the run ID so a single run can be isolated end to end.

---

## 5. How the components interact

A single booking traverses the services in the order below. Each hop is a synchronous HTTPS call; there are no queues or asynchronous workers in the booking path.

1. The caller sends a SigV4-signed request to the ticketing API Gateway.
2. API Gateway authorises the caller and routes to the appropriate Lambda handler.
3. The handler executes against its backing store:
   - `queue-position` → ElastiCache (atomic counter increment)
   - `inventory-lock` → DynamoDB inventory table (conditional seat state transition)
   - `booking-confirm` → DynamoDB bookings table **and** DynamoDB inventory table in one atomic `TransactWriteItems`
4. The response flows back through API Gateway to the caller.

For burst-load testing, the simulation stack drives traffic into step 1:

1. A test runner calls the simulation API Gateway.
2. The orchestrator Lambda validates parameters and fans out to coordinators.
3. Coordinators spawn worker Lambdas that issue SigV4-signed calls directly to the ticketing API Gateway, following the same path above.
4. Workers write per-request results to CloudWatch; the dashboard aggregates them on demand.

The two stacks share no data at rest. The simulation stack is a pure HTTP client of the ticketing API.

---

## 6. Deployment topology

The reference is provisioned as two CloudFormation stacks:

- **`ticketing-lmi`** — API Gateway, LMI capacity provider and three functions, ElastiCache Serverless, two DynamoDB tables, their CMKs, log groups, and the VPC security groups that scope network reach.
- **`ticketing-simulation`** — API Gateway, four Lambda functions, a results table, and the IAM role that lets the load worker SigV4-sign calls to the ticketing API.
