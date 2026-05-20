# Final Project: Global Ride-Sharing Platform

**Course:** CSC 36000 — Modern Distributed Computing, Spring 2026
**Group:** 5

This directory contains our group's final design submission for **Project 1: Global Ride-Sharing Platform**. The system must handle millions of concurrent real-time GPS updates, accurately match riders to drivers without double-booking, and safely process payments across distributed services. Each group member owns one slice of the architecture; together, the four documents describe a single coherent system.

## System Overview

We model the platform as four cooperating layers, ingestion → matchmaking → ledger → safety net, all sharing a common backbone of Apache Kafka (the durable event log) and CockroachDB (the system of record for trip and payment state):

```text
[Driver phones] --gRPC--> [Edge gateways + Ingestion]
                              |
                              v
                  [Kafka: driver-pings, driver-locations-by-cell, rider-requests]
                              |
                              v
                  [Matchmaking Workers]  ---ZK leader election + per-cell R-Trees
                              |
                              v
                  [CockroachDB: trips, trip_state_transitions, payments, driver_credits]
                              |
                              v
                  [Saga settlement → external Payment Provider]

(crosscutting) Tracing + metrics + checkpoints + global snapshots
```

Three design choices tie the slices together:

1. **The Kafka log is the spine.** Six independent consumer groups (matchmaking, analytics, ML, audit, surge pricing, notifications) read the same firehose at their own pace. A slow consumer never back-pressures ingestion.
2. **CockroachDB is the single system of record.** All four documents target the same cluster. The `trips` table schema defined in the Trip State Machine doc is the canonical schema; the matchmaker writes into it via guarded `UPDATE`s, and analytics reads from it via MVCC snapshot reads.
3. **Invariants are enforced at the storage layer.** "No double-booked drivers", "no double-charged riders", and "no illegal state transitions" are all enforced by database constraints (a partial unique index, a `UNIQUE(idempotency_key)`, and a state-guarded `version` check), not by hopeful application code. This is what lets the four services be developed independently without contradicting each other.

## How to Read This Submission

Read the four documents in **order**. Each one builds on the assumptions of the previous one, and the cross-references inside the docs all flow downstream:

1. **Amit Howlander — Edge Ingestion & Streaming (The Location Firehose)** — sets the topic names, partition keys, deduplication keys, and ping cadence that the rest of the system relies on.
2. **Muhammad Tufail — Matchmaking Worker & Coordination (The Dispatcher)** — consumes the topics from Kafka stream logs, performs ZK-based leader election, and commits dispatch writes against the schema from Section 3.
3. **Hurera Ranjha — Trip State Machine & Distributed Transactions (The Ledger)** — defines the canonical `trips`/`trip_state_transitions`/`payments`/`driver_credits` schema, the legal state transitions, and the Saga that settles payments against an external provider.
4. **Christopher Santana — Observability, Fault Tolerance, & Recovery (The Safety Net)** — adds the distributed tracing, metrics dashboards, snapshots, and disaster-recovery story that wrap the other three.

The two PNGs (`state-machine.png` and `sequence diagram.png`) are referenced from Student 3's document.

## Submission Index

| # | Author | Section | File |
|---|---|---|---|
| 1 | Amit Howlander      | Edge Ingestion & Streaming | [Amit_Edge_Ingestion_&_Streaming.md](./Amit_Edge_Ingestion_&_Streaming.md) |
| 2 | Muhammad Tufail     | Matchmaking Worker & Coordination | [Muhammad_Tufail_Matchmaking_Worker_&_Coordination.md](./Muhammad_Tufail_Matchmaking_Worker_&_Coordination.md)         |
| 3 | Hurera Ranjha       | Trip State Machine & Distributed Transactions | [Hurera_Ranjha_Trip_State_Machine_&_Distributed_Transactions .md](./Hurera_Ranjha_Trip_State_Machine_&_Distributed_Transactions%20.md) |
| 4 | Christopher Santana | Observability, Fault Tolerance, & Recovery | [Christopher_Santana_Fault_Tolerance_&_Recovery.md](./Christopher_Santana_Fault_Tolerance_&_Recovery.md) |
