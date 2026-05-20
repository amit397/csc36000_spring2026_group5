# System Architecture Document: Matchmaking Worker & Coordination Engine (The Dispatcher)

Muhammad Tufail
## 1. Architectural Design: Matchmaking Workers & Consensus

The worker node system responsible for pairing riders and drivers acts as the critical processing layer between high-throughput driver telemetry streams and incoming rider demand. Rather than relying on a traditional First-In, First-Out (FIFO) message queue—which lacks spatial awareness and would order matches strictly by transaction time rather than geographical proximity—or executing expensive global scans against a centralized database, this system uses a stateful, stream-partitioned processing model. Finalized trip assignments are then durably committed to **CockroachDB**, our distributed SQL system of record.

### Worker Node System Design

To achieve sub-second matching, the architecture deploys a cluster of distributed Matchmaking Workers. These workers are stateful consumers that subscribe to two Apache Kafka topics: the `rider-requests` topic and the derived `driver-locations-by-cell` topic. As established in the edge ingestion pipeline, raw driver GPS pings land in the `driver-pings` topic partitioned by `hash(driver_id)` (which preserves per-driver trajectory order and gives even broker load), and an upstream Apache Flink job re-keys them into `driver-locations-by-cell`, partitioned by Uber's H3 geospatial hexagon cells at Resolution 9 (~0.1 km² cells). The Matchmaking Worker cluster is one of the six independent Kafka consumer groups that read off the ingestion pipeline; the analytics, ML, audit, surge-pricing, and notification pipelines are the other five, each consuming at their own pace via independent offsets.

Because the ingestion service already deduplicates pings on `(driver_id, sequence_number)`, the matchmaker treats each consumed location event as unique and performs no ping-level idempotency check of its own. Dispatch-level idempotency (a duplicate retry of "assign Driver X to Rider Y") is a separate concern: every dispatch carries an `event_id` matching Hurera's `UNIQUE(trip_id, event_id)` constraint on `trip_state_transitions`, so a retried dispatch — e.g. after a Kafka consumer-group rebalance forces the same rider request to be re-read — cannot re-apply the same state transition twice. The fencing mechanism in Section 2 handles the orthogonal zombie-leader case.

```text
[Driver phones] --gRPC--> [Envoy + Ingestion svc] --> [Kafka: driver-pings]
                                                       (partitioned by hash(driver_id))
                                                                |
                                                                v
                                                        [Flink re-key job]
                                                                |
                                                                v
                                              [Kafka: driver-locations-by-cell]
                                              (partitioned by H3 cell, Res 9)
                                                                |
[Kafka: rider-requests] -----> ( Matchmaking Worker Cluster ) <-+
                                          |  (one of six consumer groups
                                          |   on driver-locations-by-cell)
                                          v
                                [Local In-Memory R-Tree per cell]
                                (RocksDB state backend
                                 for fast restart)
                                          |
                                          v
                              k_ring(cell,1) lookup → radius → ETA
                                          |
                                          v
                            [Final Dispatch -> CockroachDB]
```

Each Matchmaking Worker is assigned exclusive responsibility for a specific set of H3 cells. The worker consumes the location events for its assigned cells and maintains a local, high-performance **in-memory R-Tree** that holds the *hot, derived view* of "currently available drivers per cell." This in-memory index is the structure actually queried during matching, because the spatial query (radius / k-NN within a cell) is a natural fit for an R-Tree and not for a flat key-value lookup. At the 4-second ping cadence specified upstream, the R-Tree view is at-worst ~4 seconds stale per driver, which is well within the freshness budget for dispatch ETA computation.

When a rider request is pulled from the broker, the worker reads the coordinates, performs an H3 `k_ring(cellId, 1)` lookup to include the six immediately neighboring cells (so a rider sitting near a cell boundary still sees nearby drivers in the adjacent cell), draws a bounding radius, and queries its local R-Tree to compute the driving ETA of candidate drivers. This avoids paying the cost of a spatial query against CockroachDB on every match. Only once a match is calculated does the worker commit the authoritative trip assignment to CockroachDB. The system of record for trip and driver state remains CockroachDB; the worker-local R-Tree is an ephemeral cache derived from the Kafka log.

#### Worker State Backend

To survive process restarts without re-replaying the full Kafka topic, each worker backs its R-Tree with a local RocksDB store, following the same keyed-state pattern used by Kafka Streams and Flink. The driver-to-last-known-location map underlying the R-Tree is periodically snapshotted to RocksDB, and the corresponding Kafka offsets are committed atomically against each snapshot. On restart, the worker reloads its assigned cells from RocksDB and tails Kafka only from the last committed offset, reducing cold-start catch-up from minutes to seconds. This matters because the upstream `driver-pings` topic retains 24 hours of data on hot SSDs; without a local snapshot, a worst-case restart could replay hours of pings under exactly the conditions when Kafka is least healthy.

### The Necessity of Consensus for Leader Election

Because the system must handle regional hotspots, multiple riders in the exact same city block will submit requests simultaneously. If two independent workers are allowed to process the same geographic partition concurrently, both can evaluate the same local spatial state, identify the same optimal driver as available, and issue concurrent dispatch commands to CockroachDB. This directly violates a non-negotiable invariant of the platform: **a driver must never be double-booked**.

To prevent this, the platform enforces strict serialization of dispatch decisions within any given geographic zone. This requires distributed consensus to perform leader election among matchmaking workers: for each H3 cell (or group of cells), exactly one worker is elected as the authoritative leader, and all rider requests and driver location updates for that zone are routed exclusively through it, ensuring matching logic is executed single-threaded per cell.

To keep this consistent with the Kafka layer, the H3-cell-to-Kafka-partition mapping and the H3-cell-to-leader mapping are deliberately aligned. The Flink re-key job uses a deterministic partitioner over `driver-locations-by-cell` so that each H3 cell always maps to exactly one Kafka partition (many cells per partition, never a cell split across partitions). The same worker that the Kafka consumer-group protocol assigns to that partition is the one that contests the ZooKeeper lock for the cells in that partition. In steady state the Kafka consumer assignment and the ZK leadership agree; ZK is the authoritative arbiter when they diverge (e.g., during a consumer-group rebalance window), and a worker refuses to dispatch for a cell until it both owns the Kafka partition and holds the ZK lock. This avoids a split where the Kafka consumer for a partition is one worker but the ZK-elected leader for a cell in that partition is another.

### Evaluating Custom Consensus vs. ZooKeeper vs. etcd

Building a custom consensus protocol inside the application layer is rejected because consensus is difficult to implement. Distributed consensus requires rigorous handling of network partitions, message reordering, asymmetric failure detection, and clock skew, and implementing a custom version of Raft or Paxos introduces significant engineering risk and operational overhead for no differentiating benefit.

Between the two production-grade options:

- **Apache ZooKeeper** implements the ZAB protocol, exposes a hierarchical znode API, and offers ephemeral-sequential nodes plus watches—a combination that maps directly onto the "elect one leader per H3 cell, notify the next-in-line on failure" pattern. The Apache Curator library provides battle-tested leader-election recipes.
- **etcd** implements Raft, exposes a flat key/value API with lease-based keys and watch streams, and ships a `concurrency.NewElection` primitive. It is operationally simpler (single Go binary, no JVM), and is the default coordination service in the Kubernetes ecosystem.

For this system we choose ZooKeeper, primarily because (a) the ephemeral-sequential + watch-on-predecessor pattern is the cleanest way to avoid a thundering herd across thousands of per-cell elections, and (b) Curator's recipes reduce application-layer surface area. etcd would be a defensible alternative; the decision would flip if the rest of our infrastructure were already Go/Kubernetes-native or if we needed first-class lease primitives without ZK's session-management nuances.

Leader election is achieved using ZooKeeper's ephemeral-sequential znodes: the worker that creates the node with the lowest sequence number (e.g., `lock-0000000001`) is the authoritative leader for that cell. To avoid a thundering herd, each follower sets a ZooKeeper watch exclusively on the znode immediately preceding its own sequence number. If a leader crashes, its ephemeral node is deleted and only the next sequential follower is notified to step up.

## 2. State & Concurrency: Preventing Split-Brain Errors

In large-scale distributed infrastructure, network partitions or host resource exhaustion can isolate a worker without terminating its process. This creates a split-brain vulnerability where two nodes believe they are the legitimate leader of the same partition.

### The Split-Brain Problem

Consider the scenario where Worker A is the active leader for an H3 cell. Worker A experiences a 15-second JVM Garbage Collection pause. Because Worker A is frozen, its runtime stops sending heartbeats to ZooKeeper. After the configured session timeout of 10 seconds is breached, ZooKeeper assumes Worker A has failed, deletes its ephemeral znode, and notifies the next worker in line, Worker B. Worker B immediately assumes leadership and begins processing matches.

A few seconds later, Worker A's GC pause ends. Worker A's wall-clock and internal state were halted during the freeze; it is entirely unaware that its session expired and that a new leader was crowned. Worker A is now a **zombie leader**. If Worker A attempts to complete a pending match and write an assignment to CockroachDB, that write races with Worker B's writes and could double-book the driver.

The session timeout itself is a deliberate trade-off between failure-detection speed and false-positive failovers under transient network blips. Steady-state matching remains sub-second; failover for a given cell, however, takes at least the ZK session timeout (10 seconds in this configuration) before a new leader can safely take over.

### Distributed Leases

To minimize the window in which a zombie can do damage, leadership is managed via **distributed leases**. A lease is a strictly time-bounded lock granted by the coordination service. A worker may execute matchmaking logic only if it can confirm, against its local monotonic clock, that its lease has not expired. If the worker cannot successfully heartbeat to ZooKeeper within the lease window, it must voluntarily relinquish leadership and stop dispatching *before* the coordination service reassigns the lease to a competitor. In ZooKeeper, the session-and-ephemeral-node mechanism *is* the lease: the ephemeral znode exists only while the session is alive, and session expiry is the lease expiry.

### Fencing Tokens & Downstream Enforcement via CockroachDB

Because local clocks can still drift or pause, leases alone cannot mathematically guarantee safety at the storage layer—a frozen worker may simply not realize its lease has expired. To completely neutralize zombie leaders, the matchmaker adds a **`fencing_token` column** to Hurera's `trips` schema and enforces a monotonic-token check on every dispatch write, leveraging CockroachDB's strictly serializable transactions and Multiversion Concurrency Control (MVCC).

**Two distinct tokens, kept deliberately separate.** It is tempting to say "the ZK sequence number *is* the fencing token", but doing so conflates two values that live in two systems with different failure modes. They are kept apart in this design:

- **ZK leader token** — an ephemeral-sequential sequence number generated inside ZooKeeper at election time (e.g. Worker A's `lock-0000000100`, Worker B's `lock-0000000101`). It exists only in ZK and is what the consensus layer uses to *order leaders*. CockroachDB never sees it; ZooKeeper never sees the database.
- **`trips.fencing_token`** — a column the matchmaker adds to Hurera's `trips` schema, holding the largest leader-token that has ever successfully written this row. The DB column has its own MVCC lifecycle; ZK has no opinion on it.

The two are bridged only by the worker, which carries its current ZK leader token into each dispatch as a *value* and writes that value into `trips.fencing_token`. ZK can be unreachable without invalidating tokens already stored on rows, and the database can be queried without involving ZK. Keeping the two artifacts in separate failure domains is what lets each layer fail closed independently.

```text
ZooKeeper                                  CockroachDB (trips.fencing_token)
  |-- Worker A elected (ZK token 100)         [stored = 0 on fresh trip rows]
  |-- Worker A freezes
  |-- Session expires → Worker B elected
       (ZK token 101)                         [unchanged: still 0]
  v
Worker B (Active Leader, carries ZK token 101)
  |-- UPDATE trips
        SET ..., fencing_token = 101
        WHERE trip_id = Y AND fencing_token <= 101;
  v
CockroachDB
  |-- Predicate (stored=0 <= 101) holds; 1 row affected.
  |-- trips.fencing_token now = 101.
  v
Worker A (Zombie, still carries old ZK token 100)
  |-- UPDATE trips
        SET ..., fencing_token = 100
        WHERE trip_id = Y AND fencing_token <= 100;
  v
CockroachDB
  |-- FENCE ENFORCED: stored (101) <= 100 is false; 0 rows affected.
  |-- Worker treats the zero-row result as a fence rejection and aborts.
```

To enforce the fence downstream, every dispatch commit carries the worker's current ZK leader token into the predicate:

1. When Worker B finalizes a match, it executes a transactional `UPDATE` on the trip row, setting `fencing_token = 101` only if the currently stored token is `<= 101`. CockroachDB's serializable isolation makes the comparison and the write one atomic step.
2. When the zombie Worker A wakes up and submits its stale dispatch, it issues something equivalent to:
   ```sql
   UPDATE trips
   SET    driver_id = X, current_state = 'ASSIGNED', fencing_token = 100
   WHERE  trip_id = Y AND fencing_token <= 100;
   ```
3. Because Worker B has already advanced the stored token to 101, the predicate `101 <= 100` is false; the update affects zero rows and the transaction is effectively a no-op (the worker treats "zero rows affected" as a fence rejection and aborts).

The predicate uses `<=` rather than strict `<` so that a current leader can issue multiple updates within its own lease: after its first commit the stored token equals its own token, and strict `<` would reject every subsequent write from that same leader. With `<=`, the active leader's writes succeed (stored equals my token) and any older leader's writes (carrying a strictly smaller token) are rejected. The `<=` form also handles bootstrap cleanly: a new leader's first write succeeds against a row whose stored token came from an older leader, because that older value is strictly smaller than the new leader's own.

**Cross-row safety net via Hurera's partial unique index.** Per-row fencing closes the *same-trip* race — A and B both racing to dispatch the same rider request. It does *not* directly close the *cross-trip* race in which the zombie and the active leader pick the same driver but write to different `trip_id` rows, so each row's `fencing_token` check passes on its own. That residual case is closed by Hurera's partial unique index `one_active_trip_per_driver ON trips(driver_id) WHERE current_state IN (ASSIGNED, EN_ROUTE, IN_PROGRESS)`: only one of the two writes can persist that driver in an active state, and the second fails with a unique-index violation. Section 3 returns to this index when discussing hotspots, where its physical layout becomes the relevant contention surface.

## 3. Sharding & Hotspots: Partitioning the Active Driver Pool

The durable trip state must be partitioned across many physical nodes to handle millions of writes per second. In this section, **"active driver pool" is not a separate table** — it is the derived projection of Hurera's `trips` table defined by the partial unique index `one_active_trip_per_driver ON trips(driver_id) WHERE current_state IN (ASSIGNED, EN_ROUTE, IN_PROGRESS)`. That single constraint is what lets the matchmaker treat "the set of drivers currently on a trip" as a first-class concept without introducing a parallel store: the index *is* the pool, and the same index is also the cross-row guarantee against double-booking from Section 2. The ephemeral in-memory R-Tree maintained by each matchmaking worker is a hot cache rebuilt from Kafka; CockroachDB remains the system of record.

### Partitioning Strategy via CockroachDB Ranges

Under the hood, CockroachDB stores all data in a single sorted key-value map. To distribute data across physical nodes, it divides this map into contiguous chunks called **ranges** (default ~512 MiB). Each range is an independent Raft consensus group replicated across multiple database nodes.

Hurera's schema fixes the partitioning choices for us. The `trips` primary key is `trip_id` (UUID) and the table is **geo-partitioned by region** so each region's rows are domiciled and Raft-replicated locally (Trip State Machine doc §1.3). Two consequences fall out for the matchmaker:

```sql
-- Hurera's trips schema (relevant excerpt)
PRIMARY KEY (trip_id)                                       -- UUID, naturally well-distributed
PARTITION BY LIST (region)                                  -- regional geo-partitioning
UNIQUE INDEX one_active_trip_per_driver
    ON trips (driver_id)
    WHERE current_state IN ('ASSIGNED', 'EN_ROUTE', 'IN_PROGRESS')
```

1. **Within a region, dispatch writes scatter naturally.** Because `trip_id` is a UUID, two simultaneous `REQUESTED → ASSIGNED` updates almost never land on the same range — there is no key-prefix concentration to fight. The same holds for inserts of new `REQUESTED` rows. This is the same distribution argument that justified `hash(driver_id)` partitioning on the Kafka `driver-pings` topic in Section 1, applied at the storage layer.
2. **Spatial locality is provided by the R-Tree, not the database.** Because matchmaking never issues a spatial query against `trips` on the hot path — the worker queries its local R-Tree — we do not need the database PK to encode geography. The H3-cell layout lives in the Kafka topic (`driver-locations-by-cell`) and in the worker's R-Tree; the DB only sees the final dispatch write.

So the partitioning story is: regional geo-partitioning gives data residency and intra-region replication latency, the UUID PK gives intra-region write distribution, and the partial unique index gives us the "active driver pool" view at zero schema cost.

### Handling Geographic Hotspots

Even with UUID-keyed `trip_id`, **geographic hotspots** can still bite the matchmaker in two ways:

- **Time-localized write bursts.** When a stadium empties after a championship game, tens of thousands of rider requests and the resulting `REQUESTED` inserts plus `REQUESTED → ASSIGNED` updates concentrate in a few seconds. They are spread across many ranges by UUID, but the *aggregate* write rate on whichever ranges happen to receive them can still saturate their leaseholders.
- **Per-driver index contention on `one_active_trip_per_driver`.** Each contested dispatch attempt against the same driver must touch the same index entry; under a hot-cell burst, many workers race for the small set of drivers physically present in that cell, all writing to nearby index keys.

Two complementary mitigations are used, both relying on native CockroachDB features:

```text
Baseline hotspot scenario:
  [Stadium burst]  === thousands of writes/sec ===>  [A handful of trips ranges]  (saturation)

Mitigation 1: Load-Based Range Splitting
  [Hot range]  ---> detects high QPS / CPU --->  dynamically splits range
                                                          |
                                                          ===> Raft rebalances halves to new nodes

Mitigation 2: Hash-Sharded Index on the hot lookup
  one_active_trip_per_driver  --->  hash-sharded (S buckets)
  --->  S distinct index ranges, S leaseholders
```

#### 1. Dynamic Load-Based Range Splitting

CockroachDB monitors queries-per-second and CPU load on every range. If write load on a hot `trips` range breaches a threshold, CockroachDB triggers a **load-based split**, even if the range is well under its 512 MiB size limit. The cluster then rebalances the newly created ranges to different physical nodes via Raft, scattering the localized load without any application-level downtime or manual resharding. Because Hurera's `trip_id` PK is a UUID, splits land on essentially arbitrary boundaries and never have to track a moving geographic key.

#### 2. Hash-Sharded Indexes on the Contended Lookup Path

The partial unique index `one_active_trip_per_driver` can itself become a hotspot under a stadium-scale burst: although `driver_id` is a UUID, the *physically present* drivers in one H3 cell are a small set, so their index entries are nearby in the index key space and a handful of index ranges can carry most of the contention. CockroachDB's native `USING HASH WITH BUCKET_COUNT = S` clause splits the index across `S` shards:

```sql
UNIQUE INDEX one_active_trip_per_driver
    ON trips (driver_id) USING HASH WITH BUCKET_COUNT = 8
    WHERE current_state IN ('ASSIGNED', 'EN_ROUTE', 'IN_PROGRESS')
```

With eight buckets, contested writes against the same congested cell now hit eight independent index ranges with eight different leaseholders. Lookups by `driver_id` still touch only one bucket (the bucket is a deterministic function of `driver_id`), so the cross-row uniqueness guarantee from Section 2 — "this driver is not in another active trip" — is preserved.

Hash-sharding the *index* rather than the *primary key* is the right knob here because the table itself is already well-distributed by UUID PK; only the index that joins on `driver_id` needs help. This applies selectively to the small set of indexes that show measurable contention, so the common case retains its single-bucket lookup pattern.
