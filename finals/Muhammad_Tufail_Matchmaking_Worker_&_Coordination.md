# System Architecture Document: Matchmaking Worker & Coordination Engine (The Dispatcher)

Muhammad Tufail
## 1. Architectural Design: Matchmaking Workers & Consensus

The worker node system responsible for pairing riders and drivers acts as the critical processing layer between high-throughput driver telemetry streams and incoming rider demand. Rather than relying on a traditional First-In, First-Out (FIFO) message queue—which lacks spatial awareness and would order matches strictly by transaction time rather than geographical proximity—or executing expensive global scans against a centralized database, this system uses a stateful, stream-partitioned processing model. Finalized trip assignments are then durably committed to **CockroachDB**, our distributed SQL system of record.

### Worker Node System Design

To achieve sub-second matching, the architecture deploys a cluster of distributed Matchmaking Workers. These workers are stateful consumers that subscribe to two Apache Kafka topics: the `rider-requests` topic and the derived `driver-locations-by-cell` topic. As established in the edge ingestion pipeline, raw driver GPS pings land in the `driver-pings` topic partitioned by `hash(driver_id)` (which preserves per-driver trajectory order and gives even broker load), and an upstream Apache Flink job re-keys them into `driver-locations-by-cell`, partitioned by Uber's H3 geospatial hexagon cells at Resolution 9 (~0.1 km² cells). The Matchmaking Worker cluster is one of the six independent Kafka consumer groups that read off the ingestion pipeline; the analytics, ML, audit, surge-pricing, and notification pipelines are the other five, each consuming at their own pace via independent offsets.

Because the ingestion service already deduplicates pings on `(driver_id, sequence_number)`, the matchmaker treats each consumed location event as unique and performs no ping-level idempotency check of its own. Dispatch-level idempotency (a duplicate retry of "assign Driver X to Rider Y") is a separate concern, enforced by the `(trip_id, fencing_token)` pair on the CockroachDB write as described in Section 2.

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

Because local clocks can still drift or pause, leases alone cannot mathematically guarantee safety at the storage layer—a frozen worker may simply not realize its lease has expired. To completely neutralize zombie leaders, the system enforces **fencing tokens**, leveraging CockroachDB's strictly serializable transactions and Multiversion Concurrency Control (MVCC).

```text
ZooKeeper (Lease / Token Generator)
  |-- Grants lock to Worker A  -> Token = 100      [Worker A then freezes]
  |-- Session expires          -> Grants lock to Worker B  -> Token = 101
  v
Worker B (Active Leader)
  |-- Sends dispatch (Driver X -> Rider Y) to CockroachDB with token 101
  v
CockroachDB [Trip Database]
  |-- Executes UPDATE. Records driver X state = 'busy', fencing_token = 101.
  v
Worker A (Zombie Leader Wakes Up)
  |-- Attempts stale dispatch tagged with token 100
  v
CockroachDB [Trip Database]
  |-- FENCE ENFORCED: predicate (stored_token <= 100) fails because stored_token = 101.
```

When ZooKeeper elects a leader via an ephemeral-sequential znode, the generated sequence number serves as a natural, monotonically increasing fencing token. Worker A is elected with token 100; when it freezes and Worker B takes over, Worker B is assigned token 101. We deliberately reuse the ZK sequence number rather than introducing a parallel counter, so there is exactly one source of truth for leadership order.

To enforce the fence downstream, every dispatch commit from a Matchmaking Worker carries its token, and CockroachDB rejects writes from stale leaders:

1. When Worker B finalizes a match, it executes a transactional `UPDATE` on the driver and trip rows, setting `fencing_token = 101` only if the currently stored token is `<= 101`. Because CockroachDB uses serializable isolation, this commit is atomic.
2. When the zombie Worker A wakes up and submits its stale write, it issues something equivalent to:
   ```sql
   UPDATE driver_state
   SET    status = 'busy', trip_id = Y, fencing_token = 100
   WHERE  driver_id = X AND fencing_token <= 100;
   ```
3. CockroachDB evaluates the predicate. Because Worker B has already advanced the stored token to 101, `101 <= 100` is false; the update affects zero rows and the transaction is effectively a no-op (the worker treats "zero rows affected" as a fence rejection and aborts).

The predicate uses `<=` rather than strict `<` so that a current leader can issue multiple updates within its own lease: after its first commit the stored token equals its own token, and strict `<` would reject every subsequent write from that same leader. With `<=`, the active leader's writes succeed and any older leader's writes (carrying a strictly smaller token) are rejected.

## 3. Sharding & Hotspots: Partitioning the Active Driver Pool

The durable driver and trip state must be partitioned across many physical nodes to handle millions of writes per second. In this section, "active driver pool" refers to the durable projection of driver state in CockroachDB—status, current trip, last known cell, and fencing token—not the ephemeral in-memory R-Tree maintained by each matchmaking worker. The R-Trees are a hot cache rebuilt from Kafka; CockroachDB remains the system of record. Partitioning of this durable state relies on CockroachDB's distributed, range-based architecture.

### Partitioning Strategy via CockroachDB Ranges

Under the hood, CockroachDB stores all data in a single sorted key-value map. To distribute data across physical nodes, it divides this map into contiguous chunks called **ranges** (default ~512 MiB). Each range is an independent Raft consensus group replicated across multiple database nodes.

To match our geospatial matchmaking pattern, the driver-pool and trip tables are partitioned by geographic location, using the H3 cell index as the primary-key prefix:

```sql
PRIMARY KEY (h3_cell_id, driver_id)
```

Partitioning purely by `driver_id` would scatter drivers randomly across ranges, leading to expensive cross-node fan-out when auditing a specific geographic zone. Prefixing the primary key with `h3_cell_id` causes CockroachDB to naturally group all drivers within the same vicinity onto the same physical range, localizing reads and dispatch writes for a given zone.

### Handling Geographic Hotspots

Geospatial co-location introduces a physical vulnerability: **geographic hotspots**. When a stadium empties after a championship game, tens of thousands of *rider requests* and the resulting dispatch writes (`trips` inserts and `driver_state` updates) concentrate within a small handful of H3 cells. Because CockroachDB orders data sequentially by primary key, all writes for that `h3_cell_id` flood a single range, overwhelming the leaseholder for that range while the rest of the cluster sits idle. (Driver location updates also concentrate, but those are absorbed mostly by the Kafka log; the database-side hotspot is dominated by dispatch.)

Two complementary mitigations are used, both relying on native CockroachDB features:

```text
Baseline hotspot scenario:
  [Single H3 Cell: Stadium]  === H3-prefixed PK ===>  [Single CockroachDB Range]  (overload!)

Mitigation 1: Load-Based Range Splitting
  [Range covering hot cells]  ---> detects high QPS / CPU --->  dynamically splits range
                                                                       |
                                                                       ===> Raft rebalances halves to new nodes

Mitigation 2: Suffix-Based Salting / Hash-Sharded Indexes
  PK = (h3_cell_id, driver_id % S, driver_id)  --->  S = 5  --->  5 distinct ranges, 5 leaseholders
```

#### 1. Dynamic Load-Based Range Splitting

CockroachDB monitors queries-per-second and CPU load on every range. If write load on a hot range breaches a threshold, CockroachDB triggers a **load-based split**, even if the range is well under its 512 MiB size limit. The cluster then rebalances the newly created ranges to different physical nodes via Raft, scattering the localized load without any application-level downtime or manual resharding.

#### 2. Suffix-Based Salting & Hash-Sharded Indexes

If a hotspot is extreme enough that load-based splitting cannot keep up organically, the application enforces **deterministic salting**, either manually or via CockroachDB's native hash-sharded indexes:

```sql
PRIMARY KEY (h3_cell_id, salt_bucket, driver_id)
-- where salt_bucket = driver_id % S
```

With a salt factor `S = 5`, driver-state updates for a single congested cell are distributed across five distinct key prefixes and therefore across five independent ranges with five different leaseholders. Reads that need the full picture of a cell now require a bounded scatter-gather across the five buckets.

Salting is purely a write-side optimization and explicitly sacrifices the read-side locality that motivated prefixing the primary key with `h3_cell_id`: matchmaking and audit queries against a salted cell must now scatter-gather across `S` ranges instead of reading from one. Salting is therefore applied selectively, only to the small set of cells flagged as chronic hotspots (stadiums, airports, central business districts), so that the rare hot cells pay for parallelism while the common case retains its single-range read pattern.
