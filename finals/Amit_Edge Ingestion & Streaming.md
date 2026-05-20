Amit Howlader  
Edge Ingestion & Streaming

**Architectural Design**:  
At the core of a ride-sharing platform (matching, surge pricing, etc.) is the ride itself and knowing where each driver is right now. This section covers how the system is designed to handle the path from the GPS ping on the driver's phone to a durable stream of data consumed by the rest of the platform. Three decisions dominate the design: how the mobile client reaches the edge, which streaming system ingests the firehose (GPS data), and how that stream is partitioned to survive regional hotspots.

The data flow starts with a gRPC bidirectional stream from the driver's phone to the nearest regional Envoy edge service proxy. This gateway forwards all requests (after authentication and rate-limits) to a stateless ingestion service that deduplicates on (driver\_id, sequence\_number) and writes to a Kafka topic partitioned by driver\_id. Six consumer groups read independently. A Flink job re-keys a derived topic by H3 cell, so downstream spatial queries do not require a global scan. 

**Decision 1** \- Distributed log over message queue: A queue (RabbitMQ, SQS) consumes messages destructively: once a worker reads one, it's gone. Fan-out to N consumers means N duplicate queues, which doubles storage for every new subscriber and couples the producer to the consumer topology. A log (Kafka) keeps messages for a retention window and tracks per-consumer offsets, so six independent consumer groups read the same topic at their own pace. A slow ML pipeline doesn't back-pressure matchmaking; a buggy analytics consumer can replay from last Tuesday's offset without disturbing the others. The cost is operational complexity: rebalancing, offset management, broker tuning. Worth it because no queue setup matches a log's fan-out and replay at six consumers.  
   
**Decision 2** \- Partitioning by driver\_id with downstream re-keying: Two partition keys are viable, and choosing the wrong one breaks the system. H3-cell partitioning sounds right because spatial reads become partition-local, but it dies at hotspots: a single cell over JFK at 6pm concentrates tens of thousands of drivers onto one partition, and no broker tuning fixes that. Drivers crossing cell boundaries also re-hash mid-trip, breaking per-driver order. Partitioning by hash(driver\_id) gives even distribution and preserves trajectory order; the cost is that spatial queries become global scans. We resolve this in two stages: ingest into a driver\_id topic, then a Flink job re-keys a derived topic by H3 cell at resolution 9\. Spatial consumers read the derived topic, per-driver consumers read the original. Storage roughly doubles; ordering and locality both survive.  
   
**Decision 3** \- Durability and Consumer fan-out: Durability runs on acks=all, min.insync.replicas=2, replication.factor=3. One extra network hop per write means a single broker failure cannot lose pings. Retention is 24 hours on local SSDs for hot replay, tiered to object storage for 7 days. Multi-region uses active-active replication via uReplicator: regional clusters accept local writes, aggregate clusters hold copies from every other region, and a regional outage triggers failover in under 30 seconds with no data loss. The six consumer groups commit offsets independently, so a slow consumer never propagates back-pressure to ingestion. The log is the buffer. 

**AI & RPC Contracts**:  
The driver app holds a single long-lived gRPC stream to the regional Envoy gateway and writes one Protobuf-encoded ping every 4 seconds. gRPC was chosen over HTTP/JSON because HTTP/2 multiplexes a persistent connection (no per-ping TLS handshake), Protobuf serialization runs roughly 4 to 10 times smaller than equivalent JSON, and the gRPC stack has first-class deadline propagation that HTTP-over-JSON lacks. WebSockets were rejected because we would end up rebuilding gRPC's schema and framing layer ourselves. MQTT is defensible for IoT-style telemetry, but our downstream services are already on gRPC and the team already runs gRPC service-to-service.

```proto
service DriverLocationService {
  rpc StreamLocation(stream LocationPing) returns (stream ServerEvent);
}

message LocationPing {
  string driver_id        = 1;
  int64  sequence_number  = 2;
  double lat              = 3;
  double lng              = 4;
  float  heading          = 5;
  float  speed_mps        = 6;
  float  accuracy_m       = 7;
  int64  client_unix_ms   = 8;
}
```

The sequence\_number is the idempotency key. It is monotonically incremented per driver on the client and survives retries unchanged, so the ingestion service can deduplicate on (driver\_id, sequence\_number) regardless of how many times a single ping arrives.  
Client per-ping deadline is 1.5 seconds. The number is anchored to the ping cadence: at one ping every 4 seconds, a response that takes longer than 1.5 seconds is already being replaced by the next ping. Aggressive deadlines protect the gateway from holding dead connections during partial failures and waste no usable information. The mobile client uses gRPC's deadline header on each write within the stream, which propagates through Envoy and into the ingestion service. When the ingestion service writes to Kafka, it passes the remaining deadline rather than a fresh one, so a request that has already spent 1.2 seconds in the gateway gives Kafka 300 milliseconds, not 1.5.  
Connection-level health uses HTTP/2 PING frames every 30 seconds. Mobile networks fail silently: a phone that drops off LTE while parked underground will not get a TCP RST. PING frames detect the dead connection in seconds rather than minutes and free the gateway's socket budget. The client treats two missed PINGs as a signal to tear down and reconnect, picking a fresh Envoy instance via DNS so a single bad gateway pod does not trap thousands of drivers.  
Stale pings are never retried. The client tags each ping with a generation counter; if an ack does not arrive before the next ping is generated, the unpacked ping is discarded. The next fresh ping already carries the most recent location, and the dropped one would only add load to a recovering system for no useful information.

**Resilience & Overload**:  
The failure mode this section protects against is progressive collapse. A Kafka broker pauses for 200 milliseconds, the gateway's producer buffer fills, the gateway runs out of memory, drivers reconnect to neighboring gateways, those fill, and a recoverable hiccup becomes a regional outage. The defenses are bounded buffering, explicit overload signaling to clients, jittered retries, and circuit breakers around downstream calls.

Backpressure starts at the Kafka producer. Its internal buffer is sized at 64 megabytes per gateway instance and is configured to fail fast rather than block when full. The gateway interprets a buffer-full error as backpressure and returns gRPC RESOURCE\_EXHAUSTED to the client with a Retry-After hint. The explicit "back off" signal gives the system time to drain. Holding the connection open while waiting for buffer space converts a backpressure event into a thread-pool exhaustion event, which is exactly the cascade we are trying to prevent.

Beyond the producer buffer, the gateway runs an adaptive concurrency limit based on Little's Law. The library tracks in-flight requests and observed p99 latency; when latency rises above a threshold, the concurrency cap drops automatically and the gateway sheds new pings with RESOURCE\_EXHAUSTED before they enter the buffer. Static rate limits either waste capacity at off-peak or fail to protect under load. Adaptive limits self-tune within seconds.

The domain insight that justifies shedding aggressively: dropping a ping is acceptable. The next one arrives in 4 seconds with fresher state. Compare to dropping a payment request, where the cost is dollars and a chargeback. Different domain, different shedding policy.

Client-side retries use exponential backoff with full jitter, where sleep equals random uniform between zero and base times 2 to the power of attempt, with base of 200 milliseconds and a cap of 10 seconds. Without jitter, a brief gateway outage produces synchronized retry waves at 1, 2, and 4 seconds that prevent recovery. Full jitter smears attempts evenly across the recovery window so the gateway sees a uniform arrival distribution and drains normally. The retry budget is bounded by ping age, not attempt count: if a ping is older than 10 seconds, the client discards it instead of retrying. Stale data adds load without adding information.

Circuit breakers operate in two places. The gateway runs a breaker around its Kafka producer; if error rate crosses 50 percent over a 10-second window at minimum 20 requests of volume, the breaker opens for 30 seconds and fast-fails new pings. Half-open state admits a trickle of test traffic before closing. The mobile client runs a breaker around the gateway: after five consecutive failures, it stops for a randomized 5 to 15 second interval before resuming. Without this, a brief gateway hiccup turns into a thundering herd of millions of simultaneous retry attempts.

Bulkheading isolates connection pools per downstream. The gateway's Kafka producer thread pool is separate from its Envoy admin path and from the gRPC request-handling pool, so a slow Kafka cannot starve unrelated work or fail unrelated health checks.