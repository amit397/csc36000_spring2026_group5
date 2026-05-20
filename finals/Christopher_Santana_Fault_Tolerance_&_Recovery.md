# Student 4: Observability, Fault Tolerance, & Recovery (“The Safety Net”)

## 1. Distributed Tracing

In a global ride-sharing platform, a single trip request travels through many distributed services before the ride is completed. A rider request may begin at the API gateway, move through the matchmaking workers, update the trip service, trigger notifications, and finally reach the payment service for billing. Because these services are distributed across different machines and regions, debugging failures or slow requests becomes difficult if every service only stores local logs.

To solve this problem, the platform uses distributed tracing with context propagation. When a rider creates a trip request, the API gateway generates a globally unique trace ID. This trace ID is attached to every RPC request, Kafka message, and internal service call related to that trip. Each service then creates a span, which records the operation name, timestamps, latency, and request status.

```text
Trace ID: TRIP-48291

Rider Request
    |
    +--> API Gateway (20ms)
              |
              +--> Matchmaking Worker (120ms)
                            |
                            +--> Trip Service (40ms)
                                          |
                                          +--> Payment Service (900ms)
```

By propagating the same trace ID across all services, engineers can reconstruct the entire lifecycle of one trip request from creation to billing. This is especially important for diagnosing tail latency. Most ride requests may complete in under one second, but a small percentage may become extremely slow due to overloaded matchmaking partitions, retry storms, network congestion, or slow payment providers.

Distributed tracing allows engineers to identify exactly where the delay occurred instead of manually correlating logs from multiple systems. For example, traces may reveal that the payment service introduced 900ms of latency while the rest of the request path remained healthy.

The main tradeoff is overhead. Every service must propagate tracing metadata and store tracing spans, which increases network traffic and storage requirements. However, this overhead is justified because debugging distributed systems without tracing becomes extremely difficult at global scale.

---

## 2. Metrics & Dashboards

The ride-sharing platform requires centralized metrics dashboards to monitor system health in real time. Since the system processes millions of GPS updates and rider requests concurrently, operators need immediate visibility into failures, overload conditions, and latency spikes.

The most important metrics include:

| Metric | Purpose |
|---|---|
| Retry requests per second | Detect excessive retries |
| Timeout rate | Detect slow downstream services |
| Queue depth | Detect backlog buildup |
| p95/p99 latency | Detect tail latency |
| 5xx error rate | Detect failing services |
| Circuit breaker activations | Detect overload protection |
| Matchmaking throughput | Monitor dispatch performance |
| Driver location freshness | Ensure GPS data is current |

These metrics are especially important for detecting retry storms. A retry storm occurs when many services continuously retry failed requests, creating even more traffic and overloading the system further.

```text
Payment Service slows down
            |
            v
Trip Services retry requests
            |
            v
More traffic overloads Payment Service
            |
            v
Even more retries occur
```

For example, if the payment provider begins timing out, trip services may repeatedly resend billing requests. The retries increase pressure on the already unhealthy service, causing failures and latency to spread across the platform.

To reduce the impact of retry storms, the architecture uses exponential backoff, jitter, and circuit breakers. Exponential backoff spaces retries farther apart over time, jitter prevents synchronized retry bursts, and circuit breakers temporarily stop requests from reaching unhealthy services.

The observability stack also separates the roles of logs and traces.

| Logs | Traces |
|---|---|
| Detailed service-level events | End-to-end request visibility |
| Best for error messages | Best for latency analysis |
| Used for auditing and debugging | Used for bottleneck detection |
| Example: payment failure reason | Example: trip request delay |

Logs are useful for detailed failures such as database errors, rejected driver assignments, or payment failures. Traces are more useful when engineers need to follow one request across multiple distributed services and identify where delays or failures occurred.

Overall, metrics detect system-wide problems, traces locate the bottleneck, and logs explain the exact failure.

---

## 3. Disaster Recovery & Global Snapshots

The platform requires strong disaster recovery guarantees because trip state and payment state must remain consistent during failures. The system cannot recover into a state where a rider was charged but the trip failed, or where the same driver becomes assigned to multiple riders.

To support recovery, the architecture combines distributed logs with periodic checkpoints and snapshots. Critical events such as trip transitions, driver assignments, and payment updates are written to durable append-only logs. These logs provide replayability and allow services to reconstruct state after failures.

However, relying only on logs creates an important tradeoff. Replay-based recovery is accurate because every event can be reconstructed, but recovery becomes very slow at large scale because the system may need to replay hours of historical events before becoming operational again.

```text
Distributed Log
       |
       v
Periodic Checkpoint
       |
       v
Fast Recovery After Failure
```

To improve recovery time, the platform also creates periodic checkpoints containing snapshots of recent service state. During recovery, services first restore the latest checkpoint and then replay only the newer log entries that occurred after the checkpoint.

The system also requires globally consistent snapshots across distributed services. This is based on concepts similar to the Chandy-Lamport snapshot algorithm. Instead of stopping the entire platform, each service independently records its local state while also tracking messages that are still in transit between services.

```text
Service A ----message----> Service B
     |                         |
     |                         |
 Consistent Snapshot Taken Across System
```

This creates a globally consistent checkpoint without shutting down the distributed system.

Consistency is extremely important during recovery. Without a consistent snapshot, different services may recover conflicting information. For example:
- The payment service may think a trip was charged successfully
- The trip service may think the payment failed
- The matchmaking service may incorrectly restore a driver as available

Global snapshots prevent these contradictions by ensuring all services recover from the same logical point in time.

The system also relies on idempotency during recovery. Duplicate retries after a failure must not create duplicate charges, duplicate trip transitions, or duplicate driver assignments.

By combining replayable logs, checkpoints, global snapshots, and idempotent operations, the platform can recover safely while preserving the system’s non-negotiable invariants.
