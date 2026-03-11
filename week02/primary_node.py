#!/usr/bin/env python3
"""
primary_node.py

gRPC Coordinator for distributed prime-number computation.
Implements CoordinatorService: RegisterNode, ListNodes, Compute.

Replaces the HTTP-based primary node from week01.

Command to run: python primary_node.py --host 127.0.0.1 --port 9300 
"""

from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Tuple

import grpc
import primes_pb2_grpc
import primes_pb2

class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.nodes: Dict[str, Dict[str, Any]] = {}

    def upsert(self, node: Dict[str, Any]) -> Dict[str, Any]:
        node_id = str(node["node_id"])
        now = time.time()
        record = {
            "node_id": node_id,
            "host": str(node["host"]),
            "port": int(node["port"]),
            "cpu_count": int(node.get("cpu_count", 1)),
            "last_seen": float(node.get("ts", now)),
            "registered_at": now,
        }
        with self.lock:
            if node_id in self.nodes:
                record["registered_at"] = self.nodes[node_id].get("registered_at", now)
            self.nodes[node_id] = record
            return record

    def active_nodes(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self.lock:
            stale = [nid for nid, rec in self.nodes.items() if (now - float(rec.get("last_seen", 0))) > self.ttl_s]
            for nid in stale:
                del self.nodes[nid]
            return list(self.nodes.values())

class CoordinatorServiceServicer(primes_pb2_grpc.CoordinatorServiceServicer):
    def RegisterNode(self, request, context):   
        #We need to get the fields from protofbuf 
        node_data = {
            "node_id": request.node_id,
            "host": request.host,
            "port": request.port,
            "cpu_count" : request.cpu_count,
            "ts" : request.timestamp
        }
        #Keep track of the node
        record = REGISTRY.upsert(node_data)
        print(f"[primary_node] Registered node: {request.node_id} at {request.host}:{request.port}")

        return primes_pb2.RegisterNodeResponse(
            ok=True, 
            node=primes_pb2.NodeInfo(
                node_id=record["node_id"],
                host=record["host"],
                port=record["port"],
                cpu_count=record["cpu_count"],
                last_seen=record["last_seen"],
                registered_at=record["registered_at"]
            ),
            error=""
        )

    def ListNodes(self, request, context):
        # Get active nodes (removes stale nodes based on TTL)
        nodes = REGISTRY.active_nodes()

        # Sort for consistent ordering
        nodes.sort(key=lambda n: n["node_id"])

        # Convert each dict to NodeInfo protobuf
        node_infos = []
        for n in nodes:
            node_infos.append(primes_pb2.NodeInfo(
                node_id=n["node_id"],
                host=n["host"],
                port=n["port"],
                cpu_count=n["cpu_count"],
                last_seen=n["last_seen"],
                registered_at=n["registered_at"],
            ))

        return primes_pb2.ListNodesResponse(
            ok=True,
            nodes=node_infos,
            ttl_seconds=REGISTRY.ttl_s,
            error="",
        )

    def Compute(self, request, context):
        compute_data = {
            "low": request.low,
            "high": request.high,
            "mode": request.mode,
            "chunk": request.chunk,
            "exec_mode": request.exec_mode,
            "workers": request.workers,
            "max_return_primes": request.max_return_primes,
            "include_per_node": request.include_per_node,
            "include_per_chunk": request.include_per_chunk,
        }

        if request.high <= request.low:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("high must be > low")
            return primes_pb2.ComputeResponse(ok=False, error="high must be > low")

        nodes = REGISTRY.active_nodes()
        if not nodes:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No nodes available")
            return primes_pb2.ComputeResponse(ok=False, error="No nodes available")
        
        # Set default values for optional fields (0 means "not set")
        chunk = request.chunk if request.chunk > 0 else 500_000
        workers = request.workers if request.workers > 0 else 0  # 0 = worker uses cpu_count
        max_return_primes = request.max_return_primes if request.max_return_primes > 0 else 5000

        # Sort nodes for consistent ordering
        nodes.sort(key=lambda n: n["node_id"])

        # Split the range into slices (one per worker node)
        slices = split_into_slices(request.low, request.high, len(nodes))

        # Start timer
        t0 = time.perf_counter()

        # Helper function to call ONE worker via gRPC
        def call_worker(node, slice_low, slice_high):
            host = node["host"]
            port = node["port"]
            t_call0 = time.perf_counter()

            # Create gRPC channel (connection) to the worker
            channel = grpc.insecure_channel(
                f"{host}:{port}",
                options=[
                    ('grpc.max_connection_idle_ms', 300000),
                    ('grpc.max_connection_age_ms', 600000),
                ]
            )

            try:
                # Create a stub (client) to call the worker's methods
                stub = primes_pb2_grpc.WorkerServiceStub(channel)

                # Build the request to send to the worker
                worker_request = primes_pb2.ComputeRequest(
                    low=slice_low,
                    high=slice_high,
                    mode=request.mode,
                    chunk=chunk,
                    exec_mode=request.exec_mode,
                    workers=workers,
                    max_return_primes=max_return_primes if request.mode == primes_pb2.LIST else 0,
                )

                # Call the worker's ComputeRange method (with 1 hour timeout)
                response = stub.ComputeRange(worker_request, timeout=3600)

                t_call1 = time.perf_counter()
            finally:
                channel.close()

            # Return PerNodeResult protobuf directly
            return primes_pb2.PerNodeResult(
                node_id=node["node_id"],
                host=host,
                port=port,
                cpu_count=node.get("cpu_count", 1),
                slice_low=slice_low,
                slice_high=slice_high,
                round_trip_seconds=t_call1 - t_call0,
                node_elapsed_seconds=response.elapsed_seconds,
                sum_chunk_compute_seconds=response.sum_chunk_compute_seconds,
                total_primes=response.total_primes,
                max_prime=response.max_prime,
                primes=list(response.primes),
                primes_truncated=response.primes_truncated,
            )

        # Call all workers in parallel using ThreadPoolExecutor
        per_node_results = []

        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            # Submit a task for each node/slice pair
            futures = []
            for i, node in enumerate(nodes):
                if i < len(slices):  # Make sure we have a slice for this node
                    slice_low, slice_high = slices[i]
                    print(f"[primary] Assigning slice ({slice_low}, {slice_high}) to {node['node_id']}")
                    fut = executor.submit(call_worker, node, slice_low, slice_high)
                    futures.append(fut)

            # Collect results as they complete
            for fut in futures:
                try:
                    result = fut.result()  # This is a PerNodeResult
                    per_node_results.append(result)
                    print(f"[primary] Slice ({result.slice_low}, {result.slice_high}) done by {result.node_id}")

                except grpc.RpcError as e:
                    # Convert gRPC errors to structured status/details
                    # e.code() = status code (DEADLINE_EXCEEDED, UNAVAILABLE, etc.)
                    # e.details() = error message from the worker
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        error_msg = f"Worker timed out: {e.details()}"
                    elif e.code() == grpc.StatusCode.UNAVAILABLE:
                        error_msg = f"Worker unavailable: {e.details()}"
                    else:
                        error_msg = f"Worker error: {e.code().name} - {e.details()}"

                    print(f"[primary] {error_msg}")
                    context.set_code(e.code())
                    context.set_details(error_msg)
                    return primes_pb2.ComputeResponse(ok=False, error=error_msg)

                except Exception as e:
                    # Non-gRPC error
                    error_msg = f"Unexpected error: {str(e)}"
                    print(f"[primary] {error_msg}")
                    context.set_code(grpc.StatusCode.INTERNAL)
                    context.set_details(error_msg)
                    return primes_pb2.ComputeResponse(ok=False, error=error_msg)

        # Sort results by slice_low so primes are in order
        per_node_results.sort(key=lambda r: r.slice_low)

        # Aggregate: sum counts, find max, merge primes
        total_primes = 0
        max_prime = -1
        all_primes = []
        primes_truncated = False

        for r in per_node_results:
            # Sum up prime counts from all workers
            total_primes += r.total_primes

            # Track the largest prime found
            if r.max_prime > max_prime:
                max_prime = r.max_prime

            # For LIST mode: merge primes (with cap)
            if request.mode == primes_pb2.LIST:
                if len(all_primes) < max_return_primes:
                    remaining = max_return_primes - len(all_primes)
                    all_primes.extend(list(r.primes)[:remaining])
                    if len(r.primes) > remaining:
                        primes_truncated = True
                else:
                    primes_truncated = True

                # If worker already truncated, we're truncated too
                if r.primes_truncated:
                    primes_truncated = True

        # Stop timer
        t1 = time.perf_counter()

        # Build the final response
        return primes_pb2.ComputeResponse(
            ok=True,
            error="",
            # Echo back what was requested
            mode=request.mode,
            range_low=request.low,
            range_high=request.high,
            # Results
            total_primes=total_primes,
            max_prime=max_prime,
            primes=all_primes if request.mode == primes_pb2.LIST else [],
            primes_truncated=primes_truncated,
            max_return_primes=max_return_primes,
            # Timing
            elapsed_seconds=t1 - t0,
            sum_chunk_compute_seconds=sum(r.sum_chunk_compute_seconds for r in per_node_results),
            sum_node_compute_seconds=sum(r.node_elapsed_seconds for r in per_node_results),
            sum_node_round_trip_seconds=sum(r.round_trip_seconds for r in per_node_results),
            # Execution info
            exec_mode=request.exec_mode,
            workers=workers,
            chunks=len(slices),
            chunk_size=chunk,
            nodes_used=len(per_node_results),
            # Per-node breakdown (only if requested)
            per_node=per_node_results if request.include_per_node else [],
        )

REGISTRY = Registry(ttl_s=120)


def split_into_slices(low: int, high: int, n: int) -> List[Tuple[int, int]]:
    if n <= 0:
        return []
    total = high - low
    base = total // n
    rem = total % n
    out = []
    start = low
    for i in range(n):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            out.append((start, end))
        start = end
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation (gRPC).")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    # Create gRPC server with thread pool and connection settings
    server = grpc.server(
        ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_concurrent_streams', 100),
            ('grpc.http2.min_time_between_pings_ms', 30000),
        ]
    )

    # Add our CoordinatorService to the server
    primes_pb2_grpc.add_CoordinatorServiceServicer_to_server(
        CoordinatorServiceServicer(),
        server
    )

    # Bind to address
    bind_address = f"{args.host}:{args.port}"
    server.add_insecure_port(bind_address)

    # Start the server
    server.start()

    print(f"[primary_node] gRPC server listening on {bind_address}")
    print("  RPC RegisterNode()")
    print("  RPC ListNodes()")
    print("  RPC Compute()")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...")
        server.stop(grace=5)  # 5 seconds grace period
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()