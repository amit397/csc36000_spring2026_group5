#!/usr/bin/env python3
"""
secondary_node.py

gRPC worker node for distributed prime-number computation.
Implements WorkerService (ComputeRange, Health) and registers with coordinator.

Replaces the HTTP-based secondary node from week01.
"""

from __future__ import annotations

import argparse
import os
import socket
import threading
import time
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import grpc

import primes_pb2
import primes_pb2_grpc
from primes_in_range import get_primes


# =============================================================================
# Partitioning helpers (unchanged from week01)
# =============================================================================

def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Dict[str, Any]:
    """
    Worker for one chunk.
    Returns dict for easy aggregation.
    """
    low, high, return_list = args

    t0 = time.perf_counter()
    res = get_primes(low, high, return_list=return_list)
    t1 = time.perf_counter()

    if return_list:
        primes = list(res)  # type: ignore[arg-type]
        return {
            "low": low,
            "high": high,
            "elapsed_s": t1 - t0,
            "prime_count": len(primes),
            "max_prime": primes[-1] if primes else -1,
            "primes": primes,
        }

    count = int(res)  # type: ignore[arg-type]
    return {
        "low": low,
        "high": high,
        "elapsed_s": t1 - t0,
        "prime_count": count,
        "max_prime": -1,  # not computed in count mode to avoid extra work
    }


def compute_partitioned(
    low: int,
    high: int,
    *,
    mode: primes_pb2.Mode.ValueType = primes_pb2.COUNT,
    chunk: int = 500_000,
    exec_mode: primes_pb2.ExecMode.ValueType = primes_pb2.SINGLE,
    workers: int | None = None,
    max_return_primes: int = 5000,
    include_per_chunk: bool = False,
) -> Dict[str, Any]:
    """
    Perform partitioned computation over [low, high) using get_primes per chunk.
    Now uses protobuf enums for mode and exec_mode.
    """
    if high <= low:
        raise ValueError("high must be > low")
    if mode not in (primes_pb2.COUNT, primes_pb2.LIST):
        raise ValueError("mode must be COUNT or LIST")
    if exec_mode not in (primes_pb2.SINGLE, primes_pb2.THREADS, primes_pb2.PROCESSES):
        raise ValueError("exec_mode must be SINGLE, THREADS, or PROCESSES")

    if workers is None or workers <= 0:
        workers = os.cpu_count() or 4
    workers = max(1, int(workers))

    ranges = iter_ranges(low, high, chunk)
    want_list = (mode == primes_pb2.LIST)

    t0 = time.perf_counter()
    chunk_results: List[Dict[str, Any]] = []

    if exec_mode == primes_pb2.SINGLE:
        for a, b in ranges:
            chunk_results.append(_work_chunk((a, b, want_list)))

    elif exec_mode == primes_pb2.THREADS:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    else:  # PROCESSES
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    t1 = time.perf_counter()

    chunk_results.sort(key=lambda d: int(d["low"]))
    total_primes = sum(int(d["prime_count"]) for d in chunk_results)
    sum_chunk = sum(float(d["elapsed_s"]) for d in chunk_results)

    primes_out: List[int] | None = None
    truncated = False
    max_prime = -1

    if want_list:
        primes_out = []
        for d in chunk_results:
            ps = d.get("primes") or []
            if ps:
                max_prime = max(max_prime, int(ps[-1]))
            if len(primes_out) < max_return_primes:
                remaining = max_return_primes - len(primes_out)
                primes_out.extend(ps[:remaining])
                if len(ps) > remaining:
                    truncated = True
            else:
                truncated = True

    response: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range_low": low,
        "range_high": high,
        "chunk": chunk,
        "exec_mode": exec_mode,
        "workers": workers if exec_mode != primes_pb2.SINGLE else 1,
        "chunks": len(ranges),
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_chunk_compute_seconds": sum_chunk,
    }

    if primes_out is not None:
        response["primes"] = primes_out
        response["primes_truncated"] = truncated
        response["max_return_primes"] = max_return_primes

    return response


# =============================================================================
# gRPC WorkerService Implementation
# =============================================================================

class WorkerServiceServicer(primes_pb2_grpc.WorkerServiceServicer):
    """Implements the WorkerService gRPC service."""

    def __init__(self, node_id: str, host: str, port: int, cpu_count: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cpu_count = cpu_count

    def Health(
        self, request: primes_pb2.HealthRequest, context: grpc.ServicerContext
    ) -> primes_pb2.HealthResponse:
        """Health check RPC."""
        return primes_pb2.HealthResponse(
            ok=True,
            status="healthy",
            node_id=self.node_id,
        )

    def ComputeRange(
        self, request: primes_pb2.ComputeRequest, context: grpc.ServicerContext
    ) -> primes_pb2.ComputeResponse:
        """Compute primes for a given range."""
        try:
            # Extract parameters from protobuf request
            low = request.low
            high = request.high
            mode = request.mode if request.mode else primes_pb2.COUNT
            chunk = request.chunk if request.chunk > 0 else 500_000
            exec_mode = request.exec_mode if request.exec_mode else primes_pb2.SINGLE
            workers = request.workers if request.workers > 0 else None
            max_return_primes = request.max_return_primes if request.max_return_primes > 0 else 5000
            include_per_chunk = request.include_per_chunk

            # Call the existing compute logic
            result = compute_partitioned(
                low, high,
                mode=mode,
                chunk=chunk,
                exec_mode=exec_mode,
                workers=workers,
                max_return_primes=max_return_primes,
                include_per_chunk=include_per_chunk,
            )

            # Build protobuf response
            response = primes_pb2.ComputeResponse(
                ok=True,
                mode=result["mode"],
                range_low=result["range_low"],
                range_high=result["range_high"],
                total_primes=result["total_primes"],
                max_prime=result["max_prime"],
                elapsed_seconds=result["elapsed_seconds"],
                sum_chunk_compute_seconds=result["sum_chunk_compute_seconds"],
                exec_mode=result["exec_mode"],
                workers=result["workers"],
                chunks=result["chunks"],
                chunk_size=chunk,
                node_id=self.node_id,
            )

            # Add primes list if in LIST mode
            if "primes" in result and result["primes"] is not None:
                response.primes.extend(result["primes"])
                response.primes_truncated = result.get("primes_truncated", False)
                response.max_return_primes = result.get("max_return_primes", max_return_primes)

            return response

        except Exception as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeResponse(ok=False, error=str(e))


# =============================================================================
# Registration with Coordinator
# =============================================================================

def _guess_local_ip_for(coordinator_host: str, coordinator_port: int) -> str:
    """
    Best-effort: pick the local IP used to reach the coordinator.
    Works well in a LAN lab environment.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((coordinator_host, coordinator_port))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def start_registration_loop(
    coordinator_address: str,
    node_id: str,
    host: str,
    port: int,
    cpu_count: int,
    *,
    interval_s: int = 3600,
) -> None:
    """
    Background heartbeat: periodically re-register so coordinator can expire stale nodes.
    Uses gRPC instead of HTTP.
    """
    def loop():
        while True:
            try:
                channel = grpc.insecure_channel(coordinator_address)
                stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
                
                request = primes_pb2.RegisterNodeRequest(
                    node_id=node_id,
                    host=host,
                    port=port,
                    cpu_count=cpu_count,
                    timestamp=time.time(),
                )
                
                response = stub.RegisterNode(request, timeout=5)
                
                if response.ok:
                    print(f"[secondary_node] Registered with coordinator: {coordinator_address}")
                else:
                    print(f"[secondary_node] Registration failed: {response.error}")
                    
                channel.close()
                
            except grpc.RpcError as e:
                print(f"[secondary_node] Registration error: {e.code()}: {e.details()}")
            except Exception as e:
                print(f"[secondary_node] Registration error: {e}")
            
            time.sleep(interval_s)

    th = threading.Thread(target=loop, daemon=True)
    th.start()


# =============================================================================
# Main Entry Point
# =============================================================================

def serve(
    host: str,
    port: int,
    node_id: str,
    coordinator_address: str | None,
    advertised_host: str,
    register_interval: int,
) -> None:
    """Start the gRPC server."""
    cpu_count = os.cpu_count() or 1
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add WorkerService servicer
    servicer = WorkerServiceServicer(node_id, advertised_host, port, cpu_count)
    primes_pb2_grpc.add_WorkerServiceServicer_to_server(servicer, server)
    
    # Bind to address
    bind_address = f"{host}:{port}"
    server.add_insecure_port(bind_address)
    
    # Start registration loop if coordinator is specified
    if coordinator_address:
        start_registration_loop(
            coordinator_address,
            node_id=node_id,
            host=advertised_host,
            port=port,
            cpu_count=cpu_count,
            interval_s=max(5, register_interval),
        )
    
    # Start server
    server.start()
    
    print(f"[secondary_node] node_id={node_id}")
    print(f"[secondary_node] gRPC server listening on {bind_address}")
    print(f"[secondary_node] advertised as {advertised_host}:{port}")
    if coordinator_address:
        print(f"[secondary_node] registering to coordinator: {coordinator_address}")
    print("  RPC Health()")
    print("  RPC ComputeRange()")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[secondary_node] KeyboardInterrupt received; shutting down gracefully...")
        server.stop(grace=5)
        print("[secondary_node] server stopped.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Secondary prime worker node (gRPC server).")
    ap.add_argument("--host", default="0.0.0.0", help="Bind host (default 0.0.0.0).")
    ap.add_argument("--port", type=int, default=9100, help="Bind port (default 9100).")
    ap.add_argument("--node-id", default=None, help="Optional stable node id (default: hostname).")

    ap.add_argument("--primary", default=None, help="Coordinator gRPC address, e.g. 127.0.0.1:9200")
    ap.add_argument("--public-host", default=None, help="Host/IP to advertise to coordinator (default: auto-detect).")
    ap.add_argument("--register-interval", type=int, default=3600, help="Seconds between heartbeats (default 3600).")

    args = ap.parse_args()

    # Determine node ID
    try:
        node_id = args.node_id or socket.gethostname()
    except Exception:
        node_id = args.node_id or "worker"

    # Determine advertised host
    advertised_host = args.public_host
    if args.primary and not advertised_host:
        # Parse coordinator address to get host/port for IP detection
        parts = args.primary.split(":")
        coord_host = parts[0] if len(parts) >= 1 else "127.0.0.1"
        coord_port = int(parts[1]) if len(parts) >= 2 else 9200
        advertised_host = _guess_local_ip_for(coord_host, coord_port)
    if not advertised_host:
        advertised_host = "127.0.0.1"

    serve(
        host=args.host,
        port=args.port,
        node_id=node_id,
        coordinator_address=args.primary,
        advertised_host=advertised_host,
        register_interval=args.register_interval,
    )


if __name__ == "__main__":
    main()
