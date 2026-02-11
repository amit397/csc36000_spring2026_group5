#!/usr/bin/env python3
"""
primes_cli.py

Notes
-----
- Examples of how to run from terminal: 
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec single --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec threads --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec processes --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec distributed --time --mode count --secondary-exec processes --primary http://127.0.0.1:9200

New Command to run for test: python primes_cli.py --exec distributed --primary localhost:9200 --low 1 --high 5000000 --mode count --include-per-node --time
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Tuple
from primes_in_range import get_primes
import grpc
import primes_pb2
import primes_pb2_grpc


def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("--chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Tuple[int, int, object]:
    a, b, return_list = args
    res = get_primes(a, b, return_list=return_list)
    return (a, b, res)


def _post_json(url: str, payload: dict, timeout_s: int = 3600) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Prime counting/listing over [low, high) using local threads/processes OR distributed secondary nodes."
    )
    ap.add_argument("--low", type=int, required=True, help="Range start (inclusive).")
    ap.add_argument("--high", type=int, required=True, help="Range end (exclusive). Must be > low.")
    ap.add_argument("--mode", choices=["list", "count"], default="count")
    ap.add_argument("--chunk", type=int, default=500_000)
    ap.add_argument("--exec", choices=["single", "threads", "processes", "distributed"], default="single")
    ap.add_argument("--workers", type=int, default=(os.cpu_count() or 4))
    ap.add_argument("--max-print", type=int, default=50)
    ap.add_argument("--time", action="store_true")

    # Distributed options
    ap.add_argument("--primary", default=None, help="Primary URL, e.g. http://134.74.160.1:9200")
    ap.add_argument("--secondary-exec", choices=["single", "threads", "processes"], default="processes")
    ap.add_argument("--secondary-workers", type=int, default=None)
    ap.add_argument("--include-per-node", action="store_true")
    ap.add_argument("--max-return-primes", type=int, default=5000)

    args = ap.parse_args(argv)

    if args.high <= args.low:
        print("Error: --high must be > --low", file=sys.stderr)
        return 2

    return_list = (args.mode == "list")

    if args.exec == "distributed":
        if not args.primary:
            print("Error: --primary is required when --exec distributed", file=sys.stderr)
            return 2

        t0 = time.perf_counter()

        # Create channel to primary (coordinator)
        channel = grpc.insecure_channel(args.primary.replace("http://", ""))

        stub = primes_pb2_grpc.CoordinatorServiceStub(channel)

        # Convert mode string to protobuf enum
        mode_enum = primes_pb2.LIST if return_list else primes_pb2.COUNT

        # Convert secondary_exec string → protobuf enum
        if args.secondary_exec == "single":
            exec_enum = primes_pb2.SINGLE
        elif args.secondary_exec == "threads":
            exec_enum = primes_pb2.THREADS
        elif args.secondary_exec == "processes":
            exec_enum = primes_pb2.PROCESSES
        else:
            print("Invalid --secondary-exec value", file=sys.stderr)
            return 2

        request = primes_pb2.ComputeRequest(
            low=args.low,
            high=args.high,
            mode=mode_enum,
            chunk=args.chunk,
            exec_mode=exec_enum,
            workers=args.secondary_workers or 0,
            max_return_primes=args.max_return_primes,
            include_per_node=args.include_per_node,
            include_per_chunk=False,
        )

        response = stub.Compute(request, timeout=3600)
        channel.close()
        
        t1 = time.perf_counter()

        if not response.ok:
            print(f"Distributed error: {response.error}", file=sys.stderr)
            return 1
        
        # Convert gRPC response → dict format expected by existing CLI logic
        resp = {
            "ok": response.ok,
            "mode": response.mode,
            "range": [response.range_low, response.range_high],
            "nodes_used": response.nodes_used,
            "secondary_exec": response.exec_mode,
            "secondary_workers": response.workers,
            "chunk": response.chunk_size,
            "total_primes": response.total_primes,
            "max_prime": response.max_prime,
            "elapsed_seconds": response.elapsed_seconds,
            "sum_node_compute_seconds": response.sum_node_compute_seconds,
            "sum_node_round_trip_seconds": response.sum_node_round_trip_seconds,
        }

        if return_list:
            resp["primes"] = list(response.primes)
            resp["primes_truncated"] = response.primes_truncated
            resp["max_return_primes"] = response.max_return_primes

        if args.include_per_node:
            resp["per_node"] = [
                {
                    "node_id": r.node_id,
                    "node": {
                        "host": r.host,
                        "port": r.port,
                        "cpu_count": r.cpu_count,
                    },
                    "slice": (r.slice_low, r.slice_high),
                    "round_trip_s": r.round_trip_seconds,
                    "node_elapsed_s": r.node_elapsed_seconds,
                    "node_sum_chunk_s": r.sum_chunk_compute_seconds,
                    "total_primes": r.total_primes,
                    "max_prime": r.max_prime,
                    "primes": list(r.primes),
                    "primes_truncated": r.primes_truncated,
                }
                for r in response.per_node
            ]

        if args.mode == "count":
            print(int(resp.get("total_primes", 0)))
        else:
            primes = list(resp.get("primes", []))
            total = int(resp.get("total_primes", len(primes)))
            shown = primes[: args.max_print]
            print(f"Total primes: {total}")
            print(f"First {len(shown)} primes (from returned sample):")
            print(" ".join(map(str, shown)))
            if resp.get("primes_truncated") or total > len(primes):
                print(f"... (returned primes are capped at {resp.get('max_return_primes', args.max_return_primes)})")

        if args.time:
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, nodes_used={resp.get('nodes_used')}, secondary_exec={resp.get('secondary_exec')}, chunk={args.chunk})",
                file=sys.stderr,
            )
            if args.include_per_node and "per_node" in resp:
                print("Per-node summary:", file=sys.stderr)
                for r in resp["per_node"]:
                    print(
                        f"  {r['node_id']:>12} slice={r['slice']} primes={r['total_primes']} "
                        f"node_elapsed={r['node_elapsed_s']:.3f}s round_trip={r['round_trip_s']:.3f}s",
                        file=sys.stderr,
                    )
        return 0

    # Local paths
    ranges = iter_ranges(args.low, args.high, args.chunk)
    t0 = time.perf_counter()
    results: List[Tuple[int, int, object]] = []

    if args.exec == "single":
        for a, b in ranges:
            results.append(_work_chunk((a, b, return_list)))

    elif args.exec == "threads":
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    else:  # processes
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    t1 = time.perf_counter()
    results.sort(key=lambda x: x[0])

    if args.mode == "count":
        total = 0
        for _, _, res in results:
            total += int(res)  # type: ignore[arg-type]
        print(total)
    else:
        all_primes: List[int] = []
        for _, _, res in results:
            all_primes.extend(list(res))  # type: ignore[arg-type]
        total = len(all_primes)
        shown = all_primes[: args.max_print]
        print(f"Total primes: {total}")
        print(f"First {len(shown)} primes:")
        print(" ".join(map(str, shown)))
        if total > len(shown):
            print(f"... ({total - len(shown)} more not shown)")

    if args.time:
        print(
            f"Elapsed seconds: {t1 - t0:.6f}  "
            f"(exec={args.exec}, workers={args.workers if args.exec!='single' else 1}, chunks={len(ranges)}, chunk_size={args.chunk})",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
