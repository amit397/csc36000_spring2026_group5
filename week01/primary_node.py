#!/usr/bin/env python3
"""
primary_node.py

Primary coordinator that:
1) Maintains an in-memory registry of secondary nodes (registered by secondary_node.py)
2) Distributes prime-range computation requests to registered secondary nodes
3) Aggregates results in memory and returns a final result (count or list sample)

Endpoints
---------
GET  /health
GET  /nodes
POST /register
POST /compute
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


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


    def ping(self, host: str, port: int) -> bool:
        try:
            url = f"http://{host}:{port}/health"
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data.get("ok", False)
        except Exception:
            return False
    
    def active_nodes(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self.lock:
            stale = [nid for nid, rec in self.nodes.items() if (now - float(rec.get("last_seen", 0))) > self.ttl_s]
            
            for nid in list(self.nodes.keys()):
                print("CHECKING NODE:", self.nodes[nid])
                if not (self.ping(self.nodes[nid]["host"], self.nodes[nid]["port"])):
                    print("NODE IS STALE/UNREACHABLE:", self.nodes[nid])
                    stale.append(nid)
                else: print("NODE IS ACTIVE:", self.nodes[nid])
            for nid in stale:
                del self.nodes[nid]
            return list(self.nodes.values())


REGISTRY = Registry(ttl_s=120)


def _post_json(url: str, payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


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


def distributed_compute(payload: Dict[str, Any]) -> Dict[str, Any]:
    nodes = REGISTRY.active_nodes()
    if not nodes:
        return {
            "ok": False,
            "error": "No secondary nodes available. Please start at least one worker.",
            "status_code": 503 # Service Unavailable
        }

    low = int(payload["low"])
    high = int(payload["high"])
    if high <= low:
        raise ValueError("high must be > low")

    mode = str(payload.get("mode", "count"))
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")
    
    sec_exec = str(payload.get("secondary_exec", "processes"))
    if sec_exec not in ("single", "threads", "processes"):
        raise ValueError("secondary_exec must be single|threads|processes")

    sec_workers = payload.get("secondary_workers", None)
    if sec_workers is not None:
        sec_workers = int(sec_workers)

    max_return_primes = int(payload.get("max_return_primes", 5000))
    include_per_node = bool(payload.get("include_per_node", False))
    chunk_size = int(payload.get("chunk", 500_000))

    # Initial node snapshot to determine slicing
    # We want to slice based on currently available capacity, 
    # but the execution will adapt if nodes drop out.
    initial_nodes = REGISTRY.active_nodes()
    if not initial_nodes:
        # If no nodes initially, we might want to wait or fail. 
        # The prompt implies we should be robust.
        # But for 'slicing', we need a number. Let's wait for at least one node.
        print("[primary] No nodes available at start. Waiting for nodes...")
        while not initial_nodes:
            time.sleep(1)
            initial_nodes = REGISTRY.active_nodes()
        print(f"[primary] Found {len(initial_nodes)} nodes to start.")

    # Sort for deterministic slicing
    initial_nodes_sorted = sorted(initial_nodes, key=lambda n: n["node_id"])
    
    # We create slices based on the initial view. 
    # If nodes die, the remaining nodes will iterate through these slices.
    slices = split_into_slices(low, high, len(initial_nodes_sorted))
    
    t0 = time.perf_counter()

    # Track results
    per_node_results: List[Dict[str, Any]] = []
    
    # Work queue: which slices still need to be computed
    # We store (slice_start, slice_end) tuples
    pending_slices = slices.copy() 
    
    # Failed nodes tracking: node_id -> timestamp_when_failed
    # We will ignore failed nodes until their 'last_seen' in registry > timestamp_when_failed
    failed_nodes: Dict[str, float] = {}

    def get_candidates() -> List[Dict[str, Any]]:
        """
        Returns list of healthy nodes.
        Checks registry and filters out known failed nodes 
        unless they have re-registered (updated last_seen).
        """
        current = REGISTRY.active_nodes()
        healthy = []
        for n in current:
            nid = n["node_id"]
            if nid in failed_nodes:
                # Check if it has recovered (new heartbeat since failure)
                if float(n.get("last_seen", 0)) > failed_nodes[nid]:
                    del failed_nodes[nid]
                    healthy.append(n)
                else:
                    # Still stale/dead
                    pass
            else:
                healthy.append(n)
        return healthy

    def process_slice(start: int, end: int, node: Dict[str, Any]) -> Dict[str, Any]:
        """Calls the secondary node for a specific slice."""
        host = node["host"]
        port = node["port"]
        url = f"http://{host}:{port}/compute"
        req = {
            "low": start,
            "high": end,
            "mode": mode,
            "chunk": chunk_size,
            "exec": sec_exec,
            "workers": sec_workers,
            "max_return_primes": max_return_primes if mode == "list" else 0,
            "include_per_chunk": False,
        }
        # filter None
        req = {k: v for k, v in req.items() if v is not None}

        t_call0 = time.perf_counter()
        resp = _post_json(url, req, timeout_s=3600) 
        t_call1 = time.perf_counter()

        if not resp.get("ok"):
            raise RuntimeError(f"node {node['node_id']} returned error: {resp}")
        
        node_elapsed_s = float(resp.get("elapsed_seconds", 0.0))
        return {
            "node_id": node["node_id"],
            "node": {"host": host, "port": port, "cpu_count": node.get("cpu_count", 1)},
            "slice": (start, end),
            "round_trip_s": t_call1 - t_call0,
            "node_elapsed_s": node_elapsed_s,
            "node_sum_chunk_s": float(resp.get("sum_chunk_compute_seconds", 0.0)),
            "total_primes": int(resp.get("total_primes", 0)),
            "max_prime": int(resp.get("max_prime", -1)),
            "primes": resp.get("primes", None),
            "primes_truncated": bool(resp.get("primes_truncated", False)),
        }

    # Main execution loop
    # We use a ThreadPoolExecutor, but we manage submissions dynamically
    # to handle retries and node availability.
    
    # We'll use a larger pool to accommodate checking/waiting logic if needed,
    # but realistically we limit concurrency to the number of slices or nodes.
    max_concurrent = 32
    
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        # Map: Future -> (slice_start, slice_end, assigned_node_id)
        future_to_work: Dict[Any, Tuple[int, int, str]] = {}
        
        # While there is work to do (pending or in-flight)
        while pending_slices or future_to_work:
            
            # 1. Fill available slots with pending work
            # Only submit if we have pending slices and we aren't saturating everyone
            # Ideally we want 1 task per healthy node.
            
            candidates = get_candidates()
            
            # If no nodes are available at all, we must wait.
            if not candidates and not future_to_work:
                print("[primary] No active healthy nodes! Waiting for nodes to recover/register...")
                time.sleep(2)
                continue

            # Identify nodes that are currently busy
            busy_nodes = {assigned_nid for (_, _, assigned_nid) in future_to_work.values()}
            
            # Helper to find a free node
            free_nodes = [n for n in candidates if n["node_id"] not in busy_nodes]
            
            # If we have pending tasks and free nodes, submit them
            while pending_slices and free_nodes:
                sl = pending_slices.pop(0) # Get next slice
                node = free_nodes.pop(0)   # Get a free node
                
                print(f"[primary] Assigning slice {sl} to node {node['node_id']}")
                fut = executor.submit(process_slice, sl[0], sl[1], node)
                future_to_work[fut] = (sl[0], sl[1], node["node_id"])

            # 2. Wait for at least one future to complete (or for nodes to become available if we are stuck)
            # If we have futures running, check them.
            if future_to_work:
                # We use a short timeout so we can periodically check pending_slices/free_nodes
                # in case a new node appeared (though strictly we only care if we have pending work).
                # But more importantly, we assume wait() returns fast if things finish.
                done, not_done = as_completed(future_to_work.keys(), timeout=1.0), [] 
                
                # Check actual done list (as_completed yields iterator)
                # But wait... as_completed is an iterator. We can't use it nicely with a timeout in a loop 
                # effectively unless we break.
                # Use executor.submit and manual check? or just `wait()`
                from concurrent.futures import wait, FIRST_COMPLETED
                done_futures, not_done_futures = wait(future_to_work.keys(), timeout=1.0, return_when=FIRST_COMPLETED)
                
                for f in done_futures:
                    sl_start, sl_end, assigned_nid = future_to_work.pop(f)
                    try:
                        result = f.result()
                        per_node_results.append(result)
                        print(f"[primary] Slice {(sl_start, sl_end)} completed by {assigned_nid}")
                    except Exception as e:
                        print(f"[primary] Error on node {assigned_nid} for slice {(sl_start, sl_end)}: {e}")
                        # Mark failed
                        failed_nodes[assigned_nid] = time.time()
                        # Re-queue the work
                        print(f"[primary] Re-queuing slice {(sl_start, sl_end)}")
                        pending_slices.append((sl_start, sl_end))
            else:
                 # No futures running, but maybe we have pending slices (and no free nodes caught in loop above)
                 # just sleep a bit to avoid hot loop if waiting for nodes
                 if pending_slices:
                     time.sleep(1)


    # Aggregation (same as before)
    per_node_results.sort(key=lambda r: r["slice"][0])
    
    total_primes = 0
    max_prime = -1
    primes_sample: List[int] = []
    primes_truncated = False

    for r in per_node_results:
        total_primes += int(r["total_primes"])
        max_prime = max(max_prime, int(r["max_prime"]))
        if mode == "list" and r.get("primes") is not None:
            ps = list(r["primes"])
            if len(primes_sample) < max_return_primes:
                remaining = max_return_primes - len(primes_sample)
                primes_sample.extend(ps[:remaining])
                if len(ps) > remaining:
                    primes_truncated = True
            else:
                primes_truncated = True
            if r.get("primes_truncated"):
                primes_truncated = True

    t1 = time.perf_counter()

    resp: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "nodes_used": len(set(r["node_id"] for r in per_node_results)),  # count of nodes that actually completed work
        "secondary_exec": sec_exec,
        "secondary_workers": sec_workers,
        "chunk": chunk_size,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_node_compute_seconds": sum(float(r["node_elapsed_s"]) for r in per_node_results),
        "sum_node_round_trip_seconds": sum(float(r["round_trip_s"]) for r in per_node_results),
    }

    if mode == "list":
        resp["primes"] = primes_sample
        resp["primes_truncated"] = primes_truncated
        resp["max_return_primes"] = max_return_primesf

    if include_per_node:
        resp["per_node"] = per_node_results

    return resp


class Handler(BaseHTTPRequestHandler):
    server_version = "PrimaryPrimeCoordinator/1.0"

    def _send_json(self, obj: Dict[str, Any], code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return self._send_json({"ok": True, "status": "healthy"})
        if parsed.path == "/nodes":
            nodes = REGISTRY.active_nodes()
            nodes.sort(key=lambda n: n["node_id"])
            return self._send_json({"ok": True, "nodes": nodes, "ttl_s": REGISTRY.ttl_s})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        if parsed.path == "/register":
            for k in ("node_id", "host", "port"):
                if k not in payload:
                    return self._send_json({"ok": False, "error": f"missing field: {k}"}, code=400)
            rec = REGISTRY.upsert(payload)
            print(f"[primary_node] Added node: {payload} to registry")
            return self._send_json({"ok": True, "node": rec})

        if parsed.path == "/compute":
            try:
                for k in ("low", "high"):
                    if k not in payload:
                        raise ValueError(f"missing field: {k}")
                resp = distributed_compute(payload)
                return self._send_json(resp, code=200)
            except Exception as e:
                return self._send_json({"ok": False, "error": str(e)}, code=400)

        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def log_message(self, fmt, *args):
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[primary_node] listening on http://{args.host}:{args.port}")
    print("  GET  /health")
    print("  GET  /nodes")
    print("  POST /register")
    print("  POST /compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
