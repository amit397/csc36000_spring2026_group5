#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from common import (
    ROOT,
    RUNTIME_DIR,
    best_effort_stop_cluster_pids,
    load_cluster_if_present,
    start_process,
    write_cluster,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Start <num-replicas> (default: 5) replicas and 1 gateway; write .runtime/cluster.json"
    )
    p.add_argument("--host", default="127.0.0.1", help="Host for gateway and replicas.")
    p.add_argument("--num-replicas", type=int, default=5)
    p.add_argument("--gateway-port", type=int, default=50051)
    p.add_argument("--replica-start-port", type=int, default=50061)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.num_replicas < 1:
        print("--num-replicas must be >= 1", file=sys.stderr)
        return 2
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    existing = load_cluster_if_present()
    if existing is not None:
        best_effort_stop_cluster_pids(existing)

    host = args.host
    gateway_addr = f"{host}:{args.gateway_port}"

    replica_entries: list[dict[str, object]] = []

    # Start replicas and always pass host+port explicitly.
    for i in range(args.num_replicas):
        rid = i + 1
        port = args.replica_start_port + i
        addr = f"{host}:{port}"
        proc = start_process(
            [
                sys.executable,
                str(ROOT / "replica_admin.py"),
                "--host",
                host,
                "--port",
                str(port),
            ],
            RUNTIME_DIR / f"replica_{rid}.log",
        )
        replica_entries.append({"id": rid, "addr": addr, "pid": proc.pid})

    # Start gateway and always pass host+port explicitly.
    gw_proc = start_process(
        [
            sys.executable,
            str(ROOT / "direct_gateway.py"),
            "--host",
            host,
            "--port",
            str(args.gateway_port),
        ],
        RUNTIME_DIR / "gateway.log",
    )

    cluster = {
        "gateway": {"addr": gateway_addr, "pid": gw_proc.pid},
        "replicas": replica_entries,
    }
    write_cluster(cluster)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
