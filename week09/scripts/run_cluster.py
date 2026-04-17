#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from common import ROOT, RUNTIME_DIR, reset_cluster_state, start_process_and_wait, write_cluster


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start the week09 gateway and shard nodes.")
    parser.add_argument("--host", default="127.0.0.1")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    cluster_data = reset_cluster_state(host=args.host)

    for node_entry in cluster_data.shard_nodes:
        node_id = node_entry.id
        addr = node_entry.addr
        host, raw_port = addr.rsplit(":", 1)
        proc = start_process_and_wait(
            [
                sys.executable,
                str(ROOT / "shard_server.py"),
                "--host",
                host,
                "--port",
                raw_port,
                "--node-id",
                str(node_id),
            ],
            RUNTIME_DIR / f"node_{node_id}.log",
            addr,
        )
        node_entry.pid = proc.pid

    gateway_addr = cluster_data.gateway.addr
    host, raw_port = gateway_addr.rsplit(":", 1)
    gateway_proc = start_process_and_wait(
        [
            sys.executable,
            str(ROOT / "gateway_server.py"),
            "--host",
            host,
            "--port",
            raw_port,
        ],
        RUNTIME_DIR / "gateway.log",
        gateway_addr,
    )
    cluster_data.gateway.pid = gateway_proc.pid
    cluster_data.admin.pid = gateway_proc.pid
    write_cluster(cluster_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
