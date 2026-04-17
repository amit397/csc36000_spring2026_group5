#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from common import ROOT, RUNTIME_DIR, load_or_default, start_process_and_wait, write_cluster


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start a single week09 shard node.")
    parser.add_argument("node_id", type=int)
    parser.add_argument("--host", default="127.0.0.1")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    cluster_data = load_or_default(args.host)
    node_entry = None
    for candidate in cluster_data.shard_nodes:
        if candidate.id == args.node_id:
            node_entry = candidate
            break
    if node_entry is None:
        raise SystemExit(f"Unknown node_id={args.node_id}")
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
            str(args.node_id),
        ],
        RUNTIME_DIR / f"node_{args.node_id}.log",
        addr,
    )
    node_entry.pid = proc.pid
    write_cluster(cluster_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
