#!/usr/bin/env python3
from __future__ import annotations

import argparse

from common import best_effort_stop_pid, load_cluster_if_present, write_cluster


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stop a single week09 shard node.")
    parser.add_argument("node_id", type=int)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    cluster_data = load_cluster_if_present()
    if cluster_data is None:
        return 0
    for node_entry in cluster_data.shard_nodes:
        if node_entry.id == args.node_id:
            best_effort_stop_pid(node_entry.pid)
            node_entry.pid = None
            break
    write_cluster(cluster_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
