#!/usr/bin/env python3
from __future__ import annotations

"""
Provided cluster admin client for manual inspection.

Use this client when you want to inspect the current cluster layout while
developing or grading a submission by hand.

Provided operations:
- state

What this client is useful for:
- confirming that the gateway is up
- seeing which node owns each logical shard
- checking approximate record counts per node and logical shard
"""

import argparse
import json
import sys
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import cluster_admin_pb2
import cluster_admin_pb2_grpc


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Provided cluster admin client.")
    parser.add_argument("--admin", default="127.0.0.1:50151", help="Admin address in host:port form.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("state")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    channel = grpc.insecure_channel(args.admin)
    grpc.channel_ready_future(channel).result(timeout=10.0)
    stub = cluster_admin_pb2_grpc.ClusterAdminStub(channel)

    if args.command == "state":
        response = stub.GetClusterState(cluster_admin_pb2.GetClusterStateRequest(), timeout=5.0)
        payload = {
            "nodes": [
                {
                    "node_id": node.node_id,
                    "addr": node.addr,
                    "logical_shard_ids": list(node.logical_shard_ids),
                    "total_records": node.total_records,
                }
                for node in response.nodes
            ],
            "logical_shards": [
                {
                    "logical_shard_id": shard.logical_shard_id,
                    "owner_node_id": shard.owner_node_id,
                    "record_count": shard.record_count,
                }
                for shard in response.logical_shards
            ],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
