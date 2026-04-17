#!/usr/bin/env python3
from __future__ import annotations

from common import load_cluster_if_present, best_effort_stop_pid, best_effort_stop_listening_ports, cluster_ports, write_cluster


def main() -> int:
    cluster_data = load_cluster_if_present()
    if cluster_data is None:
        return 0
    best_effort_stop_pid(cluster_data.gateway.pid)
    for node_entry in cluster_data.shard_nodes:
        best_effort_stop_pid(node_entry.pid)
    best_effort_stop_listening_ports(cluster_ports(cluster_data))
    cluster_data.gateway.pid = None
    cluster_data.admin.pid = None
    for node_entry in cluster_data.shard_nodes:
        node_entry.pid = None
    write_cluster(cluster_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
