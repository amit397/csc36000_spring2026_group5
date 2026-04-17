from __future__ import annotations

import json
from typing import Any

import grpc

import cluster_admin_pb2
import shard_node_pb2
import shard_node_pb2_grpc

from week09_common import ClusterState, load_cluster, write_cluster


def json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def json_loads(payload_json: str) -> dict[str, Any]:
    if not payload_json:
        return {}
    return json.loads(payload_json)


class GatewayShardDirectory:
    def __init__(self):
        self._channels_by_addr: dict[str, grpc.Channel] = {}
        self._stubs_by_addr: dict[str, shard_node_pb2_grpc.ShardNodeStub] = {}

    def close(self) -> None:
        for channel in self._channels_by_addr.values():
            channel.close()
        self._channels_by_addr.clear()
        self._stubs_by_addr.clear()

    def _cluster(self) -> ClusterState:
        return load_cluster()

    def _node_entry(self, node_id: int):
        return self._cluster().shard_node(node_id)

    def _logical_shard_entry(self, logical_shard_id: int):
        return self._cluster().logical_shard(logical_shard_id)

    def logical_shard_owner_node_id(self, logical_shard_id: int) -> int:
        return self._logical_shard_entry(logical_shard_id).owner_node_id

    def owner_addr_for_logical_shard(self, logical_shard_id: int) -> str:
        owner_node_id = self.logical_shard_owner_node_id(logical_shard_id)
        return self._node_entry(owner_node_id).addr

    def node_addr(self, node_id: int) -> str:
        return self._node_entry(node_id).addr

    def _stub_for_addr(self, addr: str) -> shard_node_pb2_grpc.ShardNodeStub:
        existing = self._stubs_by_addr.get(addr)
        if existing is not None:
            return existing
        channel = grpc.insecure_channel(addr)
        stub = shard_node_pb2_grpc.ShardNodeStub(channel)
        self._channels_by_addr[addr] = channel
        self._stubs_by_addr[addr] = stub
        return stub

    def logical_shard_for_payload(self, application_name: str, operation_name: str, payload: dict[str, Any]) -> int:
        from student_impl import sharding

        partition_key = sharding.build_partition_key(application_name, operation_name, payload)
        return int(sharding.choose_logical_shard(partition_key, total_logical_shards=len(self._cluster().logical_shards)))

    def apply_to_shard(self, logical_shard_id: int, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        addr = self.owner_addr_for_logical_shard(logical_shard_id)
        response = self._stub_for_addr(addr).Apply(
            shard_node_pb2.ApplyRequest(
                logical_shard_id=logical_shard_id,
                operation=operation_name,
                payload_json=json_dumps(payload),
            ),
            timeout=5.0,
        )
        if not response.ok:
            raise RuntimeError(response.error or f"apply failed for logical shard {logical_shard_id}")
        result = json_loads(response.result_json)
        result.setdefault("routing", {"logical_shard_id": logical_shard_id, "storage_addr": addr})
        return result

    def read_from_shard(self, logical_shard_id: int, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        addr = self.owner_addr_for_logical_shard(logical_shard_id)
        response = self._stub_for_addr(addr).Read(
            shard_node_pb2.ReadRequest(
                logical_shard_id=logical_shard_id,
                query=query_name,
                payload_json=json_dumps(payload),
            ),
            timeout=5.0,
        )
        if not response.ok:
            raise RuntimeError(response.error or f"read failed for logical shard {logical_shard_id}")
        result = json_loads(response.result_json)
        result.setdefault("routing", {"logical_shard_id": logical_shard_id, "storage_addr": addr})
        return result

    def node_states(self) -> list[dict[str, Any]]:
        states: list[dict[str, Any]] = []
        cluster = self._cluster()
        for node_entry in cluster.shard_nodes:
            addr = node_entry.addr
            status = self._stub_for_addr(addr).Status(shard_node_pb2.StatusRequest(), timeout=5.0)
            logical_shards = [
                {"logical_shard_id": int(item.logical_shard_id), "record_count": int(item.record_count)}
                for item in status.logical_shards
            ]
            states.append(
                {
                    "node_id": int(status.node_id),
                    "addr": addr,
                    "logical_shards": logical_shards,
                    "total_records": sum(item["record_count"] for item in logical_shards),
                }
            )
        return states

    def perform_rebalance(self) -> cluster_admin_pb2.RebalanceResponse:
        current_states = self.node_states()
        shard_counts_by_node = {state["node_id"]: len(state["logical_shards"]) for state in current_states}
        logical_shards_by_node = {
            state["node_id"]: sorted(item["logical_shard_id"] for item in state["logical_shards"])
            for state in current_states
        }
        moves: list[dict[str, int]] = []

        while True:
            max_node_id = max(shard_counts_by_node, key=lambda node_id: shard_counts_by_node[node_id])
            min_node_id = min(shard_counts_by_node, key=lambda node_id: shard_counts_by_node[node_id])
            if shard_counts_by_node[max_node_id] - shard_counts_by_node[min_node_id] <= 1:
                break
            logical_shard_id = logical_shards_by_node[max_node_id][0]
            logical_shards_by_node[max_node_id].remove(logical_shard_id)
            logical_shards_by_node[min_node_id].append(logical_shard_id)
            logical_shards_by_node[min_node_id].sort()
            shard_counts_by_node[max_node_id] -= 1
            shard_counts_by_node[min_node_id] += 1
            moves.append(
                {
                    "logical_shard_id": logical_shard_id,
                    "from_node_id": max_node_id,
                    "to_node_id": min_node_id,
                }
            )

        cluster = self._cluster()
        moved_shards = 0
        notes: list[str] = []

        for move in moves:
            logical_shard_id = int(move["logical_shard_id"])
            from_node_id = int(move["from_node_id"])
            to_node_id = int(move["to_node_id"])
            source_addr = self.node_addr(from_node_id)
            target_addr = self.node_addr(to_node_id)

            dump_response = self._stub_for_addr(source_addr).GetShardDump(
                shard_node_pb2.GetShardDumpRequest(logical_shard_id=logical_shard_id),
                timeout=5.0,
            )
            if not dump_response.ok:
                raise RuntimeError(dump_response.error or f"could not export logical shard {logical_shard_id}")

            load_response = self._stub_for_addr(target_addr).LoadShardDump(
                shard_node_pb2.LoadShardDumpRequest(logical_shard_id=logical_shard_id, dump_json=dump_response.dump_json),
                timeout=5.0,
            )
            if not load_response.ok:
                raise RuntimeError(load_response.error or f"could not import logical shard {logical_shard_id}")

            drop_response = self._stub_for_addr(source_addr).DropShard(
                shard_node_pb2.DropShardRequest(logical_shard_id=logical_shard_id),
                timeout=5.0,
            )
            if not drop_response.ok:
                raise RuntimeError(drop_response.error or f"could not drop logical shard {logical_shard_id}")

            shard_entry = self._logical_shard_entry(logical_shard_id)
            shard_entry.owner_node_id = to_node_id
            moved_shards += 1
            notes.append(f"moved logical shard {logical_shard_id} from node {from_node_id} to node {to_node_id}")

        write_cluster(cluster)
        return cluster_admin_pb2.RebalanceResponse(moved_shards=moved_shards, notes=notes)
