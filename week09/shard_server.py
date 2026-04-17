#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc

from week09_common import GENERATED_DIRECTORY, ROOT, load_cluster_if_present, node_data_dir

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import shard_node_pb2
import shard_node_pb2_grpc

from student_impl import storage as student_storage
from student_impl import transactions as student_transactions


class StudentShardStoreAdapter:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.data_dir = node_data_dir(node_id)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _storage_path(self, logical_shard_id: int) -> Path:
        return self.data_dir / f"logical_shard_{logical_shard_id}.json"

    def _load_state(self, logical_shard_id: int) -> dict[str, Any]:
        storage_path = self._storage_path(logical_shard_id)
        if not storage_path.exists():
            return {}
        return student_storage.load_logical_shard_state(storage_path)

    def _save_state(self, logical_shard_id: int, state: dict[str, Any]) -> None:
        student_storage.save_logical_shard_state(self._storage_path(logical_shard_id), state)

    def apply(self, logical_shard_id: int, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        state = self._load_state(logical_shard_id)
        result = student_transactions.apply_local_mutation(state, operation_name, payload)
        self._save_state(logical_shard_id, state)
        return result

    def read(self, logical_shard_id: int, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        state = self._load_state(logical_shard_id)
        return student_transactions.run_local_query(state, query_name, payload)

    def dump(self, logical_shard_id: int) -> dict[str, Any]:
        state = self._load_state(logical_shard_id)
        return student_storage.export_logical_shard_state(state)

    def load_dump(self, logical_shard_id: int, dump_data: dict[str, Any]) -> None:
        state = student_storage.import_logical_shard_state(dump_data)
        self._save_state(logical_shard_id, state)

    def drop(self, logical_shard_id: int) -> None:
        storage_path = self._storage_path(logical_shard_id)
        if storage_path.exists():
            storage_path.unlink()

    def status(self) -> list[tuple[int, int]]:
        record_counts_by_shard: dict[int, int] = {}
        for storage_path in sorted(self.data_dir.glob("logical_shard_*.json")):
            logical_shard_id = int(storage_path.stem.rsplit("_", 1)[1])
            try:
                state = student_storage.load_logical_shard_state(storage_path)
                record_count = int(student_storage.count_records(state))
            except NotImplementedError:
                record_count = 0
            record_counts_by_shard[logical_shard_id] = record_count

        cluster_data = load_cluster_if_present()
        if cluster_data is not None:
            for shard_entry in cluster_data.logical_shards:
                if shard_entry.owner_node_id == self.node_id:
                    logical_shard_id = shard_entry.logical_shard_id
                    record_counts_by_shard.setdefault(logical_shard_id, 0)

        return sorted(record_counts_by_shard.items())


class ShardNodeService(shard_node_pb2_grpc.ShardNodeServicer):
    def __init__(self, node_id: int, bind_addr: str):
        self.node_id = node_id
        self.bind_addr = bind_addr
        self.store = StudentShardStoreAdapter(node_id=node_id)

    def Apply(self, request, context):
        try:
            result = self.store.apply(
                logical_shard_id=request.logical_shard_id,
                operation_name=request.operation,
                payload=json.loads(request.payload_json or "{}"),
            )
            return shard_node_pb2.ApplyResponse(ok=True, result_json=json.dumps(result))
        except NotImplementedError as exc:
            return shard_node_pb2.ApplyResponse(ok=False, error=str(exc))
        except Exception as exc:  # pragma: no cover - surfaced in tests
            return shard_node_pb2.ApplyResponse(ok=False, error=f"{type(exc).__name__}: {exc}")

    def Read(self, request, context):
        try:
            result = self.store.read(
                logical_shard_id=request.logical_shard_id,
                query_name=request.query,
                payload=json.loads(request.payload_json or "{}"),
            )
            return shard_node_pb2.ReadResponse(ok=True, result_json=json.dumps(result))
        except NotImplementedError as exc:
            return shard_node_pb2.ReadResponse(ok=False, error=str(exc))
        except Exception as exc:  # pragma: no cover - surfaced in tests
            return shard_node_pb2.ReadResponse(ok=False, error=f"{type(exc).__name__}: {exc}")

    def GetShardDump(self, request, context):
        try:
            dump_data = self.store.dump(request.logical_shard_id)
            return shard_node_pb2.GetShardDumpResponse(ok=True, dump_json=json.dumps(dump_data))
        except NotImplementedError as exc:
            return shard_node_pb2.GetShardDumpResponse(ok=False, error=str(exc))
        except Exception as exc:
            return shard_node_pb2.GetShardDumpResponse(ok=False, error=f"{type(exc).__name__}: {exc}")

    def LoadShardDump(self, request, context):
        try:
            self.store.load_dump(request.logical_shard_id, json.loads(request.dump_json or "{}"))
            return shard_node_pb2.LoadShardDumpResponse(ok=True)
        except NotImplementedError as exc:
            return shard_node_pb2.LoadShardDumpResponse(ok=False, error=str(exc))
        except Exception as exc:
            return shard_node_pb2.LoadShardDumpResponse(ok=False, error=f"{type(exc).__name__}: {exc}")

    def DropShard(self, request, context):
        try:
            self.store.drop(request.logical_shard_id)
            return shard_node_pb2.DropShardResponse(ok=True)
        except Exception as exc:
            return shard_node_pb2.DropShardResponse(ok=False, error=f"{type(exc).__name__}: {exc}")

    def Status(self, request, context):
        response = shard_node_pb2.StatusResponse(node_id=self.node_id, addr=self.bind_addr)
        for logical_shard_id, record_count in self.store.status():
            response.logical_shards.append(
                shard_node_pb2.LogicalShardInfo(logical_shard_id=logical_shard_id, record_count=record_count)
            )
        return response


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one week09 shard node server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--node-id", type=int, required=True)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    bind_addr = f"0.0.0.0:{args.port}"
    advertised_addr = f"{args.host}:{args.port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    shard_node_pb2_grpc.add_ShardNodeServicer_to_server(
        ShardNodeService(node_id=args.node_id, bind_addr=advertised_addr),
        server,
    )
    server.add_insecure_port(bind_addr)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop(grace=0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
