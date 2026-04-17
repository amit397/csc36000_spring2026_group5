from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = ROOT / ".runtime"
DATA_DIR = RUNTIME_DIR / "data"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"
GENERATED_DIRECTORY = ROOT / "generated"
STUDENT_IMPL_DIR = ROOT / "student_impl"
PROTO_DIR = ROOT / "protos"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_GATEWAY_PORT = 50151
DEFAULT_NODE_START_PORT = 50161
DEFAULT_NODE_COUNT = 3
DEFAULT_LOGICAL_SHARD_COUNT = 8


@dataclass
class EndpointInfo:
    addr: str
    pid: int | None = None


@dataclass
class ShardNodeInfo:
    id: int
    addr: str
    pid: int | None = None


@dataclass
class LogicalShardAssignment:
    logical_shard_id: int
    owner_node_id: int


@dataclass
class ClusterState:
    gateway: EndpointInfo
    admin: EndpointInfo
    shard_nodes: list[ShardNodeInfo]
    logical_shards: list[LogicalShardAssignment]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ClusterState":
        return cls(
            gateway=EndpointInfo(**payload["gateway"]),
            admin=EndpointInfo(**payload["admin"]),
            shard_nodes=[ShardNodeInfo(**item) for item in payload.get("shard_nodes", [])],
            logical_shards=[LogicalShardAssignment(**item) for item in payload.get("logical_shards", [])],
        )

    def shard_node(self, node_id: int) -> ShardNodeInfo:
        for node in self.shard_nodes:
            if node.id == node_id:
                return node
        raise KeyError(f"Unknown node_id={node_id}")

    def logical_shard(self, logical_shard_id: int) -> LogicalShardAssignment:
        for shard in self.logical_shards:
            if shard.logical_shard_id == logical_shard_id:
                return shard
        raise KeyError(f"Unknown logical_shard_id={logical_shard_id}")


def ensure_generated_protos() -> None:
    expected = [
        GENERATED_DIRECTORY / "week09_gateway_pb2.py",
        GENERATED_DIRECTORY / "week09_gateway_pb2_grpc.py",
        GENERATED_DIRECTORY / "cluster_admin_pb2.py",
        GENERATED_DIRECTORY / "cluster_admin_pb2_grpc.py",
        GENERATED_DIRECTORY / "shard_node_pb2.py",
        GENERATED_DIRECTORY / "shard_node_pb2_grpc.py",
    ]
    if all(path.exists() for path in expected):
        return

    try:
        from grpc_tools import protoc
    except ModuleNotFoundError:
        return

    GENERATED_DIRECTORY.mkdir(parents=True, exist_ok=True)
    proto_files = [str(path) for path in sorted(PROTO_DIR.glob("*.proto"))]
    protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PROTO_DIR}",
            f"--python_out={GENERATED_DIRECTORY}",
            f"--grpc_python_out={GENERATED_DIRECTORY}",
            *proto_files,
        ]
    )


ensure_generated_protos()

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def default_logical_shards(node_count: int = DEFAULT_NODE_COUNT) -> list[LogicalShardAssignment]:
    skewed_owner_sequence = [1, 1, 1, 1, 1, 2, 2, 3]
    logical_shards: list[LogicalShardAssignment] = []
    for logical_shard_id in range(DEFAULT_LOGICAL_SHARD_COUNT):
        owner = skewed_owner_sequence[logical_shard_id % len(skewed_owner_sequence)]
        owner = max(1, min(node_count, owner))
        logical_shards.append(LogicalShardAssignment(logical_shard_id=logical_shard_id, owner_node_id=owner))
    return logical_shards


def default_cluster(host: str = DEFAULT_HOST, node_count: int = DEFAULT_NODE_COUNT) -> ClusterState:
    shard_nodes: list[ShardNodeInfo] = []
    for node_id in range(1, node_count + 1):
        shard_nodes.append(ShardNodeInfo(id=node_id, addr=f"{host}:{DEFAULT_NODE_START_PORT + (node_id - 1)}"))
    return ClusterState(
        gateway=EndpointInfo(addr=f"{host}:{DEFAULT_GATEWAY_PORT}"),
        admin=EndpointInfo(addr=f"{host}:{DEFAULT_GATEWAY_PORT}"),
        shard_nodes=shard_nodes,
        logical_shards=default_logical_shards(node_count=node_count),
    )


def load_cluster_if_present() -> ClusterState | None:
    if not CLUSTER_JSON.exists():
        return None
    try:
        return ClusterState.from_dict(json.loads(CLUSTER_JSON.read_text()))
    except Exception:
        return None


def write_cluster(cluster_data: ClusterState) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    CLUSTER_JSON.write_text(json.dumps(cluster_data.to_dict(), indent=2))


def load_cluster() -> ClusterState:
    loaded = load_cluster_if_present()
    if loaded is None:
        raise FileNotFoundError(f"{CLUSTER_JSON} does not exist")
    return loaded


def shard_storage_path(node_id: int, logical_shard_id: int) -> Path:
    return DATA_DIR / f"node_{node_id}" / f"logical_shard_{logical_shard_id}.json"


def node_data_dir(node_id: int) -> Path:
    return DATA_DIR / f"node_{node_id}"
