import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple

import grpc
import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"
SCRIPTS_DIR = ROOT / "scripts"

PROTO_DIR = ROOT / "protos"
GEN_DIR = RUNTIME_DIR / "_pb"

def _run(cmd: List[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)

def _ensure_scripts_exist():
    required = [
        SCRIPTS_DIR / "run_cluster.py",
        SCRIPTS_DIR / "stop_cluster.py",
        SCRIPTS_DIR / "start_replica.py",
        SCRIPTS_DIR / "stop_replica.py",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        msg = "Missing required scripts:\n" + "\n".join(f" - {p}" for p in missing)
        pytest.fail(msg)

def _compile_protos():
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        str(PROTO_DIR / "direct_gateway.proto"),
        str(PROTO_DIR / "replica_admin.proto"),
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        pytest.fail(
            "Failed to compile protos with grpc_tools.protoc.\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}\n"
            "Make sure you installed requirements: pip install -r requirements.txt"
        )
    if str(GEN_DIR) not in sys.path:
        sys.path.insert(0, str(GEN_DIR))

@pytest.fixture(scope="session", autouse=True)
def compiled_protos():
    _compile_protos()
    yield

def load_cluster() -> Dict[str, Any]:
    if not CLUSTER_JSON.exists():
        pytest.fail(f"Expected {CLUSTER_JSON} to exist. Did scripts/run_cluster.py create it?")
    return json.loads(CLUSTER_JSON.read_text())

def wait_for_port(addr: str, timeout: float = 10.0):
    ch = grpc.insecure_channel(addr)
    grpc.channel_ready_future(ch).result(timeout=timeout)
    return ch

def import_stubs():
    import direct_gateway_pb2 as direct_pb2
    import direct_gateway_pb2_grpc as direct_grpc
    import replica_admin_pb2 as rep_pb2
    import replica_admin_pb2_grpc as rep_grpc
    return direct_pb2, direct_grpc, rep_pb2, rep_grpc

def wait_until(fn, timeout: float = 10.0, interval: float = 0.1, desc: str = "condition"):
    end = time.time() + timeout
    last_exc = None
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception as e:
            last_exc = e
        time.sleep(interval)
    if last_exc:
        raise AssertionError(f"Timed out waiting for {desc}. Last error: {last_exc}") from last_exc
    raise AssertionError(f"Timed out waiting for {desc}.")

def get_replica_statuses(replica_addrs: List[str]):
    _, _, rep_pb2, rep_grpc = import_stubs()
    statuses = []
    for addr in replica_addrs:
        ch = wait_for_port(addr, timeout=5.0)
        stub = rep_grpc.ReplicaAdminStub(ch)
        resp = stub.Status(rep_pb2.StatusRequest(), timeout=2.0)
        statuses.append((addr, resp))
    return statuses

def pick_leader(statuses):
    leaders = [(a, s) for (a, s) in statuses if s.role == 2]  # LEADER
    if len(leaders) != 1:
        return None
    return leaders[0]

@pytest.fixture()
def cluster():
    _ensure_scripts_exist()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    proc = _run([sys.executable, str(SCRIPTS_DIR / "run_cluster.py")], cwd=ROOT, check=False)
    if proc.returncode != 0:
        pytest.fail(f"run_cluster.py failed.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

    wait_until(lambda: CLUSTER_JSON.exists(), timeout=10.0, interval=0.1, desc="cluster.json creation")

    data = load_cluster()
    gateway_addr = data["gateway"]["addr"]
    replica_addrs = [r["addr"] for r in data["replicas"]]

    wait_for_port(gateway_addr, timeout=15.0)
    for addr in replica_addrs:
        wait_for_port(addr, timeout=15.0)

    def leader_ready():
        st = get_replica_statuses(replica_addrs)
        return pick_leader(st)

    wait_until(leader_ready, timeout=20.0, interval=0.2, desc="leader election")

    yield data

    proc2 = _run([sys.executable, str(SCRIPTS_DIR / "stop_cluster.py")], cwd=ROOT, check=False)
    if proc2.returncode != 0:
        print(f"[teardown] stop_cluster.py failed:\nstdout:\n{proc2.stdout}\nstderr:\n{proc2.stderr}", file=sys.stderr)

@pytest.fixture()
def gateway_stub(cluster):
    direct_pb2, direct_grpc, _, _ = import_stubs()
    gw_addr = cluster["gateway"]["addr"]
    ch = wait_for_port(gw_addr, timeout=10.0)
    return direct_pb2, direct_grpc.DirectGatewayStub(ch)

def ordered_users(u1: str, u2: str) -> Tuple[str, str]:
    return (u1, u2) if u1 <= u2 else (u2, u1)

def stop_replica(replica_id: int):
    proc = _run([sys.executable, str(SCRIPTS_DIR / "stop_replica.py"), str(replica_id)], cwd=ROOT, check=False)
    if proc.returncode != 0:
        pytest.fail(f"stop_replica.py {replica_id} failed.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

def start_replica(replica_id: int):
    proc = _run([sys.executable, str(SCRIPTS_DIR / "start_replica.py"), str(replica_id)], cwd=ROOT, check=False)
    if proc.returncode != 0:
        pytest.fail(f"start_replica.py {replica_id} failed.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

def wait_for_leader(replica_addrs: List[str], timeout: float = 20.0):
    def _leader():
        st = get_replica_statuses(replica_addrs)
        return pick_leader(st)
    return wait_until(_leader, timeout=timeout, interval=0.2, desc="new leader election")
