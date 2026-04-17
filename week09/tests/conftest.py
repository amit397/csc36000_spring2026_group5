from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"
SCRIPTS_DIR = ROOT / "scripts"
PROTO_DIR = ROOT / "protos"
GEN_DIR = RUNTIME_DIR / "_pb"


def pytest_addoption(parser):
    parser.addoption(
        "--application",
        action="store",
        default=None,
        choices=["course_registration", "wallet", "inventory"],
        help="Run the application-specific tests for the selected application.",
    )


def _run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)


def _ensure_scripts_exist() -> None:
    required = [
        SCRIPTS_DIR / "run_cluster.py",
        SCRIPTS_DIR / "stop_cluster.py",
        SCRIPTS_DIR / "start_shard.py",
        SCRIPTS_DIR / "stop_shard.py",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        pytest.fail("Missing required scripts:\n" + "\n".join(f" - {item}" for item in missing))


def _compile_protos() -> None:
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    proto_files = sorted(str(path) for path in PROTO_DIR.glob("*.proto"))
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        *proto_files,
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        pytest.fail(
            "Failed to compile week09 protos.\n"
            f"Command: {' '.join(cmd)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    if str(GEN_DIR) not in sys.path:
        sys.path.insert(0, str(GEN_DIR))


def _selected_application() -> str:
    project_choice_path = ROOT / "student_impl" / "project_choice.py"
    spec = importlib.util.spec_from_file_location("week09_project_choice", project_choice_path)
    if spec is None or spec.loader is None:
        pytest.fail(f"Could not import {project_choice_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    selected_application = getattr(module, "SELECTED_APPLICATION", "")
    if hasattr(selected_application, "value"):
        selected_application = selected_application.value
    if selected_application not in {"course_registration", "wallet", "inventory"}:
        pytest.fail(
            "student_impl/project_choice.py must define SELECTED_APPLICATION as one of "
            "'course_registration', 'wallet', or 'inventory'."
        )
    return selected_application


def _enum_or_raw(module, attribute_name: str) -> str:
    value = getattr(module, attribute_name, "")
    if hasattr(value, "value"):
        return value.value
    return value


def _strategy_declarations() -> dict[str, str]:
    project_choice_path = ROOT / "student_impl" / "project_choice.py"
    spec = importlib.util.spec_from_file_location("week09_project_choice", project_choice_path)
    if spec is None or spec.loader is None:
        pytest.fail(f"Could not import {project_choice_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    declarations = {
        "application": _enum_or_raw(module, "SELECTED_APPLICATION"),
        "sharding": _enum_or_raw(module, "SELECTED_SHARDING_TRADEOFF"),
        "isolation": _enum_or_raw(module, "SELECTED_ISOLATION_TRADEOFF"),
    }
    if declarations["application"] not in {"course_registration", "wallet", "inventory"}:
        pytest.fail(
            "student_impl/project_choice.py must define SELECTED_APPLICATION as one of "
            "'course_registration', 'wallet', or 'inventory'."
        )
    if declarations["sharding"] not in {"hash_distributed", "range_locality"}:
        pytest.fail(
            "student_impl/project_choice.py must define SELECTED_SHARDING_TRADEOFF as one of "
            "'hash_distributed' or 'range_locality'."
        )
    if declarations["isolation"] not in {"read_committed_like", "serializable_like"}:
        pytest.fail(
            "student_impl/project_choice.py must define SELECTED_ISOLATION_TRADEOFF as one of "
            "'read_committed_like' or 'serializable_like'."
        )
    return declarations


@pytest.fixture(scope="session")
def selected_application(pytestconfig) -> str:
    configured_application = _strategy_declarations()["application"]
    flagged_application = pytestconfig.getoption("application")
    if flagged_application is None:
        return configured_application
    if flagged_application != configured_application:
        pytest.fail(
            "The pytest --application flag must match student_impl/project_choice.py.\n"
            f"Flag value: {flagged_application}\n"
            f"Configured value: {configured_application}"
        )
    return flagged_application


@pytest.fixture(scope="session")
def selected_sharding_tradeoff() -> str:
    return _strategy_declarations()["sharding"]


@pytest.fixture(scope="session")
def selected_isolation_tradeoff() -> str:
    return _strategy_declarations()["isolation"]


@pytest.fixture(scope="session", autouse=True)
def compiled_protos():
    _compile_protos()
    yield


def import_stubs():
    import cluster_admin_pb2 as admin_pb2
    import cluster_admin_pb2_grpc as admin_grpc
    import week09_gateway_pb2 as gateway_pb2
    import week09_gateway_pb2_grpc as gateway_grpc
    return admin_pb2, admin_grpc, gateway_pb2, gateway_grpc


def load_cluster() -> dict[str, Any]:
    if not CLUSTER_JSON.exists():
        pytest.fail(f"Expected {CLUSTER_JSON} to exist after run_cluster.py")
    return json.loads(CLUSTER_JSON.read_text())


def wait_for_port(addr: str, timeout: float = 10.0):
    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=timeout)
    return channel


@pytest.fixture()
def cluster():
    _ensure_scripts_exist()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    proc = _run([sys.executable, str(SCRIPTS_DIR / "run_cluster.py")], cwd=ROOT, check=False)
    if proc.returncode != 0:
        pytest.fail(f"run_cluster.py failed.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")

    deadline = time.time() + 12.0
    while time.time() < deadline and not CLUSTER_JSON.exists():
        time.sleep(0.1)
    if not CLUSTER_JSON.exists():
        pytest.fail("cluster.json was not created by run_cluster.py")

    cluster_data = load_cluster()
    wait_for_port(cluster_data["gateway"]["addr"], timeout=15.0)
    for node_entry in cluster_data["shard_nodes"]:
        wait_for_port(node_entry["addr"], timeout=15.0)

    yield cluster_data

    proc2 = _run([sys.executable, str(SCRIPTS_DIR / "stop_cluster.py")], cwd=ROOT, check=False)
    if proc2.returncode != 0:
        print(f"[teardown] stop_cluster.py failed:\nstdout:\n{proc2.stdout}\nstderr:\n{proc2.stderr}", file=sys.stderr)


@pytest.fixture()
def gateway_stub(cluster):
    _, _, gateway_pb2, gateway_grpc = import_stubs()
    channel = wait_for_port(cluster["gateway"]["addr"], timeout=10.0)
    return gateway_pb2, gateway_grpc.Week09GatewayStub(channel)


@pytest.fixture()
def admin_stub(cluster):
    admin_pb2, admin_grpc, _, _ = import_stubs()
    channel = wait_for_port(cluster["admin"]["addr"], timeout=10.0)
    return admin_pb2, admin_grpc.ClusterAdminStub(channel)
