from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"


def default_cluster(host: str, replica_start_port: int) -> dict[str, Any]:
    reps: list[dict[str, Any]] = []
    for rid in range(1, 6):
        reps.append(
            {
                "id": rid,
                "addr": f"{host}:{replica_start_port + (rid - 1)}",
                "pid": None,
            }
        )
    return {
        "gateway": {"addr": f"{host}:50051", "pid": None},
        "replicas": reps,
    }


def load_or_init_cluster(host: str, replica_start_port: int) -> dict[str, Any]:
    if CLUSTER_JSON.exists():
        try:
            return json.loads(CLUSTER_JSON.read_text())
        except Exception:
            pass
    return default_cluster(host, replica_start_port)


def load_cluster_if_present() -> dict[str, Any] | None:
    if not CLUSTER_JSON.exists():
        return None
    try:
        return json.loads(CLUSTER_JSON.read_text())
    except Exception:
        return None


def find_or_create_replica_entry(
    data: dict[str, Any], replica_id: int, host: str, replica_start_port: int
) -> dict[str, Any]:
    for rep in data.get("replicas", []):
        if int(rep.get("id", -1)) == replica_id:
            return rep

    rep = {
        "id": replica_id,
        "addr": f"{host}:{replica_start_port + (replica_id - 1)}",
        "pid": None,
    }
    data.setdefault("replicas", []).append(rep)
    data["replicas"] = sorted(data["replicas"], key=lambda r: int(r.get("id", 0)))
    return rep


import time

def best_effort_stop_pid(pid: Any) -> None:
    if not isinstance(pid, int):
        return
    # On Windows, try taskkill first for a more definitive kill
    if sys.platform == "win32":
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], 
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    
    try:
        os.kill(pid, signal.SIGTERM)
        if sys.platform == "win32":
            deadline = time.time() + 5.0
            while time.time() < deadline:
                try:
                    os.kill(pid, 0)
                    time.sleep(0.1)
                except BaseException:
                    break
    except Exception:
        pass


def collect_cluster_pids(data: dict[str, Any]) -> list[int]:
    pids: list[int] = []
    gw_pid = data.get("gateway", {}).get("pid")
    if isinstance(gw_pid, int):
        pids.append(gw_pid)

    for rep in data.get("replicas", []):
        pid = rep.get("pid")
        if isinstance(pid, int):
            pids.append(pid)

    return pids


def null_out_all_pids(data: dict[str, Any]) -> None:
    if "gateway" in data and isinstance(data["gateway"], dict):
        data["gateway"]["pid"] = None
    for rep in data.get("replicas", []):
        if isinstance(rep, dict):
            rep["pid"] = None


def best_effort_stop_cluster_pids(data: dict[str, Any]) -> None:
    for pid in collect_cluster_pids(data):
        best_effort_stop_pid(pid)


def start_process(cmd: list[str], log_path: Path) -> subprocess.Popen[Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    stdin_fd = os.open(os.devnull, os.O_RDONLY)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdin=stdin_fd,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        os.close(log_fd)
        os.close(stdin_fd)
    return proc


def write_cluster(data: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    CLUSTER_JSON.write_text(json.dumps(data, indent=2))
