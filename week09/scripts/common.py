from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from week09_common import CLUSTER_JSON, RUNTIME_DIR, default_cluster, load_cluster_if_present, write_cluster
from week09_common import ClusterState


def best_effort_stop_pid(pid: Any) -> None:
    if not isinstance(pid, int):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.time() + 3.0
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def cluster_ports(cluster_data: ClusterState) -> list[int]:
    ports: set[int] = set()
    for endpoint in (cluster_data.gateway, cluster_data.admin):
        addr = endpoint.addr
        if ":" in addr:
            ports.add(int(addr.rsplit(":", 1)[1]))
    for node_entry in cluster_data.shard_nodes:
        addr = node_entry.addr
        if ":" in addr:
            ports.add(int(addr.rsplit(":", 1)[1]))
    return sorted(ports)


def best_effort_stop_listening_ports(port_numbers: list[int]) -> None:
    discovered_pids: set[int] = set()
    for port_number in port_numbers:
        proc = subprocess.run(
            ["lsof", "-t", f"-iTCP:{port_number}", "-sTCP:LISTEN"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if proc.returncode not in (0, 1):
            continue
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                discovered_pids.add(int(line))
    for pid in sorted(discovered_pids):
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


def wait_for_addr(addr: str, timeout_seconds: float = 10.0) -> bool:
    host, raw_port = addr.rsplit(":", 1)
    port = int(raw_port)
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def start_process_and_wait(cmd: list[str], log_path: Path, addr: str) -> subprocess.Popen[Any]:
    proc = start_process(cmd, log_path)
    if not wait_for_addr(addr, timeout_seconds=12.0):
        best_effort_stop_pid(proc.pid)
        raise RuntimeError(f"Timed out waiting for {addr}")
    return proc


def load_or_default(host: str) -> ClusterState:
    cluster_data = load_cluster_if_present()
    if cluster_data is not None:
        return cluster_data
    return default_cluster(host=host)


def reset_cluster_state(host: str) -> ClusterState:
    existing = load_cluster_if_present()
    if existing is not None:
        best_effort_stop_listening_ports(cluster_ports(existing))
        best_effort_stop_pid(existing.gateway.pid)
        for node_entry in existing.shard_nodes:
            best_effort_stop_pid(node_entry.pid)
    cluster_data = default_cluster(host=host)
    best_effort_stop_listening_ports(cluster_ports(cluster_data))
    return cluster_data


__all__ = [
    "ROOT",
    "RUNTIME_DIR",
    "CLUSTER_JSON",
    "best_effort_stop_listening_ports",
    "best_effort_stop_pid",
    "cluster_ports",
    "default_cluster",
    "load_or_default",
    "load_cluster_if_present",
    "reset_cluster_state",
    "start_process_and_wait",
    "write_cluster",
]
