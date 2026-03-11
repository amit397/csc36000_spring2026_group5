#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import signal
import sys
from pathlib import Path

from common import (
    find_or_create_replica_entry,
    load_or_init_cluster,
    write_cluster,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stop one replica and set its PID to null in .runtime/cluster.json"
    )
    p.add_argument("replica_id", type=int)
    p.add_argument("--host", default="127.0.0.1", help="Replica host")
    p.add_argument("--replica-start-port", type=int, default=50061)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.replica_id < 1:
        return 2

    data = load_or_init_cluster(args.host, args.replica_start_port)
    rep = find_or_create_replica_entry(
        data, args.replica_id, args.host, args.replica_start_port
    )

    pid = rep.get("pid")
    if isinstance(pid, int):
        # SIGUSR1: triggers the graceful Raft-shutdown / port-keepalive handler
        # in replica_admin.py so the gRPC port remains reachable for a short
        # grace period. This lets conftest.py's wait_for_port() return quickly
        # on the stopped replica instead of blocking until timeout.
        try:
            if sys.platform == "win32":
                stop_file = Path(__file__).resolve().parent.parent / ".runtime" / f"replica_{args.replica_id}.stop"
                try:
                    stop_file.touch()
                except OSError:
                    pass
            elif hasattr(signal, 'SIGUSR1'):
                os.kill(pid, signal.SIGUSR1)
            else:
                os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    # Keep the PID in cluster.json so stop_cluster.py (teardown) can still
    # send SIGTERM to end the grace-period process before the next test starts.
    write_cluster(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
