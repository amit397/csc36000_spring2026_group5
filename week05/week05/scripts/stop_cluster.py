#!/usr/bin/env python3
from __future__ import annotations

from common import (
    best_effort_stop_cluster_pids,
    load_cluster_if_present,
    null_out_all_pids,
    write_cluster,
)


def main() -> int:
    data = load_cluster_if_present()
    if data is None:
        # Contract: success even if nothing is running / metadata missing.
        return 0

    best_effort_stop_cluster_pids(data)
    null_out_all_pids(data)

    try:
        write_cluster(data)
    except Exception:
        # Best effort stop must still return 0.
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
