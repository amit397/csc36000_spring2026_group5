from __future__ import annotations

from pathlib import Path


def test_required_artifacts_exist(selected_application):
    root = Path(__file__).resolve().parents[1]
    required = [
        root / "gateway_server.py",
        root / "shard_server.py",
        root / "scripts" / "run_cluster.py",
        root / "scripts" / "stop_cluster.py",
        root / "scripts" / "start_shard.py",
        root / "scripts" / "stop_shard.py",
        root / "student_impl" / "project_choice.py",
        root / "student_impl" / "sharding.py",
        root / "student_impl" / "storage.py",
        root / "student_impl" / "transactions.py",
        root / "student_impl" / "README.md",
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, "Missing required starter artifacts:\n" + "\n".join(missing)
