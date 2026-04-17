from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def load_logical_shard_state(storage_path: Path) -> dict[str, Any]:
    """
    Load the durable state for a logical shard from disk.
    """
    if not storage_path.exists():
        return {}
    raw_text = storage_path.read_text()
    if not raw_text.strip():
        return {}
    loaded = json.loads(raw_text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected top-level JSON object in {storage_path}")
    return loaded


def save_logical_shard_state(storage_path: Path, state: dict[str, Any]) -> None:
    """
    Persist the logical shard state to disk.
    """
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = storage_path.with_suffix(f"{storage_path.suffix}.tmp")
    payload = json.dumps(state, indent=2, sort_keys=True)
    with temporary_path.open("w") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_path, storage_path)


def export_logical_shard_state(state: dict[str, Any]) -> dict[str, Any]:
    """
    Return a JSON-serializable dump used during rebalancing.
    """
    # Round-trip through JSON to ensure the exported form is serializable
    # and detached from any in-memory references.
    return json.loads(json.dumps(state))


def import_logical_shard_state(dump_data: dict[str, Any]) -> dict[str, Any]:
    """
    Build in-memory state from a JSON-serializable dump.
    """
    if not isinstance(dump_data, dict):
        raise ValueError("Logical shard dump must be a JSON object")
    return json.loads(json.dumps(dump_data))


def count_records(state: dict[str, Any]) -> int:
    """
    Return an approximate record count for balancing and status reporting.
    """
    if not state:
        return 0

    if "accounts" in state and isinstance(state["accounts"], dict):
        return len(state["accounts"])

    if "sections" in state and isinstance(state["sections"], dict):
        return len(state["sections"])

    if "student_sections" in state and isinstance(state["student_sections"], dict):
        return sum(len(value) for value in state["student_sections"].values() if isinstance(value, list))

    if "inventory" in state and isinstance(state["inventory"], dict):
        return len(state["inventory"])

    if "reservations" in state and isinstance(state["reservations"], dict):
        return len(state["reservations"])

    total = 0
    for value in state.values():
        if isinstance(value, dict):
            total += len(value)
        elif isinstance(value, list):
            total += len(value)
        else:
            total += 1
    return total
