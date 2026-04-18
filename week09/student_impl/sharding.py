from __future__ import annotations

import hashlib
from typing import Any


TOTAL_LOGICAL_SHARDS = 8


def build_partition_key(application_name: str, operation_name: str, payload: dict[str, Any]) -> str:
    """
    Return the partition key used to route a request.

    Students should implement a partition-key choice that matches the
    selected application's workload and explain the tradeoffs in
    student_impl/README.md.
    """
    if application_name == "inventory":
        item_id = payload.get("item_id")
        if not item_id:
            raise ValueError(f"inventory operation {operation_name!r} requires item_id in payload")
        return f"inventory:item:{item_id}"

    if application_name == "wallet":
        account_id = payload.get("account_id") or payload.get("from_account_id")
        if not account_id:
            raise ValueError(f"wallet operation {operation_name!r} requires an account identifier in payload")
        return f"wallet:account:{account_id}"

    if application_name == "course_registration":
        section_id = payload.get("section_id") or payload.get("student_id")
        if not section_id:
            raise ValueError(
                f"course_registration operation {operation_name!r} requires section_id or student_id in payload"
            )
        return f"course_registration:key:{section_id}"

    raise ValueError(f"Unsupported application for partitioning: {application_name!r}")


def choose_logical_shard(partition_key: str, total_logical_shards: int = TOTAL_LOGICAL_SHARDS) -> int:
    """
    Map a partition key to a logical shard id in the range [0, total_logical_shards).

    The tests will check that your sharding function spreads a representative
    set of keys relatively evenly across the available logical shards.
    """
    if total_logical_shards <= 0:
        raise ValueError("total_logical_shards must be positive")

    digest = hashlib.blake2b(partition_key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % total_logical_shards
