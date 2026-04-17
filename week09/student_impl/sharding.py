from __future__ import annotations

from typing import Any


TOTAL_LOGICAL_SHARDS = 8


def build_partition_key(application_name: str, operation_name: str, payload: dict[str, Any]) -> str:
    """
    Return the partition key used to route a request.

    Students should implement a partition-key choice that matches the
    selected application's workload and explain the tradeoffs in
    student_impl/README.md.
    """
    raise NotImplementedError("Implement build_partition_key() in student_impl/sharding.py")


def choose_logical_shard(partition_key: str, total_logical_shards: int = TOTAL_LOGICAL_SHARDS) -> int:
    """
    Map a partition key to a logical shard id in the range [0, total_logical_shards).

    The tests will check that your sharding function spreads a representative
    set of keys relatively evenly across the available logical shards.
    """
    raise NotImplementedError("Implement choose_logical_shard() in student_impl/sharding.py")
