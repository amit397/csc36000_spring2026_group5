from __future__ import annotations

from collections import Counter

import pytest

from student_impl import sharding


def test_hash_distributed_tradeoff_matches_distribution(selected_sharding_tradeoff):
    if selected_sharding_tradeoff != "hash_distributed":
        pytest.skip("distribution check only applies when SELECTED_SHARDING_TRADEOFF == 'hash_distributed'")

    total_logical_shards = sharding.TOTAL_LOGICAL_SHARDS
    sample_keys = [f"synthetic-key-{index:04d}" for index in range(1600)]
    counts = Counter(sharding.choose_logical_shard(key, total_logical_shards) for key in sample_keys)

    assert set(counts.keys()) <= set(range(total_logical_shards))
    assert len(counts) == total_logical_shards, "Every logical shard should receive at least some keys"

    min_count = min(counts.values())
    max_count = max(counts.values())
    assert max_count - min_count <= 60, (
        "Shard distribution is too uneven for the synthetic key set; "
        "choose a shard mapping that spreads keys more evenly"
    )


def test_range_locality_tradeoff_matches_monotonic_mapping(selected_sharding_tradeoff):
    if selected_sharding_tradeoff != "range_locality":
        pytest.skip("range-locality check only applies when SELECTED_SHARDING_TRADEOFF == 'range_locality'")

    total_logical_shards = sharding.TOTAL_LOGICAL_SHARDS
    ordered_keys = [f"ordered-key-{index:04d}" for index in range(200)]
    shard_ids = [sharding.choose_logical_shard(key, total_logical_shards) for key in ordered_keys]

    assert set(shard_ids) <= set(range(total_logical_shards))
    assert shard_ids == sorted(shard_ids), (
        "A range-locality declaration should map ordered keys in a monotonic way "
        "rather than scattering them randomly across shards"
    )
