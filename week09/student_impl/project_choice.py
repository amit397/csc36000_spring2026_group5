from enum import Enum


class ApplicationChoice(str, Enum):
    """
    Choose exactly one provided application.
    """

    COURSE_REGISTRATION = "course_registration"
    WALLET = "wallet"
    INVENTORY = "inventory"


class ShardingTradeoff(str, Enum):
    """
    Declare the main sharding tradeoff your design is aiming for.

    HASH_DISTRIBUTED
      Use this when your design is trying to spread keys fairly evenly
      across logical shards. This is a natural fit for hash-based
      partitioning or another scheme whose main goal is load distribution.

      What the tests check:
      - The sharding tests generate a representative synthetic key set
        and verify that choose_logical_shard(...) spreads those keys
        relatively evenly across the available logical shards.

      What the tests do NOT prove:
      - That your partition key is the best possible choice for the real
        application workload.
      - That your design is free of all hot-key or skew problems.

    RANGE_LOCALITY
      Use this when your design is trying to keep nearby or ordered keys
      together, for example to support range-oriented access patterns.

      What the tests check:
      - The sharding tests generate an ordered key sequence and verify
        that choose_logical_shard(...) maps those keys in a monotonic,
        range-like way rather than scattering them randomly.

      What the tests do NOT prove:
      - That your range boundaries are optimal.
      - That the resulting data distribution is ideal for all workloads.
    """

    HASH_DISTRIBUTED = "hash_distributed"
    RANGE_LOCALITY = "range_locality"


class IsolationTradeoff(str, Enum):
    """
    Declare the isolation-style tradeoff your implementation is trying to
    approximate.

    READ_COMMITTED_LIKE
      Use this when your design aims to prevent obviously inconsistent
      partial updates, but you are not claiming a serializable execution
      model for all concurrent transactions.

      Typical interpretation:
      - Readers should not observe half-applied writes from another
        transaction.
      - Basic application invariants should still hold for the selected
        workload.
      - Some higher-order concurrency anomalies may still be possible.

      What the tests check:
      - The application tests check representative correctness properties
        such as no overbooking, no partial transfer effects, and no
        inconsistent visible state after failed operations.

      What the tests do NOT prove:
      - A formal database-theory definition of read committed.
      - The absence of every concurrency anomaly.

    SERIALIZABLE_LIKE
      Use this when your design aims to make concurrent execution behave
      like some serial order, at least for the important operations in the
      selected application.

      Typical interpretation:
      - Concurrent operations should preserve invariants as if they ran
        one at a time.
      - Check-then-act races should be prevented for the selected
        workload.
      - Critical operations should not expose inconsistent intermediate
        states.

      What the tests check:
      - The application tests exercise representative conflict cases such
        as last-seat enrollment, over-allocation attempts, and transfer
        invariants.
      - Those tests are intended to catch common isolation failures in the
        selected workload.

      What the tests do NOT prove:
      - A full formal proof of serializability.
      - Correct behavior for every possible schedule or adversarial race.
    """

    READ_COMMITTED_LIKE = "read_committed_like"
    SERIALIZABLE_LIKE = "serializable_like"


# ---------------------------------------------------------------------------
# Student selections
# ---------------------------------------------------------------------------
#
# Update the three values below to match your team's implementation.
#
# The test suite reads these declarations and uses them in two ways:
#
# 1. It decides which application-specific tests should run.
# 2. It checks whether the observable behavior of your implementation is
#    broadly consistent with the declared sharding and isolation tradeoffs.
#
# These declarations are part of the grading contract. Keep them honest:
# the tests compare them against behavior, and your student_impl/README.md
# should explain why the declarations are appropriate.

SELECTED_APPLICATION = ApplicationChoice.INVENTORY
SELECTED_SHARDING_TRADEOFF = ShardingTradeoff.HASH_DISTRIBUTED
SELECTED_ISOLATION_TRADEOFF = IsolationTradeoff.SERIALIZABLE_LIKE
