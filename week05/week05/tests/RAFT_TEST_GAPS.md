# Raft Test Coverage Gaps (Black-Box API Limits)

This project has black-box tests that validate important behavior, but some Raft properties are not fully provable through the current script + gRPC surface alone.

## Covered Well (Current Tests)

- Leader election and re-election after failures.
- Up-to-date-log voting rule (stale candidate should lose).
- Progress through leader/follower/candidate crash scenarios.
- Follower catch-up after restart.
- Ordering and basic idempotency behavior at the gateway level.

## Not Fully Covered / Only Partially Covered

- Vote-once-per-term corner cases under many concurrent candidates.
- Exact election-timeout/randomized split-vote behavior (probabilistic/liveness detail).
- Full AppendEntries conflict resolution/backtracking correctness for arbitrary divergent logs.
- Strong validation of "leader append-only" invariant across long fault schedules.
- Commit rule nuances across terms (e.g., prior-term entries and commitment conditions).
- Formal log-matching safety invariant under adversarial message reorder/drop patterns.
- Read linearizability details (e.g., read-index style guarantees) across all read preferences.

## Why These Gaps Exist

- Current tests interact only through:
  - `scripts/*.py` lifecycle controls
  - `direct.DirectGateway` RPCs
  - `replica.ReplicaAdmin.Status`
- No direct RPC endpoints expose internal Raft messages/events (RequestVote/AppendEntries traces, per-follower nextIndex/matchIndex, vote records, rejection reasons, etc.).

## How To Close These Gaps

- Add deterministic fault-injection tests:
  - controlled message drop/reorder/delay between specific peers.
- Add observability hooks (test-only):
  - emitted events for RequestVote/AppendEntries decisions.
  - per-term voted-for and per-follower replication state.
- Add scenario tests for:
  - repeated split votes,
  - deep log divergence and repair,
  - cross-term commit edge conditions.

## Future Improvements

- We will possibly work on filling these gaps in the future when we look into observability (April 15)
