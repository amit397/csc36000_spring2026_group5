# Section B Implementation Notes

## Architecture Overview
The Section B implementation adds direct messaging semantics to the robust Raft state machine constructed in Section A. In this phase, we completed the core logic for the replica and the gateway to route point-to-point chat messages and safely deduct duplicates via Raft.

### Deduplication (At-Most-Once Semantics)
Idempotent submissions are achieved via deduplication tracking in the StateMachine:
We implemented deduplication by changing the `StateMachine.dedup_table` from a `set` to a `dict` mapping the combination of `(client_id, client_msg_id)` to the original `seq` number.
Instead of returning `-1` for duplicates as it originally did, the state machine now returns the actual `seq` number where the message was originally stored. 
Furthermore, the `ReplicaAdminServicer.SubmitCommand` was updated to look up the deduplication table *before* creating a new Raft log entry. If found, it instantly returns the cached `seq` number, saving the cost of a consensus round-trip.

### Gateway Retrieval Semantics 
To ensure reads fail correctly if a quorum cannot be reached (e.g., when the `LEADER_ONLY` read pref is used), we've enforced the strict evaluation of the `read_pref` enum in `direct_gateway.py`. If the gateway cannot reach a leader and `LEADER_ONLY` is demanded, the method correctly falls through to returning `grpc.StatusCode.UNAVAILABLE` instead of falsely returning an empty list (which would trick the client into thinking there is a correctly committed empty history array).

### Testing
- `test_state_machine.py`: Added 5 pure unit tests for `StateMachine` class isolation to verify idempotency, sequence assignments, and bounds checking.
- Test fixes: Adjusted test logic to account for edge cases in the replica process lifecycles.

### Windows Platform Compatibility
The test suite on Windows experienced severe flakiness primarily due to port binding collisions (error 10048).
To fix this, we modified `common.py::best_effort_stop_pid` to track process completion. Because `os.kill(pid, signal.SIGTERM)` translates to the asynchronous `TerminateProcess()` on Windows, the previous iteration of the test suite would tear down the old cluster and rapidly spawn the new cluster *before* Windows had actually freed up the port handles. A 3-second wait/poll loop (`os.kill(pid, 0)`) was added to guarantee ports are released before moving on. We also implemented a graceful `.stop` file fallback mechanism for `SIGUSR1` to mimic POSIX's `SIGUSR1` handling on Windows environments since `signal.SIGBREAK` wasn't thread-safe for the asyncio selector loop.
