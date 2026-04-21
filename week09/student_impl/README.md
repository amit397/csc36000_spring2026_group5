# Student Implementation README

Replace this template with your team's implementation writeup.

Your writeup should include:


- the selected application
- the partition key and sharding strategy
- how the shard mapping aims to keep keys reasonably evenly distributed
- the declared sharding tradeoff from `project_choice.py`
- whether transactions are single-shard or cross-shard
- the declared isolation tradeoff from `project_choice.py`
- how atomicity is achieved
- how isolation is achieved
- which anomalies your design prevents
- how your transaction logic uses the provided storage layer
- how crash recovery works
- known limitations


`Selected application:` Application C: reservation / inventory workload

`Partition Key/Strategy:` 
We chose to implement a Hash-distributed key/partition system. Hashing allows for the keys to be reasonably distributed,
and since this application doesn't necessarily need items to be grouped, hashing is an applicable partitioning strategy. 
choose_logical_shard uses BLAKE2b with 8-byte digest. It converts digest bytes to an integer and computes:
shard_id=hash_int mod total_logical_shards
With many different item_id values, a good hash function spreads outputs pseudo-randomly, so keys are balanced across shard IDs instead of clustering by lexicographic order.
This is also deterministic: same partition key always maps to same logical shard, which is essential for routing consistency.

`Transactions:`
Transactions are single shard since all operations are keyed by item_id so this means that every operation hits excactly one shard.
The declared isolation tradeoff is that the transactions are serializable, so that means that the operarations behave as if they were executed in serial.

## Durable local storage

### Provided storage layer
All persistence goes through `student_impl/storage.py`, which we did not modify:

- `load_logical_shard_state(path)` reads the shard's JSON file and returns `{}` if the file is missing or empty.
- `save_logical_shard_state(path, state)` writes the state to a sibling `.tmp` file, calls `flush()` and `os.fsync()` on that file, then does `os.replace(tmp, path)`. The rename is atomic at the filesystem level, so a reader always sees either the previous committed file or the new one — never a torn write.
- `export_logical_shard_state` / `import_logical_shard_state` round-trip the state through JSON for shard rebalancing.
- `count_records` is used only for the admin status RPC.

Each shard node owns a directory at `.runtime/data/node_<N>/`, and every logical shard it hosts is a single file `logical_shard_<K>.json` inside that directory.

### Normal operation — how sharding and transactions use storage
Routing happens at the gateway: `sharding.build_partition_key` + `choose_logical_shard` pick a single logical shard per request, and the gateway forwards the RPC to the one node that owns that shard. From that point on, all persistence is local to one shard file.

Inside the shard node (`shard_server.py`, `StudentShardStoreAdapter`), every mutation follows the same three-step cycle under a per-shard `threading.Lock`:

1. `load_logical_shard_state(...)` reads the shard file fresh from disk.
2. `transactions.apply_local_mutation(state, ...)` mutates that in-memory dict (`create_item`, `reserve_item`, or `release_reservation`).
3. `save_logical_shard_state(...)` writes the new state (tmp file → fsync → atomic rename) and only then does the RPC return success to the gateway.

Reads use the same load-under-lock pattern so a reader never observes a half-applied write. The lock is keyed by `logical_shard_id`, so different shards on the same node still run in parallel.

Because `save_logical_shard_state` returns only after fsync + rename, nothing is acknowledged to the client until the new shard file is durable. The shard server keeps no authoritative in-memory copy between requests — disk is the source of truth on every call.

### Crash behavior — what survives a process crash
There are three crash windows per mutation, and the atomic-rename contract covers all of them:

- **Crash before save starts.** The in-memory mutation is discarded. The on-disk file is still the last committed state. The client either saw an error or timed out, so it must retry.
- **Crash during save.** Data goes to `logical_shard_<K>.json.tmp` first. If we crash before `os.replace`, the real shard file is untouched and the stale `.tmp` is ignored by the load path (which only opens `logical_shard_<K>.json`). If we crash after `os.replace`, the new state is already durable.
- **Crash after save, before ack.** The write is durable. Clients may retry; inventory operations are keyed by `reservation_id`, so a duplicate `reserve_item` surfaces as an over-allocation error rather than a silent second reservation.

### Restart recovery
There is no separate recovery phase. On restart, `run_cluster.py` relaunches each shard process with the same `--node-id`, so `node_data_dir(node_id)` resolves to the same directory that holds the pre-crash shard files. The first `Apply` or `Read` after restart simply calls `load_logical_shard_state`, which returns whatever the last atomic rename committed. Cluster layout (which node owns which logical shard) is reloaded from `.runtime/cluster.json`, so routing continues to reach the same on-disk state.

### Transaction recovery
Every inventory operation is single-shard and single-step: one load, one in-memory mutation, one atomic save. There is no multi-step commit protocol, so there is no in-flight transaction state to roll forward or back. The atomic-rename guarantee of `save_logical_shard_state` is what makes transaction recovery trivial for this application: the shard file on disk is always a consistent snapshot of some serial order of committed mutations, and that is exactly the state we reload on restart.

### Atomicity under failures

Our system guarantees atomicity despite crashes by ensuring that every mutation is applied as a single atomic state transition at the storage layer.

Each operation follows a strict pattern:

1. Load the current shard state from disk
2. Apply the mutation in memory
3. Persist the entire updated state using an atomic file replacement

Because `save_logical_shard_state` writes to a temporary file and then atomically replaces the original file using `os.replace`, the system guarantees that after any crash, the shard is in exactly one of two states:

- The full effect of the transaction is present
- None of the transaction is present

There is no possible state where a transaction is partially applied.

This property directly enforces atomicity without requiring a separate write-ahead log or undo/redo mechanism, because each transaction is a single-step transformation of the shard state.

### What gets written before ack
Only the post-mutation shard state, through `save_logical_shard_state`. No write-ahead log, no per-transaction undo/redo record — the single-shard + single-step design does not need one.

### Per-shard metadata
Cluster topology (node addresses, shard ownership) lives in `.runtime/cluster.json` and is managed by the run/stop scripts, not by our student code. Per-shard application data lives in `.runtime/data/node_<N>/logical_shard_<K>.json` and is managed exclusively through `student_impl/storage.py`.

### Known durability limitations
- State is rewritten in full on every mutation. For larger shard files this would be slow; it is acceptable here because the workload is small and simplicity is more valuable than throughput.
- Recovery assumes the same `--node-id` after restart. If a node came back with a different id, it would look at an empty directory and appear to have lost data.
- We rely on `os.fsync` and `os.replace` being honored by the underlying filesystem. The project's failure model explicitly rules out disk corruption, so this is a supported assumption.

