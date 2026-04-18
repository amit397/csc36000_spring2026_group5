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
Transactions are single shard sicne all operations are keyed by item_id so this means that every operation hits excactly one shard.
The declared isolation tradeoff is that the transactions are serializable, so that means that the operarations behave as if they were executed in serial.

