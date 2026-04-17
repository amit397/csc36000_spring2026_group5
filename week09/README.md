# Week 09 Group Project: Disk-Backed Sharding + Transactions

## Summary
In week 05, your team built a distributed direct-messaging service. In this project, you will focus on two different system design problems: **sharding** and **transactions**.

Your job is to design and implement:

1. **A sharding layer** that partitions data across multiple shards
2. **A transaction layer** whose main goals are atomicity and isolation
3. **Correct use of the provided disk-backed storage layer** so data survives process crashes and restarts
4. **Support for one selected application** with the guarantees that application requires

This project is intentionally open-ended in the same way real distributed systems work is open-ended: there is no single correct sharding or transaction design. Your group must choose an approach that satisfies the behavioral guarantees required by the provided application.

---

## Team size
Groups of **3-4 students**.

---

## Learning goals
By the end of this project, your group should be able to:

- Explain the tradeoffs between different sharding strategies
- Decide when single-shard transactions are enough and when cross-shard coordination is necessary
- Implement transaction execution over durable on-disk state
- Explain atomicity as "all-or-nothing" behavior under failures
- Explain isolation as protection from incorrect interference between concurrent operations
- Match system design choices to application-level guarantees

This project builds directly on:

- Chapter 7 on sharding / partitioning
- Chapter 8 on transactions

For this project, you should especially use the ideas from the readings that:

- sharding requires choosing a good partition key
- nearby keys, hot keys, and skew can make a sharding design perform badly
- key-range sharding and hash-based sharding have different strengths and weaknesses
- transactions help applications deal with partial failure and concurrency bugs
- atomicity and isolation matter most when one logical operation touches multiple objects

---

## Big picture
You should think of the system in two layers:

1. **Your shard management and transaction layer**
   This is the main work of the project. Your code decides where data lives, how requests are routed, how multi-key operations execute, and what commit protocol is used.

2. **Provided application APIs**
   We will provide the application code and client workloads that issue reads and writes through your system. Different application options require different guarantees, and your implementation must preserve the guarantees of the application your group selects.

You are not expected to build a full production database. You are expected to build a clean, well-reasoned prototype of a distributed storage system with durable storage, sharding, and transactions.

---

## What is provided
The week 09 starter code will provide:

- Base process-launch scripts
- The RPC contracts that your code must satisfy
- Application clients that issue realistic workloads
- Blank implementation files where your group will add its code
- A black-box automated test suite

The provided blank implementation files are the files the tests will use during grading. Put your implementation in those files and keep their filenames, module layout, and required entry points unchanged.

The main student implementation files in the starter are:

- `student_impl/project_choice.py`
- `student_impl/sharding.py`
- `student_impl/storage.py` (provided storage implementation used by all projects)
- `student_impl/transactions.py`
- `student_impl/README.md`

In `student_impl/project_choice.py`, your team must also declare:

- the selected application
- the selected sharding tradeoff
- the selected isolation tradeoff

The tests will use those declarations and compare them against observable behavior in your implementation.

---

## What you must implement
Your group must implement the following major components.

### Main transaction focus
The main transaction focus of this project is **atomicity** and **isolation**.

For this assignment:

- **Atomicity** means a multi-step operation either happens completely or does not happen at all
- **Isolation** means concurrently running operations should not observe or produce incorrect intermediate states

You should think about transactions as a tool for hiding two kinds of problems from the application:

- failures in the middle of a multi-step update
- interference between concurrent operations

This follows the Chapter 8 perspective closely: transactions are valuable because they simplify the application model when faults happen in the middle of a sequence of writes and when concurrent clients might otherwise interfere with one another.

Durability still matters, but it is a supporting concern. The central question for your design should be:

"How does this system guarantee atomicity and isolation for the chosen application?"

### 1. Shard placement and routing
You must decide how objects are mapped to shards.

Possible approaches include:

- Hash-based partitioning
- Range partitioning
- Directory-based partitioning
- Hybrid approaches

Your implementation must:

- Choose a clear partition key for the chosen application
- Determine which shard owns a key or logical record
- Route client operations to the correct shard or shards
- Support lookups efficiently enough for the provided workloads
- Make the shard mapping visible to your own routing layer
- Use a sharding strategy that distributes keys relatively evenly across the available logical shards

Your design should explicitly consider the Chapter 7 tradeoffs:

- **Key-range sharding** can be good for range queries and ordered access patterns, but it can create hot shards if many writes land in nearby keys
- **Hash-based sharding** can spread load more evenly, but it can make range queries and grouping related data more difficult
- A poor partition key can create **skew**, **hot shards**, or **hot keys**
- If your design uses hashing, avoid a naive `hash(key) % current_number_of_nodes` style explanation; instead, think in terms of a fixed shard space or another mapping that does not require remapping almost everything when the layout changes

The tests will evaluate whether your shard mapping produces a relatively even distribution for a representative set of keys. In other words, students are responsible for choosing a good sharding strategy, not for implementing a separate shard-rebalancing subsystem.

### 2. Durable local storage
Week 05 used in-memory state only. This project requires persistence.

The storage layer for week 09 is provided for you in `student_impl/storage.py`. Your group does **not** need to build a storage engine or persistence format from scratch.

Your system must still use that storage layer correctly so that:

- Process crashes do not erase committed data
- Restarted nodes can reload prior state
- Transaction recovery is possible after crashes

Your submission should document how your sharding and transaction logic interacts with the provided storage layer during normal operation and after restart.

### 3. Transaction execution
You must support transactional operations over the shard layout you choose.

At minimum, your design must clearly define:

- What counts as a transaction
- How atomicity is achieved
- How isolation is achieved
- Whether reads are transactional or non-transactional
- How commit/abort works
- How failures are handled during commit

Some provided applications may be satisfiable with single-shard transactions only. Others may require cross-shard coordination. Your system should choose the simplest correct strategy that still meets the guarantees of the application your group selects.

You should pay particular attention to anomalies caused by concurrency. Your design should prevent incorrect outcomes such as:

- partially applied multi-step updates
- lost updates
- over-allocation of a limited resource
- concurrent operations that both appear valid alone but violate an invariant together

The most important cases in this project are **multi-object operations**. Following Chapter 8, your design should make it possible for one logical action to update all related records together, or none of them at all, and prevent other operations from observing an inconsistent halfway point.

### 4. Failure handling and recovery
Your system must handle:

- Process crashes and restarts
- Coordinator crashes during transaction execution
- Partial progress during multi-step transaction protocols

After restart, the system should recover durable state and continue serving requests consistently.

---

## Required design decision: choose the right algorithm for the workload
This project is not just an implementation exercise. It is also a design exercise.

Your group must choose:

- A sharding strategy appropriate for the data model and workloads
- A transaction protocol appropriate for the atomicity and isolation requirements of the provided application
- A plan for how the transaction layer uses the provided persistent storage safely during commit and recovery

Examples of transaction approaches that may be reasonable depending on the workload:

- Per-shard serial execution
- Optimistic concurrency control
- Pessimistic locking
- Two-phase commit
- Write-ahead logging with recovery records
- Saga-like compensation only if the assignment explicitly allows weaker semantics for a specific workflow

If you choose a weaker algorithm, you must be able to justify that it still satisfies the stated guarantees of the provided application.

Examples of sharding approaches that may be reasonable depending on the workload:

- Key-range sharding when ordered access or range scans are important
- Hash-based sharding when even distribution of load is more important than range locality
- Tenant-style sharding when one tenant or one top-level entity naturally groups related data together
- A fixed number of logical shards mapped by configuration, so routing is stable even if deployment details change

Your team must declare the sharding tradeoff it chose in `student_impl/project_choice.py`. The tests will then validate that declaration in a limited but concrete way. For example:

- if you declare a hash-distributed strategy, the tests will check that a representative key set is spread relatively evenly across logical shards
- if you declare a range-locality strategy, the tests will check that ordered keys map in a monotonic range-like way

Likewise, your team must declare the isolation tradeoff it chose. The tests will compare that declaration to the kinds of anomalies your implementation does or does not allow in the provided workloads. These checks are necessarily limited black-box checks, so they can confirm some representative behaviors but not formally prove an isolation level.

---

## Applications available for this project
The week 09 starter will include several fully implemented application options. These applications are meant to force design tradeoffs rather than reward a one-size-fits-all implementation.

Each group must choose **one** provided application and build a system that correctly supports that application's guarantees.

Your group is **not** required to support all applications in a single implementation.

The available application options will include workloads such as the following.

### Application A: course registration
This application models student enrollment in course sections.
Authoritative application details are in:

- `apps/course_registration_client.py`

Read that file for:

- field meanings such as `student_id`, `section_id`, and `capacity`
- the provided operations and their arguments
- the intended invariants for the application
- the behaviors the tests will exercise

### Application B: wallet / transfer workload
This application models transfers between two accounts.
Authoritative application details are in:

- `apps/wallet_client.py`

Read that file for:

- field meanings such as `account_id`, `initial_balance_cents`, and `amount_cents`
- the provided operations and their arguments
- the intended invariants for the application
- the behaviors the tests will exercise

### Application C: reservation / inventory workload
This application models inventory decrement, reservation, or booking.
Authoritative application details are in:

- `apps/inventory_client.py`

Read that file for:

- field meanings such as `item_id`, `reservation_id`, and `quantity`
- the provided operations and their arguments
- the intended invariants for the application
- the behaviors the tests will exercise

Your group is responsible only for the guarantees of the application you choose.

---

## Expected guarantees
Your implementation must clearly document the guarantees it provides for the application your group chooses.

At minimum, your `student_impl/README.md` for the final submission must state:

- The partition key or sharding rule for the chosen application's data model
- Why that partition key is a reasonable choice for the workload
- How your shard mapping aims to keep keys relatively evenly distributed
- Which sharding tradeoff was declared in `student_impl/project_choice.py`
- Whether transactions are single-shard only or may span shards
- Which isolation tradeoff was declared in `student_impl/project_choice.py`
- The isolation level your system approximates
- Which anomalies your design is intended to prevent
- How recovery works after crash when your system reloads durable state from the provided storage layer

You do not need to implement every database isolation level. You do need to implement enough correctness to satisfy the provided application your group chooses.

---

## Failure model
Assume the following:

- Nodes may crash and later restart
- Messages may be delayed
- Client retries may occur because of timeouts
- Storage on a single node is durable across process restarts

You may assume:

- No Byzantine faults
- No disk corruption
- No malicious clients
- One machine / localhost deployment by default

---

## Process and deployment model
Like week 05, everything should run on a single machine by default using configurable `host:port` addresses.

The provided scripts will start:

- A gateway or coordinator layer
- Multiple shards

The exact port layout and process contract will be defined by the week 09 starter code and tests. Keep your implementation compatible with those scripts and RPC contracts.

---

## Disk persistence requirements
Durability is a core part of this project, but the low-level storage code is provided.

Your design must address:

- What gets written to disk before acknowledging success
- How committed data is reconstructed on restart
- How in-flight transactions are recovered
- How per-shard metadata is stored

A correct but simple design built on the provided storage layer is better than an ambitious design that loses data or violates atomicity.

---

## Suggested architecture
A reasonable architecture might include:

- A front-end gateway or transaction coordinator
- A shard map / partition router
- One durable storage unit per shard
- A transaction manager or per-shard concurrency-control module

One simple architecture that aligns well with Chapter 7 is to define a fixed number of logical shards up front, route requests by partition key, and keep the durability and concurrency-control logic local to each shard whenever possible.

This is only a suggestion. You may organize the system differently if the public API and tests still pass.

---

## Deliverables
Your team must submit working code in `week09/` that includes:

1. The distributed storage implementation in the provided blank implementation files
2. Updated scripts needed to run the system
3. Any required generated RPC code if the starter expects it in-repo
4. A `student_impl/README.md` for the chosen application
5. A `CONTRIBUTIONS.md` file describing each team member's role
6. An in-class presentation explaining the team's design decisions

Your `student_impl/README.md` must explain:

- Which application the team chose
- Sharding choice
- Transaction protocol choice
- How the transaction logic uses the provided storage layer
- Recovery behavior
- Known limitations

## In-class presentation
Each group must give an in-class presentation about the system they built.

The presentation should explain:

- Which provided application the group chose
- The partition key, sharding strategy, and why they were selected
- The transaction protocol and why it provides the required atomicity and isolation
- How the design uses the provided storage layer during commit and restart recovery
- Important tradeoffs, limitations, and lessons learned

All group members should be prepared to answer questions about the design and implementation choices.

---

## Automated grading
Your submission will be evaluated primarily with automated tests for the application your group chooses.

After implementing your solution, you should run the tests locally before submitting.

Recommended workflow:

1. Set `SELECTED_APPLICATION` in `student_impl/project_choice.py` to your chosen application.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Run the tests for your application:

```bash
python -m pytest -q --application course_registration
```

Replace `course_registration` with `wallet` or `inventory` if your group chose one of those applications.

The `--application` flag must match the value in `student_impl/project_choice.py`.

The tests will check behavior such as:

- Correct routing to shards
- That the sharding function spreads a representative set of keys relatively evenly across logical shards
- Durable persistence across restart
- Atomicity of transactional operations where required by the chosen application
- Isolation under concurrent requests, especially around shared invariants
- Preservation of the chosen application's invariants
- Recovery from crashes during or around commits
- Correct behavior under concurrent clients
- Continued correctness when some shards are busy or when multi-step operations must be retried safely

The tests are black-box tests. They will interact only through the published scripts and RPC interfaces.

The tests will also expect your code to live in the provided implementation files. Do not rename those files or move the required entry points.

Passing tests is necessary but not sufficient for full credit if your documented design contradicts your implementation or if important required guarantees are missing.

---

## Manual testing
Both students and the professor should be able to run the system manually from the terminal.

Recommended manual workflow:

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Start the cluster:

```bash
python scripts/run_cluster.py
```

3. Inspect the current cluster layout if needed:

```bash
python apps/admin_client.py state
```

4. Run a representative manual smoke test for one application:

```bash
python scripts/manual_test.py --application course_registration
python scripts/manual_test.py --application wallet
python scripts/manual_test.py --application inventory
```

5. Run individual application operations by hand if you want to explore behavior in more detail:

```bash
python apps/course_registration_client.py create-section CSC36000-01 2
python apps/course_registration_client.py enroll student-1 CSC36000-01
python apps/course_registration_client.py schedule student-1

python apps/wallet_client.py create alice 10000
python apps/wallet_client.py create bob 2500
python apps/wallet_client.py transfer alice bob 1250

python apps/inventory_client.py create-item item-1 5
python apps/inventory_client.py reserve item-1 res-1 2
python apps/inventory_client.py get item-1
```

6. Stop the cluster when finished:

```bash
python scripts/stop_cluster.py
```

The manual smoke test is not a replacement for the full pytest suite. It is a quick way to verify that a submission can start, serve requests, and handle a representative happy path and unhappy path for a chosen application.

---

## Evaluation rubric
Your grade will be based on the following dimensions.

### Correctness

- The chosen application's guarantees are preserved
- Transactions preserve atomicity and isolation
- The system uses the provided storage layer correctly so committed state survives restart

### Design quality

- Sharding choice is well justified
- Sharding strategy keeps load reasonably well distributed for the expected workload
- Transaction protocol matches the workload's atomicity and isolation needs
- Failure handling is coherent
- Tradeoffs are clearly explained

### Code quality

- Clear structure
- Reasonable modularity
- Good naming and documentation
- Reproducible startup and testing workflow

### Team collaboration

- All members contributed meaningfully
- Git history reflects collaborative development
- The group can clearly explain and justify its design decisions during the in-class presentation

---

## Implementation advice
Start simple and layer functionality.

Recommended order:

1. Get one shard working on top of the provided storage layer
2. Add transaction execution on one shard
3. Add multiple shards with routing
4. Support one provided transactional application correctly
5. Extend to cross-shard coordination if needed
6. Harden crash recovery behavior

Do not start with the most complicated transaction protocol unless your workload truly requires it.

## Suggested team task breakdown
The exact division of labor is up to each group, but the project is designed so it can be split into a few major workstreams.

Every group member should still understand the full system, but assigning primary ownership helps teams move faster.

### For a group of 3

**Member 1: shard routing and gateway/coordinator layer**

- Own the request-routing path from the provided application into the storage system
- Implement shard lookup, shard mapping, and request forwarding
- Define the partition key and how keys or records map to shards for the chosen application
- Check that the chosen shard mapping spreads keys reasonably well
- Help with integration testing across all components

**Member 2: transaction and concurrency-control layer**

- Own transaction execution logic
- Implement commit and abort handling
- Implement concurrency-control decisions such as locking, validation, or per-shard serialization
- Focus on preserving atomicity and isolation under concurrent requests

**Member 3: recovery, invariants, and integration**

- Own restart and recovery behavior above the provided storage layer
- Validate that committed state reloads correctly after restart
- Check that transaction metadata and application invariants remain consistent after failures
- Build targeted integration tests around crash and restart behavior

### For a group of 4

**Member 1: shard routing and placement**

- Own shard mapping and request routing
- Define the partitioning strategy and partition key for the chosen application
- Implement routing logic and shard-aware request handling
- Check that the chosen shard mapping spreads keys reasonably well

**Member 2: transaction coordinator / commit protocol**

- Own transaction orchestration
- Implement commit/abort logic and any cross-shard coordination
- Define how failures during commit are detected and recovered

**Member 3: recovery and failure handling**

- Own restart behavior and failure-handling logic above the provided storage layer
- Verify that state survives restart correctly
- Help define how in-flight transactions are completed, retried, or cleaned up after crash

**Member 4: testing, invariants, and integration**

- Own integration testing and system validation
- Build end-to-end scenarios for concurrency, retries, crashes, and recovery
- Check that the chosen application's invariants always hold and that concurrent executions do not violate isolation
- Test that the sharding function distributes keys reasonably evenly and that routing matches the chosen shard mapping
- Help the team prepare demo material and presentation evidence

### Shared responsibilities for all groups
Even when work is divided, all members should contribute to:

- Choosing the application
- Choosing the sharding strategy
- Choosing the transaction protocol
- Reviewing pull requests and testing integration points
- Preparing the in-class presentation

Teams may choose a different breakdown, but the final `CONTRIBUTIONS.md` should clearly state who owned which parts of the system.

---

## Common mistakes to avoid

- Acknowledging writes before the durable record is safely stored
- Assuming all application operations fit in one shard without checking
- Implementing cross-shard logic without a recovery story
- Forgetting to define what happens if a coordinator crashes mid-transaction
- Choosing a sharding key that makes critical operations unnecessarily multi-shard
- Ignoring skew, hot keys, or hot shards when choosing the partition key

---

## Minimum expectations for a strong submission
A strong submission should:

- Recover correctly after restart
- Support at least one sensible sharding strategy
- Use a sharding strategy that spreads keys reasonably evenly for the chosen workload
- Preserve invariants for the chosen application through atomicity and isolation
- Clearly explain why the chosen design is correct for that workload

---

## Relationship to week 05
Week 05 focused on one style of distributed system coordination.

Week 09 focuses on **data placement, transaction correctness, and correct use of durable storage**.

You should reuse your distributed systems thinking from week 05, but the center of gravity has shifted:

- from message routing to shard routing
- from protocol mechanics to atomicity and isolation
- from in-memory state to using durable storage safely

---

## Academic honesty
All team members must contribute to the design and implementation. Be prepared to explain:

- Why your shard key was chosen
- What Chapter 7 tradeoffs your sharding choice makes
- How your shard mapping distributes keys and what skew or hotspot risks remain
- Why your transaction algorithm provides atomicity and isolation for the chosen workload
- What happens during crash recovery
- What guarantees your system does and does not provide

Use Git commits to demonstrate participation.

---

## What will appear in this directory
The completed week 09 directory will contain:

- the application-facing RPC contracts
- run/stop scripts
- application implementations and clients
- automated tests
- blank implementation files for student code

This README is the project brief. The exact filenames and API surfaces for implementation will be defined by the starter code added to this directory, and those provided implementation files will be the ones used by the tests.
