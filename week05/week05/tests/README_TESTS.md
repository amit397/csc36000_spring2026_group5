These tests are black-box.

They assume you provide:
- scripts/run_cluster.py
- scripts/stop_cluster.py
- scripts/start_replica.py <id>
- scripts/stop_replica.py <id>
and that you implement the gRPC APIs in protos/.

Run:
  pip install -r requirements.txt
  pytest -q

See also:
- `tests/RAFT_TEST_GAPS.md` for Raft properties that are not fully testable via the current black-box API surface.
