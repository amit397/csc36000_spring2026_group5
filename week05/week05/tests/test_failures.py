import uuid
import time
from conftest import load_cluster, get_replica_statuses, wait_for_leader, stop_replica, start_replica, ordered_users

def _send_some(gw, pb, alice, bob, n=5, prefix="x"):
    for i in range(n):
        gw.SendDirect(
            pb.SendDirectRequest(
                from_user=alice,
                to_user=bob,
                client_id="fail-test",
                client_msg_id=f"{prefix}-{i}-{uuid.uuid4().hex}",
                text=f"{prefix}-{i}",
            ),
            timeout=5.0,
        )

def test_follower_failure_and_recovery(cluster, gateway_stub):
    direct_pb2, gw = gateway_stub
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    st = get_replica_statuses(replica_addrs)
    follower_ids = [s.id for (_, s) in st if s.role != 2]
    victim = follower_ids[0]

    stop_replica(victim)

    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"
    _send_some(gw, direct_pb2, alice, bob, n=8, prefix="fdown")

    start_replica(victim)

    def caught_up():
        st2 = get_replica_statuses(replica_addrs)
        leader2 = [(a, s) for (a, s) in st2 if s.role == 2]
        if not leader2:
            return False
        _, ls = leader2[0]
        victim_s = [(a, s) for (a, s) in st2 if s.id == victim]
        if not victim_s:
            return False
        _, vs = victim_s[0]
        return vs.commit_index == ls.commit_index

    for _ in range(80):
        if caught_up():
            break
        time.sleep(0.25)

    assert caught_up(), "Recovered follower did not catch up to leader commit_index."

    user_a, user_b = ordered_users(alice, bob)
    victim_addr = [r["addr"] for r in data["replicas"] if r["id"] == victim][0]

    resp = gw.GetConversationHistory(
        direct_pb2.GetConversationHistoryRequest(
            user_a=user_a,
            user_b=user_b,
            after_seq=0,
            limit=100,
            read_pref=direct_pb2.REPLICA_HINT,
            replica_hint=victim_addr,
        ),
        timeout=5.0,
    )
    assert victim_addr in resp.served_by, "Read must include the hinted recovered replica in served_by."
    assert len(resp.events) >= 8, "Recovered follower should serve conversation history after catch-up."

def test_leader_failure(cluster, gateway_stub):
    direct_pb2, gw = gateway_stub
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    st = get_replica_statuses(replica_addrs)
    _, leader_status = [(a, s) for (a, s) in st if s.role == 2][0]
    leader_id = leader_status.id

    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"
    _send_some(gw, direct_pb2, alice, bob, n=5, prefix="before-leader-crash")

    stop_replica(leader_id)
    wait_for_leader(replica_addrs, timeout=25.0)

    _send_some(gw, direct_pb2, alice, bob, n=5, prefix="after-leader-crash")

    user_a, user_b = ordered_users(alice, bob)
    resp = gw.GetConversationHistory(
        direct_pb2.GetConversationHistoryRequest(
            user_a=user_a,
            user_b=user_b,
            after_seq=0,
            limit=200,
            read_pref=direct_pb2.LEADER_ONLY,
            replica_hint="",
        ),
        timeout=5.0,
    )

    texts = [e.text for e in resp.events if e.from_user == alice]
    expected = [f"before-leader-crash-{i}" for i in range(5)] + [f"after-leader-crash-{i}" for i in range(5)]
    assert texts == expected, "System must continue making progress after leader failure, preserving order."

def test_candidate_failure(cluster, gateway_stub):
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    st = get_replica_statuses(replica_addrs)
    _, leader_status = [(a, s) for (a, s) in st if s.role == 2][0]
    leader_id = leader_status.id

    stop_replica(leader_id)

    candidate_id = None
    deadline = time.time() + 10.0
    while time.time() < deadline and candidate_id is None:
        st2 = get_replica_statuses(replica_addrs)
        candidates = [s.id for (_, s) in st2 if s.role == 1]
        if candidates:
            candidate_id = candidates[0]
            break
        time.sleep(0.1)

    assert candidate_id is not None, "No candidate observed during election; cannot validate candidate-failure handling."
    stop_replica(candidate_id)

    _, new_leader_status = wait_for_leader(replica_addrs, timeout=30.0)
    assert new_leader_status.id not in (leader_id, candidate_id), "Leader must be elected despite candidate crash."
