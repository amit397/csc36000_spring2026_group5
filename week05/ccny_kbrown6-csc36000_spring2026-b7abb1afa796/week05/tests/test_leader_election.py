import uuid
from conftest import load_cluster, get_replica_statuses, wait_for_leader, stop_replica, start_replica

def test_leader_election_up_to_date_log_rule(cluster, gateway_stub):
    # A restarted/stale node must not be elected leader while its log is behind.
    direct_pb2, gw = gateway_stub
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    statuses = get_replica_statuses(replica_addrs)
    _, leader_status = [(a, s) for (a, s) in statuses if s.role == 2][0]
    leader_id = leader_status.id

    # Seed log
    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"
    for i in range(5):
        gw.SendDirect(
            direct_pb2.SendDirectRequest(
                from_user=alice,
                to_user=bob,
                client_id="election-test",
                client_msg_id=f"{i}-{uuid.uuid4().hex}",
                text=f"seed-{i}",
            ),
            timeout=5.0,
        )

    follower_ids = [s.id for (_, s) in statuses if s.role != 2]
    stale_id = follower_ids[0]

    stop_replica(stale_id)
    start_replica(stale_id)

    def stale_is_behind():
        st = get_replica_statuses(replica_addrs)
        leader_now = [(a, s) for (a, s) in st if s.role == 2]
        if not leader_now:
            return None
        _, ls = leader_now[0]
        stale = [(a, s) for (a, s) in st if s.id == stale_id]
        if not stale:
            return None
        _, ss = stale[0]
        return ss.last_log_index < ls.last_log_index

    # try briefly to observe it behind
    for _ in range(50):
        if stale_is_behind():
            break

    assert stale_is_behind(), "Stale replica did not appear behind the leader; election rule test would be inconclusive."

    stop_replica(leader_id)
    _, new_leader_status = wait_for_leader(replica_addrs, timeout=25.0)

    assert new_leader_status.id != stale_id, (
        "A stale replica (behind log) must not be elected leader per Raft up-to-date log voting rule."
    )
