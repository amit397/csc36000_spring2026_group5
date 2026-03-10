import time
import uuid

from conftest import get_replica_statuses, load_cluster, ordered_users, wait_until


def _current_leader(statuses):
    leaders = [(addr, s) for addr, s in statuses if s.role == 2]
    if len(leaders) != 1:
        return None
    return leaders[0]


def test_single_leader_per_term_observed(cluster):
    """At any observation point, there should be at most one leader for a term."""
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    for _ in range(60):
        statuses = get_replica_statuses(replica_addrs)
        leaders_by_term = {}
        for _, s in statuses:
            if s.role == 2:
                leaders_by_term.setdefault(int(s.term), set()).add(int(s.id))

        for term, leader_ids in leaders_by_term.items():
            assert len(leader_ids) <= 1, (
                f"Observed multiple leaders in term {term}: {sorted(leader_ids)}"
            )
        time.sleep(0.1)


def test_leader_stable_while_heartbeats_flow(cluster):
    """Without failures, leader should remain stable while heartbeats are flowing."""
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    initial = _current_leader(get_replica_statuses(replica_addrs))
    assert initial is not None, "Expected exactly one leader at test start."
    _, initial_status = initial
    initial_leader_id = int(initial_status.id)

    for _ in range(30):
        leader = _current_leader(get_replica_statuses(replica_addrs))
        assert leader is not None, "Expected exactly one leader while cluster is healthy."
        _, st = leader
        assert int(st.id) == initial_leader_id, (
            "Leader changed without an induced failure; heartbeat/election timing may be incorrect."
        )
        time.sleep(0.2)


def test_leader_commit_index_is_monotonic(cluster, gateway_stub):
    """Leader commit index should never move backwards as commands are committed."""
    direct_pb2, gw = gateway_stub
    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]

    leader = _current_leader(get_replica_statuses(replica_addrs))
    assert leader is not None
    _, leader_status = leader
    last_commit = int(leader_status.commit_index)

    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"

    for i in range(8):
        gw.SendDirect(
            direct_pb2.SendDirectRequest(
                from_user=alice,
                to_user=bob,
                client_id="commit-monotonic",
                client_msg_id=f"{i}-{uuid.uuid4().hex}",
                text=f"m-{i}",
            ),
            timeout=5.0,
        )

        def _leader_commit_progressed():
            now = _current_leader(get_replica_statuses(replica_addrs))
            if now is None:
                return False
            _, st = now
            commit = int(st.commit_index)
            return commit if commit >= last_commit else False

        current = wait_until(
            _leader_commit_progressed,
            timeout=8.0,
            interval=0.1,
            desc="leader commit index observation",
        )
        assert isinstance(current, int)
        assert current >= last_commit, "Leader commit_index moved backwards."
        last_commit = current


def test_client_retry_is_idempotent(gateway_stub):
    """Retrying the same (client_id, client_msg_id) must not create duplicates."""
    direct_pb2, gw = gateway_stub

    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"
    user_a, user_b = ordered_users(alice, bob)

    client_id = f"client-{uuid.uuid4().hex[:8]}"
    client_msg_id = f"msg-{uuid.uuid4().hex[:8]}"

    req = direct_pb2.SendDirectRequest(
        from_user=alice,
        to_user=bob,
        client_id=client_id,
        client_msg_id=client_msg_id,
        text="idempotent-payload",
    )

    r1 = gw.SendDirect(req, timeout=5.0)
    r2 = gw.SendDirect(req, timeout=5.0)
    r3 = gw.SendDirect(req, timeout=5.0)

    assert r1.seq == r2.seq == r3.seq, "Duplicate retry must map to the same committed sequence."

    history = gw.GetConversationHistory(
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

    matching = [
        e
        for e in history.events
        if e.client_id == client_id and e.client_msg_id == client_msg_id
    ]
    assert len(matching) == 1, "Retries with same idempotency key should produce exactly one event."
