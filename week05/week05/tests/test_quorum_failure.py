import time
import uuid

import grpc
import pytest

from conftest import get_replica_statuses, load_cluster, ordered_users, start_replica, stop_replica

EXPECTED_QUORUM_FAILURE_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.FAILED_PRECONDITION,
}


def _assert_quorum_failure_code(exc: pytest.ExceptionInfo[grpc.RpcError], op: str) -> None:
    assert exc.value.code() in EXPECTED_QUORUM_FAILURE_CODES, (
        f"Unexpected {op} failure code: {exc.value.code()}"
    )


def test_requests_fail_without_quorum(cluster, gateway_stub):
    direct_pb2, gw = gateway_stub

    data = load_cluster()
    replica_addrs = [r["addr"] for r in data["replicas"]]
    statuses = get_replica_statuses(replica_addrs)

    leader_status = [s for (_, s) in statuses if s.role == 2][0]
    follower_ids = [s.id for (_, s) in statuses if s.role != 2]

    # Bring down a majority of a 5-node cluster => no quorum.
    down_ids = [leader_status.id, follower_ids[0], follower_ids[1]]

    for rid in down_ids:
        stop_replica(rid)

    try:
        # Give election/heartbeat logic a moment to converge to "no quorum" state.
        time.sleep(1.0)

        alice = f"alice-{uuid.uuid4().hex[:8]}"
        bob = f"bob-{uuid.uuid4().hex[:8]}"
        user_a, user_b = ordered_users(alice, bob)

        with pytest.raises(grpc.RpcError) as write_exc:
            gw.SendDirect(
                direct_pb2.SendDirectRequest(
                    from_user=alice,
                    to_user=bob,
                    client_id="quorum-fail",
                    client_msg_id=f"{uuid.uuid4().hex}",
                    text="should-fail-without-quorum",
                ),
                timeout=3.0,
            )

        _assert_quorum_failure_code(write_exc, "write")

        with pytest.raises(grpc.RpcError) as read_exc:
            gw.GetConversationHistory(
                direct_pb2.GetConversationHistoryRequest(
                    user_a=user_a,
                    user_b=user_b,
                    after_seq=0,
                    limit=50,
                    read_pref=direct_pb2.LEADER_ONLY,
                    replica_hint="",
                ),
                timeout=3.0,
            )

        _assert_quorum_failure_code(read_exc, "read")
    finally:
        for rid in down_ids:
            start_replica(rid)
