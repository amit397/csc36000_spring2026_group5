import uuid
from conftest import ordered_users

def test_messages_read_in_order(gateway_stub):
    direct_pb2, gw = gateway_stub

    alice = f"alice-{uuid.uuid4().hex[:8]}"
    bob = f"bob-{uuid.uuid4().hex[:8]}"
    user_a, user_b = ordered_users(alice, bob)

    texts = [f"msg-{i}" for i in range(20)]
    for i, t in enumerate(texts):
        gw.SendDirect(
            direct_pb2.SendDirectRequest(
                from_user=alice,
                to_user=bob,
                client_id="test-client",
                client_msg_id=f"{i}-{uuid.uuid4().hex}",
                text=t,
            ),
            timeout=5.0,
        )

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

    assert resp.served_by, "Read response must include served_by with at least one replica address."

    seqs = [e.seq for e in resp.events]
    assert seqs == sorted(seqs), "Events must be returned sorted by seq."

    got_texts = [e.text for e in resp.events if e.from_user == alice]
    assert got_texts == texts, "Messages must be returned in the same order they were sent."
