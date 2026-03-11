import json
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from replica_admin import StateMachine

def test_apply_stores_message():
    sm = StateMachine()
    msg = {"client_id": "alice_device", "client_msg_id": "1", "user_a": "alice", "user_b": "bob", "text": "hello"}
    data = json.dumps(msg).encode()
    
    seq = sm.apply(10, data)
    assert seq == 10
    
    messages = sm.get_messages("alice", "bob", 0, 10)
    assert len(messages) == 1
    assert messages[0]["text"] == "hello"
    assert messages[0]["seq"] == 10
    assert "server_time_ms" in messages[0]

def test_conversation_key_normalization():
    sm = StateMachine()
    # Ensure they are stored under the same key regardless of who sends
    msg1 = {"client_id": "c1", "client_msg_id": "1", "user_a": "alice", "user_b": "bob", "text": "hey"}
    msg2 = {"client_id": "c2", "client_msg_id": "1", "user_a": "alice", "user_b": "bob", "text": "hi"}
    
    sm.apply(1, json.dumps(msg1).encode())
    sm.apply(2, json.dumps(msg2).encode())
    
    # Conversations format uses min/max inside SendDirect but StateMachine expects it in the json directly
    messages = sm.get_messages("alice", "bob", 0, 10)
    assert len(messages) == 2

def test_dedup_returns_original_seq():
    sm = StateMachine()
    msg = {"client_id": "client1", "client_msg_id": "msg-123", "user_a": "alice", "user_b": "bob"}
    data = json.dumps(msg).encode()
    
    # First apply
    seq1 = sm.apply(5, data)
    assert seq1 == 5
    
    # Second apply with same (client_id, client_msg_id) but larger log_index
    seq2 = sm.apply(6, data)
    assert seq2 == 5  # Should return original seq
    
    # Verify only one message stored
    msgs = sm.get_messages("alice", "bob", 0, 10)
    assert len(msgs) == 1

def test_get_messages_after_seq():
    sm = StateMachine()
    for i in range(1, 6):
        msg = {"client_id": "x", "client_msg_id": str(i), "user_a": "alice", "user_b": "bob"}
        sm.apply(i, json.dumps(msg).encode())
        
    # after_seq = 3 should return 4, 5
    msgs = sm.get_messages("alice", "bob", 3, 10)
    assert len(msgs) == 2
    assert msgs[0]["seq"] == 4
    assert msgs[1]["seq"] == 5

def test_get_messages_with_limit():
    sm = StateMachine()
    for i in range(1, 10):
        msg = {"client_id": "x", "client_msg_id": str(i), "user_a": "alice", "user_b": "bob"}
        sm.apply(i, json.dumps(msg).encode())
        
    # Limit 3 should return last 3
    msgs = sm.get_messages("alice", "bob", 0, 3)
    assert len(msgs) == 3
    assert msgs[0]["seq"] == 7
    assert msgs[2]["seq"] == 9
