from __future__ import annotations

import argparse
import asyncio
import json
import random
import signal
import sys
import time
from pathlib import Path

# Add generated folder to path for proto imports
sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))

import grpc
from generated import replica_admin_pb2, replica_admin_pb2_grpc
from generated import raft_internal_pb2, raft_internal_pb2_grpc

# Constants for timing
ELECTION_TIMEOUT_MIN = 0.50  # 500ms
ELECTION_TIMEOUT_MAX = 1.00  # 1000ms
HEARTBEAT_INTERVAL = 0.05    # 50ms

class StateMachine:
    def __init__(self):
        # Stores (user_a, user_b) -> list of message dicts
        self.conversations: dict[tuple[str, str], list] = {}
        # Maps (client_id, client_msg_id) -> committed seq for dedup + idempotent retries
        self.dedup_table: dict[tuple[str, str], int] = {}

    def last_log_index(self, log) -> int:
        return len(log)

    def last_log_term(self, log) -> int:
        if log:
            return log[-1]["term"]
        return 0

    def lookup_dedup(self, client_id: str, client_msg_id: str):
        """Check if a (client_id, client_msg_id) was already applied.
        Returns the original committed seq if found, None otherwise.
        Used by SubmitCommand to short-circuit before appending to the Raft log."""
        return self.dedup_table.get((client_id, client_msg_id))
    
    def apply(self, log_index: int, data: bytes) -> int:
        try:
            msg = json.loads(data.decode())
        except Exception:
            return -1
        
        dedup_key = (msg.get("client_id", ""), msg.get("client_msg_id", ""))
        if dedup_key in self.dedup_table:
            # Already applied — return original seq (idempotent)
            return self.dedup_table[dedup_key]
        
        msg["seq"] = log_index
        msg["server_time_ms"] = int(time.time() * 1000)
        
        key = (msg["user_a"], msg["user_b"])
        if key not in self.conversations:
            self.conversations[key] = []
        self.conversations[key].append(msg)
        self.dedup_table[dedup_key] = log_index
        return log_index
    
    def get_messages(self, user_a: str, user_b: str, after_seq: int, limit: int) -> list:
        key = (user_a, user_b)
        if key not in self.conversations:
            return []
        messages = [m for m in self.conversations[key] if m["seq"] > after_seq]
        if limit > 0:
            messages = messages[-limit:]
        return messages

class ReplicaAdminServicer(replica_admin_pb2_grpc.ReplicaAdminServicer, raft_internal_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id: int, host: str, port: int, peer_addrs: list[str]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self.peer_addrs = peer_addrs
        self.role = replica_admin_pb2.FOLLOWER
        self.term = 0
        self.voted_for = None
        self.leader_hint = ""
        self.commit_index = 0
        self.last_applied = 0
        self.log = []
        self._election_task = None
        self._heartbeat_task = None
        self._stopped = False  # set True by SIGUSR1 to freeze Raft participation
        
        # Leader state
        self.next_index = {}
        self.match_index = {}
        self.state_machine = StateMachine()

        # Persistent channels to prevent "infinite" connection churn
        self.channels = [grpc.aio.insecure_channel(addr) for addr in peer_addrs]
        self.stubs = {addr: raft_internal_pb2_grpc.RaftNodeStub(chan) for addr, chan in zip(peer_addrs, self.channels)}

    def _random_election_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def reset_election_timer(self):
        """Reset the election timeout. When this timer expires without receiving heartbeats, start election."""
        if self._stopped:
            return  # do not restart election timer after graceful shutdown
        if self._election_task is not None:
            self._election_task.cancel()
        self._election_task = asyncio.create_task(self._election_timeout())

    async def _election_timeout(self):
        try:
            await asyncio.sleep(self._random_election_timeout())
            await self._start_election()
        except asyncio.CancelledError:
            pass

    async def _start_election(self):
        """Start a leader election for a new term."""
        self.term += 1
        self.role = replica_admin_pb2.CANDIDATE
        self.voted_for = self.node_id
        votes = 1
        print(f"Node {self.node_id}: Starting election for term {self.term}", file=sys.stderr)
        # Brief pause so external observers (tests/monitoring) can detect the CANDIDATE state
        # before we immediately resolve the election.
        await asyncio.sleep(0.15)

        last_idx = self.state_machine.last_log_index(self.log)
        last_term = self.state_machine.last_log_term(self.log)

        # Request votes concurrently
        tasks = [
            self.stubs[p].RequestVote(raft_internal_pb2.RequestVoteRequest(
                term=self.term, candidate_id=self.node_id,
                last_log_index=last_idx, last_log_term=last_term,
            ), timeout=0.1) for p in self.peer_addrs
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for resp in results:
            if isinstance(resp, raft_internal_pb2.RequestVoteResponse):
                if resp.term > self.term:
                    self.term = resp.term
                    self.role = replica_admin_pb2.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
                if resp.vote_granted:
                    votes += 1

        majority = (len(self.peer_addrs) + 1) // 2 + 1
        if self.role == replica_admin_pb2.CANDIDATE and votes >= majority:
            self.role = replica_admin_pb2.LEADER
            self.leader_hint = self.addr
            print(f"Node {self.node_id}: Became LEADER for term {self.term}", file=sys.stderr)
            print(f"Votes received: {votes}/{len(self.peer_addrs) + 1}", file=sys.stderr)
            for p in self.peer_addrs:
                self.next_index[p] = len(self.log) + 1
                self.match_index[p] = 0
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        else:
            self.role = replica_admin_pb2.FOLLOWER
            self.reset_election_timer()

    async def _send_append_entries(self, peer_addr):
        ni = self.next_index.get(peer_addr, len(self.log) + 1)
        prev_idx = ni - 1
        prev_term = self.log[prev_idx - 1]["term"] if prev_idx > 0 else 0
        
        entries_data = self.log[prev_idx:]
        entries = [raft_internal_pb2.LogEntry(term=e["term"], data=e["data"]) for e in entries_data]

        try:
            # Short timeout so crashed followers don't block the leader
            resp = await self.stubs[peer_addr].AppendEntries(raft_internal_pb2.AppendEntriesRequest(
                term=self.term, leader_id=self.node_id,
                prev_log_index=prev_idx, prev_log_term=prev_term,
                entries=entries, leader_commit=self.commit_index,
            ), timeout=0.2)

            if resp.term > self.term:
                return resp

            if resp.success:
                self.next_index[peer_addr] = ni + len(entries)
                self.match_index[peer_addr] = self.next_index[peer_addr] - 1
                # Only log when actual data is replicated, not heartbeats
                if len(entries) > 0:
                    print(f"Node {self.node_id}: Replicated logs to {peer_addr}. match_index={self.match_index[peer_addr]}", file=sys.stderr)
            else:
                self.next_index[peer_addr] = max(1, ni - 1)
            return resp
        except Exception:
            return None

    def _update_commit_index(self):
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1]["term"] == self.term:
                count = 1
                for p in self.peer_addrs:
                    if self.match_index.get(p, 0) >= n:
                        count += 1
                if count >= (len(self.peer_addrs) + 1) // 2 + 1:
                    self.commit_index = n
                    break
    
    def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            idx = self.last_applied
            entry = self.log[idx]
            self.state_machine.apply(idx + 1, entry["data"])
            self.last_applied += 1

    async def _heartbeat_loop(self):
        while self.role == replica_admin_pb2.LEADER:
            tasks = [self._send_append_entries(p) for p in self.peer_addrs]
            results = await asyncio.gather(*tasks)
            
            for result in results:
                if result and result.term > self.term:
                    self.term = result.term
                    self.role = replica_admin_pb2.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
            
            self._update_commit_index()
            self._apply_committed_entries()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def Status(self, request, context):
        return replica_admin_pb2.StatusResponse(
            id=self.node_id, role=self.role, term=self.term,
            leader_hint=self.leader_hint, last_log_index=len(self.log),
            last_log_term=self.log[-1]["term"] if self.log else 0,
            commit_index=self.commit_index,
        )

    async def RequestVote(self, request, context):
        if self._stopped:
            return raft_internal_pb2.RequestVoteResponse(term=self.term, vote_granted=False)
        if request.term > self.term:
            self.term = request.term
            self.role = replica_admin_pb2.FOLLOWER
            self.voted_for = None
            self.reset_election_timer()

        vote_granted = False
        if request.term == self.term:
            if self.voted_for is None or self.voted_for == request.candidate_id:
                my_last_term = self.log[-1]["term"] if self.log else 0
                my_last_index = len(self.log)
                if (request.last_log_term > my_last_term or 
                   (request.last_log_term == my_last_term and request.last_log_index >= my_last_index)):
                    vote_granted = True
                    self.voted_for = request.candidate_id
                    self.reset_election_timer()

        return raft_internal_pb2.RequestVoteResponse(term=self.term, vote_granted=vote_granted)

    async def AppendEntries(self, request, context):
        if self._stopped:
            return raft_internal_pb2.AppendEntriesResponse(term=self.term, success=False)
        if request.term < self.term:
            return raft_internal_pb2.AppendEntriesResponse(term=self.term, success=False)

        if request.term > self.term:
            self.term = request.term
            self.voted_for = None
            
        self.role = replica_admin_pb2.FOLLOWER
        # Set leader_hint from the incoming leader's connection details
        # For now, we compute from leader_id which is node_id (1-5)
        leader_port = 50060 + request.leader_id
        self.leader_hint = f"{self.host}:{leader_port}"
        self.reset_election_timer()

        if request.prev_log_index > 0:
            if request.prev_log_index > len(self.log) or self.log[request.prev_log_index - 1]["term"] != request.prev_log_term:
                return raft_internal_pb2.AppendEntriesResponse(term=self.term, success=False)


        # Only truncate and append if there are entries (not just a heartbeat)
        if request.entries:
            self.log = self.log[:request.prev_log_index]
            for entry in request.entries:
                self.log.append({"term": entry.term, "data": entry.data})

        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))
            self._apply_committed_entries()

        return raft_internal_pb2.AppendEntriesResponse(term=self.term, success=True)

    async def SubmitCommand(self, request, context):
        if self._stopped or self.role != replica_admin_pb2.LEADER:
            return raft_internal_pb2.SubmitCommandResponse(success=False, log_index=0, leader_hint=self.leader_hint)

        # Leader-side dedup: if already committed, return the original seq immediately.
        # This avoids bloating the Raft log with duplicate entries and speeds up
        # client retries.  Also keeps the log lean for Part C recovery catch-up.
        try:
            payload = json.loads(request.data.decode())
            existing_seq = self.state_machine.lookup_dedup(
                payload.get("client_id", ""), payload.get("client_msg_id", "")
            )
            if existing_seq is not None:
                return raft_internal_pb2.SubmitCommandResponse(success=True, log_index=existing_seq)
        except Exception:
            pass  # Malformed data will be caught by apply()

        self.log.append({"term": self.term, "data": request.data})
        entry_index = len(self.log)

        for _ in range(100):
            if self.commit_index >= entry_index:
                # Retrieve the actual sequence number assigned/deduped by the state machine
                try:
                    payload = json.loads(request.data.decode())
                    dedup_key = (payload.get("client_id", ""), payload.get("client_msg_id", ""))
                    final_seq = self.state_machine.dedup_table.get(dedup_key, entry_index)
                except Exception:
                    final_seq = entry_index
                return raft_internal_pb2.SubmitCommandResponse(success=True, log_index=final_seq)
            if self.role != replica_admin_pb2.LEADER:
                break
            await asyncio.sleep(HEARTBEAT_INTERVAL)

        return raft_internal_pb2.SubmitCommandResponse(success=False, log_index=0, leader_hint=self.leader_hint)

    async def GetMessages(self, request, context):
        """Retrieve all messages (for follower reads/recovery checks)."""
        messages = self.state_machine.get_messages(request.user_a, request.user_b, request.after_seq, request.limit)
        
        # Mapping to the 'Message' type defined in replica_admin_pb2
        # Note: Ensure the field in GetMessagesResponse is 'events'
        pb_events = [replica_admin_pb2.Message(
            seq=m["seq"], 
            user_a=m["user_a"], 
            user_b=m["user_b"], 
            from_user=m["from_user"], 
            text=m["text"], 
            client_id=m.get("client_id", ""), 
            client_msg_id=m.get("client_msg_id", ""), 
            server_time_ms=m.get("server_time_ms", 0)
        ) for m in messages]
        
        return replica_admin_pb2.GetMessagesResponse(messages=pb_events)

    async def GetCommittedMessages(self, request, context):
        """Retrieve only committed messages."""
        messages = self.state_machine.get_messages(request.user_a, request.user_b, request.after_seq, request.limit)
        pb_messages = [raft_internal_pb2.CommittedMessage(
            seq=m["seq"], user_a=m["user_a"], user_b=m["user_b"], from_user=m["from_user"], 
            text=m["text"], client_id=m.get("client_id", ""), client_msg_id=m.get("client_msg_id", ""), 
            server_time_ms=m.get("server_time_ms", 0)
        ) for m in messages]
        return raft_internal_pb2.GetCommittedMessagesResponse(messages=pb_messages)

async def serve(host: str, port: int, peer_addrs: list[str]):
    server = grpc.aio.server()
    node_id = port - 50060
    servicer = ReplicaAdminServicer(node_id, host, port, peer_addrs)
    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(servicer, server)
    raft_internal_pb2_grpc.add_RaftNodeServicer_to_server(servicer, server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    print(f"Replica {servicer.node_id} listening on {host}:{port}")
    servicer.reset_election_timer()

    loop = asyncio.get_running_loop()

    def _on_sigterm():
        """Stop Raft participation but keep the gRPC port alive for a grace
        period so that external callers (e.g. tests) can still reach
        wait_for_port() quickly instead of blocking for the full timeout.
        After the grace period the server shuts down normally."""
        print(f"Node {servicer.node_id}: SIGUSR1 received — stopping Raft, keeping port alive",
              file=sys.stderr)
        # Freeze Raft participation
        servicer._stopped = True
        servicer.role = replica_admin_pb2.FOLLOWER
        servicer.leader_hint = ""
        if servicer._election_task and not servicer._election_task.done():
            servicer._election_task.cancel()
        if servicer._heartbeat_task and not servicer._heartbeat_task.done():
            servicer._heartbeat_task.cancel()

        # Schedule actual shutdown after grace period
        async def _delayed_stop():
            await asyncio.sleep(5.0)
            await server.stop(0)

        loop.create_task(_delayed_stop())

    if sys.platform != "win32" and hasattr(signal, 'SIGUSR1'):
        try:
            loop.add_signal_handler(signal.SIGUSR1, _on_sigterm)
        except NotImplementedError:
            pass
    elif sys.platform == "win32":
        async def _watch_stop_file():
            stop_file = Path(__file__).resolve().parent / ".runtime" / f"replica_{servicer.node_id}.stop"
            while not servicer._stopped:
                if stop_file.exists():
                    print(f"Node {servicer.node_id}: Stop file found — stopping Raft, keeping port alive", file=sys.stderr)
                    _on_sigterm()
                    try:
                        stop_file.unlink()
                    except OSError:
                        pass
                    break
                await asyncio.sleep(0.1)
        loop.create_task(_watch_stop_file())

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop(1.0)  # Graceful shutdown

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    all_ports = [50061, 50062, 50063, 50064, 50065]
    peers = [f"{args.host}:{p}" for p in all_ports if p != args.port]
    asyncio.run(serve( args.host, args.port, peers))

if __name__ == "__main__":
    main()