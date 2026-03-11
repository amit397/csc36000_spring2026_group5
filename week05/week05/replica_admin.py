from __future__ import annotations

import argparse
import asyncio
import random
import grpc

ELECTION_TIMEOUT_MIN = 0.50  # 500ms
ELECTION_TIMEOUT_MAX = 1.00  # 1000ms
HEARTBEAT_INTERVAL = 0.05    # 50ms
from generated import replica_admin_pb2, replica_admin_pb2_grpc
from generated import raft_internal_pb2, raft_internal_pb2_grpc

class ReplicaAdminServicer(replica_admin_pb2_grpc.ReplicaAdminServicer, raft_internal_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id: int, host: str, port: int, peer_addrs: list[str]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self.peer_addrs = peer_addrs  # addresses of the other 4 replicas
        self.role = replica_admin_pb2.FOLLOWER
        self.term = 0
        self.voted_for = None  # who we voted for in current term
        self.leader_hint = ""
        self.commit_index = 0
        self.log = []  # will hold log entries later
        self._election_task = None
        self._heartbeat_task = None
        # Leader-only state (initialized when becoming leader)
        self.next_index = {}   # peer_addr -> next log index to send
        self.match_index = {}  # peer_addr -> highest log index replicated

    def _random_election_timeout(self) -> float:
        """Pick a random timeout between 150-300ms."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def reset_election_timer(self):
        """Cancel old timer, start a new one. Called when we hear from a valid leader."""
        if self._election_task is not None:
            self._election_task.cancel()
        self._election_task = asyncio.create_task(self._election_timeout())

    async def _election_timeout(self):
        try:
            await asyncio.sleep(self._random_election_timeout())
            print(f"Node {self.node_id}: election timeout, starting election")
            await self._start_election()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Node {self.node_id}: election error: {e}")
            self.reset_election_timer()

    async def _start_election(self):
        self.term += 1
        self.role = replica_admin_pb2.CANDIDATE
        self.voted_for = self.node_id
        votes = 1
        last_idx = len(self.log)
        last_term = self.log[-1]["term"] if self.log else 0

        tasks = [self._request_vote_from(p, last_idx, last_term) for p in self.peer_addrs]
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, Exception):
                continue
            if result.term > self.term:
                self.term = result.term
                self.role = replica_admin_pb2.FOLLOWER
                self.voted_for = None
                self.reset_election_timer()
                return
            if result.vote_granted:
                votes += 1

        majority = (len(self.peer_addrs) + 1) // 2 + 1
        print(f"Node {self.node_id}: term {self.term} got {votes}/{majority} votes")
        if self.role == replica_admin_pb2.CANDIDATE and votes >= majority:
            self.role = replica_admin_pb2.LEADER
            self.leader_hint = self.addr
            for p in self.peer_addrs:
                self.next_index[p] = len(self.log) + 1
                self.match_index[p] = 0
            print(f"Node {self.node_id}: became LEADER for term {self.term}")
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        else:
            self.role = replica_admin_pb2.FOLLOWER
            self.reset_election_timer()

    async def _request_vote_from(self, peer_addr, last_idx, last_term):
        async with grpc.aio.insecure_channel(peer_addr) as channel:
            stub = raft_internal_pb2_grpc.RaftNodeStub(channel)
            return await stub.RequestVote(raft_internal_pb2.RequestVoteRequest(
                term=self.term, candidate_id=self.node_id,
                last_log_index=last_idx, last_log_term=last_term,
            ), timeout=0.1)

    async def _send_append_entries(self, peer_addr):
        ni = self.next_index.get(peer_addr, len(self.log) + 1)
        prev_idx = ni - 1
        prev_term = self.log[prev_idx - 1]["term"] if prev_idx > 0 else 0
        # Entries to send: everything from next_index onward
        entries = [
            raft_internal_pb2.LogEntry(term=e["term"], data=e["data"])
            for e in self.log[prev_idx:]
        ]
        async with grpc.aio.insecure_channel(peer_addr) as channel:
            stub = raft_internal_pb2_grpc.RaftNodeStub(channel)
            resp = await stub.AppendEntries(raft_internal_pb2.AppendEntriesRequest(
                term=self.term, leader_id=self.node_id,
                prev_log_index=prev_idx, prev_log_term=prev_term,
                entries=entries, leader_commit=self.commit_index,
            ), timeout=0.1)
        if resp.term > self.term:
            return resp  # caller handles step-down
        if resp.success:
            self.next_index[peer_addr] = ni + len(entries)
            self.match_index[peer_addr] = self.next_index[peer_addr] - 1
        else:
            # Decrement next_index to find where logs match
            self.next_index[peer_addr] = max(1, ni - 1)
        return resp

    def _update_commit_index(self):
        # Find the highest index replicated on a majority
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1]["term"] == self.term:
                count = 1  # count self
                for p in self.peer_addrs:
                    if self.match_index.get(p, 0) >= n:
                        count += 1
                if count >= (len(self.peer_addrs) + 1) // 2 + 1:
                    self.commit_index = n
                    break

    async def _heartbeat_loop(self):
        while self.role == replica_admin_pb2.LEADER:
            tasks = [self._send_append_entries(p) for p in self.peer_addrs]
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, Exception):
                    continue
                if result.term > self.term:
                    self.term = result.term
                    self.role = replica_admin_pb2.FOLLOWER
                    self.voted_for = None
                    self.reset_election_timer()
                    return
            self._update_commit_index()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def Status(self, request, context):
        last_log_index = len(self.log)
        last_log_term = self.log[-1]["term"] if self.log else 0

        return replica_admin_pb2.StatusResponse(
            id=self.node_id,
            role=self.role,
            term=self.term,
            leader_hint=self.leader_hint,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
            commit_index=self.commit_index,
        )

    async def RequestVote(self, request, context):
        """A candidate is asking us for our vote."""
        # If candidate has a higher term, update ours
        if request.term > self.term:
            self.term = request.term
            self.role = replica_admin_pb2.FOLLOWER
            self.voted_for = None

        vote_granted = False

        # Only vote if: same term, AND we haven't voted yet (or already voted for them)
        if request.term == self.term:
            if self.voted_for is None or self.voted_for == request.candidate_id:
                # Check candidate's log is at least as up-to-date as ours
                my_last_term = self.log[-1]["term"] if self.log else 0
                my_last_index = len(self.log)

                candidate_up_to_date = (
                    request.last_log_term > my_last_term
                    or (request.last_log_term == my_last_term
                        and request.last_log_index >= my_last_index)
                )
                if candidate_up_to_date:
                    vote_granted = True
                    self.voted_for = request.candidate_id
                    self.reset_election_timer()  # voted — reset timer

        return raft_internal_pb2.RequestVoteResponse(
            term=self.term,
            vote_granted=vote_granted,
        )

    async def AppendEntries(self, request, context):
        # Reject if leader's term is behind ours
        if request.term < self.term:
            return raft_internal_pb2.AppendEntriesResponse(
                term=self.term, success=False,
            )

        # Valid leader — update our state
        if request.term > self.term:
            self.term = request.term
            self.voted_for = None
        self.role = replica_admin_pb2.FOLLOWER
        self.leader_hint = f"{self.host}:{request.leader_id + 50060}"
        self.reset_election_timer()

        # Log consistency check
        if request.prev_log_index > 0:
            if request.prev_log_index > len(self.log):
                return raft_internal_pb2.AppendEntriesResponse(
                    term=self.term, success=False,
                )
            if self.log[request.prev_log_index - 1]["term"] != request.prev_log_term:
                # Mismatch — truncate from here
                self.log = self.log[:request.prev_log_index - 1]
                return raft_internal_pb2.AppendEntriesResponse(
                    term=self.term, success=False,
                )

        # Append new entries
        for i, entry in enumerate(request.entries):
            idx = request.prev_log_index + i  # 0-based index in our log
            if idx < len(self.log):
                if self.log[idx]["term"] != entry.term:
                    self.log = self.log[:idx]  # truncate conflicting
                    self.log.append({"term": entry.term, "data": entry.data})
            else:
                self.log.append({"term": entry.term, "data": entry.data})

        # Update commit index
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, len(self.log))

        return raft_internal_pb2.AppendEntriesResponse(
            term=self.term, success=True,
        )

    async def SubmitCommand(self, request, context):
        # Only the leader can accept commands
        if self.role != replica_admin_pb2.LEADER:
            return raft_internal_pb2.SubmitCommandResponse(
                success=False, log_index=0, leader_hint=self.leader_hint,
            )

        # Append to our log
        self.log.append({"term": self.term, "data": request.data})
        entry_index = len(self.log)

        # Wait for it to be committed (replicated to majority)
        for _ in range(100):  # try for up to 5 seconds (100 * 50ms)
            if self.commit_index >= entry_index:
                return raft_internal_pb2.SubmitCommandResponse(
                    success=True, log_index=entry_index,
                )
            if self.role != replica_admin_pb2.LEADER:
                break
            await asyncio.sleep(HEARTBEAT_INTERVAL)

        return raft_internal_pb2.SubmitCommandResponse(
            success=False, log_index=0, leader_hint=self.leader_hint,
        )


async def serve(host: str, port: int, peer_addrs: list[str]):
    server = grpc.aio.server()
    servicer = ReplicaAdminServicer(port - 50060, host, port, peer_addrs)
    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(servicer, server)
    raft_internal_pb2_grpc.add_RaftNodeServicer_to_server(servicer, server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    print(f"Replica {servicer.node_id} listening on {host}:{port}")
    servicer.reset_election_timer()
    await server.wait_for_termination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    all_ports = [50061, 50062, 50063, 50064, 50065]
    peers = [f"{args.host}:{p}" for p in all_ports if p != args.port]
    asyncio.run(serve(args.host, args.port, peers))


if __name__ == "__main__":
    main()
