#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Optional

# Add generated folder to path for proto imports
sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))

import grpc


# Import both client and gateway protos since proto imports don't expose imported types
from generated import direct_client_pb2 as client_pb
from generated import direct_gateway_pb2 as gateway_pb
from generated import direct_gateway_pb2_grpc as gateway_grpc
from generated import replica_admin_pb2
from generated import replica_admin_pb2_grpc
from generated import raft_internal_pb2
from generated import raft_internal_pb2_grpc


ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"


class DirectGatewayServicer(gateway_grpc.DirectGatewayServicer):
    def __init__(self, host, port, replica_addrs):
        self.host = host
        self.port = port
        
        # Load replica addresses from cluster.json
        self.replica_addrs: list[str] = []
        self._load_replica_addrs()

        # Keep track of last known leader
        self.leader_addr: Optional[str] = None

    def _load_replica_addrs(self):
        """Load replica addresses from .runtime/cluster.json."""
        if not CLUSTER_JSON.exists():
            print("[gateway] warning: cluster.json not found", file=sys.stderr)
            return
        
        try:
            data = json.loads(CLUSTER_JSON.read_text())
            self.replica_addrs = [r["addr"] for r in data.get("replicas", [])]
        except Exception as e:
            print(f"[gateway] error loading cluster.json: {e}", file=sys.stderr)

    async def _find_leader(self) -> Optional[str]:
        """Query replicas to find the leader."""
        if self.leader_addr:
            # Try cached leader first
            try:
                status = await self._get_status(self.leader_addr)
                if status and status.role == replica_admin_pb2.LEADER:
                    return self.leader_addr
            except Exception:
                pass

        # Search all replicas with a few retries for election to settle
        for attempt in range(10):
            for addr in self.replica_addrs:
                try:
                    status = await self._get_status(addr)
                    if status and status.role == replica_admin_pb2.LEADER:
                        self.leader_addr = addr
                        return addr
                    elif status and status.leader_hint:
                        # Use hint
                        try:
                            hint_status = await self._get_status(status.leader_hint)
                            if hint_status and hint_status.role == replica_admin_pb2.LEADER:
                                self.leader_addr = status.leader_hint
                                return status.leader_hint
                        except Exception:
                            pass
                except Exception:
                    continue
            if attempt < 2:
                await asyncio.sleep(0.5)

        return None

    async def _ping(self, addr: str) -> bool:
        """Return True if the replica at addr responds to a channel-ready check within 1 s."""
        try:
            async with grpc.aio.insecure_channel(addr) as channel:
                await asyncio.wait_for(channel.channel_ready(), timeout=3.0)
                return True
        except Exception:
            return False

    async def _prune_dead_replicas(self):
        """Reload the replica list from cluster.json, ping every node concurrently,
        and remove any that do not respond before forwarding the real request."""
        # Re-read cluster.json so recovered replicas come back
        self._load_replica_addrs()

        results = await asyncio.gather(
            *[self._ping(addr) for addr in self.replica_addrs],
            return_exceptions=True,
        )
        alive = []
        for addr, ok in zip(self.replica_addrs, results):
            if ok is True:
                alive.append(addr)
            else:
                print(f"[gateway] ping failed for {addr} — removing from active list", file=sys.stderr)
                if self.leader_addr == addr:
                    self.leader_addr = None
        self.replica_addrs = alive

    async def _get_status(self, addr: str) -> Optional[replica_admin_pb2.StatusResponse]:
        """Get status from a replica."""
        try:
            async with grpc.aio.insecure_channel(addr) as channel:
                stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                return await stub.Status(
                    replica_admin_pb2.StatusRequest(),
                    timeout=1.0
                )
        except Exception:
            return None

    async def SendDirect(self, request: client_pb.SendDirectRequest, context) -> client_pb.SendDirectResponse:
        """Handle a direct message send."""
        try:
            # Ping all nodes first; remove unresponsive ones before forwarding
            await self._prune_dead_replicas()
            print(f"[gateway] SendDirect: from={request.from_user} to={request.to_user} text={request.text[:30]}", file=sys.stderr)
            
            # Serialize message as JSON
            msg = {
                "from_user": request.from_user,
                "to_user": request.to_user,
                "client_id": request.client_id,
                "client_msg_id": request.client_msg_id,
                "text": request.text,
                "user_a": min(request.from_user, request.to_user),
                "user_b": max(request.from_user, request.to_user),
            }
            data = json.dumps(msg).encode()
            
            # Submit to Raft leader
            leader_addr = await self._find_leader()
            if not leader_addr:
                print(f"[gateway] SendDirect: no leader found, replicas={self.replica_addrs}", file=sys.stderr)
                context.set_details("No leader found")
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return client_pb.SendDirectResponse()
            
            print(f"[gateway] SendDirect: found leader at {leader_addr}", file=sys.stderr)
            
            try:
                async with grpc.aio.insecure_channel(leader_addr) as channel:
                    stub = raft_internal_pb2_grpc.RaftNodeStub(channel)
                    print(f"[gateway] SendDirect: calling SubmitCommand on {leader_addr}", file=sys.stderr)
                    resp = await stub.SubmitCommand(
                        raft_internal_pb2.SubmitCommandRequest(data=data),
                        timeout=5.0
                    )
                    print(f"[gateway] SendDirect: SubmitCommand response success={resp.success} log_index={resp.log_index}", file=sys.stderr)
                    if resp.success:
                        return client_pb.SendDirectResponse(
                            seq=resp.log_index,
                            server_time_ms=int(time.time() * 1000),
                        )
                    else:
                        print(f"[gateway] SendDirect: SubmitCommand returned success=False", file=sys.stderr)
                        context.set_details("Failed to submit to leader")
                        context.set_code(grpc.StatusCode.UNAVAILABLE)
                        return client_pb.SendDirectResponse()
            except Exception as e:
                print(f"[gateway] SendDirect: SubmitCommand exception: {type(e).__name__}: {e}", file=sys.stderr)
                context.set_details(str(e))
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return client_pb.SendDirectResponse()
                
        except Exception as e:
            print(f"[gateway] SendDirect: unhandled exception: {type(e).__name__}: {e}", file=sys.stderr)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return client_pb.SendDirectResponse()

    async def GetConversationHistory(
        self,
        request: gateway_pb.GetConversationHistoryRequest,
        context
    ) -> gateway_pb.GetConversationHistoryResponse:
        """Handle conversation history request."""
        try:
            # Ping all nodes first; remove unresponsive ones before forwarding
            await self._prune_dead_replicas()
            user_a = request.user_a
            user_b = request.user_b
            after_seq = request.after_seq
            limit = request.limit if request.limit > 0 else 200
            
            messages = None
            served_by = []
            
            # enum values: LEADER_ONLY=0, ANY_REPLICA=1, REPLICA_HINT=2
            try:
                read_pref = int(request.read_pref) if request.read_pref is not None else 0
            except:
                read_pref = 0
            try:
                replica_hint = str(request.replica_hint) if request.replica_hint else ""
            except:
                replica_hint = ""
            
            # If replica hint is given, try that first
            if read_pref == 2 and replica_hint:  # REPLICA_HINT = 2
                try:
                    async with grpc.aio.insecure_channel(replica_hint) as channel:
                        stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                        resp = await stub.GetMessages(
                            replica_admin_pb2.GetMessagesRequest(
                                user_a=user_a,
                                user_b=user_b,
                                after_seq=after_seq,
                                limit=limit,
                            ),
                            timeout=2.0
                        )
                        messages = resp.messages
                        served_by = [replica_hint]
                except Exception as e:
                    print(f"[gateway] error reading from replica_hint {replica_hint}: {e}", file=sys.stderr)
            
            # If leader-only or no replica_hint worked, try leader
            if messages is None and read_pref != 1:  # ANY_REPLICA = 1
                leader_addr = await self._find_leader()
                if leader_addr:
                    try:
                        async with grpc.aio.insecure_channel(leader_addr) as channel:
                            stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                            resp = await stub.GetMessages(
                                replica_admin_pb2.GetMessagesRequest(
                                    user_a=user_a,
                                    user_b=user_b,
                                    after_seq=after_seq,
                                    limit=limit,
                                ),
                                timeout=2.0
                            )
                            messages = resp.messages
                            served_by = [leader_addr]
                    except Exception as e:
                        print(f"[gateway] error reading from leader: {e}", file=sys.stderr)
            
            # If still no messages, try any replica (only if ANY_REPLICA or if we want to fallback arbitrarily? Actually, only fallback if ANY_REPLICA is allowed)
            if messages is None and read_pref == 1:
                for addr in self.replica_addrs:
                    try:
                        async with grpc.aio.insecure_channel(addr) as channel:
                            stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                            resp = await stub.GetMessages(
                                replica_admin_pb2.GetMessagesRequest(
                                    user_a=user_a,
                                    user_b=user_b,
                                    after_seq=after_seq,
                                    limit=limit,
                                ),
                                timeout=2.0
                            )
                            messages = resp.messages
                            served_by = [addr]
                            break
                    except Exception:
                        continue
            
            if messages is None:
                context.set_details("No suitable replica found to serve read")
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return gateway_pb.GetConversationHistoryResponse()
            
            # Convert to DirectEvent format
            events = [
                client_pb.DirectEvent(
                    seq=m.seq,
                    server_time_ms=m.server_time_ms,
                    user_a=m.user_a,
                    user_b=m.user_b,
                    from_user=m.from_user,
                    client_id=m.client_id,
                    client_msg_id=m.client_msg_id,
                    text=m.text,
                )
                for m in messages
            ]

            return gateway_pb.GetConversationHistoryResponse(events=events, served_by=served_by)
        except Exception as e:
            print(f"[gateway] GetConversationHistory error: {type(e).__name__}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            return gateway_pb.GetConversationHistoryResponse()

    async def SubscribeConversation(
        self,
        request: client_pb.SubscribeConversationRequest,
        context
    ):
        """Handle subscription to a conversation (server-side streaming)."""
        try:
            user_a = request.user_a
            user_b = request.user_b
            
            # First send history
            try:
                if self.replica_addrs:
                    async with grpc.aio.insecure_channel(self.replica_addrs[0]) as channel:
                        stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                        resp = await stub.GetMessages(
                            replica_admin_pb2.GetMessagesRequest(
                                user_a=user_a,
                                user_b=user_b,
                                after_seq=request.after_seq,
                                limit=0,
                            ),
                            timeout=2.0
                        )
                        
                        for m in resp.messages:
                            yield client_pb.DirectEvent(
                                seq=m.seq,
                                server_time_ms=m.server_time_ms,
                                user_a=m.user_a,
                                user_b=m.user_b,
                                from_user=m.from_user,
                                client_id=m.client_id,
                                client_msg_id=m.client_msg_id,
                                text=m.text,
                            )
            except Exception as e:
                print(f"[gateway] error sending history: {e}", file=sys.stderr)
            
            # Subscribe to future messages by polling replicas
            last_seq = max(request.after_seq, 0)
            poll_interval = 0.5  # seconds
            timeout_at = time.time() + 300  # 5 minute timeout
            
            while True:
                if time.time() > timeout_at:
                    break
                
                # Poll replicas for new messages
                try:
                    leader_addr = await self._find_leader()
                    poll_addr = leader_addr or (self.replica_addrs[0] if self.replica_addrs else None)
                    
                    if poll_addr:
                        async with grpc.aio.insecure_channel(poll_addr) as channel:
                            stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
                            resp = await stub.GetMessages(
                                replica_admin_pb2.GetMessagesRequest(
                                    user_a=user_a,
                                    user_b=user_b,
                                    after_seq=last_seq,
                                    limit=0,
                                ),
                                timeout=2.0
                            )
                            
                            for m in resp.messages:
                                if m.seq > last_seq:
                                    yield client_pb.DirectEvent(
                                        seq=m.seq,
                                        server_time_ms=m.server_time_ms,
                                        user_a=m.user_a,
                                        user_b=m.user_b,
                                        from_user=m.from_user,
                                        client_id=m.client_id,
                                        client_msg_id=m.client_msg_id,
                                        text=m.text,
                                    )
                                    last_seq = m.seq
                except Exception as e:
                    print(f"[gateway] poll error: {e}", file=sys.stderr)
                
                await asyncio.sleep(poll_interval)

        except Exception as e:
            print(f"[gateway] SubscribeConversation error: {e}", file=sys.stderr)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)


async def serve(host, port, replica_addrs):
    server = grpc.aio.server()
    servicer = DirectGatewayServicer(host, port, replica_addrs)
    gateway_grpc.add_DirectGatewayServicer_to_server(servicer, server)
    
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    print(f"Gateway started on {listen_addr}")
    await server.wait_for_termination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--replica_addrs", type=str, required=False, default="127.0.0.1:50052")
    args = parser.parse_args()

    # Split the comma-separated string into a list
    replicas = args.replica_addrs.split(",")
    asyncio.run(serve(args.host, args.port, replicas))


if __name__ == "__main__":
    main()
