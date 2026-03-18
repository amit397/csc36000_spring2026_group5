#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

# Add generated folder to path for proto imports
sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))

import grpc

import direct_client_pb2 as pb
import direct_gateway_pb2 as gw_pb
import direct_gateway_pb2_grpc as gw_grpc



@dataclass
class ClientConfig:
    gateway_addr: str
    me: str
    peer: str
    client_id: str
    history_limit: int = 50


def conv_pair(a: str, b: str) -> tuple[str, str]:
    """Deterministically order participants for a conversation."""
    return (a, b) if a <= b else (b, a)


class DirectChatClient:
    """
    Terminal 1:1 chat client that talks only to a proxy/gateway via gRPC.
    The gateway handles routing to the correct Raft leader and streaming committed events back.
    """

    def __init__(self, cfg: ClientConfig):
        self.cfg = cfg
        self._stop = asyncio.Event()
        self._last_seen_seq: int = 0

        # Dedup: avoid printing our own message twice if:
        # (a) we print on ACK and (b) the stream also delivers the same event.
        # This client does NOT print on ACK by default, but we keep this anyway.
        self._recent_client_msg_ids: set[str] = set()
        self._recent_client_msg_ids_max = 200

        self.user_a, self.user_b = conv_pair(cfg.me, cfg.peer)

    async def run(self) -> int:
        async with grpc.aio.insecure_channel(self.cfg.gateway_addr) as channel:
            stub = gw_grpc.DirectGatewayStub(channel)

            await self._catch_up_history(stub)

            sub_task = asyncio.create_task(self._subscribe_loop(stub))
            input_task = asyncio.create_task(self._stdin_loop(stub))

            done, pending = await asyncio.wait(
                {sub_task, input_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            self._stop.set()
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for t in done:
                exc = t.exception()
                if exc:
                    print(f"[client] error: {exc}", file=sys.stderr)
                    return 1

        return 0

    async def _catch_up_history(self, stub: pb_grpc.DirectGatewayStub) -> None:
        try:
            resp = await stub.GetConversationHistory(
                gw_pb.GetConversationHistoryRequest(
                    user_a=self.user_a,
                    user_b=self.user_b,
                    after_seq=0,
                    limit=self.cfg.history_limit,
                ),
                timeout=5.0,
            )
            if resp.events:
                print(f"[client] history (last {len(resp.events)}):")
                for ev in resp.events:
                    self._print_event(ev)
                    self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))
                print("[client] --- live ---")
            else:
                print("[client] --- live ---")
        except grpc.aio.AioRpcError:
            print("[client] (no history available) --- live ---")

    async def _subscribe_loop(self, stub: pb_grpc.DirectGatewayStub) -> None:
        backoff = 0.25
        max_backoff = 3.0

        while not self._stop.is_set():
            req = pb.SubscribeConversationRequest(
                user_a=self.user_a,
                user_b=self.user_b,
                after_seq=max(0, self._last_seen_seq),
            )
            try:
                stream = stub.SubscribeConversation(req, timeout=None)
                async for ev in stream:
                    if self._stop.is_set():
                        break

                    # Dedup our own messages if needed
                    if ev.client_id == self.cfg.client_id and ev.client_msg_id in self._recent_client_msg_ids:
                        self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))
                        continue

                    self._print_event(ev)
                    self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))

                backoff = 0.25
            except grpc.aio.AioRpcError as e:
                if self._stop.is_set():
                    return
                print(f"[client] subscribe error: {e.code().name} {e.details()} (reconnecting...)", file=sys.stderr)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

    async def _stdin_loop(self, stub: pb_grpc.DirectGatewayStub) -> None:
        print(f"[client] chatting as '{self.cfg.me}' with '{self.cfg.peer}'")
        print("[client] type messages + Enter. Commands: /help, /quit")
        while not self._stop.is_set():
            line = await asyncio.to_thread(sys.stdin.readline)
            if line == "":  # EOF
                self._stop.set()
                return

            msg = line.strip()
            if not msg:
                continue

            if msg in ("/quit", "/exit"):
                self._stop.set()
                return

            if msg == "/help":
                print("Commands: /help, /quit")
                continue

            await self._send_direct(stub, msg)

    async def _send_direct(self, stub: pb_grpc.DirectGatewayStub, text: str) -> None:
        client_msg_id = str(uuid.uuid4())
        self._remember_client_msg_id(client_msg_id)

        req = pb.SendDirectRequest(
            from_user=self.cfg.me,
            to_user=self.cfg.peer,
            client_id=self.cfg.client_id,
            client_msg_id=client_msg_id,
            text=text,
        )

        try:
            resp = await stub.SendDirect(req, timeout=5.0)
            # We rely on the stream for display. But we can keep seq in case stream is delayed.
            self._last_seen_seq = max(self._last_seen_seq, int(resp.seq))
        except grpc.aio.AioRpcError as e:
            print(f"[client] send failed: {e.code().name} {e.details()}", file=sys.stderr)

    def _remember_client_msg_id(self, client_msg_id: str) -> None:
        self._recent_client_msg_ids.add(client_msg_id)
        if len(self._recent_client_msg_ids) > self._recent_client_msg_ids_max:
            # Drop arbitrary elements to keep bounded
            for _ in range(len(self._recent_client_msg_ids) - self._recent_client_msg_ids_max):
                self._recent_client_msg_ids.pop()

    def _print_event(self, ev: pb.DirectEvent) -> None:
        ts = ""
        if ev.server_time_ms:
            ts = time.strftime("%H:%M:%S", time.localtime(ev.server_time_ms / 1000)) + " "

        who = "me" if ev.from_user == self.cfg.me else ev.from_user
        print(f"{ts}[{ev.seq}] {who}: {ev.text}")


def parse_args() -> ClientConfig:
    ap = argparse.ArgumentParser(description="1:1 gRPC Chat Client (talks to gateway/proxy)")
    ap.add_argument("--gateway", required=True, help="Gateway address host:port (insecure).")
    ap.add_argument("--me", required=True, help="My user id / nickname.")
    ap.add_argument("--peer", required=True, help="Peer user id / nickname.")
    ap.add_argument("--client-id", default=None, help="Stable client id for retries (defaults to a UUID).")
    ap.add_argument("--history", type=int, default=50, help="History events to fetch on start.")
    args = ap.parse_args()

    return ClientConfig(
        gateway_addr=args.gateway,
        me=args.me,
        peer=args.peer,
        client_id=args.client_id or str(uuid.uuid4()),
        history_limit=args.history,
    )


async def main() -> int:
    cfg = parse_args()
    client = DirectChatClient(cfg)
    return await client.run()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(0)
