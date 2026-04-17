from __future__ import annotations

import sys
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import week09_gateway_pb2_grpc


def connect_gateway(addr: str):
    channel = grpc.insecure_channel(addr)
    grpc.channel_ready_future(channel).result(timeout=10.0)
    return week09_gateway_pb2_grpc.Week09GatewayStub(channel)
