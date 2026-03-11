import asyncio
import uuid
import grpc
import sys
from pathlib import Path

# Add generated folder to path for proto imports
sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))

import direct_client_pb2 as pb
import direct_gateway_pb2_grpc as gw_grpc

async def run_demo():
    gateway_addr = "127.0.0.1:50051"
    client_id = "presentation-demo-client"
    msg_id = str(uuid.uuid4())
    
    print(f"--- Section B: Deduplication Demo ---")
    print(f"Targeting Gateway: {gateway_addr}")
    print(f"Using ClientMsgID: {msg_id}\n")

    async with grpc.aio.insecure_channel(gateway_addr) as channel:
        stub = gw_grpc.DirectGatewayStub(channel)
        
        # 1. First send (with retry for leader election)
        print("Step 1: Sending message (waiting for leader election if needed)...")
        req1 = pb.SendDirectRequest(
            from_user="alice",
            to_user="bob",
            client_id=client_id,
            client_msg_id=msg_id,
            text="Hello world (first try)"
        )
        
        resp1 = None
        for i in range(10):
            try:
                resp1 = await stub.SendDirect(req1, timeout=5.0)
                print(f"Result 1 -> Sequence Number: {resp1.seq}\n")
                break
            except Exception as e:
                if "No leader found" in str(e) or "StatusCode.UNAVAILABLE" in str(e):
                    print(f"  [Wait] Leader not ready yet, retrying... ({i+1}/10)")
                    await asyncio.sleep(2.0)
                else:
                    print(f"Error 1: {e}")
                    return
        
        if not resp1:
            print("Failed to find leader after multiple retries.")
            return

        # 2. Second send (Retry)
        print("Step 2: Sending same message ID again (Retry simulation)...")
        req2 = pb.SendDirectRequest(
            from_user="alice",
            to_user="bob",
            client_id=client_id,
            client_msg_id=msg_id,
            text="Hello world (RETRY)" # Text doesn't matter, ID does
        )
        try:
            resp2 = await stub.SendDirect(req2, timeout=5.0)
            print(f"Result 2 -> Sequence Number: {resp2.seq}")
            
            if resp1.seq == resp2.seq:
                print("\n[SUCCESS] Idempotency confirmed! Both returns identical sequence numbers.")
            else:
                print("\n[FAILURE] Sequence numbers differ.")
        except Exception as e:
            print(f"Error 2: {e}")

if __name__ == "__main__":
    asyncio.run(run_demo())
