#!/usr/bin/env python3
from __future__ import annotations

"""
Provided reservation / inventory application client.

Application model:
- item_id: identifies one inventory item or resource
- quantity: either the total quantity for an item or the requested reservation quantity
- reservation_id: identifies one reservation for one item

Provided operations:
- create-item item_id quantity
- reserve item_id reservation_id quantity
- release item_id reservation_id
- get item_id

What the application expects from the student implementation:
- Available inventory must never go negative.
- Reservations must not over-allocate the item.
- Failed reservation or release operations must not corrupt state.
- Releasing a reservation should restore availability correctly.

What the tests do with these values:
- Create items with known quantities.
- Make successful reservations and verify total/reserved/available counts.
- Attempt over-allocation and verify it fails without partial effects.
- Release an existing reservation and verify availability is restored.
- Attempt to release an unknown reservation and verify state is unchanged.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_client import connect_gateway
from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import week09_gateway_pb2


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Provided inventory client.")
    parser.add_argument("--gateway", default="127.0.0.1:50151", help="Gateway address in host:port form.")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-item")
    create.add_argument("item_id", help="Inventory item identifier.")
    create.add_argument("quantity", type=int, help="Total quantity available for the item.")

    reserve = sub.add_parser("reserve")
    reserve.add_argument("item_id", help="Inventory item identifier.")
    reserve.add_argument("reservation_id", help="Reservation identifier.")
    reserve.add_argument("quantity", type=int, help="Requested reservation quantity.")

    release = sub.add_parser("release")
    release.add_argument("item_id", help="Inventory item identifier.")
    release.add_argument("reservation_id", help="Reservation identifier.")

    get_inventory = sub.add_parser("get")
    get_inventory.add_argument("item_id", help="Inventory item identifier.")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    stub = connect_gateway(args.gateway)
    if args.command == "create-item":
        print(stub.CreateInventoryItem(week09_gateway_pb2.CreateInventoryItemRequest(item_id=args.item_id, quantity=args.quantity)))
    elif args.command == "reserve":
        print(stub.ReserveItem(week09_gateway_pb2.ReserveItemRequest(item_id=args.item_id, reservation_id=args.reservation_id, quantity=args.quantity)))
    elif args.command == "release":
        print(stub.ReleaseReservation(week09_gateway_pb2.ReleaseReservationRequest(item_id=args.item_id, reservation_id=args.reservation_id)))
    else:
        print(stub.GetInventory(week09_gateway_pb2.GetInventoryRequest(item_id=args.item_id)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
