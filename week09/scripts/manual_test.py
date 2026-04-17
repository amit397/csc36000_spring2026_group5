#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import week09_gateway_pb2

from apps.common_client import connect_gateway


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a representative manual smoke test for one application."
    )
    parser.add_argument(
        "--application",
        required=True,
        choices=["course_registration", "wallet", "inventory"],
        help="Application scenario to exercise.",
    )
    parser.add_argument(
        "--gateway",
        default="127.0.0.1:50151",
        help="Gateway address in host:port form.",
    )
    return parser


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def expect_rpc_failure(callable_obj, description: str) -> None:
    try:
        callable_obj()
    except grpc.RpcError as exc:
        print(f"[ok] {description}: failed as expected ({exc.code().name})")
        return
    raise AssertionError(f"{description}: expected RPC failure but call succeeded")


def run_course_registration(stub) -> None:
    print("[run] course registration smoke test")
    section_id = "MANUAL-CSC36000-01"
    first_student = "manual-student-a"
    second_student = "manual-student-b"

    create = stub.CreateSection(
        week09_gateway_pb2.CreateSectionRequest(section_id=section_id, capacity=1),
        timeout=5.0,
    )
    print(f"[ok] created section {create.section_id} capacity={create.capacity}")

    enroll = stub.Enroll(
        week09_gateway_pb2.EnrollRequest(student_id=first_student, section_id=section_id),
        timeout=5.0,
    )
    expect(enroll.committed is True, "first enrollment should commit")
    expect(enroll.enrolled_count == 1, "first enrollment should produce count=1")
    print(f"[ok] enrolled {first_student} in {section_id}")

    schedule = stub.GetStudentSchedule(
        week09_gateway_pb2.GetStudentScheduleRequest(student_id=first_student),
        timeout=5.0,
    )
    roster = stub.GetSectionRoster(
        week09_gateway_pb2.GetSectionRosterRequest(section_id=section_id),
        timeout=5.0,
    )
    expect(section_id in schedule.section_ids, "student schedule should contain the section")
    expect(first_student in roster.student_ids, "section roster should contain the student")
    print("[ok] schedule and roster agree after enrollment")

    expect_rpc_failure(
        lambda: stub.Enroll(
            week09_gateway_pb2.EnrollRequest(student_id=second_student, section_id=section_id),
            timeout=5.0,
        ),
        "last-seat conflicting enrollment",
    )

    roster_after = stub.GetSectionRoster(
        week09_gateway_pb2.GetSectionRosterRequest(section_id=section_id),
        timeout=5.0,
    )
    expect(list(roster_after.student_ids) == [first_student], "roster should remain unchanged after failed enrollment")
    print("[ok] failed enrollment left state unchanged")


def run_wallet(stub) -> None:
    print("[run] wallet smoke test")
    account_a = "manual-wallet-a"
    account_b = "manual-wallet-b"

    stub.CreateAccount(
        week09_gateway_pb2.CreateAccountRequest(account_id=account_a, initial_balance_cents=10000),
        timeout=5.0,
    )
    stub.CreateAccount(
        week09_gateway_pb2.CreateAccountRequest(account_id=account_b, initial_balance_cents=2500),
        timeout=5.0,
    )
    print("[ok] created accounts")

    transfer = stub.Transfer(
        week09_gateway_pb2.TransferRequest(
            from_account_id=account_a,
            to_account_id=account_b,
            amount_cents=1250,
        ),
        timeout=5.0,
    )
    expect(transfer.committed is True, "transfer should commit")
    expect(transfer.from_balance_cents == 8750, "source balance should be debited")
    expect(transfer.to_balance_cents == 3750, "destination balance should be credited")
    print("[ok] successful transfer preserved expected balances")

    expect_rpc_failure(
        lambda: stub.Transfer(
            week09_gateway_pb2.TransferRequest(
                from_account_id=account_b,
                to_account_id=account_a,
                amount_cents=999999,
            ),
            timeout=5.0,
        ),
        "insufficient-funds transfer",
    )

    account_a_state = stub.GetAccount(
        week09_gateway_pb2.GetAccountRequest(account_id=account_a),
        timeout=5.0,
    )
    account_b_state = stub.GetAccount(
        week09_gateway_pb2.GetAccountRequest(account_id=account_b),
        timeout=5.0,
    )
    expect(account_a_state.balance_cents == 8750, "source balance should remain unchanged after failed transfer")
    expect(account_b_state.balance_cents == 3750, "destination balance should remain unchanged after failed transfer")
    print("[ok] failed transfer left balances unchanged")


def run_inventory(stub) -> None:
    print("[run] inventory smoke test")
    item_id = "manual-item-1"
    known_reservation = "manual-res-1"
    unknown_reservation = "manual-res-missing"

    stub.CreateInventoryItem(
        week09_gateway_pb2.CreateInventoryItemRequest(item_id=item_id, quantity=5),
        timeout=5.0,
    )
    print("[ok] created inventory item")

    reserve = stub.ReserveItem(
        week09_gateway_pb2.ReserveItemRequest(item_id=item_id, reservation_id=known_reservation, quantity=2),
        timeout=5.0,
    )
    expect(reserve.committed is True, "reservation should commit")
    expect(reserve.remaining_quantity == 3, "remaining quantity should be updated")
    print("[ok] successful reservation reduced available quantity")

    inventory = stub.GetInventory(
        week09_gateway_pb2.GetInventoryRequest(item_id=item_id),
        timeout=5.0,
    )
    expect(inventory.total_quantity == 5, "total quantity should remain 5")
    expect(inventory.reserved_quantity == 2, "reserved quantity should be 2")
    expect(inventory.available_quantity == 3, "available quantity should be 3")
    print("[ok] inventory summary matches reservation")

    expect_rpc_failure(
        lambda: stub.ReleaseReservation(
            week09_gateway_pb2.ReleaseReservationRequest(item_id=item_id, reservation_id=unknown_reservation),
            timeout=5.0,
        ),
        "release of unknown reservation",
    )

    inventory_after_failure = stub.GetInventory(
        week09_gateway_pb2.GetInventoryRequest(item_id=item_id),
        timeout=5.0,
    )
    expect(inventory_after_failure.reserved_quantity == 2, "failed release should not change reserved quantity")
    expect(inventory_after_failure.available_quantity == 3, "failed release should not change available quantity")
    print("[ok] failed release left inventory unchanged")


def main() -> int:
    args = build_arg_parser().parse_args()
    stub = connect_gateway(args.gateway)

    if args.application == "course_registration":
        run_course_registration(stub)
    elif args.application == "wallet":
        run_wallet(stub)
    else:
        run_inventory(stub)

    print(f"[done] manual smoke test passed for {args.application}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
