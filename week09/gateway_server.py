#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from concurrent import futures
from typing import Any

import grpc

from week09_common import GENERATED_DIRECTORY

if str(GENERATED_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(GENERATED_DIRECTORY))

import cluster_admin_pb2
import cluster_admin_pb2_grpc
import week09_gateway_pb2
import week09_gateway_pb2_grpc

from student_impl import transactions as student_transactions
from week09_runtime import GatewayShardDirectory


def _routing_items(result: dict[str, Any], repeated_key: str = "served_by"):
    routing_entries = result.get(repeated_key, [])
    if isinstance(routing_entries, dict):
        routing_entries = [routing_entries]
    normalized = []
    for entry in routing_entries:
        if isinstance(entry, dict):
            normalized.append(
                week09_gateway_pb2.RoutingInfo(
                    logical_shard_id=int(entry.get("logical_shard_id", -1)),
                    storage_addr=str(entry.get("storage_addr", "")),
                )
            )
    return normalized


class GatewayService(week09_gateway_pb2_grpc.Week09GatewayServicer):
    def __init__(self, directory: GatewayShardDirectory):
        self.directory = directory

    def _execute(self, application_name: str, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return student_transactions.execute_gateway_request(
            application_name=application_name,
            operation_name=operation_name,
            payload=payload,
            router=self.directory,
            transport=self.directory,
        )

    def _abort_unimplemented(self, context, exc: Exception):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, str(exc))

    def CreateSection(self, request, context):
        try:
            result = self._execute("course_registration", "create_section", {"section_id": request.section_id, "capacity": request.capacity})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        routing_entries = _routing_items(result, repeated_key="routing")
        return week09_gateway_pb2.CreateSectionResponse(
            section_id=str(result.get("section_id", request.section_id)),
            capacity=int(result.get("capacity", request.capacity)),
            routing=routing_entries[0] if routing_entries else week09_gateway_pb2.RoutingInfo(),
        )

    def Enroll(self, request, context):
        try:
            result = self._execute("course_registration", "enroll", {"student_id": request.student_id, "section_id": request.section_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.EnrollResponse(
            committed=bool(result.get("committed", False)),
            enrolled_count=int(result.get("enrolled_count", 0)),
            capacity=int(result.get("capacity", 0)),
            served_by=_routing_items(result),
        )

    def Drop(self, request, context):
        try:
            result = self._execute("course_registration", "drop", {"student_id": request.student_id, "section_id": request.section_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.DropResponse(
            committed=bool(result.get("committed", False)),
            enrolled_count=int(result.get("enrolled_count", 0)),
            capacity=int(result.get("capacity", 0)),
            served_by=_routing_items(result),
        )

    def GetStudentSchedule(self, request, context):
        try:
            result = self._execute("course_registration", "get_student_schedule", {"student_id": request.student_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.GetStudentScheduleResponse(
            section_ids=list(result.get("section_ids", [])),
            served_by=_routing_items(result),
        )

    def GetSectionRoster(self, request, context):
        try:
            result = self._execute("course_registration", "get_section_roster", {"section_id": request.section_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.GetSectionRosterResponse(
            student_ids=list(result.get("student_ids", [])),
            capacity=int(result.get("capacity", 0)),
            served_by=_routing_items(result),
        )

    def CreateAccount(self, request, context):
        try:
            result = self._execute("wallet", "create_account", {"account_id": request.account_id, "initial_balance_cents": request.initial_balance_cents})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        routing_entries = _routing_items(result, repeated_key="routing")
        return week09_gateway_pb2.CreateAccountResponse(
            account_id=str(result.get("account_id", request.account_id)),
            balance_cents=int(result.get("balance_cents", 0)),
            routing=routing_entries[0] if routing_entries else week09_gateway_pb2.RoutingInfo(),
        )

    def GetAccount(self, request, context):
        try:
            result = self._execute("wallet", "get_account", {"account_id": request.account_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        routing_entries = _routing_items(result, repeated_key="routing")
        return week09_gateway_pb2.GetAccountResponse(
            account_id=str(result.get("account_id", request.account_id)),
            balance_cents=int(result.get("balance_cents", 0)),
            routing=routing_entries[0] if routing_entries else week09_gateway_pb2.RoutingInfo(),
        )

    def Transfer(self, request, context):
        try:
            result = self._execute(
                "wallet",
                "transfer",
                {
                    "from_account_id": request.from_account_id,
                    "to_account_id": request.to_account_id,
                    "amount_cents": request.amount_cents,
                },
            )
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.TransferResponse(
            committed=bool(result.get("committed", False)),
            from_balance_cents=int(result.get("from_balance_cents", 0)),
            to_balance_cents=int(result.get("to_balance_cents", 0)),
            served_by=_routing_items(result),
        )

    def CreateInventoryItem(self, request, context):
        try:
            result = self._execute(
                "inventory",
                "create_item",
                {"item_id": request.item_id, "quantity": request.quantity},
            )
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        routing_entries = _routing_items(result, repeated_key="routing")
        return week09_gateway_pb2.CreateInventoryItemResponse(
            item_id=str(result.get("item_id", request.item_id)),
            quantity=int(result.get("quantity", request.quantity)),
            routing=routing_entries[0] if routing_entries else week09_gateway_pb2.RoutingInfo(),
        )

    def ReserveItem(self, request, context):
        try:
            result = self._execute(
                "inventory",
                "reserve_item",
                {"item_id": request.item_id, "reservation_id": request.reservation_id, "quantity": request.quantity},
            )
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.ReserveItemResponse(
            committed=bool(result.get("committed", False)),
            remaining_quantity=int(result.get("remaining_quantity", 0)),
            served_by=_routing_items(result),
        )

    def ReleaseReservation(self, request, context):
        try:
            result = self._execute(
                "inventory",
                "release_reservation",
                {"item_id": request.item_id, "reservation_id": request.reservation_id},
            )
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.ReleaseReservationResponse(
            committed=bool(result.get("committed", False)),
            remaining_quantity=int(result.get("remaining_quantity", 0)),
            served_by=_routing_items(result),
        )

    def GetInventory(self, request, context):
        try:
            result = self._execute("inventory", "get_inventory", {"item_id": request.item_id})
        except NotImplementedError as exc:
            self._abort_unimplemented(context, exc)
        return week09_gateway_pb2.GetInventoryResponse(
            item_id=str(result.get("item_id", request.item_id)),
            total_quantity=int(result.get("total_quantity", 0)),
            reserved_quantity=int(result.get("reserved_quantity", 0)),
            available_quantity=int(result.get("available_quantity", 0)),
            served_by=_routing_items(result),
        )


class AdminService(cluster_admin_pb2_grpc.ClusterAdminServicer):
    def __init__(self, directory: GatewayShardDirectory):
        self.directory = directory

    def Rebalance(self, request, context):
        try:
            return self.directory.perform_rebalance()
        except NotImplementedError as exc:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, str(exc))
        except Exception as exc:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"{type(exc).__name__}: {exc}")

    def GetClusterState(self, request, context):
        node_states = self.directory.node_states()
        response = cluster_admin_pb2.GetClusterStateResponse()
        for node_state in node_states:
            response.nodes.append(
                cluster_admin_pb2.ShardNodeState(
                    node_id=node_state["node_id"],
                    addr=node_state["addr"],
                    logical_shard_ids=[item["logical_shard_id"] for item in node_state["logical_shards"]],
                    total_records=node_state["total_records"],
                )
            )
        logical_shard_to_record_count: dict[int, int] = {}
        for node_state in node_states:
            for item in node_state["logical_shards"]:
                logical_shard_to_record_count[item["logical_shard_id"]] = item["record_count"]
        for shard_entry in sorted(self.directory._cluster().logical_shards, key=lambda item: item.logical_shard_id):
            logical_shard_id = shard_entry.logical_shard_id
            response.logical_shards.append(
                cluster_admin_pb2.LogicalShardState(
                    logical_shard_id=logical_shard_id,
                    owner_node_id=self.directory.logical_shard_owner_node_id(logical_shard_id),
                    record_count=logical_shard_to_record_count.get(logical_shard_id, 0),
                )
            )
        return response


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the week09 gateway/admin server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50151)
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    bind_addr = f"0.0.0.0:{args.port}"
    directory = GatewayShardDirectory()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    week09_gateway_pb2_grpc.add_Week09GatewayServicer_to_server(GatewayService(directory), server)
    cluster_admin_pb2_grpc.add_ClusterAdminServicer_to_server(AdminService(directory), server)
    server.add_insecure_port(bind_addr)
    server.start()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        directory.close()
        server.stop(grace=0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
