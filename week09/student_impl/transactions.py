from __future__ import annotations

from typing import Any, Protocol


class CoordinatorTransport(Protocol):
    def apply_to_shard(self, logical_shard_id: int, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def read_from_shard(self, logical_shard_id: int, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class RouterView(Protocol):
    def logical_shard_for_payload(self, application_name: str, operation_name: str, payload: dict[str, Any]) -> int:
        ...

    def owner_addr_for_logical_shard(self, logical_shard_id: int) -> str:
        ...


def execute_gateway_request(
    application_name: str,
    operation_name: str,
    payload: dict[str, Any],
    router: RouterView,
    transport: CoordinatorTransport,
) -> dict[str, Any]:
    """
    Execute one application request at the gateway layer.

    Students should implement the transaction logic for their chosen
    application here. The returned dictionary must be JSON-serializable.
    """
    raise NotImplementedError("Implement execute_gateway_request() in student_impl/transactions.py")


def apply_local_mutation(state: dict[str, Any], operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a single-shard mutation to local shard state and return a
    JSON-serializable result.
    """
    raise NotImplementedError("Implement apply_local_mutation() in student_impl/transactions.py")


def run_local_query(state: dict[str, Any], query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a single-shard read against local shard state and return a
    JSON-serializable result.
    """
    raise NotImplementedError("Implement run_local_query() in student_impl/transactions.py")
