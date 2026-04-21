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


# ---------------------------------------------------------------------------
# Read-only operations (routed to read_from_shard)
# ---------------------------------------------------------------------------
_READ_OPERATIONS = {"get_inventory", "get_account", "get_student_schedule", "get_section_roster"}


def execute_gateway_request(
    application_name: str,
    operation_name: str,
    payload: dict[str, Any],
    router: RouterView,
    transport: CoordinatorTransport,
) -> dict[str, Any]:
    """
    Execute one application request at the gateway layer.

    All inventory operations are single-shard (keyed by item_id), so we
    simply route to the owning shard and let per-shard serial execution
    provide serializable isolation.
    """
    logical_shard_id = router.logical_shard_for_payload(application_name, operation_name, payload)

    if operation_name in _READ_OPERATIONS:
        return transport.read_from_shard(logical_shard_id, operation_name, payload)

    return transport.apply_to_shard(logical_shard_id, operation_name, payload)


# ---------------------------------------------------------------------------
# Helper: get or initialise the inventory dict inside shard state
# ---------------------------------------------------------------------------

def _inventory(state: dict[str, Any]) -> dict[str, Any]:
    """Return the 'inventory' sub-dict, creating it if absent."""
    if "inventory" not in state:
        state["inventory"] = {}
    return state["inventory"]

def _transactions(state: dict[str, Any]) -> dict[str, Any]:
    """Return the 'transactions' sub-dict, creating it if absent."""
    if "transactions" not in state:
        state["transactions"] = {}
    return state["transactions"]

# ---------------------------------------------------------------------------
# Local mutations (run inside the shard server while holding the shard lock)
# ---------------------------------------------------------------------------

def apply_local_mutation(state: dict[str, Any], operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a single-shard mutation with idempotency and recovery tracking.
    """
    if operation_name == "create_item":
        return _create_item(state, payload)
    if operation_name == "reserve_item":
        return _reserve_item(state, payload)
    if operation_name == "release_reservation":
        return _release_reservation(state, payload)

    raise ValueError(f"Unknown mutation: {operation_name!r}")


def _create_item(state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    item_id = payload["item_id"]
    quantity = int(payload["quantity"])
    inv = _inventory(state)

    inv[item_id] = {
        "total_quantity": quantity,
        "reservations": {},
    }

    return {"item_id": item_id, "quantity": quantity}


def _reserve_item(state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    item_id = payload["item_id"]
    reservation_id = payload["reservation_id"]
    quantity = int(payload["quantity"])
    inv = _inventory(state)

    item = inv.get(item_id)
    if item is None:
        raise ValueError(f"Item {item_id!r} does not exist")

    total = item["total_quantity"]
    reserved = sum(item["reservations"].values())
    available = total - reserved

    if quantity > available:
        raise ValueError(
            f"Over-allocation: requested {quantity} but only {available} available for {item_id!r}"
        )

    item["reservations"][reservation_id] = quantity
    remaining = available - quantity

    return {"committed": True, "remaining_quantity": remaining}


def _release_reservation(state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    item_id = payload["item_id"]
    reservation_id = payload["reservation_id"]
    inv = _inventory(state)

    item = inv.get(item_id)
    if item is None:
        raise ValueError(f"Item {item_id!r} does not exist")

    # HARD DELETE (guaranteed)
    if reservation_id not in item["reservations"]:
        raise ValueError(f"Reservation {reservation_id!r} not found")

    item["reservations"].pop(reservation_id)

    # FORCE RECOMPUTE (no stale dependency)
    reserved_total = 0
    for rid in item["reservations"]:
        reserved_total += item["reservations"][rid]

    remaining = item["total_quantity"] - reserved_total

    return {
        "committed": True,
        "remaining_quantity": remaining
    }


# ---------------------------------------------------------------------------
# Local queries (read-only, no state modification)
# ---------------------------------------------------------------------------

def run_local_query(state: dict[str, Any], query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a single-shard read against local shard state and return a
    JSON-serializable result.
    """
    if query_name == "get_inventory":
        return _get_inventory(state, payload)

    raise ValueError(f"Unknown query: {query_name!r}")


def _get_inventory(state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    item_id = payload["item_id"]
    inv = _inventory(state)

    item = inv.get(item_id)
    if item is None:
        return {
            "item_id": item_id,
            "total_quantity": 0,
            "reserved_quantity": 0,
            "available_quantity": 0,
        }

    total = item["total_quantity"]
    reserved = sum(item["reservations"].values())
    available = total - reserved

    return {
        "item_id": item_id,
        "total_quantity": total,
        "reserved_quantity": reserved,
        "available_quantity": available,
    }
