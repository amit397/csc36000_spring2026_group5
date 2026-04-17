from __future__ import annotations

import pytest


def test_inventory_reservation_preserves_availability(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("inventory tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(gateway_pb2.CreateInventoryItemRequest(item_id="item-1", quantity=5), timeout=5.0)
    reservation = stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-1", reservation_id="r-1", quantity=2),
        timeout=5.0,
    )
    assert reservation.committed is True
    inventory = stub.GetInventory(gateway_pb2.GetInventoryRequest(item_id="item-1"), timeout=5.0)
    assert inventory.total_quantity == 5
    assert inventory.reserved_quantity == 2
    assert inventory.available_quantity == 3


def test_inventory_prevents_over_allocation(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("inventory tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(gateway_pb2.CreateInventoryItemRequest(item_id="item-2", quantity=1), timeout=5.0)
    first = stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-2", reservation_id="r-1", quantity=1),
        timeout=5.0,
    )
    assert first.committed is True
    with pytest.raises(Exception):
        stub.ReserveItem(
            gateway_pb2.ReserveItemRequest(item_id="item-2", reservation_id="r-2", quantity=1),
            timeout=5.0,
        )

    inventory = stub.GetInventory(gateway_pb2.GetInventoryRequest(item_id="item-2"), timeout=5.0)
    assert inventory.total_quantity == 1
    assert inventory.reserved_quantity == 1
    assert inventory.available_quantity == 0


def test_inventory_happy_path_release_restores_availability(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("inventory tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(gateway_pb2.CreateInventoryItemRequest(item_id="item-3", quantity=4), timeout=5.0)
    reserve = stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-3", reservation_id="r-release", quantity=3),
        timeout=5.0,
    )
    assert reserve.committed is True

    release = stub.ReleaseReservation(
        gateway_pb2.ReleaseReservationRequest(item_id="item-3", reservation_id="r-release"),
        timeout=5.0,
    )
    assert release.committed is True
    assert release.remaining_quantity == 4

    inventory = stub.GetInventory(gateway_pb2.GetInventoryRequest(item_id="item-3"), timeout=5.0)
    assert inventory.reserved_quantity == 0
    assert inventory.available_quantity == 4


def test_inventory_unhappy_path_unknown_reservation_does_not_change_state(selected_application, gateway_stub):
    if selected_application != "inventory":
        pytest.skip("inventory tests only run when SELECTED_APPLICATION == 'inventory'")

    gateway_pb2, stub = gateway_stub
    stub.CreateInventoryItem(gateway_pb2.CreateInventoryItemRequest(item_id="item-4", quantity=2), timeout=5.0)
    stub.ReserveItem(
        gateway_pb2.ReserveItemRequest(item_id="item-4", reservation_id="r-known", quantity=1),
        timeout=5.0,
    )

    with pytest.raises(Exception):
        stub.ReleaseReservation(
            gateway_pb2.ReleaseReservationRequest(item_id="item-4", reservation_id="r-missing"),
            timeout=5.0,
        )

    inventory = stub.GetInventory(gateway_pb2.GetInventoryRequest(item_id="item-4"), timeout=5.0)
    assert inventory.total_quantity == 2
    assert inventory.reserved_quantity == 1
    assert inventory.available_quantity == 1
