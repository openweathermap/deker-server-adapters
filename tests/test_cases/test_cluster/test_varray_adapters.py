import json

from typing import TYPE_CHECKING, List
from unittest.mock import patch

from deker.arrays import VArray

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


if TYPE_CHECKING:
    from httpx import Request


def test_get_node_by_id(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: List[str]):
    with patch.object(varray, "primary_attributes", {}):
        # Check window slides

        node = server_varray_adapter.get_host_url(server_varray_adapter.get_node(varray))
        assert node in nodes_urls


def test_get_node_by_primary(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: List[str]):
    with patch.object(varray, "primary_attributes", {"foo": "bar"}):
        # Check window slides

        node = server_varray_adapter.get_host_url(server_varray_adapter.get_node(varray))
        assert node in nodes_urls


def test_get_node_give_same_result(varray: VArray, server_varray_adapter: ServerVarrayAdapter):
    first_node = server_varray_adapter.get_node(varray)
    for _ in range(10):
        node = server_varray_adapter.get_node(varray)
        assert node == first_node


def test_array_generate_id(
    varray: VArray,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock,
    collection,
    server_array_adapter: ServerArrayAdapter,
):
    httpx_mock.add_response(method="POST", json=varray.as_dict, status_code=201)
    data = varray.as_dict
    data.update({"id": None, "id_": None, "primary_attributes": None})
    server_varray_adapter.create(
        {
            **data,
            "adapter": server_varray_adapter,
            "collection": collection,
            "array_adapter": server_array_adapter,
        }
    )
    requests: List[Request] = httpx_mock.get_requests()
    for request in requests:
        if request.method == "POST":
            assert json.loads(request.content.decode())["id_"]