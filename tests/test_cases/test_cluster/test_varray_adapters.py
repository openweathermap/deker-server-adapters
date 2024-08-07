import json
import re

from typing import TYPE_CHECKING, List
from unittest.mock import patch

import pytest

from deker.arrays import VArray
from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import FilteringByIdInClusterIsForbidden
from deker_server_adapters.utils.hashing import get_hash_key
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


if TYPE_CHECKING:
    from httpx import Request


def test_get_node_by_id(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: List[str]):
    with patch.object(varray, "primary_attributes", {}):
        # Check window slides

        node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
        assert node in nodes_urls


def test_get_node_by_primary(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: List[str]):
    with patch.object(varray, "primary_attributes", {"foo": "bar"}):
        # Check window slides

        node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
        assert node in nodes_urls


def test_get_node_give_same_result(varray: VArray, server_varray_adapter: ServerVarrayAdapter):
    first_node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
    for _ in range(10):
        node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
        assert node == first_node


def test_array_generate_id(
    varray: VArray,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock,
    varray_collection,
    server_array_adapter: ServerArrayAdapter,
):
    httpx_mock.add_response(method="POST", json=varray.as_dict, status_code=201)
    data = varray.as_dict
    data.update({"id": None, "id_": None, "primary_attributes": None})
    server_varray_adapter.create(
        {
            **data,
            "adapter": server_varray_adapter,
            "collection": varray_collection,
            "array_adapter": server_array_adapter,
        }
    )
    requests: List[Request] = httpx_mock.get_requests()
    for request in requests:
        if request.method == "POST":
            assert json.loads(request.content.decode())["id_"]


def test_read_meta_success(varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerArrayAdapter, ctx: CTX):
    node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
    httpx_mock.add_response(
        json=varray.as_dict,
        method="GET",
        url=re.compile(f"{node}/v1/collection/{varray.collection}/varray/by-id/{varray.id}"),
    )
    assert server_varray_adapter.read_meta(varray) == json.loads(json.dumps(varray.as_dict))


def test_filter_by_id_is_not_allowed(varray_collection_with_primary_attributes):
    with pytest.raises(FilteringByIdInClusterIsForbidden):
        varray_collection_with_primary_attributes.filter({"id": "foo"}).last()
