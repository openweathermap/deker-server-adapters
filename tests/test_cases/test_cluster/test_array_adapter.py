import json
import re

from typing import TYPE_CHECKING, List
from unittest.mock import patch

import numpy as np
import pytest

from deker.arrays import Array
from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import FilteringByIdInClusterIsForbidden
from deker_server_adapters.utils import get_node_from_hash_ring


if TYPE_CHECKING:
    from httpx import Request


def test_read_meta_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, ctx: CTX):
    node = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
    httpx_mock.add_response(
        json=array.as_dict,
        method="GET",
        url=re.compile(f"{node}/v1/collection/{array.collection}/array/by-id/{array.id}"),
    )
    assert server_array_adapter.read_meta(array) == json.loads(json.dumps(array.as_dict))


def test_get_node_by_id(array: Array, server_array_adapter: ServerArrayAdapter, nodes_urls: List[str]):
    with patch.object(array, "primary_attributes", {}):
        node = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
        assert node in nodes_urls


def test_get_node_by_primary(array: Array, server_array_adapter: ServerArrayAdapter, nodes_urls: List[str]):
    with patch.object(array, "primary_attributes", {"foo": "bar"}):
        node = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
        assert node in nodes_urls


def test_get_node_give_same_result(array: Array, server_array_adapter: ServerArrayAdapter):
    first_node = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
    for _ in range(10):
        node = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
        assert node == first_node


def test_array_read_from_specific_node(array: Array, server_array_adapter: ServerArrayAdapter, httpx_mock: HTTPXMock):
    host = server_array_adapter.get_host_url(get_node_from_hash_ring(array, server_array_adapter.hash_ring))
    httpx_mock.add_response(url=re.compile(host), content=np.zeros(shape=(1,)).tobytes())
    server_array_adapter.read_data(array, ...)


def test_array_generate_id(array: Array, server_array_adapter: ServerArrayAdapter, httpx_mock, collection):
    httpx_mock.add_response(method="POST", json=array.as_dict, status_code=201)
    data = array.as_dict
    data.update({"id": None, "id_": None, "primary_attributes": {}})
    server_array_adapter.create({**data, "adapter": server_array_adapter, "collection": collection})
    requests: List[Request] = httpx_mock.get_requests()
    for request in requests:
        if request.method == "POST":
            assert json.loads(request.content.decode())["id_"]


def test_iter_success(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
):
    for index, node in enumerate(server_array_adapter.nodes):
        response = [] if index == 0 else [array.as_dict]
        httpx_mock.add_response(url=re.compile(node), json=response)

    arrays = []
    for array_ in server_array_adapter:
        arrays.append(array_)

    assert arrays == [json.loads(json.dumps(array.as_dict))]


def test_filter_by_id_is_not_allowed(collection_with_primary_attributes):
    with pytest.raises(FilteringByIdInClusterIsForbidden):
        collection_with_primary_attributes.filter({"id": "foo"}).last()
