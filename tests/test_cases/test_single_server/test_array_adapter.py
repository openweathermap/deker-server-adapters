import json
import re

import numpy as np
import pytest

from deker.arrays import Array
from deker.ctx import CTX
from httpx import ReadTimeout
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import DekerTimeoutServer


def test_read_meta_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, ctx: CTX):
    httpx_mock.add_response(
        json=array.as_dict,
        method="GET",
        url=re.compile(f"{ctx.uri.raw_url.rstrip('/')}/v1/collection/{array.collection}/array/by-id/{array.id}"),
    )
    assert server_array_adapter.read_meta(array) == json.loads(json.dumps(array.as_dict))


def test_iter_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, ctx):
    httpx_mock.add_response(
        url=re.compile(f"{ctx.uri.raw_url.rstrip('/')}/v1/collection/{array.collection}/arrays"), json=[array.as_dict]
    )

    arrays = []
    for array_ in server_array_adapter:
        arrays.append(array_)

    assert arrays == [json.loads(json.dumps(array.as_dict))]


def test_nodes_returns_list_with_base_url(ctx, server_array_adapter: ServerArrayAdapter):
    assert server_array_adapter.nodes_urls == [ctx.uri.raw_url]


def test_hash_ring_is_not_available_in_single_mode(server_array_adapter: ServerArrayAdapter):
    with pytest.raises(AttributeError, match="single server mode"):
        server_array_adapter.hash_ring


def test_read_data_httpx_timeout(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    """In single mode an httpx timeout is not swallowed by node fallback and becomes DekerTimeoutServer."""
    httpx_mock.add_exception(ReadTimeout("slow server"), url=re.compile(r".*array.*"))
    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.read_data(array, np.index_exp[:])


def test_update_httpx_timeout(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_exception(ReadTimeout("slow server"), url=re.compile(r".*array.*"))
    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.update(array, np.index_exp[:], np.zeros(shape=(1,), dtype=array.dtype))
