import json
import re

from deker.arrays import Array
from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter


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
