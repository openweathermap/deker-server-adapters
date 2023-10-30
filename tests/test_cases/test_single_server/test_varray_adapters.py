import json
import re

from deker.arrays import VArray
from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.varray_adapter import ServerVarrayAdapter


def test_read_meta_success(varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, ctx: CTX):
    httpx_mock.add_response(
        json=varray.as_dict,
        method="GET",
        url=re.compile(f"{ctx.uri.raw_url.rstrip('/')}/v1/collection/{varray.collection}/varray/by-id/{varray.id}"),
    )
    assert server_varray_adapter.read_meta(varray) == json.loads(json.dumps(varray.as_dict))
