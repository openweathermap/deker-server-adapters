import re

import httpx

from cantok import DefaultToken
from deker import Array, VArray
from deker.ctx import CTX
from deker_tools.typings import Sentinel
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.consts import LAST_MODIFIED_HEADER, NON_LEADER_WRITE
from deker_server_adapters.models import Status
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


def test_new_cluster_config_is_applied_after_non_leader_error(
    ctx: CTX, httpx_mock: HTTPXMock, mocked_ping, server_varray_adapter: ServerVarrayAdapter, varray: VArray
):
    new_cluster_config = {
        **mocked_ping,
        "leader_id": "8381202B-8C95-487A-B9B5-0B527056804A",
        "current": [
            {
                "id": "8381202B-8C95-487A-B9B5-0B527056804A",
                "host": "newhost.owm.io",
                "port": 80,
                "protocol": "http",
                "storage": "file:///tmp/deker_server",
            }
        ],
        "raft": [
            {
                "id": "8381202B-8C95-487A-B9B5-0B527056804A",
                "host": "newhost.owm.io",
                "port": 80,
                "protocol": "http",
                "storage": "file:///tmp/deker_server",
            }
        ],
    }
    # Error
    httpx_mock.add_response(
        status_code=NON_LEADER_WRITE,
        method="PUT",
        json=new_cluster_config,
        url=re.compile(f"{ctx.extra['cluster_config'].leader.url.raw_url}"),
    )
    # Ok, on the new leader only
    httpx_mock.add_response(status_code=200, method="PUT", json={}, url=re.compile(r"http://newhost\.owm\.io/.*"))
    server_varray_adapter.update_meta_custom_attributes(varray, {}, timeout=Sentinel, token=DefaultToken())
    assert server_varray_adapter.ctx.extra["cluster_config"].leader.host == "newhost.owm.io"
    assert str(server_varray_adapter.client.base_url) == "http://newhost.owm.io"


def test_hash_conflict_without_last_modified_header_keeps_the_old_one(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mocked_ping: dict, array: Array
):
    """A 412 without the header still applies the config and retries, the stored hash is untouched."""
    client = server_array_adapter.client
    before = client.headers.get(LAST_MODIFIED_HEADER)
    calls = {"array": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/status"):
            return httpx.Response(200, text=Status.UNMOVED.value)
        calls["array"] += 1
        if calls["array"] == 1:
            return httpx.Response(412, json=mocked_ping)
        return httpx.Response(200, json=array.as_dict)

    httpx_mock.add_callback(respond)
    server_array_adapter.read_meta(array)
    assert calls["array"] == 2
    assert client.headers.get(LAST_MODIFIED_HEADER) == before
