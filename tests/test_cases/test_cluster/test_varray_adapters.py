import json
import re

from typing import TYPE_CHECKING
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest

from cantok import DefaultToken
from deker.arrays import VArray
from deker.ctx import CTX
from deker_tools.typings import Sentinel
from pytest_httpx import HTTPXMock

from tests.conftest import operation_kwargs

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.consts import LAST_MODIFIED_HEADER, NON_LEADER_WRITE
from deker_server_adapters.errors import DekerClusterError, FilteringByIdInClusterIsForbidden
from deker_server_adapters.models import Status
from deker_server_adapters.utils.hashing import get_hash_key
from deker_server_adapters.varray_adapter import ServerVarrayAdapter

if TYPE_CHECKING:
    from httpx import Request


def test_get_node_by_id(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: list[str]):
    with patch.object(varray, "primary_attributes", {}):
        # Check window slides

        node = server_varray_adapter.hash_ring.get_node(get_hash_key(varray)).url.raw_url
        assert node in nodes_urls


def test_get_node_by_primary(varray: VArray, server_varray_adapter: ServerVarrayAdapter, nodes_urls: list[str]):
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
    data.update({"id": None, "id_": None, "primary_attributes": {}})
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        server_varray_adapter.create_from_dict(
            {
                **data,
                "adapter": server_varray_adapter,
                "collection": varray_collection,
                "array_adapter": server_array_adapter,
            },
            timeout=Sentinel,
            token=DefaultToken(),
        )
    requests: list[Request] = httpx_mock.get_requests()
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
        varray_collection_with_primary_attributes.filter({"id": str(uuid4())}).last()


def test_hash_updated(httpx_mock: HTTPXMock, server_varray_adapter: ServerArrayAdapter, mocked_ping, varray):
    class RequestCounter:
        def __init__(self):
            self.count = 0

    request_counter = RequestCounter()

    def limited_mock_response(request, extensions, request_counter, mocked_ping, data) -> httpx.Response:
        url_to_mock = re.compile(r".*/v1/collection/.*/varray/by-id/.*")

        if url_to_mock.search(str(request.url)):
            request_counter.count += 1

            if request_counter.count < 2:
                return httpx.Response(412, json=mocked_ping, headers={LAST_MODIFIED_HEADER: "new-hash"})
            else:
                return httpx.Response(200, json=data)

        elif "status" in str(request.url):
            return httpx.Response(text=Status.UNMOVED.value)

        return httpx.Response(404, json={"error": "Not found"})

    httpx_mock.add_callback(
        lambda request: limited_mock_response(request, None, request_counter, mocked_ping, varray.as_dict)
    )
    server_varray_adapter.read_meta(varray)
    assert server_varray_adapter.client.headers[LAST_MODIFIED_HEADER] == "new-hash"


NEW_LEADER = {
    "id": "8381202B-8C95-487A-B9B5-0B527056804A",
    "host": "newhost.owm.io",
    "port": 80,
    "protocol": "http",
    "storage": "file:///tmp/deker_server",
}


@pytest.mark.parametrize(
    ("write", "extra_args", "method", "expected_status"),
    [
        pytest.param("update_meta_custom_attributes", ({},), "PUT", 200, id="update-attributes"),
        pytest.param("delete", (), "DELETE", 200, id="delete"),
        pytest.param("create", (), "POST", 201, id="create"),
    ],
)
def test_varray_write_after_a_leader_change_goes_to_the_new_leader(
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    varray: VArray,
    mocked_ping: dict,
    write: str,
    extra_args: tuple,
    method: str,
    expected_status: int,
):
    """A VArray write answered 421 by the old leader is repeated on the leader of the config of that answer."""
    old_leader = server_varray_adapter.ctx.extra["cluster_config"].leader.url.raw_url
    new_config = {**mocked_ping, "leader_id": NEW_LEADER["id"], "current": [NEW_LEADER], "raft": [NEW_LEADER]}
    httpx_mock.add_response(
        status_code=NON_LEADER_WRITE, method=method, json=new_config, url=re.compile(f"{old_leader}.*")
    )
    httpx_mock.add_response(
        status_code=expected_status,
        method=method,
        json={"id": varray.id},
        url=re.compile(r"http://newhost\.owm\.io/.*"),
    )

    getattr(server_varray_adapter, write)(varray, *extra_args, **operation_kwargs(write))

    assert [request.url.host for request in httpx_mock.get_requests(method=method)] == ["localhost", "newhost.owm.io"]
    assert server_varray_adapter.ctx.extra["cluster_config"].leader.id == NEW_LEADER["id"]


def test_varray_write_fails_when_the_config_of_the_non_leader_answer_has_no_leader(
    httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, varray: VArray, mocked_ping: dict
):
    """A 421 whose config names a leader outside the RAFT nodes is an error, nothing is repeated."""
    old_leader = server_varray_adapter.ctx.extra["cluster_config"].leader.url.raw_url
    new_config = {**mocked_ping, "leader_id": NEW_LEADER["id"]}
    httpx_mock.add_response(
        status_code=NON_LEADER_WRITE, method="PUT", json=new_config, url=re.compile(f"{old_leader}.*")
    )

    with pytest.raises(DekerClusterError, match="No leader has been found"):
        server_varray_adapter.update_meta_custom_attributes(varray, {}, timeout=Sentinel, token=DefaultToken())

    assert len(httpx_mock.get_requests(method="PUT")) == 1
