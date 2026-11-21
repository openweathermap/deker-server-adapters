import json
import re

from typing import TYPE_CHECKING
from uuid import uuid4

import httpx
import numpy as np
import pytest

from cantok import DefaultToken
from deker.arrays import Array
from deker.ctx import CTX
from deker_tools.hash_ring import get_hash_key
from deker_tools.typings import Sentinel
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.consts import LAST_MODIFIED_HEADER
from deker_server_adapters.errors import DekerServerError, FilteringByIdInClusterIsForbidden
from deker_server_adapters.models import Status

if TYPE_CHECKING:
    from httpx import Request


def test_read_meta_success(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    ctx: CTX,
    mocked_filestatus_check_unmoved: None,
):
    node = server_array_adapter.hash_ring.get_node(get_hash_key(array)).url.raw_url
    httpx_mock.add_response(
        json=array.as_dict,
        method="GET",
        url=re.compile(f"{node}/v1/collection/{array.collection}/array/by-id/{array.id}"),
    )
    assert server_array_adapter.read_meta(array) == json.loads(json.dumps(array.as_dict))


def test_array_read_from_specific_node(
    array: Array, server_array_adapter: ServerArrayAdapter, httpx_mock: HTTPXMock, mocked_filestatus_check_unmoved: None
):
    host = server_array_adapter.hash_ring.get_node(get_hash_key(array)).url.raw_url
    httpx_mock.add_response(url=re.compile(f"{host}.*array.*"), content=np.zeros(shape=(1,)).tobytes())
    server_array_adapter.read_data(array, ..., timeout=Sentinel, token=DefaultToken())


def test_array_generate_id(array: Array, server_array_adapter: ServerArrayAdapter, httpx_mock, collection):
    httpx_mock.add_response(method="POST", json=array.as_dict, status_code=201)
    data = array.as_dict
    data.update({"id": None, "id_": None, "primary_attributes": {}})
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        server_array_adapter.create_from_dict(
            {**data, "adapter": server_array_adapter, "collection": collection}, timeout=Sentinel, token=DefaultToken()
        )
    requests: list[Request] = httpx_mock.get_requests()
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
        httpx_mock.add_response(url=re.compile(node.url.raw_url), json=response)

    arrays = []
    for array_ in server_array_adapter:
        arrays.append(array_)

    assert arrays == [json.loads(json.dumps(array.as_dict))]


def test_filter_by_id_is_not_allowed(collection_with_primary_attributes):
    with pytest.raises(FilteringByIdInClusterIsForbidden):
        collection_with_primary_attributes.filter({"id": str(uuid4())}).last()


def test_hash_updated(httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mocked_ping, array, status: str):
    class RequestCounter:
        def __init__(self):
            self.count = 0

    request_counter = RequestCounter()

    def limited_mock_response(request, extensions, request_counter, mocked_ping, data) -> httpx.Response:
        url_to_mock = re.compile(r".*/v1/collection/.*/array/by-id/.*")

        if str(request.url).endswith("/status"):  # the status lives under the same by-id path now
            return httpx.Response(text=Status.UNMOVED.value, status_code=200)

        if url_to_mock.search(str(request.url)):
            request_counter.count += 1

            if request_counter.count < 2:
                return httpx.Response(412, json=mocked_ping, headers={LAST_MODIFIED_HEADER: "new-hash"})
            else:
                return httpx.Response(200, json=data)

        return httpx.Response(404, json={"error": "Not found"})

    httpx_mock.add_callback(
        lambda request: limited_mock_response(request, None, request_counter, mocked_ping, array.as_dict)
    )
    server_array_adapter.read_meta(array)
    assert server_array_adapter.client.headers[LAST_MODIFIED_HEADER] == "new-hash"


@pytest.mark.parametrize(
    ("method", "args", "message"),
    [
        pytest.param("create", (), "Couldn't create the array", id="create"),
        pytest.param("delete", (), "Couldn't delete the array", id="delete"),
        pytest.param("read_data", (np.index_exp[:],), "Couldn't read the array", id="read"),
        pytest.param("update", (np.index_exp[:], np.zeros(1, dtype=np.int64)), "Couldn't update array", id="update"),
    ],
)
def test_a_node_that_does_not_answer_is_the_server_error_of_the_operation(
    method: str,
    args: tuple,
    message: str,
    array: Array,
    server_array_adapter: ServerArrayAdapter,
    httpx_mock: HTTPXMock,
    request: pytest.FixtureRequest,
):
    """No answer of the node of the Array is DekerServerError with what the adapter was doing, and no response."""
    if method != "create":
        request.getfixturevalue("mocked_filestatus_check_unmoved")
    httpx_mock.add_exception(httpx.ConnectError("refused"), url=re.compile(r".*/array.*"))

    with pytest.raises(DekerServerError) as info:
        getattr(server_array_adapter, method)(array, *args, timeout=Sentinel, token=DefaultToken())

    assert type(info.value) is DekerServerError
    assert str(info.value) == message
