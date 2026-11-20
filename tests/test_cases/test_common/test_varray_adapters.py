import json
import logging
import re
import warnings

from urllib.parse import unquote
from uuid import uuid4

import numpy as np
import pytest

from deker.arrays import VArray
from deker.collection import Collection
from deker.errors import DekerArrayTypeError, DekerLockError, DekerValidationError, DekerVSubsetError
from deker_tools.slices import slices_to_str
from pytest_httpx import HTTPXMock

from tests.conftest import CLUSTER_MODE
from tests.params.slices import invalid_index_params, valid_index_params

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import DekerClusterError, DekerServerError, DekerTimeoutServer
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


def test_create_success(
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    varray: VArray,
    server_array_adapter: ServerArrayAdapter,
    varray_collection: Collection,
):
    instance_id = str(uuid4())
    httpx_mock.add_response(status_code=201, json={"id": instance_id})
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        array = server_varray_adapter.create(
            {
                **varray.as_dict,
                "adapter": server_varray_adapter,
                "array_adapter": server_array_adapter,
                "collection": varray_collection,
            }
        )
    assert isinstance(array, VArray)
    assert array.id == instance_id


def test_create_from_varray_instance(httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, varray: VArray):
    """A VArray instance is sent as is and returned as is, without any warning."""
    httpx_mock.add_response(url=re.compile(r".*/varrays$"), status_code=201, json={"id": varray.id})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert server_varray_adapter.create(varray) is varray
    request = httpx_mock.get_requests()[-1]
    assert request.method == "POST"
    assert json.loads(request.content)["id_"] == varray.id


@pytest.mark.parametrize("status_code", [200, 400, 404, 500])
def test_create_from_varray_instance_fails_on_any_status_but_created(
    httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, varray: VArray, status_code: int
):
    httpx_mock.add_response(url=re.compile(r".*/varrays$"), status_code=status_code, json={"id": varray.id})
    with pytest.raises(DekerServerError):
        server_varray_adapter.create(varray)


def test_create_from_dict_builds_the_varray_from_the_response(
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    varray: VArray,
    server_array_adapter: ServerArrayAdapter,
    varray_collection: Collection,
):
    instance_id = str(uuid4())
    httpx_mock.add_response(url=re.compile(r".*/varrays$"), status_code=201, json={"id": instance_id})
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        created = server_varray_adapter.create_from_dict(
            {
                **varray.as_dict,
                "adapter": server_varray_adapter,
                "array_adapter": server_array_adapter,
                "collection": varray_collection,
                "id_": None,
            }
        )
    assert isinstance(created, VArray)
    assert created.id == instance_id


def test_create_fails_no_id(
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    varray: VArray,
    server_array_adapter: ServerArrayAdapter,
    varray_collection: Collection,
):
    httpx_mock.add_response(status_code=201)
    with pytest.raises(DekerServerError), pytest.warns(DeprecationWarning, match="3.0.0"):
        server_varray_adapter.create(
            {
                **varray.as_dict,
                "adapter": server_varray_adapter,
                "array_adapter": server_array_adapter,
                "collection": varray_collection,
            }
        )


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("create", ()),
        ("delete", ()),
        ("read_meta", ()),
        ("clear", (np.index_exp[:],)),
        ("update_meta_custom_attributes", ({"foo": "bar"},)),
        ("read_data", (np.index_exp[:])),
        ("update", (np.index_exp[:], np.zeros(shape=(1,), dtype=np.int64))),
    ],
)
def test_collection_raises_500(
    method: str,
    args: tuple,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock: HTTPXMock,
    varray: VArray,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        getattr(server_varray_adapter, method)(varray, *args)


def test_read_meta_success(varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter):
    httpx_mock.add_response(json=varray.as_dict, url=re.compile(r".*varray.*"))
    assert server_varray_adapter.read_meta(varray) == json.loads(json.dumps(varray.as_dict))


def test_update_meta_success(varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter):
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    assert server_varray_adapter.update_meta_custom_attributes(varray, {}) is None


def test_delete_success(varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter):
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    assert server_varray_adapter.delete(varray) is None


def test_read_data_success(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*varray.*"))
    assert server_varray_adapter.read_data(varray, np.index_exp[:]) == data


def test_read_data_deker_timeout(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*varray.*"))
    with pytest.raises(DekerTimeoutServer):
        server_varray_adapter.read_data(varray, np.index_exp[:])


def test_update_success(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,), dtype=varray.dtype)
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    assert server_varray_adapter.update(varray, np.index_exp[:], data) is None


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(np.array([7], dtype=np.int64), id="ndarray"),
        pytest.param([7], id="list"),
        pytest.param(7, id="int"),
    ],
)
def test_update_sends_the_data_cast_to_the_varray_dtype(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None, data
):
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    server_varray_adapter.update(varray, np.index_exp[:], data)
    request = httpx_mock.get_requests()[-1]
    assert request.method == "PUT"
    assert request.content == np.array([7], dtype=varray.dtype).tobytes()


@pytest.mark.parametrize(
    ("data", "error"),
    [
        pytest.param(np.zeros((1,), dtype=np.float64), DekerArrayTypeError, id="wrong-dtype"),
        pytest.param(1.5, DekerArrayTypeError, id="float-into-int"),
        pytest.param(np.zeros((2,), dtype=np.int64), IndexError, id="wrong-shape"),
    ],
)
def test_update_rejects_invalid_data_before_any_request(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, data, error
):
    """No status check and no PUT: the data is rejected before the adapter touches the network."""
    with pytest.raises(error):
        server_varray_adapter.update(varray, np.index_exp[:], data)
    assert not [request for request in httpx_mock.get_requests() if request.method == "PUT"]


def test_update_deker_timeout(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*varray.*"))
    data = np.zeros(shape=(1,), dtype=varray.dtype)

    with pytest.raises(DekerTimeoutServer):
        server_varray_adapter.update(varray, np.index_exp[:], data)


def test_clear_success(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    assert server_varray_adapter.clear(varray, np.index_exp[:]) is None


def test_clear_deker_timeout(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*varray.*"))

    with pytest.raises(DekerTimeoutServer):
        server_varray_adapter.clear(varray, np.index_exp[:])


def test_iter_success(
    varray: VArray,
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
):
    httpx_mock.add_response(url=re.compile(server_varray_adapter.collection_path.raw_url), json=[varray.as_dict])
    arrays = []
    for array_ in server_varray_adapter:
        arrays.append(array_)

    assert arrays == [json.loads(json.dumps(varray.as_dict))]


def test_read_data_single_number(
    varray: VArray,
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    collection: Collection,
    mock_status: None,
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    assert server_varray_adapter.read_data(varray, np.index_exp[0]) == data[0]


@pytest.mark.parametrize("index_exp", valid_index_params)
def test_read_data_asks_the_server_for_the_bounds_as_positions(bounds_varray: VArray, httpx_mock: HTTPXMock, index_exp):
    """The bounds string of the request carries the positions of the subset, and the body streams into its shape.

    A label, a datetime or a timedelta is converted to positions by the subset, before the adapter: the server
    never sees them.
    """
    subset = bounds_varray[index_exp]
    shape = np.shape(np.empty(subset.shape))
    expected = np.arange(np.prod(shape), dtype=bounds_varray.dtype).reshape(shape)
    httpx_mock.add_response(content=expected.tobytes(), url=re.compile(r".*varray.*"))

    result = bounds_varray._adapter.read_data(bounds_varray, subset.bounds)

    request = httpx_mock.get_requests()[-1]
    assert unquote(request.url.path).endswith(f"/subset/{slices_to_str(subset.bounds)}/data")
    assert np.shape(result) == shape
    assert np.array_equal(result, expected)


@pytest.mark.parametrize("index_exp", valid_index_params)
def test_update_sends_the_region_of_the_bounds(bounds_varray: VArray, httpx_mock: HTTPXMock, index_exp):
    """The bounds string of the request carries the positions of the subset, the body is the region of its shape."""
    subset = bounds_varray[index_exp]
    shape = np.shape(np.empty(subset.shape))
    data = np.arange(np.prod(shape), dtype=bounds_varray.dtype).reshape(shape)
    httpx_mock.add_response(url=re.compile(r".*varray.*"))

    bounds_varray._adapter.update(bounds_varray, subset.bounds, data)

    request = httpx_mock.get_requests()[-1]
    assert request.method == "PUT"
    assert unquote(request.url.path).endswith(f"/subset/{slices_to_str(subset.bounds)}/data")
    assert request.content == data.tobytes()


@pytest.mark.parametrize("index_exp", invalid_index_params)
def test_invalid_bounds_never_reach_the_server(bounds_varray: VArray, httpx_mock: HTTPXMock, index_exp):
    """What DEKER refuses as bounds is refused by the subset, before any request."""
    with pytest.raises((IndexError, TypeError, ValueError, DekerValidationError)):
        bounds_varray[index_exp]
    assert not [request for request in httpx_mock.get_requests() if "/subset/" in request.url.path]


HELD_METHODS = [
    ("delete", ()),
    ("read_meta", ()),
    ("clear", (np.index_exp[:],)),
    ("update_meta_custom_attributes", ({"foo": "bar"},)),
    ("read_data", (np.index_exp[:],)),
    ("update", (np.index_exp[:], np.zeros(shape=(1,), dtype=np.int64))),
]
# the data methods of a VArray ask the node for the status of the file during a rebalancing, the others do not
STATUS_CHECKING_METHODS = {"clear", "read_data", "update"}


@pytest.mark.parametrize("lookup", ["by-id", "by-primary"])
def test_held_array_lookup_raises_the_lock_error_of_deker(
    lookup: str,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock: HTTPXMock,
    varray_collection: Collection,
    varray_collection_with_primary_attributes: Collection,
    primary_attributes: dict,
):
    """A 423 on a lookup is DekerLockError with the message of the server: a reader has met a writer."""
    message = "Array 0f8e is locked for update operation, cannot be read."
    httpx_mock.add_response(status_code=423, json={"code": 423, "message": message}, url=re.compile(rf".*/{lookup}/.*"))
    with pytest.raises(DekerLockError, match=re.escape(message)):
        if lookup == "by-id":
            server_varray_adapter.get_by_id("id", varray_collection, server_varray_adapter, None)
        else:
            server_varray_adapter.get_by_primary_attributes(
                primary_attributes,
                varray_collection_with_primary_attributes.varray_schema,
                varray_collection_with_primary_attributes,
                server_varray_adapter,
                None,
            )


@pytest.mark.parametrize(("method", "args"), HELD_METHODS)
def test_held_array_raises_the_lock_error_of_deker(
    method: str,
    args: tuple,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock: HTTPXMock,
    varray: VArray,
    request: pytest.FixtureRequest,
    mode: str,
):
    """A 423 of the server is DekerLockError with the message of the server, as a held array raises locally."""
    if mode == CLUSTER_MODE and method in STATUS_CHECKING_METHODS:
        request.getfixturevalue("mocked_filestatus_check_unmoved")
    message = "Array 0f8e is locked for update operation, cannot be read."
    httpx_mock.add_response(status_code=423, json={"code": 423, "message": message}, url=re.compile(r".*varray.*"))
    with pytest.raises(DekerLockError, match=re.escape(message)):
        getattr(server_varray_adapter, method)(varray, *args)


def test_written_partly_raises_the_group_error_of_deker(
    server_varray_adapter: ServerVarrayAdapter, httpx_mock: HTTPXMock, varray: VArray, mock_status: None
):
    """A 409 of the data route is DekerVSubsetError with the counts of the server; the causes stay on the server."""
    message = "VArray(0f8e)[:] is written partly: 1 of 8 Arrays failed, the others are written"
    httpx_mock.add_response(status_code=409, json={"code": 409, "message": message}, url=re.compile(r".*varray.*"))
    with pytest.raises(DekerVSubsetError, match=re.escape(message)) as info:
        server_varray_adapter.update(varray, np.index_exp[:], np.zeros(shape=(1,), dtype=np.int64))
    assert info.value.exceptions == []


UNAVAILABLE_ANSWERS = [
    pytest.param(
        "Cluster has no leader at the moment, try again later", "ClusterUnavailableError", "1", 1, id="no-leader"
    ),
    pytest.param("Cluster is rebalancing, try again later", "ClusterRebalancingError", "10", 10, id="rebalancing"),
    pytest.param("Node is out of sync and restarting, use another node", "RAFTNodeDesyncError", "5", 5, id="desync"),
    pytest.param("Service unavailable", "", None, None, id="a-proxy-without-the-header"),
]


@pytest.mark.parametrize(("method", "args"), HELD_METHODS)
@pytest.mark.parametrize(("message", "class_", "header", "retry_after"), UNAVAILABLE_ANSWERS)
def test_unavailable_raises_the_cluster_error_with_the_delay(
    method: str,
    args: tuple,
    message: str,
    class_: str,
    header: str | None,
    retry_after: int | None,
    server_varray_adapter: ServerVarrayAdapter,
    httpx_mock: HTTPXMock,
    varray: VArray,
    request: pytest.FixtureRequest,
    mode: str,
    caplog: pytest.LogCaptureFixture,
):
    """A 503 is DekerClusterError with the phrase of the server and the seconds of Retry-After; nothing is retried."""
    if mode == CLUSTER_MODE and method in STATUS_CHECKING_METHODS:
        request.getfixturevalue("mocked_filestatus_check_unmoved")
    headers = {"Retry-After": header} if header else {}
    body = {"code": 503, "message": message, "class": class_} if class_ else {"message": message}
    httpx_mock.add_response(status_code=503, json=body, headers=headers, url=re.compile(r".*varray.*"))
    with caplog.at_level(logging.ERROR, logger="deker_server_adapters.base"):
        with pytest.raises(DekerClusterError, match=re.escape(message)) as info:
            getattr(server_varray_adapter, method)(varray, *args)
    assert info.value.retry_after == retry_after
    # a read of the metadata falls over to the other nodes, as before; no node is asked twice
    nodes_asked = [str(r.url.netloc) for r in httpx_mock.get_requests() if "varray" in str(r.url)]
    assert len(nodes_asked) == len(set(nodes_asked))
    assert message in caplog.text
