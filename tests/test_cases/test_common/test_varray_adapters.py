import json
import re
import warnings

from uuid import uuid4

import numpy as np
import pytest

from deker.arrays import VArray
from deker.collection import Collection
from deker.errors import DekerArrayTypeError
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer
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
