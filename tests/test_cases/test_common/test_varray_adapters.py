import json
import re

from uuid import uuid4

import numpy as np
import pytest

from deker.arrays import VArray
from deker.collection import Collection
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
    array = server_varray_adapter.create(
        {
            **varray.as_dict,
            "adapter": server_varray_adapter,
            "array_adapter": server_array_adapter,
            "collection": varray_collection,
        }
    )
    assert array
    assert array.id == instance_id


def test_create_fails_no_id(
    httpx_mock: HTTPXMock,
    server_varray_adapter: ServerVarrayAdapter,
    varray: VArray,
    server_array_adapter: ServerArrayAdapter,
    varray_collection: Collection,
):
    httpx_mock.add_response(status_code=201)
    with pytest.raises(DekerServerError):
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
    (
        ("create", ()),
        ("delete", ()),
        ("read_meta", ()),
        ("clear", (np.index_exp[:],)),
        ("update_meta_custom_attributes", ({"foo": "bar"},)),
        ("read_data", (np.index_exp[:])),
        ("update", (np.index_exp[:], np.zeros(shape=(1,)))),
    ),
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
        if method == "create":
            varray = varray.as_dict
        call_args = (varray, *args)
        getattr(server_varray_adapter, method)(*call_args)


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
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(url=re.compile(r".*varray.*"))
    assert server_varray_adapter.update(varray, np.index_exp[:], data) is None


def test_update_deker_timeout(
    varray: VArray, httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*varray.*"))
    data = np.zeros(shape=(1,))

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
