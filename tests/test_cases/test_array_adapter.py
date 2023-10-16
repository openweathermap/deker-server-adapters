import json
from typing import List

from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pytest

from deker.arrays import Array
from deker.collection import Collection
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer


def test_create_success(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    array: Array,
    collection: Collection,
):
    instance_id = str(uuid4())
    httpx_mock.add_response(status_code=201, json={"id": instance_id})
    array = server_array_adapter.create({**array.as_dict, "adapter": server_array_adapter, "collection": collection})
    assert array
    assert array.id == instance_id


def test_create_fails_no_id(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    array: Array,
    collection: Collection,
):
    httpx_mock.add_response(status_code=201)
    with pytest.raises(DekerServerError):
        server_array_adapter.create({**array.as_dict, "adapter": server_array_adapter, "collection": collection})


@pytest.mark.parametrize(
    "method, args",
    (
        ("create", tuple()),
        ("delete", tuple()),
        ("read_meta", tuple()),
        ("clear", (np.index_exp[:],)),
        ("update_meta_custom_attributes", ({"foo": "bar"},)),
        (
            "read_data",
            (np.index_exp[:]),
        ),
        ("update", (np.index_exp[:], np.zeros(shape=(1,)))),
    ),
)
def test_collection_raises_500(
    method: str,
    args: tuple,
    server_array_adapter: ServerArrayAdapter,
    httpx_mock: HTTPXMock,
    array: Array,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        if method == "create":
            array = array.as_dict
        call_args = (array, *args)
        getattr(server_array_adapter, method)(*call_args)


def test_read_meta_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(json=array.as_dict)
    assert server_array_adapter.read_meta(array) == json.loads(json.dumps(array.as_dict))


def test_update_meta_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response()
    assert server_array_adapter.update_meta_custom_attributes(array, {}) is None


def test_delete_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response()
    assert server_array_adapter.delete(array) is None


def test_read_data_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes())
    assert server_array_adapter.read_data(array, np.index_exp[:]) == data


def test_read_non_array_data_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(content=np.array(1).tobytes())
    res = server_array_adapter.read_data(array, np.index_exp[0])
    print(res)
    assert isinstance(res, array.dtype)
    assert server_array_adapter.read_data(array, np.index_exp[0]) == 1


def test_read_null_data_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(content=np.array([np.nan]).tobytes())
    with patch.object(array.schema, "dtype", np.float64):
        res = server_array_adapter.read_data(array, np.index_exp[:])
    assert isinstance(res, np.ndarray)
    assert np.isnan(res)


def test_read_data_deker_timeout(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(status_code=504)
    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.read_data(array, np.index_exp[:])


def test_update_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response()
    assert server_array_adapter.update(array, np.index_exp[:], data) is None


def test_update_deker_timeout(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(status_code=504)
    data = np.zeros(shape=(1,))

    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.update(array, np.index_exp[:], data)


def test_clear_success(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response()
    assert server_array_adapter.clear(array, np.index_exp[:]) is None


def test_clear_deker_timeout(array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter):
    httpx_mock.add_response(status_code=504)

    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.clear(array, np.index_exp[:])


def test_delete_all_by_vid(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(json=[{"id": 2, "primary_attributes": {"vid": 1}}])
    assert server_array_adapter.delete_all_by_vid("1", collection) is None


def test_delete_all_by_raises(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        server_array_adapter.delete_all_by_vid("1", collection)


def test_by_primary(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(json=array.as_dict)
    assert (
        server_array_adapter.get_by_primary_attributes(
            {}, collection.array_schema, collection, server_array_adapter, None
        ).as_dict
        == array.as_dict
    )


def test_by_id(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(json=array.as_dict)
    assert server_array_adapter.get_by_id("id", collection, server_array_adapter, None).as_dict == array.as_dict


def test_by_id_fail(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        server_array_adapter.get_by_id("id", collection, server_array_adapter, None)


def test_iter_fail(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        for _ in server_array_adapter:
            pass


def test_iter_success(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
):
    httpx_mock.add_response(json=[array.as_dict])
    arrays = []
    for array_ in server_array_adapter:
        arrays.append(array_)

    assert arrays == [json.loads(json.dumps(array.as_dict))]



def test_get_node_by_id(array: Array, server_array_adapter: ServerArrayAdapter, nodes: List[str]):
    with patch.object(array, "primary_attributes", {}):
        # Check window slides

        node = server_array_adapter.get_node(array)
        assert node in nodes
     


def test_get_node_by_primary(array: Array, server_array_adapter: ServerArrayAdapter, nodes: List[str]):
    with patch.object(array, "primary_attributes", {"foo": "bar"}):
        # Check window slides

        node = server_array_adapter.get_node(array)
        assert node in nodes


def test_get_node_give_same_result(array: Array, server_array_adapter: ServerArrayAdapter, nodes: List[str]):
    first_node = server_array_adapter.get_node(array)
    for _ in range(10):
        node = server_array_adapter.get_node(array)
        assert node == first_node

