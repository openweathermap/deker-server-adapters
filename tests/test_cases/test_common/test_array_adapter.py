import re

from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pytest

from deker.arrays import Array
from deker.collection import Collection
from pytest_httpx import HTTPXMock

from tests.conftest import CLUSTER_MODE

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer


def test_create_success(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    array: Array,
    collection: Collection,
):
    instance_id = str(uuid4())
    httpx_mock.add_response(url=re.compile(r".*array.*"), status_code=201, json={"id": instance_id})
    data = array.as_dict
    array = server_array_adapter.create(
        {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]}
    )
    assert array
    assert array.id == instance_id


def test_create_fails_no_id(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    array: Array,
    collection: Collection,
):
    httpx_mock.add_response(status_code=201, url=re.compile(r".*array.*"))
    data = array.as_dict
    with pytest.raises(DekerServerError):
        server_array_adapter.create(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]}
        )


@pytest.mark.parametrize(
    ("method", "args"),
    (
        ("create", ()),
        ("delete", ()),
        ("read_meta", ()),
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
    request: pytest.FixtureRequest,
    mode: str,
):
    """Test creation of collection."""
    if mode == CLUSTER_MODE and method != "create":
        request.getfixturevalue("mocked_filestatus_check_unmoved")

    httpx_mock.add_response(status_code=500, url=re.compile(r".*array.*"))
    with pytest.raises(DekerServerError):
        if method == "create":
            array = array.as_dict
            array["id_"] = array["id"]
        call_args = (array, *args)
        getattr(server_array_adapter, method)(*call_args)


def test_update_meta_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mode: str, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.update_meta_custom_attributes(array, {}) is None


def test_delete_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.delete(array) is None


def test_read_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    assert server_array_adapter.read_data(array, np.index_exp[:]) == data


def test_read_non_array_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.array(1).tobytes(), url=re.compile(r".*array.*"))
    res = server_array_adapter.read_data(array, np.index_exp[0])
    assert isinstance(res, array.dtype)
    assert server_array_adapter.read_data(array, np.index_exp[0]) == 1


def test_read_null_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.array([np.nan]).tobytes(), url=re.compile(r".*array.*"))
    with patch.object(array.schema, "dtype", np.float64):
        res = server_array_adapter.read_data(array, np.index_exp[:])
    assert isinstance(res, np.ndarray)
    assert np.isnan(res)


def test_read_data_deker_timeout(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*array.*"))
    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.read_data(array, np.index_exp[:])


def test_update_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.update(array, np.index_exp[:], data) is None


def test_update_deker_timeout(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*array.*"))
    data = np.zeros(shape=(1,))

    with pytest.raises(DekerTimeoutServer):
        server_array_adapter.update(array, np.index_exp[:], data)


def test_clear_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.clear(array, np.index_exp[:]) is None


def test_clear_deker_timeout(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(status_code=504, url=re.compile(r".*array.*"))

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
    httpx_mock.add_response(status_code=500, url=re.compile(r".*array.*"))
    with pytest.raises(DekerServerError):
        server_array_adapter.delete_all_by_vid("1", collection)


def test_by_primary(
    array_with_attributes: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection_with_primary_attributes: Collection,
    primary_attributes,
):
    httpx_mock.add_response(json=array_with_attributes.as_dict, url=re.compile(r".*array.*"))
    assert (
        server_array_adapter.get_by_primary_attributes(
            primary_attributes,
            collection_with_primary_attributes.array_schema,
            collection_with_primary_attributes,
            server_array_adapter,
            None,
        ).as_dict
        == array_with_attributes.as_dict
    )


def test_by_id(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(json=array.as_dict, url=re.compile(r".*array.*"))
    assert server_array_adapter.get_by_id("id", collection, server_array_adapter, None).as_dict == array.as_dict


def test_by_id_fail(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(status_code=500, url=re.compile(r".*array.*"))
    with pytest.raises(DekerServerError):
        server_array_adapter.get_by_id("id", collection, server_array_adapter, None)


def test_read_data_single_number(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection, mock_status
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    assert server_array_adapter.read_data(array, np.index_exp[0]) == data[0]
