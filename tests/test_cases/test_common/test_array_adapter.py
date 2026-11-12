import json
import re

from unittest.mock import patch
from urllib.parse import unquote
from uuid import uuid4

import numpy as np
import pytest

from deker import ArraySchema, AttributeSchema, DimensionSchema
from deker.arrays import Array
from deker.collection import Collection
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from httpx import ReadTimeout
from pytest_httpx import HTTPXMock

from tests.conftest import CLUSTER_MODE

from deker_server_adapters import base
from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer
from deker_server_adapters.factory import AdaptersFactory
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


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
    [
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
    ],
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
    httpx_mock.add_response(content=np.array(1).tobytes(), url=re.compile(r".*subset.*"))
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


def test_read_data_streams_the_body_in_chunks(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None, monkeypatch
):
    """An 8-byte int64 body arrives in three 3-byte chunks and lands in the result intact."""
    monkeypatch.setattr(base, "READ_CHUNK_SIZE", 3)
    data = np.array([123456789], dtype=array.dtype)
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    result = server_array_adapter.read_data(array, np.index_exp[:])
    assert np.array_equal(result, data)
    assert result.flags.writeable


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (np.zeros(shape=(1,)).tobytes()[:-1], "7 bytes for the subset, 8 expected"),
        (np.zeros(shape=(2,)).tobytes(), "more than 8 bytes"),
    ],
)
def test_read_data_body_size_must_match_the_subset(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None, body, message
):
    httpx_mock.add_response(content=body, url=re.compile(r".*array.*"))
    with pytest.raises(DekerServerError, match=message):
        server_array_adapter.read_data(array, np.index_exp[:])


def test_read_data_timeout_while_streaming(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.zeros(shape=(1,)).tobytes(), url=re.compile(r".*array.*"))
    with patch.object(ServerArrayAdapter, "_fill_from_response", side_effect=ReadTimeout("slow node")):
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


def test_read_data_returns_writable_array(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection, mock_status
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    read_data = server_array_adapter.read_data(array, np.index_exp[:])
    read_data[0] = 0
    assert read_data[0] == 0


def test_create_from_array_instance(httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array):
    """An Array instance is sent as is and returned as is."""
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={"id": array.id})
    assert server_array_adapter.create(array) is array
    request = httpx_mock.get_requests()[-1]
    assert request.method == "POST"
    assert json.loads(request.content)["id_"] == array.id


def test_create_response_without_id(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array, collection: Collection
):
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={})
    data = array.as_dict
    with pytest.raises(DekerServerError, match="ID field"):
        server_array_adapter.create(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]}
        )


def test_is_deleted_is_always_false(server_array_adapter: ServerArrayAdapter, array: Array):
    assert server_array_adapter.is_deleted(array) is False


@pytest.fixture
def collection_with_tuple_and_custom_attributes(
    adapter_factory: AdaptersFactory, collection_adapter: ServerCollectionAdapter
) -> Collection:
    schema = ArraySchema(
        dimensions=[DimensionSchema(name="x", size=1)],
        attributes=[
            AttributeSchema(name="pos", dtype=tuple, primary=True),
            AttributeSchema(name="note", dtype=str, primary=False),
        ],
        dtype=int,
    )
    return Collection(
        name="test",
        schema=schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )


def test_by_primary_uses_only_primary_attributes_and_stringifies_tuples(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection_with_tuple_and_custom_attributes: Collection,
):
    """A tuple attribute goes into the path as ``str(tuple)``, a custom attribute does not go at all."""
    collection = collection_with_tuple_and_custom_attributes
    httpx_mock.add_response(status_code=404, url=re.compile(r".*/by-primary/.*"))
    found = server_array_adapter.get_by_primary_attributes(
        {"pos": [1, 2]}, collection.array_schema, collection, server_array_adapter, None
    )
    assert found is None
    assert unquote(str(httpx_mock.get_requests()[-1].url)).endswith("/array/by-primary/(1, 2)")


def test_by_primary_fail(
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection_with_primary_attributes: Collection,
    primary_attributes: dict,
):
    httpx_mock.add_response(status_code=500, url=re.compile(r".*/by-primary/.*"))
    with pytest.raises(DekerServerError):
        server_array_adapter.get_by_primary_attributes(
            primary_attributes,
            collection_with_primary_attributes.array_schema,
            collection_with_primary_attributes,
            server_array_adapter,
            None,
        )


def test_by_id_not_found(httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection):
    httpx_mock.add_response(status_code=404, url=re.compile(r".*/by-id/.*"))
    assert server_array_adapter.get_by_id("id", collection, server_array_adapter, None) is None


def test_iter_skips_a_node_that_fails_to_list_arrays(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, caplog: pytest.LogCaptureFixture
):
    httpx_mock.add_response(status_code=500, url=re.compile(r".*/arrays$"))
    assert list(server_array_adapter) == []
    assert "Couldn't fetch arrays from node" in caplog.text


def test_iter_raises_when_varrays_cannot_be_listed(httpx_mock: HTTPXMock, server_varray_adapter: ServerVarrayAdapter):
    httpx_mock.add_response(status_code=500, url=re.compile(r".*/varrays$"))
    with pytest.raises(DekerServerError, match="list of arrays"):
        list(server_varray_adapter)


def test_delete_all_by_vid_reports_arrays_that_failed_to_delete(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection
):
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/arrays/$"),
        json=[
            {"id": "tile-1", "primary_attributes": {"vid": "v"}},
            {"id": "tile-2", "primary_attributes": {"vid": "v"}},
            {"id": "other", "primary_attributes": {"vid": "another"}},
        ],
    )
    httpx_mock.add_response(method="DELETE", url=re.compile(r".*/by-id/tile-1$"), status_code=200)
    httpx_mock.add_response(method="DELETE", url=re.compile(r".*/by-id/tile-2$"), status_code=500)
    with pytest.raises(DekerServerError, match=r"Failed arrays: \['tile-2'\]"):
        server_array_adapter.delete_all_by_vid("v", collection)


def test_delete_all_by_vid_fails_when_arrays_cannot_be_listed(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection
):
    httpx_mock.add_response(method="GET", url=re.compile(r".*/arrays/$"), status_code=500)
    with pytest.raises(DekerServerError, match="list of the arrays"):
        server_array_adapter.delete_all_by_vid("v", collection)
