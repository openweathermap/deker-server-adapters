import json
import re
import warnings

from functools import partial
from unittest.mock import patch
from urllib.parse import unquote
from uuid import uuid4

import httpx
import numpy as np
import pytest

from cantok import DefaultToken, SimpleToken
from deker import ArraySchema, AttributeSchema, DimensionSchema
from deker.arrays import Array
from deker.collection import Collection
from deker.ctx import CTX
from deker.errors import DekerArrayTypeError, DekerFilterError, DekerLockError, DekerTimeoutError, DekerValidationError
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.cancel import TokenCancelledError, cancellation_scope, write_token
from deker_tools.slices import slices_to_str
from deker_tools.typings import Sentinel, SentinelType
from httpx import ReadTimeout
from pytest_httpx import HTTPXMock

from tests.conftest import CLUSTER_MODE, FIRED_TOKENS, cancel_and_answer, fired_token, operation_kwargs
from tests.params.slices import invalid_index_params, valid_index_params

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
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        array = server_array_adapter.create(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]},
            timeout=Sentinel,
            token=DefaultToken(),
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
    with pytest.raises(DekerServerError), pytest.warns(DeprecationWarning, match="3.0.0"):
        server_array_adapter.create(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]},
            timeout=Sentinel,
            token=DefaultToken(),
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
        ("update", (np.index_exp[:], np.zeros(shape=(1,), dtype=np.int64))),
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
        getattr(server_array_adapter, method)(array, *args, **operation_kwargs(method))


def test_update_meta_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mode: str, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.update_meta_custom_attributes(array, {}, timeout=Sentinel, token=DefaultToken()) is None


def test_delete_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.delete(array, timeout=Sentinel, token=DefaultToken()) is None


def test_read_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    assert server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken()) == data


def test_read_non_array_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.array(1).tobytes(), url=re.compile(r".*subset.*"))
    res = server_array_adapter.read_data(array, np.index_exp[0], timeout=Sentinel, token=DefaultToken())
    assert isinstance(res, array.dtype)
    assert server_array_adapter.read_data(array, np.index_exp[0], timeout=Sentinel, token=DefaultToken()) == 1


def test_read_null_data_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.array([np.nan]).tobytes(), url=re.compile(r".*array.*"))
    with patch.object(array.schema, "dtype", np.float64):
        res = server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
    assert isinstance(res, np.ndarray)
    assert np.isnan(res)


def test_read_data_streams_the_body_in_chunks(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None, monkeypatch
):
    """An 8-byte int64 body arrives in three 3-byte chunks and lands in the result intact."""
    monkeypatch.setattr(base, "READ_CHUNK_SIZE", 3)
    data = np.array([123456789], dtype=array.dtype)
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    result = server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
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
        server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())


def test_read_data_timeout_while_streaming(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(content=np.zeros(shape=(1,)).tobytes(), url=re.compile(r".*array.*"))
    with patch.object(ServerArrayAdapter, "_fill_from_response", side_effect=ReadTimeout("slow node")):
        with pytest.raises(DekerTimeoutServer):
            server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())


def test_update_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    data = np.zeros(shape=(1,), dtype=array.dtype)
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.update(array, np.index_exp[:], data, timeout=Sentinel, token=DefaultToken()) is None


@pytest.fixture
def wide_array(
    adapter_factory: AdaptersFactory,
    collection_adapter: ServerCollectionAdapter,
    server_array_adapter: ServerArrayAdapter,
) -> Array:
    """A 2x3 int32 array: enough cells to tell a full write from a partial one and a scalar from a region."""
    schema = ArraySchema(
        dimensions=[DimensionSchema(name="y", size=2), DimensionSchema(name="x", size=3)], dtype=np.int32
    )
    collection = Collection(
        name="wide",
        schema=schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )
    return Array(collection, server_array_adapter)


UPDATE_BODY_CASES = [
    pytest.param(np.index_exp[:], np.arange(6, dtype=np.int32).reshape(2, 3), id="ndarray-whole"),
    pytest.param(np.index_exp[1], np.array([7, 8, 9], dtype=np.int32), id="ndarray-row"),
    pytest.param(np.index_exp[:, 1:], np.array([[1, 2], [3, 4]], dtype=np.int32), id="ndarray-region"),
    pytest.param(np.index_exp[:], [[1, 2, 3], [4, 5, 6]], id="list-whole"),
    pytest.param(np.index_exp[:], [[-(2**31), 0, 2**31 - 1], [1, 2, 3]], id="list-int32-bounds"),
    pytest.param(np.index_exp[0], (7, 8, 9), id="tuple-row"),
    pytest.param(np.index_exp[1, 2], 42, id="int-cell"),
    pytest.param(np.index_exp[1, 2], np.int32(42), id="numpy-scalar-cell"),
    pytest.param(np.index_exp[:], 5, id="int-fills-whole"),
    pytest.param(np.index_exp[0, :], 5, id="int-fills-row"),
    pytest.param(np.index_exp[::-1, ::2], np.array([[1, 2], [3, 4]], dtype=np.int32), id="ndarray-steps"),
]


@pytest.mark.parametrize(("bounds", "data"), UPDATE_BODY_CASES)
def test_update_sends_the_data_cast_to_the_array_dtype_and_shape(
    wide_array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None, bounds, data
):
    """Whatever the user passes, the body is the bytes of the region in the array dtype, as the local adapter stores."""
    httpx_mock.add_response(url=re.compile(r".*/array/.*"))
    expected = np.broadcast_to(np.asarray(data, dtype=wide_array.dtype), wide_array[bounds].shape)

    server_array_adapter.update(wide_array, bounds, data, timeout=Sentinel, token=DefaultToken())

    request = httpx_mock.get_requests()[-1]
    assert request.method == "PUT"
    assert request.headers["Content-Type"] == "application/octet-stream"
    assert request.content == expected.tobytes()
    assert np.frombuffer(request.content, dtype=wide_array.dtype).size == expected.size


UPDATE_INVALID_CASES = [
    pytest.param(np.index_exp[:], np.zeros((2, 3), dtype=np.float64), DekerArrayTypeError, id="wrong-ndarray-dtype"),
    pytest.param(np.index_exp[:], np.zeros((2, 3), dtype=np.int64), DekerArrayTypeError, id="wider-int-dtype"),
    pytest.param(np.index_exp[:], [[1.5, 2, 3], [4, 5, 6]], DekerArrayTypeError, id="list-with-float"),
    pytest.param(np.index_exp[0, 0], 1.5, DekerArrayTypeError, id="float-into-int-cell"),
    pytest.param(np.index_exp[0, 0], 2**31, DekerArrayTypeError, id="int-out-of-int32-range"),
    pytest.param(np.index_exp[0], [1, 2, -(2**31) - 1], DekerArrayTypeError, id="list-out-of-int32-range"),
    pytest.param(np.index_exp[0, 0], "7", DekerArrayTypeError, id="string"),
    pytest.param(np.index_exp[0, 0], {"a": 1}, DekerArrayTypeError, id="dict"),
    pytest.param(np.index_exp[:], np.zeros((3, 2), dtype=np.int32), IndexError, id="transposed-shape"),
    pytest.param(np.index_exp[0], np.zeros((2, 3), dtype=np.int32), IndexError, id="too-many-cells"),
    pytest.param(np.index_exp[:], [1, 2, 3], IndexError, id="list-wrong-shape"),
]


@pytest.mark.parametrize(("bounds", "data", "error"), UPDATE_INVALID_CASES)
def test_update_rejects_invalid_data_before_any_request(
    wide_array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, bounds, data, error
):
    """No status check and no PUT: the data is rejected before the adapter touches the network."""
    with pytest.raises(error):
        server_array_adapter.update(wide_array, bounds, data, timeout=Sentinel, token=DefaultToken())
    assert not [request for request in httpx_mock.get_requests() if request.method == "PUT"]


def test_clear_sends_an_empty_body(
    wide_array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*/array/.*"))
    server_array_adapter.clear(wide_array, np.index_exp[:, 1], timeout=Sentinel, token=DefaultToken())
    request = httpx_mock.get_requests()[-1]
    assert request.method == "PUT"
    assert request.content == b""


def test_clear_success(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, mock_status: None
):
    httpx_mock.add_response(url=re.compile(r".*array.*"))
    assert server_array_adapter.clear(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken()) is None


def test_delete_all_by_vid(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(json=[{"id": 2, "primary_attributes": {"vid": 1}}])
    assert server_array_adapter.delete_all_by_vid("1", collection, timeout=Sentinel, token=DefaultToken()) is None


def test_delete_all_by_raises(
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    httpx_mock.add_response(status_code=500, url=re.compile(r".*array.*"))
    with pytest.raises(DekerServerError):
        server_array_adapter.delete_all_by_vid("1", collection, timeout=Sentinel, token=DefaultToken())


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
    assert server_array_adapter.read_data(array, np.index_exp[0], timeout=Sentinel, token=DefaultToken()) == data[0]


def test_read_data_returns_writable_array(
    array: Array, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection, mock_status
):
    data = np.zeros(shape=(1,))
    httpx_mock.add_response(content=data.tobytes(), url=re.compile(r".*array.*"))
    read_data = server_array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
    read_data[0] = 0
    assert read_data[0] == 0


def test_create_from_array_instance(httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array):
    """An Array instance is sent as is and returned as is, without any warning."""
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={"id": array.id})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert server_array_adapter.create(array, timeout=Sentinel, token=DefaultToken()) is array
    request = httpx_mock.get_requests()[-1]
    assert request.method == "POST"
    assert json.loads(request.content)["id_"] == array.id


@pytest.mark.parametrize("status_code", [200, 400, 404, 500])
def test_create_from_array_instance_fails_on_any_status_but_created(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array, status_code: int
):
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=status_code, json={"id": array.id})
    with pytest.raises(DekerServerError):
        server_array_adapter.create(array, timeout=Sentinel, token=DefaultToken())


def test_create_from_dict_builds_the_array_from_the_response(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array, collection: Collection
):
    """The dict form lives in its own deprecated method; the id comes from the server response."""
    instance_id = str(uuid4())
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={"id": instance_id})
    data = array.as_dict
    with pytest.warns(DeprecationWarning, match="3.0.0"):
        created = server_array_adapter.create_from_dict(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": None},
            timeout=Sentinel,
            token=DefaultToken(),
        )
    assert isinstance(created, Array)
    assert created.id == instance_id
    assert created.primary_attributes == array.primary_attributes
    assert created.custom_attributes == array.custom_attributes


def test_create_with_a_dict_delegates_to_create_from_dict(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array, collection: Collection
):
    instance_id = str(uuid4())
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={"id": instance_id})
    data = {**array.as_dict, "adapter": server_array_adapter, "collection": collection, "id_": None}
    token = DefaultToken()
    with patch.object(ServerArrayAdapter, "create_from_dict", wraps=server_array_adapter.create_from_dict) as spy:
        with pytest.warns(DeprecationWarning, match="3.0.0"):
            created = server_array_adapter.create(data, timeout=Sentinel, token=token)
    spy.assert_called_once_with(data, timeout=Sentinel, token=token)
    assert created.id == instance_id


def test_create_response_without_id(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, array: Array, collection: Collection
):
    httpx_mock.add_response(url=re.compile(r".*/arrays$"), status_code=201, json={})
    data = array.as_dict
    with pytest.raises(DekerServerError, match="ID field"), pytest.warns(DeprecationWarning, match="3.0.0"):
        server_array_adapter.create(
            {**data, "adapter": server_array_adapter, "collection": collection, "id_": data["id"]},
            timeout=Sentinel,
            token=DefaultToken(),
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


class TestGetByPrimaryAttributesChecksTheAttributes:
    """The whole-array search refuses an empty or incomplete set of attributes before asking the server."""

    def test_a_schema_without_primary_attributes_is_refused(
        self, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection
    ):
        with pytest.raises(DekerFilterError, match="primary attributes"):
            server_array_adapter.get_by_primary_attributes(
                {}, collection.array_schema, collection, server_array_adapter, None
            )
        assert not [r for r in httpx_mock.get_requests() if "by-primary" in str(r.url)]

    def test_incomplete_attributes_are_refused(
        self,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_primary_attributes: Collection,
    ):
        collection = collection_with_primary_attributes
        with pytest.raises(DekerFilterError, match="is missing"):
            server_array_adapter.get_by_primary_attributes(
                {}, collection.array_schema, collection, server_array_adapter, None
            )
        assert not [r for r in httpx_mock.get_requests() if "by-primary" in str(r.url)]


class TestGetIdByPrimaryAttributes:
    """The id by the primary attributes comes from the same request as the whole array."""

    def test_returns_the_id_of_the_found_array(
        self,
        array_with_attributes: Array,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_primary_attributes: Collection,
        primary_attributes: dict,
    ):
        httpx_mock.add_response(json=array_with_attributes.as_dict, url=re.compile(r".*/by-primary/.*"))
        found = server_array_adapter.get_id_by_primary_attributes(
            primary_attributes, collection_with_primary_attributes.array_schema
        )
        assert found == array_with_attributes.id

    def test_returns_none_when_the_server_has_no_such_array(
        self,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_primary_attributes: Collection,
        primary_attributes: dict,
    ):
        httpx_mock.add_response(status_code=404, url=re.compile(r".*/by-primary/.*"))
        assert (
            server_array_adapter.get_id_by_primary_attributes(
                primary_attributes, collection_with_primary_attributes.array_schema
            )
            is None
        )

    def test_raises_on_a_server_error(
        self,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_primary_attributes: Collection,
        primary_attributes: dict,
    ):
        httpx_mock.add_response(status_code=500, url=re.compile(r".*/by-primary/.*"))
        with pytest.raises(DekerServerError):
            server_array_adapter.get_id_by_primary_attributes(
                primary_attributes, collection_with_primary_attributes.array_schema
            )

    def test_a_schema_without_primary_attributes_is_refused(
        self, httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection
    ):
        """The server is not even asked: nothing identifies an array without primary attributes."""
        with pytest.raises(DekerFilterError, match="primary attributes"):
            server_array_adapter.get_id_by_primary_attributes({}, collection.array_schema)
        assert not [r for r in httpx_mock.get_requests() if "by-primary" in str(r.url)]

    def test_incomplete_attributes_are_refused(
        self,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_primary_attributes: Collection,
    ):
        with pytest.raises(DekerFilterError, match="is missing"):
            server_array_adapter.get_id_by_primary_attributes({}, collection_with_primary_attributes.array_schema)
        assert not [r for r in httpx_mock.get_requests() if "by-primary" in str(r.url)]

    def test_asks_by_the_same_path_as_the_whole_array(
        self,
        httpx_mock: HTTPXMock,
        server_array_adapter: ServerArrayAdapter,
        collection_with_tuple_and_custom_attributes: Collection,
    ):
        collection = collection_with_tuple_and_custom_attributes
        httpx_mock.add_response(status_code=404, url=re.compile(r".*/by-primary/.*"))
        server_array_adapter.get_id_by_primary_attributes({"pos": [1, 2]}, collection.array_schema)
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
        server_array_adapter.delete_all_by_vid("v", collection, timeout=Sentinel, token=DefaultToken())


def test_delete_all_by_vid_fails_when_arrays_cannot_be_listed(
    httpx_mock: HTTPXMock, server_array_adapter: ServerArrayAdapter, collection: Collection
):
    httpx_mock.add_response(method="GET", url=re.compile(r".*/arrays/$"), status_code=500)
    with pytest.raises(DekerServerError, match="list of the arrays"):
        server_array_adapter.delete_all_by_vid("v", collection, timeout=Sentinel, token=DefaultToken())


@pytest.mark.parametrize("index_exp", valid_index_params)
def test_read_data_asks_the_server_for_the_bounds_as_positions(bounds_array: Array, httpx_mock: HTTPXMock, index_exp):
    """The bounds string of the request carries the positions of the subset, and the body streams into its shape.

    A label, a datetime or a timedelta is converted to positions by the subset, before the adapter: the server
    never sees them.
    """
    subset = bounds_array[index_exp]
    shape = np.shape(np.empty(subset.shape))
    expected = np.arange(np.prod(shape), dtype=bounds_array.dtype).reshape(shape)
    httpx_mock.add_response(content=expected.tobytes(), url=re.compile(r".*array.*"))

    result = bounds_array._adapter.read_data(bounds_array, subset.bounds, timeout=Sentinel, token=DefaultToken())

    request = httpx_mock.get_requests()[-1]
    assert unquote(request.url.path).endswith(f"/subset/{slices_to_str(subset.bounds)}/data")
    assert np.shape(result) == shape
    assert np.array_equal(result, expected)


@pytest.mark.parametrize("index_exp", valid_index_params)
def test_update_sends_the_region_of_the_bounds(bounds_array: Array, httpx_mock: HTTPXMock, index_exp):
    """The bounds string of the request carries the positions of the subset, the body is the region of its shape."""
    subset = bounds_array[index_exp]
    shape = np.shape(np.empty(subset.shape))
    data = np.arange(np.prod(shape), dtype=bounds_array.dtype).reshape(shape)
    httpx_mock.add_response(url=re.compile(r".*array.*"))

    bounds_array._adapter.update(bounds_array, subset.bounds, data, timeout=Sentinel, token=DefaultToken())

    request = httpx_mock.get_requests()[-1]
    assert request.method == "PUT"
    assert unquote(request.url.path).endswith(f"/subset/{slices_to_str(subset.bounds)}/data")
    assert request.content == data.tobytes()


@pytest.mark.parametrize("index_exp", invalid_index_params)
def test_invalid_bounds_never_reach_the_server(bounds_array: Array, httpx_mock: HTTPXMock, index_exp):
    """What DEKER refuses as bounds is refused by the subset, before any request."""
    with pytest.raises((IndexError, TypeError, ValueError, DekerValidationError)):
        bounds_array[index_exp]
    assert not [request for request in httpx_mock.get_requests() if "/subset/" in request.url.path]


HELD_METHODS = [
    ("delete", ()),
    ("read_meta", ()),
    ("clear", (np.index_exp[:],)),
    ("update_meta_custom_attributes", ({"foo": "bar"},)),
    ("read_data", (np.index_exp[:],)),
    ("update", (np.index_exp[:], np.zeros(shape=(1,), dtype=np.int64))),
]


@pytest.mark.parametrize("lookup", ["by-id", "by-primary"])
def test_held_array_lookup_raises_the_lock_error_of_deker(
    lookup: str,
    server_array_adapter: ServerArrayAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
    collection_with_primary_attributes: Collection,
    primary_attributes: dict,
):
    """A 423 on a lookup is DekerLockError with the message of the server: a reader has met a writer."""
    message = "Array 0f8e is locked for update operation, cannot be read."
    httpx_mock.add_response(status_code=423, json={"code": 423, "message": message}, url=re.compile(rf".*/{lookup}/.*"))
    with pytest.raises(DekerLockError, match=re.escape(message)):
        if lookup == "by-id":
            server_array_adapter.get_by_id("id", collection, server_array_adapter, None)
        else:
            server_array_adapter.get_by_primary_attributes(
                primary_attributes,
                collection_with_primary_attributes.array_schema,
                collection_with_primary_attributes,
                server_array_adapter,
                None,
            )


@pytest.mark.parametrize(
    "body", [pytest.param("held by a proxy", id="text"), pytest.param('{"code": 423}', id="no-message")]
)
def test_held_array_without_a_message_raises_the_body_of_the_server(
    body: str, server_array_adapter: ServerArrayAdapter, httpx_mock: HTTPXMock, array: Array, mock_status: None
):
    """A 423 whose body is not the JSON of the server is DekerLockError with the body as it is."""
    httpx_mock.add_response(status_code=423, text=body, url=re.compile(r".*array.*"))
    with pytest.raises(DekerLockError, match=re.escape(body)):
        server_array_adapter.read_meta(array)


@pytest.mark.parametrize(("method", "args"), HELD_METHODS)
def test_held_array_raises_the_lock_error_of_deker(
    method: str,
    args: tuple,
    server_array_adapter: ServerArrayAdapter,
    httpx_mock: HTTPXMock,
    array: Array,
    request: pytest.FixtureRequest,
    mode: str,
):
    """A 423 of the server is DekerLockError with the message of the server, as a held array raises locally."""
    if mode == CLUSTER_MODE:
        request.getfixturevalue("mocked_filestatus_check_unmoved")
    message = "Array 0f8e is locked for update operation, cannot be read."
    httpx_mock.add_response(status_code=423, json={"code": 423, "message": message}, url=re.compile(r".*array.*"))
    with pytest.raises(DekerLockError, match=re.escape(message)):
        getattr(server_array_adapter, method)(array, *args, **operation_kwargs(method))


# The operations of the Array adapter: the method, its arguments after the Array, the status and the body of the
# answer of the server on success, and the setting of the client the timeout of the request defaults to.
TIMED_CALLS = [
    pytest.param("create", (), 201, b"", "write_lock_timeout", id="create"),
    pytest.param("delete", (), 200, b"", "write_lock_timeout", id="delete"),
    pytest.param("update_meta_custom_attributes", ({"foo": "bar"},), 200, b"", "write_lock_timeout", id="attributes"),
    pytest.param(
        "read_data", (np.index_exp[:],), 200, np.zeros(1, dtype=np.int64).tobytes(), "read_timeout", id="read"
    ),
    pytest.param("update", (np.index_exp[:], np.zeros(1, dtype=np.int64)), 200, b"", "write_lock_timeout", id="update"),
    pytest.param("clear", (np.index_exp[:],), 200, b"", "write_lock_timeout", id="clear"),
]
WRITE_CALLS = [param for param in TIMED_CALLS if param.id != "read"]


def expect_status_check(method: str, mode: str, request: pytest.FixtureRequest) -> None:
    """Mock the status check a request of a rebalancing cluster makes first; the creation of an Array makes none."""
    if mode == CLUSTER_MODE and method != "create":
        request.getfixturevalue("mocked_filestatus_check_unmoved")


def array_requests(httpx_mock: HTTPXMock) -> list[httpx.Request]:
    """The requests of the Array routes: the ping of the factory and the status checks of a rebalancing left out."""
    return [r for r in httpx_mock.get_requests() if "/array" in r.url.path and not r.url.path.endswith("/status")]


@pytest.mark.parametrize(("method", "args", "status", "body", "setting"), TIMED_CALLS)
@pytest.mark.parametrize(
    ("timeout", "parameter"),
    [pytest.param(30, "30", id="given"), pytest.param(Sentinel, None, id="not-given")],
)
def test_the_timeout_of_the_call_goes_to_the_server(
    method: str,
    args: tuple,
    status: int,
    body: bytes,
    setting: str,
    timeout: int | SentinelType,
    parameter: str | None,
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    ctx: CTX,
    mode: str,
    request: pytest.FixtureRequest,
):
    """A given timeout is the query parameter; the http client waits for it, or for the setting, and 5 s more."""
    expect_status_check(method, mode, request)
    httpx_mock.add_response(url=re.compile(r".*/array.*"), status_code=status, content=body)

    getattr(server_array_adapter, method)(array, *args, timeout=timeout, token=DefaultToken())

    [sent] = array_requests(httpx_mock)
    assert sent.url.params.get("timeout") == parameter
    seconds = 35 if timeout == 30 else getattr(ctx.config, setting) + 5
    assert sent.extensions["timeout"] == httpx.Timeout(seconds).as_dict()


@pytest.mark.parametrize(("method", "args", "status", "body", "setting"), TIMED_CALLS)
@pytest.mark.parametrize(("kind", "error"), FIRED_TOKENS)
def test_a_fired_token_sends_nothing(
    method: str,
    args: tuple,
    status: int,
    body: bytes,
    setting: str,
    kind: str,
    error: type[TokenCancelledError],
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
):
    sent = len(httpx_mock.get_requests())

    with pytest.raises(error) as info:
        getattr(server_array_adapter, method)(array, *args, timeout=Sentinel, token=fired_token(kind))

    assert type(info.value) is error
    assert httpx_mock.get_requests()[sent:] == []


@pytest.mark.parametrize(("method", "args", "status", "body", "setting"), WRITE_CALLS)
def test_a_write_cancelled_once_sent_goes_to_its_end(
    method: str,
    args: tuple,
    status: int,
    body: bytes,
    setting: str,
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    mode: str,
    request: pytest.FixtureRequest,
):
    """The start flag is closed before the request: the client going away during it cancels nothing."""
    expect_status_check(method, mode, request)
    scope = SimpleToken()
    httpx_mock.add_callback(partial(cancel_and_answer, scope, status, body), url=re.compile(r".*/array.*"))
    with cancellation_scope(scope):
        token = write_token()

    getattr(server_array_adapter, method)(array, *args, timeout=Sentinel, token=token)

    assert scope.cancelled
    assert not token.cancelled
    assert len(array_requests(httpx_mock)) == 1


@pytest.mark.parametrize(("method", "args", "status", "body", "setting"), WRITE_CALLS)
def test_a_write_cancelled_before_it_is_sent_sends_nothing(
    method: str,
    args: tuple,
    status: int,
    body: bytes,
    setting: str,
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
):
    scope = SimpleToken()
    with cancellation_scope(scope):
        token = write_token()
    scope.cancel()

    with pytest.raises(TokenCancelledError):
        getattr(server_array_adapter, method)(array, *args, timeout=Sentinel, token=token)

    assert array_requests(httpx_mock) == []


@pytest.mark.parametrize(("method", "args", "status", "body", "setting"), TIMED_CALLS)
def test_a_gateway_timeout_is_the_timeout_error_of_deker(
    method: str,
    args: tuple,
    status: int,
    body: bytes,
    setting: str,
    array: Array,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    mode: str,
    request: pytest.FixtureRequest,
):
    """A 504 of the server is the read timeout of the operation: DekerTimeoutError with the phrase of the server."""
    expect_status_check(method, mode, request)
    message = "Read timeout of 30 seconds is over"
    httpx_mock.add_response(url=re.compile(r".*/array.*"), status_code=504, json={"code": 504, "message": message})

    with pytest.raises(DekerTimeoutError, match=message):
        getattr(server_array_adapter, method)(array, *args, timeout=30, token=DefaultToken())


VID_ARRAYS = [
    {"id": "tile-1", "primary_attributes": {"vid": "v"}},
    {"id": "tile-2", "primary_attributes": {"vid": "v"}},
    {"id": "other", "primary_attributes": {"vid": "another"}},
]


@pytest.mark.parametrize(
    ("timeout", "parameter", "seconds"),
    [pytest.param(30, "30", 35, id="given"), pytest.param(Sentinel, None, 6, id="not-given")],
)
def test_delete_all_by_vid_sends_the_timeout_with_every_deletion(
    timeout: int | SentinelType,
    parameter: str | None,
    seconds: int,
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    """Every Array is deleted with the timeout of the call; not given - with ``write_lock_timeout`` (1 s) and 5 s."""
    httpx_mock.add_response(method="GET", url=re.compile(r".*/arrays/$"), json=VID_ARRAYS)
    httpx_mock.add_response(method="DELETE", url=re.compile(r".*/by-id/.*"))

    server_array_adapter.delete_all_by_vid("v", collection, timeout=timeout, token=DefaultToken())

    deletions = httpx_mock.get_requests(method="DELETE")
    assert sorted(request.url.path.rsplit("/", 1)[-1] for request in deletions) == ["tile-1", "tile-2"]
    assert [request.url.params.get("timeout") for request in deletions] == [parameter, parameter]
    assert [request.extensions["timeout"] for request in deletions] == [httpx.Timeout(seconds).as_dict()] * 2


@pytest.mark.parametrize(("kind", "error"), FIRED_TOKENS)
def test_delete_all_by_vid_with_a_fired_token_sends_nothing(
    kind: str,
    error: type[TokenCancelledError],
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    sent = len(httpx_mock.get_requests())

    with pytest.raises(error):
        server_array_adapter.delete_all_by_vid("v", collection, timeout=Sentinel, token=fired_token(kind))

    assert httpx_mock.get_requests()[sent:] == []


@pytest.mark.parametrize(
    ("cancelled_on", "deleted"),
    [
        pytest.param("GET", [], id="while-listing-deletes-nothing"),
        pytest.param("DELETE", ["tile-1", "tile-2"], id="once-deleting-deletes-all"),
    ],
)
def test_delete_all_by_vid_cancelled_on_the_way(
    cancelled_on: str,
    deleted: list[str],
    httpx_mock: HTTPXMock,
    server_array_adapter: ServerArrayAdapter,
    collection: Collection,
):
    """Cancelled before the first Array is started, nothing is deleted; after, every Array is deleted."""
    scope = SimpleToken()
    listing = json.dumps(VID_ARRAYS).encode()
    if cancelled_on == "GET":
        httpx_mock.add_callback(partial(cancel_and_answer, scope, 200, listing), url=re.compile(r".*/arrays/$"))
    else:
        httpx_mock.add_response(method="GET", url=re.compile(r".*/arrays/$"), json=VID_ARRAYS)
        httpx_mock.add_callback(partial(cancel_and_answer, scope, 200, b""), url=re.compile(r".*/by-id/tile-1$"))
        httpx_mock.add_response(method="DELETE", url=re.compile(r".*/by-id/tile-2$"))
    with cancellation_scope(scope):
        token = write_token()

    if deleted:
        server_array_adapter.delete_all_by_vid("v", collection, timeout=Sentinel, token=token)
    else:
        with pytest.raises(TokenCancelledError):
            server_array_adapter.delete_all_by_vid("v", collection, timeout=Sentinel, token=token)

    assert scope.cancelled
    paths = sorted(request.url.path.rsplit("/", 1)[-1] for request in httpx_mock.get_requests(method="DELETE"))
    assert paths == deleted
