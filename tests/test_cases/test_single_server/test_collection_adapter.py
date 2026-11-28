import json

import pytest

from cantok import DefaultToken
from deker.collection import Collection
from deker.errors import DekerAdapterClosedError, DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker_tools.typings import Sentinel
from pytest_httpx import HTTPXMock

from tests.conftest import operation_kwargs

from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.factory import AdaptersFactory


def test_collection_create(
    collection_adapter: ServerCollectionAdapter,
    adapter_factory: AdaptersFactory,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=201)
    assert collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken()) is None


@pytest.mark.parametrize("method", ["create", "delete", "clear", "is_deleted"])
def test_closed_collection_adapter_raises(
    method: str,
    collection_adapter: ServerCollectionAdapter,
    collection: Collection,
):
    """Test a collection adapter of a closed context answers with DekerAdapterClosedError."""
    collection_adapter.ctx.is_closed = True
    try:
        with pytest.raises(DekerAdapterClosedError):
            getattr(collection_adapter, method)(collection, **operation_kwargs(method))
        with pytest.raises(DekerAdapterClosedError):
            collection_adapter.read(collection.name)
        with pytest.raises(DekerAdapterClosedError):
            list(collection_adapter)
    finally:
        collection_adapter.ctx.is_closed = False


def test_closed_factory_raises(adapter_factory: AdaptersFactory, collection: Collection):
    """Test a closed factory marks its own context and gives no adapters."""
    adapter_factory.close()
    assert adapter_factory.ctx.is_closed
    with pytest.raises(DekerAdapterClosedError):
        adapter_factory.get_collection_adapter()
    with pytest.raises(DekerAdapterClosedError):
        adapter_factory.get_array_adapter(collection.path, adapter_factory.ctx.storage_adapter)


@pytest.mark.parametrize("method", ["create", "delete", "read", "clear"])
def test_collection_raises_500(
    method: str,
    collection_adapter: ServerCollectionAdapter,
    adapter_factory: AdaptersFactory,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=500)
    with pytest.raises(DekerServerError):
        getattr(collection_adapter, method)(collection, **operation_kwargs(method))


def test_collection_create_raises_already_exits(
    collection_adapter: ServerCollectionAdapter,
    adapter_factory: AdaptersFactory,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=400, json={"parameters": ["collection_name"]})
    with pytest.raises(DekerCollectionAlreadyExistsError):
        collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken())


def test_collection_delete_success(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response()
    assert collection_adapter.delete(collection=collection, **operation_kwargs("delete")) is None


def test_collection_delete_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.delete(collection=collection, **operation_kwargs("delete"))


def test_collection_read_success(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(json=collection.as_dict)
    assert collection_adapter.read(collection.name) == json.loads(json.dumps(collection.as_dict))


def test_collection_read_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.read(collection.name)


def test_collection_clear_success(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response()
    assert collection_adapter.clear(collection, **operation_kwargs("clear")) is None


def test_collection_clear_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.clear(collection, **operation_kwargs("clear"))
