import json
import re

import pytest

from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from pytest_httpx import HTTPXMock

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
    assert collection_adapter.create(collection) is None


@pytest.mark.parametrize("method", ("create", "delete", "read", "clear"))
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
        getattr(collection_adapter, method)(collection)


def test_collection_create_raises_already_exits(
    collection_adapter: ServerCollectionAdapter,
    adapter_factory: AdaptersFactory,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    """Test creation of collection."""
    httpx_mock.add_response(status_code=400, json={"parameters": ["collection_name"]})
    with pytest.raises(DekerCollectionAlreadyExistsError):
        collection_adapter.create(collection)


def test_collection_delete_success(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response()
    assert collection_adapter.delete(collection=collection) is None


def test_collection_delete_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.delete(collection=collection)


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
    assert collection_adapter.clear(collection) is None


def test_collection_clear_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.clear(collection)


@pytest.mark.xfail(reason="Request is not mocked properly")
def test_client_iter_success(
    collection: Collection, httpx_mock: HTTPXMock, collection_adapter: ServerCollectionAdapter, ctx
):
    httpx_mock.add_response(
        url=re.compile(f"{ctx.uri.raw_url.rstrip('/')}/v1/collections/"), json=[collection.as_dict], status_code=200
    )

    cols = []
    for col in collection_adapter:
        cols.append(col)

    assert cols == [json.loads(json.dumps(collection.as_dict))]
