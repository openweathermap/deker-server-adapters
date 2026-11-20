import json
import re

import pytest

from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from pytest_httpx import HTTPXMock

from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.errors import DekerClusterError, DekerServerError
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


def test_client_iter_success(
    collection: Collection, httpx_mock: HTTPXMock, collection_adapter: ServerCollectionAdapter, ctx
):
    httpx_mock.add_response(url=re.compile(r".*/v1/collections$"), json=[collection.as_dict], status_code=200)

    cols = []
    for col in collection_adapter:
        cols.append(col)

    assert cols == [json.loads(json.dumps(collection.as_dict))]


def test_is_deleted_is_always_false(collection_adapter: ServerCollectionAdapter, collection: Collection):
    assert collection_adapter.is_deleted(collection) is False


def test_client_iter_fails_when_collections_cannot_be_listed(
    httpx_mock: HTTPXMock, collection_adapter: ServerCollectionAdapter
):
    httpx_mock.add_response(url=re.compile(r".*/v1/collections$"), status_code=500)
    with pytest.raises(DekerServerError, match="list of collections"):
        list(collection_adapter)


@pytest.mark.parametrize(
    ("method", "args", "http_method"),
    [
        pytest.param("create", (), "POST", id="create"),
        pytest.param("delete", (), "DELETE", id="delete"),
    ],
)
def test_unavailable_raises_the_cluster_error_with_the_delay(
    method: str,
    args: tuple,
    http_method: str,
    collection_adapter: ServerCollectionAdapter,
    collection: Collection,
    httpx_mock: HTTPXMock,
):
    """A 503 on a collection write is DekerClusterError with the phrase of the server and the seconds to wait."""
    message = "Cluster has no leader at the moment, try again later"
    httpx_mock.add_response(
        status_code=503,
        method=http_method,
        json={"code": 503, "message": message, "class": "ClusterUnavailableError"},
        headers={"Retry-After": "1"},
        url=re.compile(r".*/v1/collection.*"),
    )
    with pytest.raises(DekerClusterError, match=re.escape(message)) as info:
        getattr(collection_adapter, method)(collection, *args)
    assert info.value.retry_after == 1
