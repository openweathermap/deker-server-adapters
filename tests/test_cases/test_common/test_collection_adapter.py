import json
import re

from functools import partial

import httpx
import pytest

from cantok import DefaultToken, SimpleToken
from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker_tools.cancel import TokenCancelledError, cancellation_scope, write_token
from deker_tools.typings import Sentinel, SentinelType
from pytest_httpx import HTTPXMock

from tests.conftest import FIRED_TOKENS, cancel_and_answer, fired_token, operation_kwargs

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
    assert collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken()) is None


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
    assert collection_adapter.delete(collection=collection, timeout=Sentinel, token=DefaultToken()) is None


def test_collection_delete_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.delete(collection=collection, timeout=Sentinel, token=DefaultToken())


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
    assert collection_adapter.clear(collection, timeout=Sentinel, token=DefaultToken()) is None


def test_collection_clear_raises_not_found(
    collection_adapter: ServerCollectionAdapter,
    httpx_mock: HTTPXMock,
    collection: Collection,
):
    httpx_mock.add_response(status_code=404)
    with pytest.raises(DekerCollectionNotExistsError):
        collection_adapter.clear(collection, timeout=Sentinel, token=DefaultToken())


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
        getattr(collection_adapter, method)(collection, *args, **operation_kwargs(method))
    assert info.value.retry_after == 1


# The operations of the collection adapter: the method and the status of the answer of the server on success; the
# timeout of each request defaults to ``write_lock_timeout`` of the client.
TIMED_CALLS = [
    pytest.param("create", 201, id="create"),
    pytest.param("delete", 200, id="delete"),
    pytest.param("clear", 200, id="clear"),
]


def collection_requests(httpx_mock: HTTPXMock) -> list[httpx.Request]:
    """The requests of the collection routes: the ping of the factory left out."""
    return [request for request in httpx_mock.get_requests() if "/collection" in request.url.path]


@pytest.mark.parametrize(("method", "status"), TIMED_CALLS)
@pytest.mark.parametrize(
    ("timeout", "parameter", "seconds"),
    [pytest.param(30, "30", 35, id="given"), pytest.param(Sentinel, None, 6, id="not-given")],
)
def test_the_timeout_of_the_call_goes_to_the_server(
    method: str,
    status: int,
    timeout: int | SentinelType,
    parameter: str | None,
    seconds: int,
    collection_adapter: ServerCollectionAdapter,
    collection: Collection,
    httpx_mock: HTTPXMock,
):
    """A given timeout is the query parameter; the client waits for it, or ``write_lock_timeout`` (1 s), 5 s more."""
    httpx_mock.add_response(url=re.compile(r".*/v1/collection.*"), status_code=status)

    getattr(collection_adapter, method)(collection, timeout=timeout, token=DefaultToken())

    [sent] = collection_requests(httpx_mock)
    assert sent.url.params.get("timeout") == parameter
    assert sent.extensions["timeout"] == httpx.Timeout(seconds).as_dict()


@pytest.mark.parametrize(("method", "status"), TIMED_CALLS)
@pytest.mark.parametrize(("kind", "error"), FIRED_TOKENS)
def test_a_fired_token_sends_nothing(
    method: str,
    status: int,
    kind: str,
    error: type[TokenCancelledError],
    collection_adapter: ServerCollectionAdapter,
    collection: Collection,
    httpx_mock: HTTPXMock,
):
    with pytest.raises(error) as info:
        getattr(collection_adapter, method)(collection, timeout=Sentinel, token=fired_token(kind))

    assert type(info.value) is error
    assert collection_requests(httpx_mock) == []


@pytest.mark.parametrize(("method", "status"), TIMED_CALLS)
@pytest.mark.parametrize(
    ("cancelled_during", "sent"),
    [
        pytest.param(False, 0, id="cancelled-before-sends-nothing"),
        pytest.param(True, 1, id="cancelled-once-sent-goes-to-its-end"),
    ],
)
def test_a_write_cancelled_from_outside(
    method: str,
    status: int,
    cancelled_during: bool,
    sent: int,
    collection_adapter: ServerCollectionAdapter,
    collection: Collection,
    httpx_mock: HTTPXMock,
):
    """The start flag is closed right before the request: a cancellation stops the write only before it."""
    scope = SimpleToken()
    with cancellation_scope(scope):
        token = write_token()
    if cancelled_during:
        httpx_mock.add_callback(partial(cancel_and_answer, scope, status, b""), url=re.compile(r".*/v1/collection.*"))
        getattr(collection_adapter, method)(collection, timeout=Sentinel, token=token)
        assert not token.cancelled
    else:
        scope.cancel()
        with pytest.raises(TokenCancelledError):
            getattr(collection_adapter, method)(collection, timeout=Sentinel, token=token)

    assert scope.cancelled
    assert len(collection_requests(httpx_mock)) == sent
