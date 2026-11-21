import re

import pytest

from deker.ctx import CTX
from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter
from httpx import Limits
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from tests.conftest import CLUSTER_MODE

from deker_server_adapters import factory as factory_module
from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import (
    KEEPALIVE_EXPIRY,
    LAST_MODIFIED_HEADER,
    MAX_CONNECTIONS,
    MAX_KEEPALIVE_CONNECTIONS,
)
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.factory import AdaptersFactory
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


def test_get_server_array_adapter(adapter_factory: AdaptersFactory, collection_path: Uri):
    assert isinstance(
        adapter_factory.get_array_adapter(collection_path, HDF5StorageAdapter),
        ServerArrayAdapter,
    )


def test_get_server_varray_adapter(adapter_factory: AdaptersFactory, collection_path: Uri):
    assert isinstance(
        adapter_factory.get_varray_adapter(collection_path, storage_adapter=HDF5StorageAdapter),
        ServerVarrayAdapter,
    )


def test_get_collection_adapter(adapter_factory: AdaptersFactory):
    assert isinstance(adapter_factory.get_collection_adapter(), ServerCollectionAdapter)


def test_a_factory_that_failed_its_healthcheck_closes_quietly():
    """The factory is dropped before its base is initialised when the server does not answer: close has only the
    http client to close, and must not fail on the missing context."""
    factory = AdaptersFactory.__new__(AdaptersFactory)
    factory.httpx_client = HttpxClient()
    factory.close()
    assert factory.httpx_client.is_closed


def test_the_factory_limits_the_keepalive_of_its_client(ctx: CTX, base_uri: Uri, mocker: MockerFixture, mock_ping):
    """The http client keeps an idle connection KEEPALIVE_EXPIRY seconds, unless httpx_conf of the client says else."""
    spy = mocker.spy(factory_module, "HttpxClient")
    AdaptersFactory(ctx, uri=base_uri).close()
    limits = spy.call_args.kwargs["limits"]
    assert limits.keepalive_expiry == KEEPALIVE_EXPIRY
    assert limits.max_connections == MAX_CONNECTIONS
    assert limits.max_keepalive_connections == MAX_KEEPALIVE_CONNECTIONS


def test_httpx_conf_of_the_client_overrides_the_limits(ctx: CTX, base_uri: Uri, mocker: MockerFixture, mock_ping):
    spy = mocker.spy(factory_module, "HttpxClient")
    ctx.extra["httpx_conf"] = {"limits": Limits(keepalive_expiry=1)}
    AdaptersFactory(ctx, uri=base_uri).close()
    assert spy.call_args.kwargs["limits"].keepalive_expiry == 1


SERVER_TIMEOUTS = {"read_timeout": 90, "write_lock_timeout": 45, "max_read_timeout": 300, "max_write_lock_timeout": 200}


def ping_answer(mode: str, mocked_ping: dict, answer: dict) -> dict:
    """The JSON of the ping: the cluster config in a cluster, nothing else on a single server, and the answer."""
    if mode == CLUSTER_MODE:
        return {**mocked_ping, **answer}
    return answer


@pytest.mark.parametrize(
    ("answer", "read", "write", "limits"),
    [
        pytest.param(
            {"timeouts": SERVER_TIMEOUTS},
            90,
            45,
            {"max_read_timeout": 300, "max_write_lock_timeout": 200},
            id="the-server-tells-its-timeouts",
        ),
        pytest.param({}, 3, 1, None, id="an-older-server-tells-none"),
    ],
)
def test_the_timeouts_of_the_server_become_the_settings_of_the_client(
    answer: dict,
    read: int,
    write: int,
    limits: dict | None,
    ctx: CTX,
    base_uri: Uri,
    mode: str,
    mocked_ping: dict,
    httpx_mock: HTTPXMock,
):
    """The defaults of the server are the defaults of the operations; its limits are kept for the logs."""
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/v1/ping.*"),
        json=ping_answer(mode, mocked_ping, answer),
        headers={LAST_MODIFIED_HEADER: "foo"},
    )

    factory = AdaptersFactory(ctx, uri=base_uri)
    try:
        assert factory.ctx.config.read_timeout == read
        assert factory.ctx.config.write_lock_timeout == write
        assert factory.ctx.extra.get("timeout_limits") == limits
    finally:
        factory.close()


@pytest.mark.parametrize(
    "timeouts",
    [
        pytest.param({**SERVER_TIMEOUTS, "read_timeout": 0}, id="zero"),
        pytest.param({**SERVER_TIMEOUTS, "write_lock_timeout": -1}, id="negative"),
        pytest.param({**SERVER_TIMEOUTS, "read_timeout": 1.5}, id="float"),
        pytest.param({**SERVER_TIMEOUTS, "read_timeout": "60"}, id="string"),
        pytest.param({**SERVER_TIMEOUTS, "max_read_timeout": True}, id="bool"),
        pytest.param({**SERVER_TIMEOUTS, "max_write_lock_timeout": None}, id="null"),
        pytest.param({k: v for k, v in SERVER_TIMEOUTS.items() if k != "write_lock_timeout"}, id="missing"),
        pytest.param([60, 60], id="not-an-object"),
    ],
)
def test_invalid_timeouts_of_the_server_are_refused(
    timeouts: object, ctx: CTX, base_uri: Uri, mode: str, mocked_ping: dict, httpx_mock: HTTPXMock
):
    """Nothing is applied from timeouts the client cannot use: the factory refuses to connect."""
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r".*/v1/ping.*"),
        json=ping_answer(mode, mocked_ping, {"timeouts": timeouts}),
        headers={LAST_MODIFIED_HEADER: "foo"},
    )

    with pytest.raises(DekerServerError, match="timeouts"):
        AdaptersFactory(ctx, uri=base_uri)

    assert ctx.config.read_timeout == 3
    assert ctx.config.write_lock_timeout == 1
