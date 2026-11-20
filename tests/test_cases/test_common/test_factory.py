from deker.ctx import CTX
from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter
from httpx import Limits
from pytest_mock import MockerFixture

from deker_server_adapters import factory as factory_module
from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import KEEPALIVE_EXPIRY, MAX_CONNECTIONS, MAX_KEEPALIVE_CONNECTIONS
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
