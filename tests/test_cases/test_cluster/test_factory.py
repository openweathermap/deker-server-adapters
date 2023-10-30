import re

from typing import Dict

import pytest

from deker.ctx import CTX
from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter
from pytest_httpx import HTTPXMock

from deker_server_adapters.errors import DekerClusterError
from deker_server_adapters.factory import AdaptersFactory


def test_auth_factory(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")

    kwargs = {key: getattr(uri, key) for key in uri._fields}
    kwargs["servers"] = ["http://localhost:8000"]
    uri = Uri(**kwargs)

    factory = AdaptersFactory(ctx, uri)
    assert factory.httpx_client.auth


def test_auth_factory_close(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")
    kwargs = {key: getattr(uri, key) for key in uri._fields}
    kwargs["servers"] = ["http://localhost:8000"]
    uri = Uri(**kwargs)

    factory = AdaptersFactory(ctx, uri)
    factory.close()
    assert factory.httpx_client.is_closed


def test_ctx_has_values_from_server(ctx, mocked_ping: Dict, mock_ping):
    uri = Uri.create("http://test:test@localhost/")
    kwargs = {key: getattr(uri, key) for key in uri._fields}
    kwargs["servers"] = ["http://localhost:8000"]
    uri = Uri(**kwargs)

    factory = AdaptersFactory(ctx, uri)
    vadapter = factory.get_varray_adapter("/col", HDF5StorageAdapter)
    adapter = factory.get_array_adapter("/coll", HDF5StorageAdapter)

    assert vadapter.hash_ring.nodes == [node["id"] for node in mocked_ping["current_nodes"]]
    assert adapter.hash_ring.nodes == [node["id"] for node in mocked_ping["current_nodes"]]


def test_if_cluster_raises_error_on_empty_response(httpx_mock: HTTPXMock, ctx: CTX):
    httpx_mock.add_response(url=re.compile(r".*ping"), method="GET")
    with pytest.raises(DekerClusterError):
        uri = Uri.create("http://test:test@localhost/")
        kwargs = {key: getattr(uri, key) for key in uri._fields}
        kwargs["servers"] = ["http://localhost:8000"]
        uri = Uri(**kwargs)
        AdaptersFactory(ctx, uri)


def test_factory_set_leader(ctx, mocked_ping, mock_ping):
    uri = Uri.create("http://test:test@localhost/")
    kwargs = {key: getattr(uri, key) for key in uri._fields}
    kwargs["servers"] = ["http://localhost:8000"]
    uri = Uri(**kwargs)

    factory = AdaptersFactory(ctx, uri)
    leader = mocked_ping["current_nodes"][0]
    leader_url = f"{leader['protocol']}://{leader['host']}:{leader['port']}"
    assert str(factory.ctx.extra["httpx_client"].base_url) == leader_url
    collection_adapter = factory.get_collection_adapter()
    assert leader_url in collection_adapter.collections_resource.raw_url
