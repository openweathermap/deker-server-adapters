import re

from unittest.mock import patch

from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter

from ..mocks import MockedAdaptersFactory
from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.factory import AdaptersFactory
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


def test_auth_factory(ctx):
    uri = Uri.create("http://test:test@localhost/")
    factory = MockedAdaptersFactory(ctx, uri)
    assert factory.httpx_client.auth


def test_auth_factory_close(ctx):
    uri = Uri.create("http://test:test@localhost/")
    factory = MockedAdaptersFactory(ctx, uri)
    factory.close()
    assert factory.httpx_client.is_closed


def test_ctx_has_values_from_server(ctx, httpx_mock):
    uri = Uri.create("http://test:test@localhost/")
    servers = ["http://localhost:8031"]

    httpx_mock.add_response(method="get", url=re.compile(r".*\/v1\/ping"), json={"servers": servers})
    factory = AdaptersFactory(ctx, uri)
    vadapter = factory.get_varray_adapter("/col", HDF5StorageAdapter)
    adapter = factory.get_array_adapter("/coll", HDF5StorageAdapter)

    assert vadapter.hash_ring.nodes == servers
    assert adapter.hash_ring.nodes == servers
