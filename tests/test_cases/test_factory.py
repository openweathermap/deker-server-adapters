import re

from typing import Dict
from uuid import uuid4

from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter

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


def test_auth_factory(ctx, mock_healthcheck):
    uri = Uri.create("http://test:test@localhost/")
    factory = AdaptersFactory(ctx, uri)
    assert factory.httpx_client.auth


def test_auth_factory_close(ctx, mock_healthcheck):
    uri = Uri.create("http://test:test@localhost/")
    factory = AdaptersFactory(ctx, uri)
    factory.close()
    assert factory.httpx_client.is_closed


def test_ctx_has_values_from_server(ctx, httpx_mock, mock_healthcheck, mocked_ping: Dict):
    uri = Uri.create("http://test:test@localhost/")

    factory = AdaptersFactory(ctx, uri)
    vadapter = factory.get_varray_adapter("/col", HDF5StorageAdapter)
    adapter = factory.get_array_adapter("/coll", HDF5StorageAdapter)

    assert vadapter.hash_ring.nodes == [node["id"] for node in mocked_ping["current_nodes"]]
    assert adapter.hash_ring.nodes == [node["id"] for node in mocked_ping["current_nodes"]]
