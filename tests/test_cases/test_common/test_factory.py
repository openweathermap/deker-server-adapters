from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5 import HDF5StorageAdapter

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
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
