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
