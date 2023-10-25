import re

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from uuid import uuid4

import pytest

from deker import ArraySchema, DimensionSchema, VArraySchema
from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from pytest_httpx import HTTPXMock
from pytest_mock import MockerFixture

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.factory import AdaptersFactory
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


@pytest.fixture()
def mocked_ping() -> Dict:
    return {
        "mode": "cluster",
        "this_id": "8381202B-8C95-487A-B9B5-0B527056804E",
        "leader_id": "8381202B-8C95-487A-B9B5-0B527056804E",
        "current_nodes": [
            {
                "id": "8381202B-8C95-487A-B9B5-0B527056804E",
                "host": "host1.owm.io",
                "port": 443,
                "protocol": "http",
            },
        ],
    }


@pytest.fixture()
def mock_healthcheck(httpx_mock: HTTPXMock, mocked_ping):
    httpx_mock.add_response(method="GET", url=re.compile(r".*\/v1\/ping.*"), json=mocked_ping)


@pytest.fixture(scope="session")
def nodes() -> List[Dict]:
    return [
        {
            "id": "8381202B-8C95-487A-B9B5-0B527056804E",
            "host": "localhost",
            "port": 8000,
            "protocol": "http",
        },
        {
            "id": "8381202B-8C95-487A-B9B5-0B5270568040",
            "host": "localhost",
            "port": 8012,
            "protocol": "http",
        },
    ]


@pytest.fixture()
def nodes_urls(nodes) -> List[str]:
    urls = []
    for node in nodes:
        url = f"{node['protocol']}://{node['host']}:{node['port']}"
        urls.append(url)
    return urls


@pytest.fixture(scope="session")
def collection_path(nodes: List[str]) -> Uri:
    uri = Uri.create("http://localhost:8000/v1/collection")
    uri.servers = nodes
    return uri


@pytest.fixture()
def ctx(session_mocker: MockerFixture, collection_path: Uri, nodes: List[str]) -> CTX:
    ctx = CTX(
        uri=collection_path,
        config=DekerConfig(
            uri=str(collection_path.raw_url),
            workers=1,
            write_lock_timeout=1,
            write_lock_check_interval=1,
            memory_limit=40000,
        ),
        storage_adapter=HDF5StorageAdapter,  # Just for CTX
        executor=ThreadPoolExecutor(max_workers=1),
    )
    with HttpxClient(base_url="http://localhost:8000/") as client:
        ctx.extra["httpx_client"] = client
        ctx.extra["hash_ring"] = HashRing([node["id"] for node in nodes])
        ctx.extra["leader_node"] = Uri.create("http://localhost:8000")
        ctx.extra["nodes_mapping"] = {
            "8381202B-8C95-487A-B9B5-0B527056804E": ["http://localhost:8000"],
            "8381202B-8C95-487A-B9B5-0B5270568040": ["http://localhost:8012"],
        }
        ctx.extra["nodes"] = ["http://localhost:8000"]
        client.ctx = ctx
        client.cluster_mode = True
        yield ctx


@pytest.fixture()
def adapter_factory(ctx: CTX, collection_path: Uri, mock_healthcheck) -> AdaptersFactory:
    return AdaptersFactory(ctx, uri=collection_path)


@pytest.fixture()
def server_array_adapter(collection_path: Uri, ctx: CTX, adapter_factory) -> ServerArrayAdapter:
    return ServerArrayAdapter(
        collection_path,
        ctx,
        ThreadPoolExecutor(max_workers=1),
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def server_varray_adapter(collection_path: Uri, ctx: CTX, adapter_factory) -> ServerVarrayAdapter:
    return ServerVarrayAdapter(
        collection_path,
        ctx,
        ThreadPoolExecutor(max_workers=1),
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def collection_adapter(ctx: CTX) -> ServerCollectionAdapter:
    return ServerCollectionAdapter(ctx)


@pytest.fixture()
def collection(adapter_factory: AdaptersFactory, collection_adapter: ServerCollectionAdapter) -> Collection:
    array_schema = ArraySchema(dimensions=[DimensionSchema(name="x", size=1)], dtype=int)
    collection = Collection(
        name="test",
        schema=array_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )
    return collection


@pytest.fixture()
def varray_collection(collection_adapter: ServerCollectionAdapter, adapter_factory: AdaptersFactory) -> Collection:
    varray_schema = VArraySchema(dimensions=[DimensionSchema(name="x", size=1)], dtype=int, vgrid=(1,))
    collection = Collection(
        name="test",
        schema=varray_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )
    return collection


@pytest.fixture()
def array(collection: Collection, server_array_adapter: ServerArrayAdapter) -> Array:
    return Array(collection, server_array_adapter)


@pytest.fixture()
def varray(varray_collection: Collection, adapter_factory: AdaptersFactory) -> VArray:
    """Returns an instance of VArray."""
    return VArray(
        collection=varray_collection,
        adapter=adapter_factory.get_varray_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter),
        array_adapter=adapter_factory.get_array_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter),
        primary_attributes={},
        custom_attributes={},
        id_=str(uuid4()),
    )


@pytest.fixture()
def rate_limits_headers() -> Dict:
    return {"RateLimit-Limit": "10", "RateLimit-Remaining": "10", "RateLimit-Reset": "60"}
