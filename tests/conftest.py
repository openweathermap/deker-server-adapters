import re

from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Literal
from uuid import uuid4

import pytest

from deker import ArraySchema, AttributeSchema, DimensionSchema, VArraySchema
from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.uri import Uri
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from pytest_httpx import HTTPXMock

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.cluster_config import apply_config
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import LAST_MODIFIED_HEADER
from deker_server_adapters.factory import AdaptersFactory
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils.version import get_api_version
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


CLUSTER_MODE = "cluster"
SINGLE_MODE = "single"


pytest_plugins = ["tests.plugins.cluster"]


@pytest.fixture(params=[SINGLE_MODE, CLUSTER_MODE])
def mode(request) -> str:
    return request.param


@pytest.fixture()
def base_uri(mode, base_cluster_uri):
    if mode == SINGLE_MODE:
        return Uri.create("http://localhost:8000")

    return base_cluster_uri


@pytest.fixture()
def collection_path(base_uri: Uri, collection: Collection) -> Uri:
    return base_uri / f"{get_api_version()}/collection/{collection.name}"


@pytest.fixture()
def ctx(mode, base_uri: Uri, nodes: List[dict], mocked_ping: dict) -> CTX:
    ctx = CTX(
        uri=base_uri,
        config=DekerConfig(
            uri=str(base_uri.raw_url),
            workers=1,
            write_lock_timeout=1,
            write_lock_check_interval=1,
            memory_limit=40000,
        ),
        storage_adapter=HDF5StorageAdapter,  # Just for CTX
        executor=ThreadPoolExecutor(max_workers=1),
    )
    with HttpxClient(base_url=base_uri.raw_url) as client:
        client.ctx = ctx
        ctx.extra["httpx_client"] = client

        if mode == CLUSTER_MODE:
            apply_config(mocked_ping, ctx)
        yield ctx


@pytest.fixture()
def mock_ping(mode: str, httpx_mock: HTTPXMock, mocked_ping: Dict):
    if mode == CLUSTER_MODE:
        httpx_mock.add_response(
            method="GET", url=re.compile(r".*\/v1\/ping.*"), json=mocked_ping, headers={LAST_MODIFIED_HEADER: "foo"}
        )
    else:
        httpx_mock.add_response(method="GET", url=re.compile(r".*\/v1\/ping.*"), status_code=200)


@pytest.fixture()
def adapter_factory(mode: str, ctx: CTX, base_uri: Uri, mock_ping) -> AdaptersFactory:
    return AdaptersFactory(ctx, uri=base_uri)


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
    return Collection(
        name="test",
        schema=array_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture
def collection_with_primary_attributes(
    adapter_factory: AdaptersFactory, collection_adapter: ServerCollectionAdapter
) -> Collection:
    array_schema = ArraySchema(
        dimensions=[DimensionSchema(name="x", size=1)],
        attributes=[
            AttributeSchema(name="foo", dtype=str, primary=True),
            AttributeSchema(name="bar", dtype=str, primary=True),
        ],
        dtype=int,
    )
    return Collection(
        name="test",
        schema=array_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def varray_collection(collection_adapter: ServerCollectionAdapter, adapter_factory: AdaptersFactory) -> Collection:
    varray_schema = VArraySchema(dimensions=[DimensionSchema(name="x", size=1)], dtype=int, vgrid=(1,))
    return Collection(
        name="test",
        schema=varray_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def varray_collection_with_primary_attributes(
    collection_adapter: ServerCollectionAdapter, adapter_factory: AdaptersFactory
) -> Collection:
    varray_schema = VArraySchema(
        dimensions=[DimensionSchema(name="x", size=1)],
        dtype=int,
        vgrid=(1,),
        attributes=[
            AttributeSchema(name="foo", dtype=str, primary=True),
            AttributeSchema(name="bar", dtype=str, primary=True),
        ],
    )
    return Collection(
        name="test",
        schema=varray_schema,
        adapter=collection_adapter,
        factory=adapter_factory,
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def primary_attributes() -> dict:
    return {"foo": "bar", "bar": "baz"}


@pytest.fixture()
def array(collection: Collection, server_array_adapter: ServerArrayAdapter) -> Array:
    return Array(collection, server_array_adapter)


@pytest.fixture()
def array_with_attributes(
    collection_with_primary_attributes: Collection, server_array_adapter: ServerArrayAdapter, primary_attributes: dict
) -> Array:
    return Array(collection_with_primary_attributes, server_array_adapter, primary_attributes=primary_attributes)


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
def varray_with_attributes(
    varray_collection_with_primary_attributes: Collection, adapter_factory: AdaptersFactory, primary_attributes: dict
) -> VArray:
    return VArray(
        collection=varray_collection_with_primary_attributes,
        adapter=adapter_factory.get_varray_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter),
        array_adapter=adapter_factory.get_array_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter),
        primary_attributes=primary_attributes,
        custom_attributes={},
        id_=str(uuid4()),
    )


@pytest.fixture()
def rate_limits_headers() -> Dict:
    return {"RateLimit-Limit": "10", "RateLimit-Remaining": "10", "RateLimit-Reset": "60"}


@pytest.fixture()
def array_url_path(array: Array) -> str:
    return f"/{get_api_version()}/collection/{array.collection}/array/by-id/{array.id}"


@pytest.fixture()
def mock_status(mode: str, request: pytest.FixtureRequest):
    if mode == CLUSTER_MODE:
        request.getfixturevalue("mocked_filestatus_check_unmoved")
