import builtins

from concurrent.futures import ThreadPoolExecutor

from deker.ABC.base_adapters import BaseStorageAdapter, BaseVArrayAdapter
from deker.arrays import VArray
from deker.ctx import CTX
from deker.uri import Uri

from deker_server_adapters.base import ServerArrayAdapterMixin
from deker_server_adapters.consts import ArrayType


class ServerVarrayAdapter(ServerArrayAdapterMixin[VArray], BaseVArrayAdapter[Uri]):
    """Server adapter for varrays."""

    type = ArrayType.varray

    def __init__(
        self,
        collection_path: Uri,
        ctx: CTX,
        executor: ThreadPoolExecutor,
        storage_adapter: builtins.type["BaseStorageAdapter"],  # `type` is shadowed by the class attribute
    ) -> None:
        super().__init__(
            collection_path,
            ctx,
            executor,
            storage_adapter,
        )
        self.executor = executor
