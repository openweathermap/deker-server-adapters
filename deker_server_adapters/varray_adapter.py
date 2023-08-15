from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Type, Union

from deker.ABC.base_adapters import BaseStorageAdapter, BaseVArrayAdapter
from deker.ctx import CTX

from deker_server_adapters.base import ServerArrayAdapterMixin
from deker_server_adapters.consts import ArrayType


if TYPE_CHECKING:
    from deker.uri import Uri


class ServerVarrayAdapter(ServerArrayAdapterMixin, BaseVArrayAdapter):
    """Server adapter for varrays."""

    type = ArrayType.varray

    def __init__(
        self,
        collection_path: Union["Uri", Path],
        ctx: CTX,
        executor: ThreadPoolExecutor,
        storage_adapter: Type["BaseStorageAdapter"],
    ) -> None:
        super().__init__(
            collection_path,
            ctx,
            executor,
            storage_adapter,
        )
        self.executor = executor
