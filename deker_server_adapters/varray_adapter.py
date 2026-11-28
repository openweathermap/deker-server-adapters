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
        """ServerVArrayAdapter constructor.

        :param collection_path: uri of the collection
        :param ctx: context of the client
        :param executor: thread pool the Arrays of a VArray are processed in
        :param storage_adapter: type of the adapter of the files, unused on the server side
        """
        super().__init__(
            collection_path,
            ctx,
            executor,
            storage_adapter,
        )
        self.executor = executor

    def delete_main_file(self, array: VArray, *, background: bool, _empty_trash: bool) -> None:
        """Do nothing: the request of ``delete`` has deleted the whole VArray on the server, its json file too.

        :param array: not used: see above
        :param background: not used: see above
        :param _empty_trash: not used: see above
        """
