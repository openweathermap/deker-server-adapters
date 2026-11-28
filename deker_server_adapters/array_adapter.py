import builtins

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from cantok import AbstractToken
from deker.ABC.base_adapters import BaseArrayAdapter, BaseStorageAdapter
from deker.arrays import Array
from deker.ctx import CTX
from deker.uri import Uri
from deker_tools.cancel import check, mark_started

from deker_server_adapters.base import ServerArrayAdapterMixin
from deker_server_adapters.consts import STATUS_OK, ArrayType
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient

if TYPE_CHECKING:
    from deker import Collection


class ServerArrayAdapter(ServerArrayAdapterMixin[Array], BaseArrayAdapter[Uri]):
    """Adapter of the ``Arrays`` of a collection on a DEKER™ server.

    In cluster mode an ``Array`` lives on the node the hash ring picks by its primary attributes.
    """

    client: HttpxClient
    type = ArrayType.array
    executor: ThreadPoolExecutor

    def __init__(
        self,
        collection_path: Uri,
        ctx: CTX,
        executor: ThreadPoolExecutor,
        storage_adapter: builtins.type["BaseStorageAdapter"],  # `type` is shadowed by the class attribute
    ) -> None:
        """ServerArrayAdapter constructor.

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

    def _delete_by_id(self, id_: str) -> bool:
        """Try to delete an Array by its id.

        :param id_: Id of array.
        :returns: ``True`` if the array was deleted, ``False`` if the server reported a failure
        """
        try:
            response = self.client.delete(f"{self.collection_path.raw_url}/array/by-id/{id_}")
            if response.status_code != STATUS_OK:
                raise DekerServerError(response, "Couldn't delete the array")
        except Exception:
            return False
        return True

    def delete_all_by_vid(self, vid: str, collection: "Collection", *, token: AbstractToken) -> None:
        """Delete all the Arrays of a VArray, one request per Array in the executor.

        The start flag of the token is closed before the first deletion is sent: from then on every Array is
        deleted, whatever happens to the token.

        :param vid: Id of virtual array
        :param collection: taken for the common signature of the adapters and not used here: the Arrays are found in
          the list of the Arrays of the collection of the adapter
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the first deletion is sent
        :raises DekerServerError: if the list of the arrays could not be fetched or some Arrays were not deleted
        """
        check(token)
        all_arrays_response = self.client.get(f"{self.collection_path.raw_url}/arrays/")
        if all_arrays_response.status_code == STATUS_OK:
            arrays_with_vid = [
                array["id"] for array in all_arrays_response.json() if array["primary_attributes"].get("vid") == vid
            ]
            mark_started(token)
            results = self.executor.map(self._delete_by_id, arrays_with_vid)
            errors = []
            for i, result in enumerate(results):
                if not result:
                    errors.append(arrays_with_vid[i])
            if errors:
                raise DekerServerError(message=f"Not all arrays were deleted. Failed arrays: {errors}")
            return
        raise DekerServerError(all_arrays_response, "Couldn't fetch list of the arrays")
