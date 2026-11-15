import builtins

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from deker.ABC.base_adapters import BaseArrayAdapter, BaseStorageAdapter
from deker.arrays import Array
from deker.ctx import CTX
from deker.uri import Uri

from deker_server_adapters.base import ServerArrayAdapterMixin
from deker_server_adapters.consts import STATUS_OK, ArrayType
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient

if TYPE_CHECKING:
    from deker import Collection


class ServerArrayAdapter(ServerArrayAdapterMixin[Array], BaseArrayAdapter[Uri]):
    """Server realization for array adapter."""

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
        super().__init__(
            collection_path,
            ctx,
            executor,
            storage_adapter,
        )
        self.executor = executor

    def delete_all_by_vid(self, vid: str, collection: "Collection") -> None:
        """Delete all arrays that have that vid.

        :param vid: Id of virtual array
        :param collection: Instance of deker's collection
        """
        all_arrays_response = self.client.get(f"{self.collection_path.raw_url}/arrays/")
        if all_arrays_response.status_code == STATUS_OK:
            arrays_with_vid = [
                array["id"] for array in all_arrays_response.json() if array["primary_attributes"].get("vid") == vid
            ]

            def delete_by_id(id_: str) -> bool:
                """Try to delete array with given id.

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

            results = self.executor.map(delete_by_id, arrays_with_vid)
            errors = []
            for i, result in enumerate(results):
                if not result:
                    errors.append(arrays_with_vid[i])
            if errors:
                raise DekerServerError(
                    None,
                    f"Not all arrays were deleted. Failed arrays: {errors}",
                )
            return
        raise DekerServerError(all_arrays_response, "Couldn't fetch list of the arrays")
