from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Type, Union

from deker.ABC.base_adapters import BaseArrayAdapter, BaseStorageAdapter
from deker.ctx import CTX

from deker_server_adapters.base import ServerArrayAdapterMixin
from deker_server_adapters.consts import STATUS_OK, ArrayType
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient


if TYPE_CHECKING:
    from deker import Collection
    from deker.uri import Uri


class ServerArrayAdapter(ServerArrayAdapterMixin, BaseArrayAdapter):
    """Server realization for array adapter."""

    client: HttpxClient
    type = ArrayType.array
    executor: ThreadPoolExecutor

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
