from collections.abc import Generator

from deker import Collection
from deker.ABC import BaseCollectionAdapter
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.uri import Uri
from httpx import Client

from deker_server_adapters.base import ServerAdapterMixin
from deker_server_adapters.consts import BAD_REQUEST, COLLECTION_NAME_PARAM, NOT_FOUND, STATUS_CREATED, STATUS_OK
from deker_server_adapters.errors import DekerServerError


class ServerCollectionAdapter(ServerAdapterMixin, BaseCollectionAdapter):
    """Server realization for collection adapter."""

    COLLECTION_URL_PREFIX = "v1/collection"
    COLLECTIONS_URL_PREFIX = "v1/collections"
    client: Client
    metadata_version = "0.2"

    def delete(self, collection: "Collection") -> None:
        """Delete collection from server.

        :param collection: Deker's collection instance
        """
        response = self.client.delete(
            f"/{self.COLLECTION_URL_PREFIX}/{collection.name}",
        )

        if response.status_code == STATUS_OK:
            return

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise DekerServerError(response, "Collection wasn't deleted")

    def create(self, collection: "Collection") -> None:
        """Create collection.

        :param collection: Deker's collection instance
        """
        data = collection.as_dict
        response = self.client.post(f"/{self.COLLECTIONS_URL_PREFIX}", json=data)

        if response.status_code == STATUS_CREATED:
            return

        if response.status_code == BAD_REQUEST and COLLECTION_NAME_PARAM in response.json().get("parameters", []):
            raise DekerCollectionAlreadyExistsError

        raise DekerServerError(response, "Collection wasn't created")

    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Name of collection
        """
        response = self.client.get(f"/{self.COLLECTION_URL_PREFIX}/{name}")

        if response.status_code == STATUS_OK:
            return response.json()

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise DekerServerError(response, f"Couldn't get the collection {name=}")

    def clear(self, collection: "Collection") -> None:
        """Clear collection.

        :param collection: Instance of Deker's collection
        :return:
        """
        array_type = "varrays" if collection.varray_schema else "arrays"
        response = self.client.put(
            f"/{self.COLLECTION_URL_PREFIX}/{collection.name}/{array_type}",
            json=[],
        )

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        if response.status_code == STATUS_OK:
            return

        raise DekerServerError(
            response,
            f"Couldn't clear the collection {collection.name}",
        )

    @property
    def collections_resource(self) -> Uri:
        """Full path with to collections resource."""
        return self.uri / self.COLLECTION_URL_PREFIX

    def is_deleted(self, collection: Collection) -> bool:
        """Check if method is deleted.

        :param collection: Collection instance
        :return:
        """
        return False

    def __iter__(self) -> Generator[dict, None, None]:
        all_collections_response = self.client.get(f"/{self.COLLECTIONS_URL_PREFIX}")
        if all_collections_response.status_code != STATUS_OK:
            raise DekerServerError(
                all_collections_response,
                "Couldn't get list of collections",
            )

        results = all_collections_response.json()
        yield from results
