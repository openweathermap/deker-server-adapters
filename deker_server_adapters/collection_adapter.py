from collections.abc import Generator

from deker import Collection
from deker.ABC import BaseCollectionAdapter
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.uri import Uri

from deker_server_adapters.base import BaseServerAdapterMixin
from deker_server_adapters.consts import BAD_REQUEST, COLLECTION_NAME_PARAM, NOT_FOUND, STATUS_CREATED, STATUS_OK
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils import get_api_version, make_request


class ServerCollectionAdapter(BaseServerAdapterMixin, BaseCollectionAdapter):
    """Server realization for collection adapter."""

    client: HttpxClient
    metadata_version = "0.2"

    @property
    def collection_url_prefix(self) -> str:
        """Return prefix to collection."""
        return f"{get_api_version()}/collection"

    @property
    def collections_url_prefix(self) -> str:
        """Return prefix to collections."""
        return f"{get_api_version()}/collections"

    def delete(self, collection: "Collection") -> None:
        """Delete collection from server.

        :param collection: Deker's collection instance
        """
        response = self.client.delete(
            f"/{self.collection_url_prefix}/{collection.name}",
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
        response = self.client.post(f"/{self.collections_url_prefix}", json=data)

        if response.status_code == STATUS_CREATED:
            return

        if response.status_code == BAD_REQUEST and COLLECTION_NAME_PARAM in response.json().get("parameters", []):
            raise DekerCollectionAlreadyExistsError

        raise DekerServerError(response, "Collection wasn't created")

    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Name of collection
        """
        url = f"{self.collection_url_prefix}/{name}"
        if self.client.cluster_mode:
            response = make_request(url=url, nodes=self.nodes, client=self.client)
        else:
            response = self.client.get(url)

        if response and response.status_code == STATUS_OK:
            return response.json()

        if response and response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise DekerServerError(response, f"Couldn't get the collection {name=}")

    def clear(self, collection: "Collection") -> None:
        """Clear collection.

        :param collection: Instance of Deker's collection
        :return:
        """
        array_type = "varrays" if collection.varray_schema else "arrays"
        response = self.client.put(
            f"/{self.collection_url_prefix}/{collection.name}/{array_type}",
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
        return Uri.create(str(self.client.base_url)) / self.collection_url_prefix

    def is_deleted(self, collection: Collection) -> bool:
        """Check if collection is deleted.

        :param collection: Collection instance
        :return:
        """
        return False

    def __iter__(self) -> Generator[dict, None, None]:
        nodes = self.nodes if self.client.cluster_mode else [str(self.client.base_url)]
        all_collections_response = make_request(url=self.collections_url_prefix, nodes=nodes, client=self.client)
        if all_collections_response is None or all_collections_response.status_code != STATUS_OK:
            raise DekerServerError(
                all_collections_response,
                "Couldn't get list of collections",
            )

        results = all_collections_response.json()
        yield from results
