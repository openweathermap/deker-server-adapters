from collections.abc import Generator

from deker import Collection
from deker.ABC import BaseCollectionAdapter
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.tools.decorators import check_ctx_state
from deker.uri import Uri

from deker_server_adapters.base import BaseServerAdapterMixin
from deker_server_adapters.consts import BAD_REQUEST, COLLECTION_NAME_PARAM, NOT_FOUND, STATUS_CREATED, STATUS_OK
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils.requests import make_request
from deker_server_adapters.utils.version import get_api_version


class ServerCollectionAdapter(BaseServerAdapterMixin, BaseCollectionAdapter[Uri]):
    """Adapter of the collections of a DEKER™ server: every operation is a request to its API."""

    client: HttpxClient
    metadata_version = "0.2"

    @property
    def collection_url_prefix(self) -> str:
        """Return the path prefix of one collection in the API, with the API version."""
        return f"{get_api_version()}/collection"

    @property
    def collections_url_prefix(self) -> str:
        """Return the path prefix of the collections list in the API, with the API version."""
        return f"{get_api_version()}/collections"

    @check_ctx_state
    def delete(self, collection: "Collection") -> None:
        """Delete collection from server.

        :param collection: Deker's collection instance
        :raises DekerCollectionNotExistsError: if the server answered 404
        :raises DekerServerError: if the server answered anything else but 200
        """
        response = self.client.delete(
            f"/{self.collection_url_prefix}/{collection.name}",
        )

        if response.status_code == STATUS_OK:
            return

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise DekerServerError(response, "Collection wasn't deleted")

    @check_ctx_state
    def create(self, collection: "Collection") -> None:
        """Create collection on the server.

        :param collection: Deker's collection instance
        :raises DekerCollectionAlreadyExistsError: if the server rejected the name as taken
        :raises DekerServerError: if the server answered anything else but 201
        """
        data = collection.as_dict
        response = self.client.post(f"/{self.collections_url_prefix}", json=data)
        if response.status_code == STATUS_CREATED:
            return

        if response.status_code == BAD_REQUEST and COLLECTION_NAME_PARAM in response.json().get("parameters", []):
            raise DekerCollectionAlreadyExistsError

        raise DekerServerError(response, "Collection wasn't created")

    @check_ctx_state
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Name of collection
        :returns: collection metadata as a dict
        :raises DekerCollectionNotExistsError: if the server answered 404
        :raises DekerServerError: if the server answered anything else but 200
        """
        url = f"{self.collection_url_prefix}/{name}"
        if self.client.cluster_mode:
            response = make_request(url=url, nodes=self.nodes_urls, client=self.client)
        else:
            response = self.client.get(url)

        if response and response.status_code == STATUS_OK:
            return response.json()

        if response and response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise DekerServerError(response, f"Couldn't get the collection {name=}")

    @check_ctx_state
    def clear(self, collection: "Collection") -> None:
        """Remove every array of the collection on the server.

        :param collection: Instance of Deker's collection
        :raises DekerCollectionNotExistsError: if the server answered 404
        :raises DekerServerError: if the server answered anything else but 200
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
        """Get the uri of the collections resource of the server."""
        return Uri.create(str(self.client.base_url)) / self.collection_url_prefix

    @check_ctx_state
    def is_deleted(self, collection: Collection) -> bool:
        """Check if collection is deleted: not checked, the server answers 404 to a deleted collection.

        :param collection: Collection instance
        :returns: always ``False``
        """
        return False

    @check_ctx_state
    def __iter__(self) -> Generator[dict, None, None]:
        """Iterate over the metadata of the collections of the server.

        :yields: the metadata of every collection
        :raises DekerServerError: if the server answered anything but 200
        """
        nodes = [node.url.raw_url for node in self.nodes] if self.client.cluster_mode else [str(self.client.base_url)]
        all_collections_response = make_request(url=self.collections_url_prefix, nodes=nodes, client=self.client)
        if all_collections_response is None or all_collections_response.status_code != STATUS_OK:
            raise DekerServerError(
                all_collections_response,
                "Couldn't get list of collections",
            )

        results = all_collections_response.json()
        yield from results
