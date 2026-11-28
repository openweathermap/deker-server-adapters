from collections.abc import Generator

from cantok import AbstractToken, DefaultToken
from deker import Collection
from deker.ABC import BaseCollectionAdapter
from deker.errors import DekerCollectionAlreadyExistsError, DekerCollectionNotExistsError
from deker.tools.decorators import check_ctx_state
from deker.uri import Uri
from deker_tools.cancel import check, mark_started
from deker_tools.typings import Sentinel, SentinelType

from deker_server_adapters.base import BaseServerAdapterMixin, raise_for_status, timeout_kwargs
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
    def delete(self, collection: "Collection", *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Delete collection from server: one request, the server deletes it and empties its trash.

        :param collection: Deker's collection instance
        :param background: sent to the server: it answers right after the deletion and empties its trash in the
          background
        :param _empty_trash: taken for the common signature of the adapters and not used here: the server empties its
          trash after each deletion it makes, by ``background``
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerCollectionNotExistsError: if the server answered 404
        :raises DekerClusterError: if the server answered 503: it cannot take the request now
        :raises DekerServerError: if the server answered anything else but 200
        """
        check(token)
        mark_started(token)
        response = self.client.delete(
            f"/{self.collection_url_prefix}/{collection.name}", params={"background": str(background).lower()}
        )

        if response.status_code == STATUS_OK:
            return

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise_for_status(response, "Collection wasn't deleted")

    @check_ctx_state
    def create(self, collection: "Collection", *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Create collection on the server.

        :param collection: Deker's collection instance
        :param timeout: seconds to wait for the lock of the collection, sent to the server; ``Sentinel`` - the
          default of the server
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerCollectionAlreadyExistsError: if the server rejected the name as taken
        :raises DekerClusterError: if the server answered 503: it cannot take the request now
        :raises DekerServerError: if the server answered anything else but 201
        """
        check(token)
        data = collection.as_dict
        request_kwargs = timeout_kwargs(timeout=timeout, default=self.ctx.config.write_lock_timeout)
        mark_started(token)
        response = self.client.post(f"/{self.collections_url_prefix}", json=data, **request_kwargs)
        if response.status_code == STATUS_CREATED:
            return

        if response.status_code == BAD_REQUEST and COLLECTION_NAME_PARAM in response.json().get("parameters", []):
            raise DekerCollectionAlreadyExistsError

        raise_for_status(response, "Collection wasn't created")

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
            response = make_request(url=url, nodes=self.nodes_urls, client=self.client, token=DefaultToken())
        else:
            response = self.client.get(url)

        if response and response.status_code == STATUS_OK:
            return response.json()

        if response and response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        raise_for_status(response, f"Couldn't get the collection {name=}")

    @check_ctx_state
    def clear(self, collection: "Collection", *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Remove every array of the collection on the server: one request, the server clears it and empties its trash.

        :param collection: Instance of Deker's collection
        :param background: sent to the server: it answers right after the clearing and empties its trash in the
          background
        :param _empty_trash: taken for the common signature of the adapters and not used here: the server empties its
          trash after each clearing it makes, by ``background``
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerCollectionNotExistsError: if the server answered 404
        :raises DekerClusterError: if the server answered 503: it cannot take the request now
        :raises DekerTimeoutError: if the server answered 504
        :raises DekerServerError: if the server answered anything else but 200
        """
        check(token)
        array_type = "varrays" if collection.varray_schema else "arrays"
        mark_started(token)
        response = self.client.put(
            f"/{self.collection_url_prefix}/{collection.name}/{array_type}",
            json=[],
            params={"background": str(background).lower()},
        )

        if response.status_code == NOT_FOUND:
            raise DekerCollectionNotExistsError

        if response.status_code == STATUS_OK:
            return

        raise_for_status(response, f"Couldn't clear the collection {collection.name}")

    @check_ctx_state
    def empty_trash(self, *, background: bool) -> None:
        """Remove everything from the trash of the server: ``DELETE /trash``.

        :param background: the server answers at once and removes in the background, its error goes to its log
        :raises DekerClusterError: if the server answered 503: it cannot take the request now
        :raises DekerTimeoutError: if the server answered 504
        :raises DekerServerError: if the server answered anything else but 200, for instance 500 when the trash is not
          emptied completely
        """
        response = self.client.delete(f"/{get_api_version()}/trash", params={"background": str(background).lower()})
        if response.status_code == STATUS_OK:
            return

        raise_for_status(response, "The trash wasn't emptied")

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
        all_collections_response = make_request(
            url=self.collections_url_prefix, nodes=nodes, client=self.client, token=DefaultToken()
        )
        if all_collections_response is None or all_collections_response.status_code != STATUS_OK:
            raise DekerServerError(
                Sentinel if all_collections_response is None else all_collections_response,
                "Couldn't get list of collections",
            )

        results = all_collections_response.json()
        yield from results
