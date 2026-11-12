import logging

from collections.abc import Generator
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Optional, Union

import numpy as np

from deker import Array, Collection, VArray
from deker.ABC.base_array import BaseArray
from deker.ctx import CTX
from deker.tools.array import get_id
from deker.tools.time import convert_datetime_attrs_to_iso
from deker.types import ArrayMeta, Numeric, Slice
from deker.uri import Uri
from deker_tools.slices import slice_converter
from httpx import Response, TimeoutException
from numpy import ndarray

from deker_server_adapters.cluster_config import ClusterConfig, Node
from deker_server_adapters.consts import NOT_FOUND, READ_CHUNK_SIZE, STATUS_CREATED, STATUS_OK, TIMEOUT, ArrayType
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer, FilteringByIdInClusterIsForbidden
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils.requests import make_request, request_in_cluster

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_schemas import BaseArraysSchema

logger = logging.getLogger(__name__)


def create_array_from_meta(
    type: ArrayType,
    *args: Any,
    **kwargs: Any,
) -> Array | VArray:
    """Override class method.

    :param type: Type of Array
    :param *args: Create array meta params
    :param **kwargs: Create array meta kwargs
    :returns: Array or VArray built from its meta
    """
    class_ = type.value
    return class_._create_from_meta(*args, **kwargs)


class BaseServerAdapterMixin:
    """Mixin for network communication."""

    ctx: CTX

    @property
    def client(self) -> HttpxClient:
        """Return client singleton."""
        # We don't need to worry about passing args here, cause it's a singleton.
        return self.ctx.extra["httpx_client"]

    @property
    def hash_ring(self) -> HashRing[Node]:
        """Return HashRing instance."""
        hash_ring = self.ctx.extra.get("hash_ring")
        if not hash_ring:
            msg = "Attempt to use cluster logic in single server mode"
            raise AttributeError(msg)
        return hash_ring

    @property
    def nodes(self) -> list[Node]:
        """Return list of nodes."""
        config: ClusterConfig = self.ctx.extra.get("cluster_config")
        if not config:
            host, port = self.ctx.uri.netloc.split(":")
            return [Node(host=host, port=port, protocol=self.ctx.uri.scheme)]
        return config.current

    @property
    def nodes_urls(self) -> list[str]:
        """Raw urls of the nodes."""
        return [node.url.raw_url for node in self.nodes]


class ServerArrayAdapterMixin(BaseServerAdapterMixin):
    """Mixin with server logic for adapters."""

    type: ArrayType
    collection_path: Uri

    def __merge_node_and_collection_path(self, node: str) -> str:
        """Create new url from collection_path path and node host.

        :param node: Node to merge with
        :returns: node host joined with the collection path
        """
        return f"{node.rstrip('/')}{self.collection_path.path}"

    @property
    def collection_host(self) -> str:
        """Merge scheme with it's path."""
        return f"{self.collection_path.scheme}{Uri.__annotations__['netloc']['separator']}{self.collection_path.netloc}"

    @property
    def path_stripped(self) -> str:
        """Strip slash on collection path."""
        return self.collection_path.path.rstrip("/")

    def create(self, array: Union[dict, "BaseArray"]) -> BaseArray:
        """Create array on server.

        :param array: Array instance or dict
        :returns: created Array or VArray instance
        """
        if isinstance(array, dict):
            primary_attributes = array["primary_attributes"]
            custom_attributes = array["custom_attributes"]
            id_ = array.get("id_") or (self.client.cluster_mode and get_id() or None)
            array["id_"] = id_
        else:
            primary_attributes = array.primary_attributes
            custom_attributes = array.custom_attributes
            id_ = array.id

        kwargs = {
            "primary_attributes": convert_datetime_attrs_to_iso(primary_attributes),
            "custom_attributes": convert_datetime_attrs_to_iso(custom_attributes),
            "id_": id_,
        }

        request_kwargs: dict[str, Any] = {"json": kwargs}
        url = f"{self.path_stripped}/{self.type.name}s"
        if self.type == ArrayType.array and self.client.cluster_mode:
            response = request_in_cluster(url, array, self.ctx, method="POST", request_kwargs=request_kwargs)
        else:
            response = self.client.post(f"{self.collection_host}{url}", **request_kwargs)

        if not response or response.status_code != STATUS_CREATED:
            raise DekerServerError(response, "Couldn't create an array")

        try:
            data = response.json()
        except JSONDecodeError:
            raise DekerServerError(response, "Couldn't parse json")

        if isinstance(array, dict):
            instance_id = data.get("id")
            if not instance_id:
                raise DekerServerError(response, "Server response doesn't contain ID field")

            kwargs = {
                "collection": array["collection"],
                "adapter": array["adapter"],
                "id_": instance_id,
                "primary_attributes": array["primary_attributes"],
                "custom_attributes": array["custom_attributes"],
            }

            model = self.type.value
            if model == VArray:
                kwargs["array_adapter"] = array["array_adapter"]
            return model(**kwargs)
        return array

    def read_meta(self, array: "BaseArray") -> ArrayMeta:
        """Read metadata of (v)array.

        :param array: Instance of (v)array
        :return:
        """
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"

        # Only Varray can be located on different nodes yet.
        if self.type == ArrayType.varray and self.client.cluster_mode:
            response = make_request(url=url, nodes=self.nodes_urls, client=self.client, retry_on_hash_failure=True)
        elif self.client.cluster_mode:
            response = request_in_cluster(url, array, self.ctx, True)
        else:
            response = self.client.get(f"{self.collection_host}{url}")

        if response is None or response.status_code != STATUS_OK:
            raise DekerServerError(response, "Couldn't fetch an array")

        return response.json()

    def update_meta_custom_attributes(
        self,
        array: "BaseArray",
        attributes: dict,
    ) -> None:
        """Update custom attributes of (v)array.

        :param array: Instance of (v)array
        :param attributes: dict with attributes to update
        """
        # Writing is always on leader node for non array

        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"
        request_kwargs: dict[str, Any] = {"json": {"custom_attributes": convert_datetime_attrs_to_iso(attributes)}}
        if self.client.cluster_mode and self.type == ArrayType.array:
            response = request_in_cluster(
                url, array, self.ctx, should_check_status=True, method="PUT", request_kwargs=request_kwargs
            )
        else:
            response = self.client.put(f"{self.collection_host}{url}", **request_kwargs)
        if not response or response.status_code != STATUS_OK:
            raise DekerServerError(response, "Couldn't update attributes")

    def delete(self, array: "BaseArray") -> None:
        """Delete array on server.

        :param array: Array/Varray to be deleted
        """
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"

        if self.client.cluster_mode and self.type == ArrayType.array:
            response = request_in_cluster(url, array, self.ctx, should_check_status=True, method="DELETE")
        else:
            response = self.client.delete(f"{self.collection_host}{url}")

        if not response or response.status_code != STATUS_OK:
            raise DekerServerError(response, f"Couldn't delete the {self.type.name}")

    @staticmethod
    def _fill_from_response(result: ndarray, response: Response) -> None:
        """Copy the response body chunk by chunk into ``result``.

        :param result: array to fill, its byte size must match the body
        :param response: streamed response with the raw array bytes
        :raises DekerServerError: if the body size differs from the array size
        """
        buffer = result.reshape(-1).view(np.uint8)
        offset = 0
        for chunk in response.iter_bytes(READ_CHUNK_SIZE):
            end = offset + len(chunk)
            if end > buffer.size:
                raise DekerServerError(response, f"Server sent more than {buffer.size} bytes for the subset")
            buffer[offset:end] = np.frombuffer(chunk, dtype=np.uint8)
            offset = end
        if offset != buffer.size:
            raise DekerServerError(response, f"Server sent {offset} bytes for the subset, {buffer.size} expected")

    def read_data(
        self,
        array: "BaseArray",
        bounds: Slice,
    ) -> Numeric | ndarray | None:
        """Read data from array/varray.

        :param array: From what array we read
        :param bounds: What part of array we read
        :return:
        """
        bounds_ = slice_converter[bounds]
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}/subset/{bounds_}/data"
        headers = {"Accept": "application/octet-stream"}
        try:
            if self.client.cluster_mode:
                response = request_in_cluster(
                    url, array, self.ctx, should_check_status=True, request_kwargs={"headers": headers}, stream=True
                )
            else:
                response = self.client.request("GET", f"{self.collection_host}{url}", headers=headers, stream=True)
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )

        if not response or response.status_code not in [STATUS_OK, TIMEOUT]:
            raise DekerServerError(response, "Couldn't read the array")

        if response.status_code == TIMEOUT:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )

        # the body is streamed straight into the result: no intermediate copy of the whole payload
        result = np.empty(array[bounds].shape, dtype=array.dtype)
        try:
            self._fill_from_response(result, response)
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )
        finally:
            response.close()
        if result.ndim == 0:
            return result[()]
        return result

    def update(self, array: "BaseArray", bounds: Slice, data: Numeric) -> None:
        """Update array/varray on server.

        :param array: Array/Varray that will be updated
        :param bounds: Part of the array to update
        :param data: Data that will replace current values
        """
        bounds = slice_converter[bounds]
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}/subset/{bounds}/data"
        request_kwargs: dict[str, Any] = {"headers": {"Content-Type": "application/octet-stream"}}
        if hasattr(data, "tobytes"):
            request_kwargs["data"] = data.tobytes()
        else:
            request_kwargs["data"] = bytes(data)
        # We write (v)array through the node it belongs in cluster
        try:
            if self.client.cluster_mode:
                response = request_in_cluster(
                    url, array, self.ctx, should_check_status=True, method="PUT", request_kwargs=request_kwargs
                )
            else:
                response = self.client.put(f"{self.collection_host}{url}", **request_kwargs)
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} update {array}",
            )

        if not response or response.status_code not in [STATUS_OK, TIMEOUT]:
            raise DekerServerError(response, "Couldn't update array")

        if response.status_code == TIMEOUT:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} update {array}",
            )

    def clear(self, array: "BaseArray", bounds: Slice) -> None:
        """Clear array/varray.

        :param array: Instance of array/varray
        :param bounds: Part of the array/varray that will be cleared
        """
        return self.update(array, bounds, [])

    def is_deleted(self, array: "BaseArray") -> bool:
        """Check if array/varray was deleted on server.

        :param array: Instance of array/varray
        :returns: ``True`` if the array no longer exists on the server
        """
        return False

    def __create_array_from_response(self, response: Response, kwargs: dict) -> BaseArray | None:
        """Create array from json meta.

        :param response: response from server
        :param kwargs: Kwargs for array creation
        :returns: array built from the response meta, or ``None`` if the response is empty
        """
        if response.status_code == NOT_FOUND:  # pragma: no cover  # guard: callers return None on 404 themselves
            return None
        if response.status_code == STATUS_OK:
            return create_array_from_meta(**{**kwargs, "meta": response.json()})

        raise DekerServerError(response, "Couldn't fetch an array")  # pragma: no cover  # guard: callers raise first

    def get_by_primary_attributes(
        self,
        primary_attributes: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> Optional["BaseArray"]:
        """Get array by given primary attribute.

        :param primary_attributes: Key attributes
        :param schema: Array or Varray schema
        :param collection: Collection instance
        :param array_adapter: Adapter of arrays
        :param varray_adapter: Adapter of varrays
        :returns: matching Array or VArray, or ``None`` if nothing was found
        """
        attrs = []
        primary_attributes = convert_datetime_attrs_to_iso(primary_attributes)
        for attr in schema.attributes:
            if not attr.primary:
                continue

            if attr.dtype == tuple:
                attrs.append(str(tuple(primary_attributes[attr.name])))
            else:
                attrs.append(str(primary_attributes[attr.name]))

        primary_path = "/".join(attrs)

        # We read an Array through the node it belongs
        url = f"{self.path_stripped}/{self.type.name}/by-primary/{primary_path}"
        if self.client.cluster_mode:
            response = request_in_cluster(url, array={"primary_attributes": primary_attributes}, ctx=self.ctx)
        else:
            response = self.client.get(
                f"{self.collection_host}{url}",
            )
        if not response or response.status_code not in [STATUS_OK, NOT_FOUND]:
            raise DekerServerError(response, "Couldn't fetch array")
        if response.status_code == NOT_FOUND:
            return None
        return self.__create_array_from_response(
            response,
            {
                "type": self.type,
                "collection": collection,
                "array_adapter": array_adapter,
                "varray_adapter": varray_adapter,
            },
        )

    def get_by_id(
        self,
        id_: str,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> Optional["BaseArray"]:
        """Get and array/varray by given ID.

        :param id_: ID of array/varray
        :param collection: Deker's collection instance
        :param array_adapter: Array adapter
        :param varray_adapter: Varray adapter
        :return:
        """
        url = f"{self.path_stripped}/{self.type.name}/by-id/{id_}"

        if self.client.cluster_mode:
            # Hash from ID and hash from primary attributes are different
            # So if schema has primary attributes,
            # it means that hash has been calculated using primary custom_attributes
            # Therefore, we cannot let filter by id.
            schema = collection.varray_schema or collection.array_schema
            if schema.primary_attributes:
                raise FilteringByIdInClusterIsForbidden

            response = request_in_cluster(
                url,
                {"id_": id_},
                self.ctx,
            )
        else:
            response = self.client.get(
                f"{self.collection_host}{url}",
            )

        if not response or response.status_code not in [STATUS_OK, NOT_FOUND]:
            raise DekerServerError(response, "Couldn't fetch array")

        if response.status_code == NOT_FOUND:
            return None

        return self.__create_array_from_response(
            response,
            {
                "type": self.type,
                "collection": collection,
                "array_adapter": array_adapter,
                "varray_adapter": varray_adapter,
            },
        )

    def __iter__(self) -> Generator["ArrayMeta", None, None]:
        urls = [f"{self.collection_path.raw_url}/{self.type.name}s"]
        if self.type == ArrayType.array and self.client.cluster_mode:
            urls = [
                f"{self.__merge_node_and_collection_path(node.url.raw_url)}/{self.type.name}s" for node in self.nodes
            ]

        for url in urls:
            response = self.client.get(url)

            if response.status_code != STATUS_OK:
                if self.type == ArrayType.array:
                    logger.warning(f"Couldn't fetch arrays from node: {url}")
                else:
                    raise DekerServerError(response, "Couldn't get list of arrays")
            else:
                yield from response.json()
