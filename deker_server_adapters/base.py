import logging
import warnings

from collections.abc import Generator
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, NoReturn, Optional, cast

import numpy as np

from cantok import AbstractToken, DefaultToken
from deker import Array, Collection, VArray
from deker.ABC.base_adapters import IArrayAdapter
from deker.ctx import CTX
from deker.errors import DekerLockError, DekerTimeoutError, DekerVSubsetError
from deker.tools.array import get_id
from deker.tools.time import convert_datetime_attrs_to_iso
from deker.types import ArrayMeta, Data, Numeric, Slice
from deker.types.private.typings import ArrayT
from deker.uri import Uri
from deker_tools.cancel import check, mark_started, resolve_timeout
from deker_tools.hash_ring import HashRing
from deker_tools.slices import slices_to_str
from deker_tools.typings import SentinelType, is_sentinel
from httpx import Response, Timeout, TimeoutException
from numpy import ndarray

from deker_server_adapters.cluster_config import ClusterConfig, Node
from deker_server_adapters.consts import (
    LOCKED,
    NETWORK_TIMEOUT_MARGIN,
    NOT_FOUND,
    READ_CHUNK_SIZE,
    RETRY_AFTER_HEADER,
    SERVICE_UNAVAILABLE,
    STATUS_CREATED,
    STATUS_OK,
    TIMEOUT,
    TIMEOUT_PARAM,
    WRITTEN_PARTLY,
    ArrayType,
)
from deker_server_adapters.errors import (
    DekerClusterError,
    DekerServerError,
    DekerTimeoutServer,
    FilteringByIdInClusterIsForbidden,
)
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils.requests import make_request, request_in_cluster

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_schemas import BaseArraysSchema

logger = logging.getLogger(__name__)


def server_message(response: Response) -> str:
    """Get the message of an error response of the server.

    :param response: the error response
    :returns: the ``message`` of its JSON body, or the whole body when it is not such JSON
    """
    try:
        return str(response.json()["message"])
    except (JSONDecodeError, KeyError, TypeError):
        return response.text


def retry_after_of(response: Response) -> int | None:
    """Read the seconds of the Retry-After header of a response.

    :param response: the response
    :returns: the seconds, ``None`` when the header is absent or not a number
    """
    value = response.headers.get(RETRY_AFTER_HEADER)
    if value is None or not value.strip().isdigit():
        return None
    return int(value)


def timeout_kwargs(*, timeout: int | SentinelType, default: int) -> dict[str, Any]:
    """Build the arguments of a request for the timeout of its operation.

    The server gets the timeout only when the call gave it, otherwise it applies its own default. The http client
    waits for the answer as long as the operation may take on the server, and ``NETWORK_TIMEOUT_MARGIN`` more.

    :param timeout: seconds the call gave to the operation, or ``Sentinel``
    :param default: seconds of the setting of the client the timeout of the operation defaults to
    :returns: the ``timeout`` of the http client, and the ``params`` with the timeout when the call gave it
    """
    kwargs: dict[str, Any] = {"timeout": Timeout(resolve_timeout(timeout, default) + NETWORK_TIMEOUT_MARGIN)}
    if not is_sentinel(timeout):
        kwargs["params"] = {TIMEOUT_PARAM: str(timeout)}
    return kwargs


def raise_for_status(response: Response | None, message: str) -> NoReturn:
    """Raise the error of deker for a response of the server which is not 200.

    :param response: the response, ``None`` when no node answered
    :param message: what the adapter was doing, for the generic error
    :raises DekerLockError: if the server answered 423: the array is held by another process
    :raises DekerVSubsetError: if the server answered 409: some Arrays of the VArray could not be written
    :raises DekerTimeoutError: if the server answered 504: the read timeout of the operation is over
    :raises DekerClusterError: if the server answered 503: it cannot take the request now, with the seconds to wait
    :raises DekerServerError: for any other status, or if no node answered
    """
    if response is None:
        raise DekerServerError(message=message)
    if response.status_code == LOCKED:
        raise DekerLockError(server_message(response))
    if response.status_code == WRITTEN_PARTLY:
        raise DekerVSubsetError(server_message(response), [])
    if response.status_code == TIMEOUT:
        raise DekerTimeoutError(f"{message}: {server_message(response)}")
    if response.status_code == SERVICE_UNAVAILABLE:
        retry_after = retry_after_of(response)
        logger.error(
            "%s: %s answered 503 %s, retry after %s s",
            message,
            response.url.host,
            server_message(response),
            retry_after,
        )
        raise DekerClusterError(response, message, retry_after=retry_after)
    raise DekerServerError(response, message)


def create_array_from_meta(
    type: ArrayType,
    *args: Any,
    **kwargs: Any,
) -> Array | VArray:
    r"""Build an Array or a VArray from its metadata, by the kind.

    :param type: kind of the array
    :param \*args: arguments of ``_create_from_meta``
    :param \**kwargs: keyword arguments of ``_create_from_meta``
    :returns: Array or VArray built from its meta
    """
    class_ = type.value
    return class_._create_from_meta(*args, **kwargs)


class BaseServerAdapterMixin:
    """Mixin for network communication: the http client, the hash ring and the nodes shared through the context."""

    ctx: CTX

    @property
    def client(self) -> HttpxClient:
        """Return the http client shared by the adapters of the context."""
        # We don't need to worry about passing args here, cause it's a singleton.
        return self.ctx.extra["httpx_client"]

    @property
    def hash_ring(self) -> HashRing[Node]:
        """Return the hash ring of the cluster.

        :raises AttributeError: in the single server mode, which has no ring
        """
        hash_ring = self.ctx.extra.get("hash_ring")
        if not hash_ring:
            msg = "Attempt to use cluster logic in single server mode"
            raise AttributeError(msg)
        return hash_ring

    @property
    def nodes(self) -> list[Node]:
        """Return the nodes of the cluster, or the single server as the only node."""
        config: ClusterConfig | None = self.ctx.extra.get("cluster_config")
        if not config:
            host, port = self.ctx.uri.netloc.split(":")
            return [Node(host=host, port=port, protocol=self.ctx.uri.scheme)]
        return config.current

    @property
    def nodes_urls(self) -> list[str]:
        """Raw urls of the nodes."""
        return [node.url.raw_url for node in self.nodes]


class ServerArrayAdapterMixin(BaseServerAdapterMixin, IArrayAdapter[Uri, ArrayT]):
    """Mixin with server logic for adapters.

    ``ArrayT`` is the kind of arrays the adapter manages: ``Array`` or ``VArray``.
    """

    type: ArrayType

    def __merge_node_and_collection_path(self, node: str) -> str:
        """Create new url from collection_path path and node host.

        :param node: Node to merge with
        :returns: node host joined with the collection path
        """
        return f"{node.rstrip('/')}{self.collection_path.path}"

    @property
    def path_stripped(self) -> str:
        """Get the path of the collection uri without the trailing slash."""
        return self.collection_path.path.rstrip("/")

    def __post_array(
        self, array: ArrayT | dict, kwargs: dict[str, Any], *, timeout: int | SentinelType, token: AbstractToken
    ) -> Response:
        """Send the request creating an array and check that it was created.

        :param array: Array/VArray instance or its description, used to pick the node in cluster mode
        :param kwargs: json body of the request
        :param timeout: seconds the call gave to the creation, or ``Sentinel``
        :param token: token of the write operation; its start flag is closed right before the request
        :returns: the server response
        :raises DekerServerError: if the server answered anything but 201
        """
        request_kwargs: dict[str, Any] = {
            "json": kwargs,
            **timeout_kwargs(timeout=timeout, default=self.ctx.config.write_lock_timeout),
        }
        url = f"{self.path_stripped}/{self.type.name}s"
        mark_started(token)
        if self.type == ArrayType.array and self.client.cluster_mode:
            response = request_in_cluster(
                url, array, self.ctx, method="POST", request_kwargs=request_kwargs, token=token
            )
        else:
            response = self.client.post(url, **request_kwargs)

        if response is None or response.status_code != STATUS_CREATED:
            raise_for_status(response, f"Couldn't create the {self.type.name}")
        return response

    def create(self, array: ArrayT | dict, *, timeout: int | SentinelType, token: AbstractToken) -> ArrayT:
        """Create array on server.

        :param array: Array/VArray instance; a dict is accepted for backward compatibility only,
            see ``create_from_dict``
        :param timeout: seconds the call gave to the creation, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation
        :returns: the same instance, once it is created on the server
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerServerError: if the server answered anything but 201
        """
        check(token)
        if isinstance(array, dict):
            return self.create_from_dict(array, timeout=timeout, token=token)

        kwargs = {
            "primary_attributes": convert_datetime_attrs_to_iso(array.primary_attributes),
            "custom_attributes": convert_datetime_attrs_to_iso(array.custom_attributes),
            "id_": array.id,
        }
        self.__post_array(array, kwargs, timeout=timeout, token=token)
        return array

    # TODO: remove in 3.0.0 together with the dict branch of ``create``
    def create_from_dict(self, data: dict, *, timeout: int | SentinelType, token: AbstractToken) -> ArrayT:
        """Create array on server from its description and build the instance from the server response.

        .. deprecated:: 2.0.0
           Deker creates arrays from instances only; this method will be removed in 3.0.0.

        :param data: dict with ``primary_attributes``, ``custom_attributes``, ``collection``, ``adapter``,
            ``array_adapter`` for a VArray, and an optional ``id_``; without an id the server assigns one
            (in cluster mode the id is generated by the client, as the node is picked by it)
        :param timeout: seconds the call gave to the creation, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation
        :returns: Array/VArray built from the description and the id in the server response
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerServerError: if the server answered anything but 201 or the response has no id
        """
        check(token)
        warnings.warn(
            "Creating arrays from a dict is deprecated and will be removed in 3.0.0; "
            "pass an Array or VArray instance to create instead",
            DeprecationWarning,
            stacklevel=2,
        )
        primary_attributes = data["primary_attributes"]
        custom_attributes = data["custom_attributes"]
        id_ = data.get("id_") or (self.client.cluster_mode and get_id() or None)
        data["id_"] = id_

        kwargs = {
            "primary_attributes": convert_datetime_attrs_to_iso(primary_attributes),
            "custom_attributes": convert_datetime_attrs_to_iso(custom_attributes),
            "id_": id_,
        }
        response = self.__post_array(data, kwargs, timeout=timeout, token=token)

        try:
            body = response.json()
        except JSONDecodeError:
            raise DekerServerError(response, "Couldn't parse json")

        instance_id = body.get("id")
        if not instance_id:
            raise DekerServerError(response, "Server response doesn't contain ID field")

        kwargs = {
            "collection": data["collection"],
            "adapter": data["adapter"],
            "id_": instance_id,
            "primary_attributes": data["primary_attributes"],
            "custom_attributes": data["custom_attributes"],
        }

        model = self.type.value
        if model == VArray:
            kwargs["array_adapter"] = data["array_adapter"]
        return cast("ArrayT", model(**kwargs))

    def read_meta(self, array: ArrayT) -> ArrayMeta:
        """Read metadata of (v)array from the server.

        :param array: Instance of (v)array
        :returns: the metadata
        :raises DekerLockError: if the array is held by another process
        :raises DekerServerError: if the server answered anything but 200
        """
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"

        # Only Varray can be located on different nodes yet.
        if self.type == ArrayType.varray and self.client.cluster_mode:
            response = make_request(
                url=url, nodes=self.nodes_urls, client=self.client, retry_on_hash_failure=True, token=DefaultToken()
            )
        elif self.client.cluster_mode:
            response = request_in_cluster(url, array, self.ctx, True, token=DefaultToken())
        else:
            response = self.client.get(url)

        if response is None or response.status_code != STATUS_OK:
            raise_for_status(response, "Couldn't fetch an array")

        return response.json()

    def update_meta_custom_attributes(
        self,
        array: ArrayT,
        attributes: dict,
        *,
        timeout: int | SentinelType,
        token: AbstractToken,
    ) -> None:
        """Update custom attributes of (v)array on the server.

        :param array: Instance of (v)array
        :param attributes: dict with attributes to update
        :param timeout: seconds to wait for the lock of the file, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerLockError: if the array is held by another process
        :raises DekerServerError: if the server answered anything but 200
        """
        check(token)
        # Writing is always on leader node for non array

        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"
        request_kwargs: dict[str, Any] = {
            "json": {"custom_attributes": convert_datetime_attrs_to_iso(attributes)},
            **timeout_kwargs(timeout=timeout, default=self.ctx.config.write_lock_timeout),
        }
        mark_started(token)
        if self.client.cluster_mode and self.type == ArrayType.array:
            response = request_in_cluster(
                url, array, self.ctx, should_check_status=True, method="PUT", request_kwargs=request_kwargs, token=token
            )
        else:
            response = self.client.put(url, **request_kwargs)
        if response is None or response.status_code != STATUS_OK:
            raise_for_status(response, "Couldn't update attributes")

    def delete(self, array: ArrayT, *, background: bool, _empty_trash: bool, token: AbstractToken) -> None:
        """Delete array on server: one request, the server deletes the whole Array or VArray and empties its trash.

        :param array: Array/Varray to be deleted
        :param background: sent to the server: it answers right after the deletion and empties its trash in the
          background
        :param _empty_trash: taken for the common signature of the adapters and not used here: the server empties its
          trash after each deletion it makes, by ``background``
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerLockError: if the server answered 423
        :raises DekerServerError: if the server answered anything but 200
        """
        check(token)
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}"
        request_kwargs: dict[str, Any] = {"params": {"background": str(background).lower()}}

        mark_started(token)
        if self.client.cluster_mode and self.type == ArrayType.array:
            response = request_in_cluster(
                url,
                array,
                self.ctx,
                should_check_status=True,
                method="DELETE",
                request_kwargs=request_kwargs,
                token=token,
            )
        else:
            response = self.client.delete(url, **request_kwargs)

        if response is None or response.status_code != STATUS_OK:
            raise_for_status(response, f"Couldn't delete the {self.type.name}")

    def empty_trash(self, *, background: bool) -> None:
        """Do nothing: the server empties its trash after each deletion it makes, by the background of the request.

        :param background: not used: see above
        """

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
        array: ArrayT,
        bounds: Slice,
        *,
        timeout: int | SentinelType,
        token: AbstractToken,
    ) -> Numeric | ndarray:
        """Read data from array/varray, streaming the response into an array of the shape of the bounds.

        :param array: From what array we read
        :param bounds: What part of array we read
        :param timeout: seconds of the read, sent to the server; ``Sentinel`` - the default of the server
        :param token: token of the read operation
        :returns: data within the bounds; a scalar for a single cell
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerLockError: if the array is held by a writer
        :raises DekerTimeoutError: if the server answered 504: the read took longer than its timeout
        :raises DekerServerError: if the server answered anything but 200, or sent a body of another size
        :raises DekerTimeoutServer: if the server did not answer in time
        """
        check(token)
        bounds_ = slices_to_str(bounds)
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}/subset/{bounds_}/data"
        request_kwargs: dict[str, Any] = {
            "headers": {"Accept": "application/octet-stream"},
            **timeout_kwargs(timeout=timeout, default=self.ctx.config.read_timeout),
        }
        try:
            if self.client.cluster_mode:
                response = request_in_cluster(
                    url,
                    array,
                    self.ctx,
                    should_check_status=True,
                    request_kwargs=request_kwargs,
                    stream=True,
                    token=token,
                )
            else:
                response = self.client.request("GET", url, stream=True, **request_kwargs)
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )

        if response is None or response.status_code != STATUS_OK:
            raise_for_status(response, "Couldn't read the array")

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

    def __put_data(
        self, array: ArrayT, bounds: Slice, body: bytes, *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Send the bytes of a region of the array to the server.

        :param array: Array/Varray that will be updated
        :param bounds: Part of the array to update
        :param body: raw bytes of the region in the array dtype; empty bytes clear the region
        :param timeout: seconds to wait for the lock of each file, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation; its start flag is closed right before the request
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerLockError: if the array is held by another process
        :raises DekerVSubsetError: if some Arrays of the VArray could not be written, the others are written
        :raises DekerServerError: if the server answered anything but 200
        :raises DekerTimeoutServer: if the server did not answer in time
        """
        bounds_ = slices_to_str(bounds)
        url = f"{self.path_stripped}/{self.type.name}/by-id/{array.id}/subset/{bounds_}/data"
        request_kwargs: dict[str, Any] = {
            "headers": {"Content-Type": "application/octet-stream"},
            "data": body,
            **timeout_kwargs(timeout=timeout, default=self.ctx.config.write_lock_timeout),
        }
        mark_started(token)
        # We write (v)array through the node it belongs in cluster
        try:
            if self.client.cluster_mode:
                response = request_in_cluster(
                    url,
                    array,
                    self.ctx,
                    should_check_status=True,
                    method="PUT",
                    request_kwargs=request_kwargs,
                    token=token,
                )
            else:
                response = self.client.put(url, **request_kwargs)
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} update {array}",
            )

        if response is None or response.status_code != STATUS_OK:
            raise_for_status(response, "Couldn't update array")

    def update(
        self, array: ArrayT, bounds: Slice, data: Data, *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Update array/varray on server.

        The data is validated and cast to the array dtype and the shape of the region before it is sent,
        the same way the local adapter stores it: a scalar fills the whole region.

        :param array: Array/Varray that will be updated
        :param bounds: Part of the array to update
        :param data: Data that will replace current values
        :param timeout: seconds to wait for the lock of each file, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerArrayTypeError: if the data cannot be cast to the array dtype without loss
        :raises IndexError: if the data shape does not match the region
        """
        check(token)
        processed = self._process_data(array.dtype, array.shape, data, bounds)
        region = np.broadcast_to(processed, array[bounds].shape)
        self.__put_data(array, bounds, region.tobytes(), timeout=timeout, token=token)

    def clear(self, array: ArrayT, bounds: Slice, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Clear a region of the array/varray on the server: an empty body is sent for it.

        :param array: Instance of array/varray
        :param bounds: Part of the array/varray that will be cleared
        :param timeout: seconds to wait for the lock of each file, sent to the server; ``Sentinel`` - the default of
          the server
        :param token: token of the write operation
        :raises TokenCancelledError: if the token is cancelled before the request is sent
        :raises DekerServerError: if the server answered anything but 200
        :raises DekerTimeoutServer: if the server did not answer in time
        """
        check(token)
        self.__put_data(array, bounds, b"", timeout=timeout, token=token)

    def is_deleted(self, array: ArrayT) -> bool:
        """Check if array/varray was deleted on server: not checked, the server answers 404 to a deleted array.

        :param array: Instance of array/varray
        :returns: always ``False``
        """
        return False

    def __create_array_from_response(self, response: Response, kwargs: dict) -> ArrayT | None:
        """Create array from json meta.

        :param response: response from server
        :param kwargs: Kwargs for array creation
        :returns: array built from the response meta, or ``None`` if the response is empty
        """
        if response.status_code == NOT_FOUND:  # pragma: no cover  # guard: callers return None on 404 themselves
            return None
        if response.status_code == STATUS_OK:
            return cast("ArrayT", create_array_from_meta(**{**kwargs, "meta": response.json()}))

        raise DekerServerError(response, "Couldn't fetch an array")  # pragma: no cover  # guard: callers raise first

    def _response_by_primary_attributes(self, primary_attributes: dict, schema: "BaseArraysSchema") -> Response | None:
        """Ask the server for the array with the primary attributes.

        :param primary_attributes: Key attributes
        :param schema: Array or Varray schema
        :returns: the response with the metadata of the array, or ``None`` if the server has no such array
        :raises DekerLockError: if the array is held by a writer
        :raises DekerServerError: if the server answered anything but 200 or 404
        """
        attrs = []
        primary_attributes = convert_datetime_attrs_to_iso(primary_attributes) or {}
        for attr in schema.primary_attributes or ():
            if attr.dtype == tuple:
                attrs.append(str(tuple(primary_attributes[attr.name])))
            else:
                attrs.append(str(primary_attributes[attr.name]))

        primary_path = "/".join(attrs)

        # We read an Array through the node it belongs
        url = f"{self.path_stripped}/{self.type.name}/by-primary/{primary_path}"
        if self.client.cluster_mode:
            response = request_in_cluster(
                url, array={"primary_attributes": primary_attributes}, ctx=self.ctx, token=DefaultToken()
            )
        else:
            response = self.client.get(
                url,
            )
        if not response or response.status_code not in [STATUS_OK, NOT_FOUND]:
            raise_for_status(response, "Couldn't fetch array")
        if response.status_code == NOT_FOUND:
            return None
        return response

    def get_by_primary_attributes(
        self,
        primary_attributes: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Get array by given primary attribute.

        :param primary_attributes: Key attributes
        :param schema: Array or Varray schema
        :param collection: Collection instance
        :param array_adapter: Adapter of arrays
        :param varray_adapter: Adapter of varrays
        :returns: matching Array or VArray, or ``None`` if nothing was found
        :raises DekerFilterError: if the attributes are not exactly the primary attributes of the schema
        :raises DekerServerError: if the server answered anything but 200 or 404
        """
        response = self._response_by_primary_attributes(
            self._check_primary_attributes(primary_attributes, schema), schema
        )
        if response is None:
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

    def get_id_by_primary_attributes(self, primary_attributes: dict, schema: "BaseArraysSchema") -> str | None:
        """Get the id of the array with the primary attributes: the server answers with the whole metadata anyway.

        :param primary_attributes: Key attributes
        :param schema: Array or Varray schema
        :returns: the id, or ``None`` if the server has no such array
        :raises DekerFilterError: if the attributes are not exactly the primary attributes of the schema
        :raises DekerServerError: if the server answered anything but 200 or 404
        """
        response = self._response_by_primary_attributes(
            self._check_primary_attributes(primary_attributes, schema), schema
        )
        if response is None:
            return None
        return cast("str", response.json()["id"])

    def get_by_id(
        self,
        id_: str,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Get an array/varray by given ID.

        :param id_: ID of array/varray
        :param collection: Deker's collection instance
        :param array_adapter: Array adapter
        :param varray_adapter: Varray adapter
        :returns: the array, or ``None`` if the server has none with the id
        :raises FilteringByIdInClusterIsForbidden: in cluster mode for a schema with primary attributes: the node
          of an array is chosen by them, not by the id
        :raises DekerLockError: if the array is held by a writer
        :raises DekerServerError: if the server answered anything but 200 or 404
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

            response = request_in_cluster(url, {"id_": id_}, self.ctx, token=DefaultToken())
        else:
            response = self.client.get(
                url,
            )

        if not response or response.status_code not in [STATUS_OK, NOT_FOUND]:
            raise_for_status(response, "Couldn't fetch array")

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
        """Iterate over the metadata of the arrays of the collection; in cluster mode the Arrays of every node.

        :yields: the metadata of every array
        """
        urls = [f"{self.collection_path.raw_url}/{self.type.name}s"]
        if self.type == ArrayType.array and self.client.cluster_mode:
            urls = [
                f"{self.__merge_node_and_collection_path(node.url.raw_url)}/{self.type.name}s" for node in self.nodes
            ]

        for url in urls:
            response = self.client.get(url)

            if response.status_code != STATUS_OK:
                if self.type == ArrayType.array:
                    logger.warning("Couldn't fetch arrays from node: %s", url)
                else:
                    raise DekerServerError(response, "Couldn't get list of arrays")
            else:
                yield from response.json()
