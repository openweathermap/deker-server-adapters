import logging

from collections.abc import Generator
from datetime import datetime
from json import JSONDecodeError
from random import choice
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import numpy as np

from deker import Array, Collection, VArray
from deker.ABC.base_array import BaseArray
from deker.ctx import CTX
from deker.tools.array import get_id
from deker.tools.time import convert_datetime_attrs_to_iso
from deker.types import ArrayMeta, Numeric, Slice
from deker.uri import Uri
from deker_tools.slices import slice_converter
from deker_tools.time import get_utc
from httpx import Response, TimeoutException
from numpy import ndarray

from deker_server_adapters.consts import NOT_FOUND, STATUS_CREATED, STATUS_OK, TIMEOUT, ArrayType
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer, FilteringByIdInClusterIsForbidden
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils import get_hash_by_primary_attrs, get_hash_key, make_request


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_schemas import BaseArraysSchema

logger = logging.getLogger(__name__)


def create_array_from_meta(
    type: ArrayType,
    *args: Any,
    **kwargs: Any,
) -> Union[Array, VArray]:
    """Override class method.

    :param type: Type of Array
    :param args: Create array meta params
    :param kwargs: Create array meta kwargs
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
        return self.ctx.extra["httpx_client"]  # type: ignore[attr-defined]

    @property
    def hash_ring(self) -> HashRing:
        """Return HashRing instance."""
        hash_ring = self.ctx.extra.get("hash_ring")
        if not hash_ring:
            raise AttributeError("Attempt to use cluster logic in single server mode")
        return hash_ring  # type: ignore[attr-defined]

    @property
    def nodes(self) -> List[str]:
        """Return list of nodes."""
        nodes = self.ctx.extra["nodes"]  # type: ignore[attr-defined]
        if not nodes:
            raise AttributeError("Attempt to use cluster logic in single server mode")
        return nodes


class ServerArrayAdapterMixin(BaseServerAdapterMixin):
    """Mixin with server logic for adapters."""

    type: ArrayType
    collection_path: Uri

    def get_host_url(self, id_: Optional[str]) -> str:
        """Get random node with given id.

        :param id_: ID of node
        """
        assert id_, "Node ID is None"
        mapping_id_to_url = self.ctx.extra.get("nodes_mapping")
        if mapping_id_to_url is None:
            raise AttributeError("Attempt to use cluster logic in single server mode")
        hosts = mapping_id_to_url[id_]
        return choice(hosts)

    def get_node(self, array: BaseArray) -> str:
        """Get hash for primary attributes or id.

        :param array: Array or varray
        """
        if not array.primary_attributes:
            return self.hash_ring.get_node(array.id) or ""

        return self.get_node_by_primary_attrs(array.primary_attributes)

    def get_node_by_primary_attrs(self, primary_attributes: Dict) -> str:
        """Get hash by primary attributes.

        :param primary_attributes: Dict of primary attributes
        """
        attrs_to_join = []
        for attr in primary_attributes:
            attribute = primary_attributes[attr]
            if attr == "v_position":
                value = "-".join(str(el) for el in attribute)
            else:
                value = get_utc(attribute).isoformat() if isinstance(attribute, datetime) else str(attribute)
            attrs_to_join.append(value)
        return self.hash_ring.get_node("/".join(attrs_to_join)) or ""

    def __merge_node_and_collection_path(self, node: str) -> str:
        """Create new url from collection_path path and node host.

        :param node: Node to merge with
        """
        return f"{node.rstrip('/')}{self.collection_path.path}"

    def create(self, array: Union[dict, "BaseArray"]) -> BaseArray:
        """Create array on server.

        :param array: Array instance or dict
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

        path = self.collection_path.raw_url.rstrip("/")

        if self.type == ArrayType.array and self.client.cluster_mode:
            node_id = self.hash_ring.get_node(get_hash_key(array))
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)

        response = self.client.post(f"{path}/{self.type.name}s", json=kwargs)
        if response.status_code != STATUS_CREATED:
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
        url = f"/{self.type.name}/by-id/{array.id}"

        # Only Varray can be located on different nodes yet.
        if self.type == ArrayType.varray and self.client.cluster_mode:
            response = make_request(url=f"{self.collection_path.path}{url}", nodes=self.nodes, client=self.client)
        elif self.client.cluster_mode:
            node_id = self.get_node(array)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
            response = self.client.get(f"{path.rstrip('/')}{url}")
        else:
            response = self.client.get(f"{self.collection_path.raw_url.rstrip('/')}{url}")

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
        path = self.collection_path.raw_url
        if self.client.cluster_mode and self.type == ArrayType.array:
            node_id = self.get_node(array)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)

        response = self.client.put(
            f"{path}/{self.type.name}/by-id/{array.id}",
            json={"custom_attributes": convert_datetime_attrs_to_iso(attributes)},
        )
        if response.status_code != STATUS_OK:
            raise DekerServerError(response, "Couldn't update attributes")

    def delete(self, array: "BaseArray") -> None:
        """Delete array on server.

        :param array: Array/Varray to be deleted
        """
        if self.client.cluster_mode and self.type == ArrayType.array:
            node_id = self.get_node(array)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
        else:
            path = self.collection_path.raw_url.rstrip("/")
        resource = self.client.delete(f"{path}/{self.type.name}/by-id/{array.id}")
        if resource.status_code != STATUS_OK:
            raise DekerServerError(resource, f"Couldn't delete the {self.type.name}")

    def read_data(
        self,
        array: "BaseArray",
        bounds: Slice,
    ) -> Union[Numeric, ndarray, None]:
        """Read data from array/varray.

        :param array: From what array we read
        :param bounds: What part of array we read
        :return:
        """
        bounds_ = slice_converter[bounds]
        if self.client.cluster_mode:
            node_id = self.get_node(array)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
        else:
            path = self.collection_path.raw_url.rstrip("/")
        try:
            response = self.client.get(
                f"{path}/" f"{self.type.name}/" f"by-id/" f"{array.id}/" f"subset/" f"{bounds_}/" f"data",
                headers={"Accept": "application/octet-stream"},
            )
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )

        if response.status_code == STATUS_OK:
            numpy_array = np.fromstring(response.read(), dtype=array.dtype)  # type: ignore[call-overload]
            shape = array[bounds].shape
            if not shape and numpy_array.size:
                return numpy_array[0]

            return numpy_array.reshape(shape)
        if response.status_code == TIMEOUT:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )
        raise DekerServerError(response, "Couldn't read the array")

    def update(self, array: "BaseArray", bounds: Slice, data: Numeric) -> None:
        """Update array/varray on server.

        :param array: Array/Varray that will be updated
        :param bounds: Part of the array to update
        :param data: Data that will replace current values
        :return:
        """
        bounds = slice_converter[bounds]

        # We write (v)array through the node it belongs in cluster
        if self.client.cluster_mode:
            node_id = self.get_node(array)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
        else:
            path = self.collection_path.raw_url.rstrip("/")
        try:
            if hasattr(data, "tolist"):
                data = data.tolist()
            response = self.client.put(
                f"{path}/{self.type.name}/by-id/{array.id}/subset/{bounds}/data",
                json=data,
            )

        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} update {array}",
            )
        if response.status_code == STATUS_OK:
            return
        if response.status_code == TIMEOUT:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} update {array}",
            )
        raise DekerServerError(response, "Couldn't update array")

    def clear(self, array: "BaseArray", bounds: Slice) -> None:
        """Clear array/varray.

        :param array: Instance of array/varray
        :param bounds: Part of the array/varray that will be cleared
        """
        return self.update(array, bounds, [])

    def is_deleted(self, array: "BaseArray") -> bool:
        """Check if array/varray was deleted on server.

        :param array: Instance of array/varray
        """
        return False

    def __create_array_from_response(self, response: Response, kwargs: Dict) -> Optional[BaseArray]:
        """Create array from json meta.

        :param response: response from server
        :param kwargs: Kwargs for array creation
        """
        if response.status_code == NOT_FOUND:
            return None
        if response.status_code == STATUS_OK:
            return create_array_from_meta(**{**kwargs, "meta": response.json()})

        raise DekerServerError(response, "Couldn't fetch an array")

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
        """
        attrs = []
        primary_attributes = convert_datetime_attrs_to_iso(primary_attributes)
        for attr in schema.attributes:
            if not attr.primary:
                continue

            if attr.dtype == tuple:
                attrs.append(str(tuple(primary_attributes[attr.name])))  # type: ignore[index]
            else:
                attrs.append(str(primary_attributes[attr.name]))  # type: ignore[index]

        primary_path = "/".join(attrs)

        # We read an Array through the node it belongs
        if self.client.cluster_mode:
            node_id = self.hash_ring.get_node(get_hash_by_primary_attrs(primary_attributes))
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
        else:
            path = self.collection_path.raw_url.rstrip("/")

        response = self.client.get(
            f"{path}/{self.type.name}/by-primary/{primary_path}",
        )

        return self.__create_array_from_response(
            response,
            dict(
                type=self.type,
                collection=collection,
                array_adapter=array_adapter,
                varray_adapter=varray_adapter,
            ),
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
        if self.client.cluster_mode:
            # Hash from ID and hash from primary attributes are different
            # So if schema has primary attributes,
            # it means that hash has been calculated using primary custom_attributes
            # Therefore, we cannot let filter by id.
            schema = collection.varray_schema or collection.array_schema
            if schema.primary_attributes:
                raise FilteringByIdInClusterIsForbidden

            node_id = self.hash_ring.get_node(id_)
            node = self.get_host_url(node_id)
            path = self.__merge_node_and_collection_path(node)
        else:
            path = self.collection_path.raw_url.rstrip("/")

        response = self.client.get(
            f"{path}/{self.type.name}/by-id/{id_}",
        )

        return self.__create_array_from_response(
            response,
            dict(
                type=self.type,
                collection=collection,
                array_adapter=array_adapter,
                varray_adapter=varray_adapter,
            ),
        )

    def __iter__(self) -> Generator["ArrayMeta", None, None]:
        urls = [f"{self.collection_path.raw_url}/{self.type.name}s"]
        if self.type == ArrayType.array and self.client.cluster_mode:
            urls = [f"{self.__merge_node_and_collection_path(node)}/{self.type.name}s" for node in self.nodes]

        for url in urls:
            response = self.client.get(url)

            if response.status_code != STATUS_OK:
                if self.type == ArrayType.array:
                    logger.warning(f"Couldn't fetch arrays from node: {url}")
                else:
                    raise DekerServerError(response, "Couldn't get list of arrays")
            else:
                yield from response.json()
