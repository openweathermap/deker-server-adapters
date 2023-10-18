from collections.abc import Generator
from datetime import datetime
from json import JSONDecodeError
from random import choice
from typing import TYPE_CHECKING, Any, List, Optional, Union

import numpy as np

from deker import Array, Collection, VArray
from deker.ABC.base_array import BaseArray
from deker.tools.time import convert_datetime_attrs_to_iso
from deker.types import ArrayMeta, Numeric, Slice
from deker.uri import Uri
from deker_tools.slices import slice_converter
from httpx import Client, TimeoutException
from numpy import ndarray

from deker_server_adapters.consts import NOT_FOUND, STATUS_CREATED, STATUS_OK, TIMEOUT, ArrayType
from deker_server_adapters.errors import DekerServerError, DekerTimeoutServer
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.utils import request_in_cluster


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_schemas import BaseArraysSchema


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

    @property
    def leader_node(self) -> Uri:
        """Return leader node."""
        return self.ctx.extra["leader_node"]  # type: ignore[attr-defined]

    @property
    def client(self) -> Client:
        """Return client singleton."""
        # We don't need to worry about passing args here, cause it's a singleton.
        return self.ctx.extra["httpx_client"]  # type: ignore[attr-defined]

    @property
    def hash_ring(self) -> HashRing:
        """Return HashRing instance."""
        return self.ctx.extra["hash_ring"]  # type: ignore[attr-defined]

    @property
    def nodes(self) -> List[str]:
        """Return list of nodes."""
        return self.ctx.extra["nodes"]  # type: ignore[attr-defined]


class ServerArrayAdapterMixin(BaseServerAdapterMixin):
    """Mixin with server logic for adapters."""

    type: ArrayType
    collection_path: Uri

    def get_host_url(self, id_: str) -> str:
        """Get random node with given id.

        :param id_: ID of node
        """
        hosts = self.ctx.extra["nodes_mapping"][id_]  # type: ignore[attr-defined]
        return choice(hosts)

    def get_node(self, array: BaseArray) -> str:
        """Get hash for primary attributes or id.

        :param array: Array or varray
        """
        if not array.primary_attributes:
            return self.hash_ring.get_node(array.id) or ""

        attrs_to_join = []
        for attr in array.primary_attributes:
            attribute = array.primary_attributes[attr]
            if attr == "v_position":
                value = "-".join(str(el) for el in attribute)
            else:
                value = attribute.isoformat() if isinstance(attribute, datetime) else str(attribute)
            attrs_to_join.append(value)
        return self.hash_ring.get_node("/".join(attrs_to_join)) or ""

    def create(self, array: Union[dict, "BaseArray"]) -> BaseArray:
        """Create array on server.

        :param array: Array instance
        :return:
        """
        response = self.client.post(
            f"{self.collection_path.raw_url}/{self.type.name}s",
            json={
                "primary_attributes": convert_datetime_attrs_to_iso(
                    array["primary_attributes"],
                ),
                "custom_attributes": convert_datetime_attrs_to_iso(
                    array["custom_attributes"],
                ),
            },
        )
        if response.status_code != STATUS_CREATED:
            raise DekerServerError(response, "Couldn't create an array")

        try:
            data = response.json()
        except JSONDecodeError:
            raise DekerServerError(response, "Couldn't parse json")

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

    def read_meta(self, array: "BaseArray") -> ArrayMeta:
        """Read metadata of (v)array.

        :param array: Instance of (v)array
        :return:
        """
        url = (f"/{self.type.name}/by-id/{array.id}",)
        response = request_in_cluster(url=url, nodes=self.nodes, client=self.client)
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
        response = self.client.put(
            f"{self.collection_path.raw_url}/{self.type.name}/by-id/{array.id}",
            json={"custom_attributes": convert_datetime_attrs_to_iso(attributes)},
        )
        if response.status_code != STATUS_OK:
            raise DekerServerError(response, "Couldn't update attributes")

    def delete(self, array: "BaseArray") -> None:
        """Delete array on server.

        :param array: Array/Varray to be deleted
        """
        resource = self.client.delete(f"{self.collection_path.raw_url}/{self.type.name}/by-id/{array.id}")
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
        node = self.get_node(array)
        try:
            response = self.client.get(
                f"{node}/v1/collection/{array.collection}/{self.type.name}/by-id/{array.id}/subset/{bounds_}/data",
                headers={"Accept": "application/octet-stream"},
            )
        except TimeoutException:
            raise DekerTimeoutServer(
                message=f"Timeout on {self.type.name} read {array}",
            )

        if response.status_code == STATUS_OK:
            numpy_array = np.frombuffer(response.read(), dtype=array.dtype)
            shape = array[bounds].shape
            if not shape and numpy_array:
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
        node = self.get_node(array)
        try:
            if hasattr(data, "tolist"):
                data = data.tolist()

            response = self.client.put(
                f"{node}/v1/collection/{array.collection}/{self.type.name}/by-id/{array.id}/subset/{bounds}/data",
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
        response = self.client.get(
            f"{self.collection_path.raw_url}/{self.type.name}/by-primary/{primary_path}",
        )
        if response.status_code == NOT_FOUND:
            return None
        if response.status_code == STATUS_OK:
            return create_array_from_meta(
                type=self.type,
                collection=collection,
                meta=response.json(),
                array_adapter=array_adapter,
                varray_adapter=varray_adapter,
            )
        return None

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
        response = self.client.get(
            f"{self.collection_path.raw_url}/{self.type.name}/by-id/{id_}",
        )
        if response.status_code == NOT_FOUND:
            return None
        if response.status_code == STATUS_OK:
            return create_array_from_meta(
                type=self.type,
                collection=collection,
                meta=response.json(),
                array_adapter=array_adapter,
                varray_adapter=varray_adapter,
            )

        raise DekerServerError(response, "Couldn't fetch an array")

    def __iter__(self) -> Generator["ArrayMeta", None, None]:
        response = self.client.get(f"{self.collection_path.raw_url}/{self.type.name}s")
        if response.status_code != STATUS_OK:
            raise DekerServerError(response, "Couldn't get list of arrays")

        yield from response.json()
