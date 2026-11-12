from datetime import datetime

from deker.ABC import BaseArray
from deker_tools.time import get_utc

from deker_server_adapters.errors import HashRingError


def get_hash_by_primary_attrs(primary_attributes: dict) -> str:
    """Get hash by primary attributes.

    :param primary_attributes: Dict of primary attributes
    :returns: primary attribute values joined with ``/``
    """
    attrs_to_join = []
    for attr in primary_attributes:
        attribute = primary_attributes[attr]
        if attr == "v_position":
            value = "-".join(str(el) for el in attribute)
        else:
            value = get_utc(attribute).isoformat() if isinstance(attribute, datetime) else str(attribute)
        attrs_to_join.append(value)
    return "/".join(attrs_to_join) or ""


def get_id_and_primary_attributes(array: dict | BaseArray) -> tuple[str | None, dict | None]:
    """Get ID and primary attributes of an array, depending o its type.

    :param array: Array instance or its meta info.
    :returns: array id and its primary attributes; a meta dict may lack either of them
    """
    if isinstance(array, dict):
        primary_attributes = array.get("primary_attributes")
        id_ = array.get("id_")
    else:
        primary_attributes = array.primary_attributes
        id_ = array.id

    return id_, primary_attributes


def get_hash_key(array: dict | BaseArray) -> str:
    """Get hash key for an array.

    :param array: Instance of (V)Array
    :returns: array id, or the primary attributes hash if there are primary attributes
    :raises HashRingError: if the array has neither primary attributes nor an id
    """
    id_, primary_attributes = get_id_and_primary_attributes(array)

    if primary_attributes:
        return get_hash_by_primary_attrs(primary_attributes)

    if id_ is None:
        raise HashRingError("Array has neither primary attributes nor an id, nothing to hash")
    return id_
