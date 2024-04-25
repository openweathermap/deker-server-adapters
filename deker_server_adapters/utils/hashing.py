from datetime import datetime
from typing import Dict, Tuple, Union

from deker.ABC import BaseArray
from deker_tools.time import get_utc


def get_hash_by_primary_attrs(primary_attributes: Dict) -> str:
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
    return "/".join(attrs_to_join) or ""


def get_id_and_primary_attributes(array: Union[dict, BaseArray]) -> Tuple[str, dict]:
    """Get ID and primary attributes of an array, depending o its type.

    :param array: Array instance or its meta info.
    """
    if isinstance(array, dict):
        primary_attributes = array.get("primary_attributes")
        id_ = array.get("id_")
    else:
        primary_attributes = array.primary_attributes
        id_ = array.id

    return id_, primary_attributes  # type: ignore[return-value]


def get_hash_key(array: Union[dict, BaseArray]) -> str:
    """Get hash key for an array.

    :param array: Instance of (V)Array
    """
    id_, primary_attributes = get_id_and_primary_attributes(array)

    if primary_attributes:
        return get_hash_by_primary_attrs(primary_attributes)

    return id_  # type: ignore[return-value]
