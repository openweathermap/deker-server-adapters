import traceback

from collections import defaultdict
from datetime import datetime
from logging import getLogger
from random import randint
from typing import Dict, List, Optional, Set, Tuple, Union

from deker.ABC import BaseArray
from deker_tools.time import get_utc
from httpx import Client, Response

from deker_server_adapters.consts import STATUS_OK


logger = getLogger(__name__)


def _request(url: str, node: str, client: Client, request_kwargs: Optional[Dict] = None) -> Optional[Response]:
    """Internal request func - Make GET request on given node.

    :param url: What we request
    :param request_kwargs: Kwargs for request
    :param node: Node for requesting
    :param client: Httpx Client
    """
    response = None
    request_url = f"{node.rstrip('/')}/{url.lstrip('/')}"
    try:
        if request_kwargs:
            response = client.get(request_url, **request_kwargs)
        else:
            response = client.get(request_url)
    except Exception as e:
        traceback.print_exc(-1)
        logger.exception(f"Coudn't get response from {node}", exc_info=e)  # noqa

    return response


def make_request(
    url: str, nodes: Union[List, Tuple, Set], client: Client, request_kwargs: Optional[Dict] = None
) -> Optional[Response]:
    """Make GET request on random node, while response is not received.

    :param url: What we request
    :param request_kwargs: Kwargs for request
    :param nodes: Nodes for requesting
    :param client: Httpx Client
    """
    response = None
    nodes = list(nodes)
    if len(nodes) == 1:
        node = nodes.pop(0)
        response = _request(url, node, client, request_kwargs)
    else:
        while nodes and (response is None or response.status_code != STATUS_OK):
            index = randint(0, len(nodes) - 1)
            node = nodes.pop(index)

            response = _request(url, node, client, request_kwargs)

    return response


def get_api_version() -> str:
    """Get API Version."""
    return "v1"


def get_leader_and_nodes_mapping(cluster_config: Dict) -> Tuple[Optional[str], List, defaultdict, List]:
    """Figure out leader from cluster config.

    :param cluster_config: Cluster configuration
    """
    # IDs used in hash ring
    ids = []
    # Mapping from ID to host
    id_to_host_mapping = defaultdict(list)
    leader_node = None
    nodes = []

    # Fill Ids and Mappings
    for node in cluster_config["current_nodes"]:
        url = f"{node['protocol']}://{node['host']}:{node['port']}"
        nodes.append(url)
        ids.append(node["id"])
        id_to_host_mapping[node["id"]].append(url)
        if node["id"] == cluster_config["leader_id"]:
            leader_node = url

    return leader_node, ids, id_to_host_mapping, nodes


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


def get_hash_key(array: Union[dict, BaseArray]) -> str:
    """Get hash key for an array.

    :param array: Instance of (V)Array
    """
    if isinstance(array, dict):
        primary_attributes = array.get("primary_attributes")
        id_ = array.get("id_")
    else:
        primary_attributes = array.primary_attributes
        id_ = array.id

    if primary_attributes:
        return get_hash_by_primary_attrs(primary_attributes)

    return id_  # type: ignore[return-value]
