import traceback

from logging import getLogger
from random import randint
from typing import Dict, Optional, Sequence

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

    try:
        if request_kwargs:
            response = client.get(f"{node}/{url}", **request_kwargs)
        else:
            response = client.get(f"{node}/{url}")
    except Exception:
        logger.error(f"Coudn't get response from {node}")  # noqa
        traceback.print_exc()

    return response


def make_request(
    url: str, nodes: Sequence, client: Client, request_kwargs: Optional[Dict] = None
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
