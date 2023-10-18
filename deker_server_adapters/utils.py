import traceback

from logging import getLogger
from random import randint
from typing import Dict, Optional, Sequence

from httpx import Client, Response

from deker_server_adapters.consts import STATUS_OK


logger = getLogger(__name__)


def request_in_cluster(
    url: str, nodes: Sequence, client: Client, request_kwargs: Optional[Dict] = None
) -> Optional[Response]:
    """Make GET request on random node, while response is not recieved.

    :param url: What we request
    :param request_kwargs: Kwargs for request
    :param nodes: Nodes for requesting
    :param client: Httpx Client
    """
    response = None
    nodes = list(nodes)
    while nodes and (response is None or response.status_code != STATUS_OK):
        index = randint(0, len(nodes) - 1)
        node = nodes.pop(index)

        try:
            if request_kwargs:
                response = client.get(f"{node}/{url}", **request_kwargs)
            else:
                response = client.get(f"{node}/{url}")
        except Exception:
            logger.error(f"Coudn't get response from {node}")  # noqa
            traceback.print_exc()
            continue
    print(response)
    return response
