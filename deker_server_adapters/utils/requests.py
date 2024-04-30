import traceback

from logging import getLogger
from random import randint
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union

from deker.ABC import BaseArray
from deker.ctx import CTX
from httpx import Client, Response

from deker_server_adapters.consts import STATUS_OK
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.models import Status
from deker_server_adapters.utils.hashing import get_hash_key, get_id_and_primary_attributes


if TYPE_CHECKING:
    from deker_server_adapters.httpx_client import HttpxClient

logger = getLogger(__name__)


def _request(
    url: str, node: str, client: Client, method: str = "GET", request_kwargs: Optional[Dict] = None
) -> Optional[Response]:
    """Internal request func - Make GET request on given node.

    :param url: What we request
    :param node: Node for requesting
    :param client: Httpx Client
    :param method: Http method
    :param request_kwargs: Kwargs for request
    """
    response = None
    request_url = f"{node.rstrip('/')}/{url.lstrip('/')}"
    request_kwargs = request_kwargs or {}
    try:
        response = client.request(method, request_url, **request_kwargs)
    except Exception as e:
        traceback.print_exc(-1)
        logger.exception(f"Coudn't get response from {node}", exc_info=e)  # noqa

    return response


def make_request(
    url: str, nodes: Union[List, Tuple, Set], client: Client, method: str = "GET", request_kwargs: Optional[Dict] = None
) -> Optional[Response]:
    """Make GET request on random node, while response is not received.

    :param method: HTTP Method
    :param url: What we request
    :param request_kwargs: Kwargs for request
    :param nodes: Nodes for requesting
    :param client: Httpx Client
    """
    response = None
    nodes = list(nodes)
    if len(nodes) == 1:
        node = nodes.pop(0)
        response = _request(url, node, client, method, request_kwargs)
    else:
        while nodes and (response is None or response.status_code != STATUS_OK):
            index = randint(0, len(nodes) - 1)
            node = nodes.pop(index)

            response = _request(url, node, client, method, request_kwargs)

    return response


def check_status(ctx: CTX, array: BaseArray) -> Status:
    """Check status of file on the server.

    :param ctx: Application context
    :param array: Instance of array to check
    """
    client: "HttpxClient" = ctx.extra["httpx_client"]
    node = ctx.extra["hash_ring"].get_node(get_hash_key(array))
    id_, _ = get_id_and_primary_attributes(array)
    url = node.url / f"status/{id_}"
    response = client.get(url.raw_url)

    if not response or response.status_code != STATUS_OK:
        raise DekerServerError(response, "File status check failed")

    return Status(response.text)


def request_in_cluster(
    url: str,
    array: BaseArray,
    ctx: CTX,
    should_check_status: bool = False,
    method: str = "GET",
    request_kwargs: Optional[dict] = None,
) -> Optional[Response]:
    """Make request in cluster.

    Before sending request, it retrieves a fresh config from the server.
    Then, if needed, checks status of file on the server to determine where to send request

    :param url: url which to request
    :param array: Array instance
    :param ctx: Application contxet
    :param should_check_status: If we should check whether the file has been moved or not
    :param method: Http method
    :param request_kwargs: Extra data for request
    """
    from deker_server_adapters.cluster_config import request_and_apply_config

    # Retrieve fresh config
    request_and_apply_config(ctx)
    client = ctx.extra["httpx_client"]

    node = ctx.extra["hash_ring"].get_node(get_hash_key(array))

    # Check status of file
    if should_check_status:
        status = check_status(ctx, array)
        if status == Status.MOVED:
            node = ctx.extra["hash_ring_target"].get_node(get_hash_key(array))

    # Acquire locks
    # TODO: Lock acquiring logic if needed
    # Make request
    return make_request(url, [node.url.raw_url], client, method=method, request_kwargs=request_kwargs)
