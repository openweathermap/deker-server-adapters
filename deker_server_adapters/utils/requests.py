import traceback

from logging import getLogger
from random import randint
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union

from deker.ABC import BaseArray
from deker.ctx import CTX
from httpx import Response

from deker_server_adapters.consts import REBALANCING_STATUS, STATUS_OK
from deker_server_adapters.errors import DekerServerError, InvalidConfigHash
from deker_server_adapters.models import Status
from deker_server_adapters.utils.hashing import get_hash_key, get_id_and_primary_attributes
from deker_server_adapters.utils.version import get_api_version


if TYPE_CHECKING:
    from deker_server_adapters.httpx_client import HttpxClient
    from deker_server_adapters.cluster_config import Node

logger = getLogger(__name__)


def _request(
    url: str,
    node: str,
    client: "HttpxClient",
    method: str = "GET",
    request_kwargs: Optional[Dict] = None,
    retry_on_hash_failure: bool = False,
) -> Optional[Response]:
    """Internal request func - Make GET request on given node.

    :param url: What we request
    :param retry_on_hash_failure: If we should retry on invalid hash
    :param node: Node for requesting
    :param client: Httpx Client
    :param method: Http method
    :param request_kwargs: Kwargs for request
    """
    response = None
    request_url = f"{node.rstrip('/')}/{url.lstrip('/')}"
    request_kwargs = request_kwargs or {}
    try:
        response = client.request(method, request_url, **request_kwargs, retry_on_hash_failure=retry_on_hash_failure)
    except InvalidConfigHash:
        raise
    except Exception as e:
        traceback.print_exc(-1)
        logger.exception(f"Coudn't get response from {node}", exc_info=e)  # noqa

    return response


def make_request(
    url: str,
    nodes: Union[List, Tuple, Set],
    client: "HttpxClient",
    method: str = "GET",
    request_kwargs: Optional[Dict] = None,
    retry_on_hash_failure: bool = False,
) -> Optional[Response]:
    """Make GET request on random node, while response is not received.

    :param retry_on_hash_failure: If we should retry on invalid hash
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
        response = _request(url, node, client, method, request_kwargs, retry_on_hash_failure=retry_on_hash_failure)
    else:
        while nodes and (response is None or response.status_code != STATUS_OK):
            index = randint(0, len(nodes) - 1)
            node = nodes.pop(index)

            response = _request(url, node, client, method, request_kwargs, retry_on_hash_failure=retry_on_hash_failure)

    return response


def check_status(ctx: CTX, array: BaseArray) -> Status:
    """Check status of file on the server.

    :param ctx: Application context
    :param array: Instance of array to check
    """
    client: "HttpxClient" = ctx.extra["httpx_client"]
    node = ctx.extra["hash_ring"].get_node(get_hash_key(array))
    id_, _ = get_id_and_primary_attributes(array)
    url = node.url / f"{get_api_version()}/cluster/collection/{array.collection}/array/by-id/{id_}/status"
    response = client.get(url.raw_url)
    if not response or response.status_code != STATUS_OK:
        raise DekerServerError(response, "File status check failed")
    return Status(response.text.replace('"', ""))


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
    :param ctx: Application context
    :param should_check_status: If we should check whether the file has been moved or not
    :param method: Http method
    :param request_kwargs: Extra data for request
    """
    def _check_status(initial_node: "Node") -> "Node":
        """Closure for getting status of config file on the server and retrieving correct node for request.

        :param initial_node: Node to return if there are no updates.
        """
        if should_check_status and ctx.extra["cluster_config"].cluster_status == REBALANCING_STATUS:
            status = check_status(ctx, array)
            if status == Status.MOVED:
                return ctx.extra["hash_ring_target"].get_node(get_hash_key(array))
        return initial_node

    # Retrieve fresh config
    client = ctx.extra["httpx_client"]

    base_node = ctx.extra["hash_ring"].get_node(get_hash_key(array))

    target_node = _check_status(base_node)
    # Acquire locks
    # TODO: Lock acquiring logic if needed
    # Make request
    try:
        return make_request(url, [target_node.url.raw_url], client, method=method, request_kwargs=request_kwargs)
    except InvalidConfigHash:
        updated_node = _check_status(target_node)
        return make_request(url, [updated_node.url.raw_url], client, method=method, request_kwargs=request_kwargs)
