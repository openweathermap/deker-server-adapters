from logging import getLogger
from random import randint
from typing import TYPE_CHECKING

from deker.ABC import BaseArray
from deker.ctx import CTX
from httpx import Response

from deker_server_adapters.consts import REBALANCING_STATUS, STATUS_OK
from deker_server_adapters.errors import DekerServerError, InvalidConfigHash
from deker_server_adapters.models import Status
from deker_server_adapters.utils.hashing import get_hash_key, get_id_and_primary_attributes
from deker_server_adapters.utils.version import get_api_version

if TYPE_CHECKING:
    from deker_server_adapters.cluster_config import Node
    from deker_server_adapters.httpx_client import HttpxClient

logger = getLogger(__name__)


def _request(
    url: str,
    node: str,
    client: "HttpxClient",
    method: str = "GET",
    request_kwargs: dict | None = None,
    retry_on_hash_failure: bool = False,
    stream: bool = False,
) -> Response | None:
    """Internal request func - Make GET request on given node.

    :param url: What we request
    :param retry_on_hash_failure: If we should retry on invalid hash
    :param node: Node for requesting
    :param client: Httpx Client
    :param method: Http method
    :param request_kwargs: Kwargs for request
    :param stream: leave the body of a successful response unread
    :returns: response, or ``None`` if the node did not answer
    """
    response = None
    request_url = f"{node.rstrip('/')}/{url.lstrip('/')}"
    request_kwargs = request_kwargs or {}
    try:
        response = client.request(
            method, request_url, **request_kwargs, retry_on_hash_failure=retry_on_hash_failure, stream=stream
        )
    except InvalidConfigHash:
        raise
    except Exception as e:
        logger.exception(f"Couldn't get response from {node}", exc_info=e)

    return response


def make_request(
    url: str,
    nodes: list | tuple | set,
    client: "HttpxClient",
    method: str = "GET",
    request_kwargs: dict | None = None,
    retry_on_hash_failure: bool = False,
    stream: bool = False,
) -> Response | None:
    """Make GET request on random node, while response is not received.

    :param retry_on_hash_failure: If we should retry on invalid hash
    :param method: HTTP Method
    :param url: What we request
    :param request_kwargs: Kwargs for request
    :param nodes: Nodes for requesting
    :param client: Httpx Client
    :param stream: leave the body of a successful response unread
    :returns: first received response, or ``None`` if no node answered
    """
    response = None
    nodes = list(nodes)
    if len(nodes) == 1:
        node = nodes.pop(0)
        response = _request(
            url, node, client, method, request_kwargs, retry_on_hash_failure=retry_on_hash_failure, stream=stream
        )
    else:
        while nodes and (response is None or response.status_code != STATUS_OK):
            index = randint(0, len(nodes) - 1)
            node = nodes.pop(index)

            response = _request(
                url, node, client, method, request_kwargs, retry_on_hash_failure=retry_on_hash_failure, stream=stream
            )

    return response


def check_status(ctx: CTX, array: BaseArray) -> Status:
    """Check status of file on the server.

    :param ctx: Application context
    :param array: Instance of array to check
    :returns: status of the array file on the server
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
    array: BaseArray | dict,
    ctx: CTX,
    should_check_status: bool = False,
    method: str = "GET",
    request_kwargs: dict | None = None,
    stream: bool = False,
) -> Response | None:
    """Make request in cluster.

    Before sending request, it retrieves a fresh config from the server.
    Then, if needed, checks status of file on the server to determine where to send request

    :param url: url which to request
    :param array: Array instance, or a dict with its id and/or primary attributes (then no status check is possible)
    :param ctx: Application context
    :param should_check_status: If we should check whether the file has been moved or not
    :param method: Http method
    :param request_kwargs: Extra data for request
    :param stream: leave the body of a successful response unread
    :returns: response from the node responsible for the array, or ``None`` if it did not answer
    """

    def _check_status(initial_node: "Node") -> "Node":
        """Closure for getting status of config file on the server and retrieving correct node for request.

        :param initial_node: Node to return if there are no updates.
        :returns: node that currently holds the array
        """
        if should_check_status and ctx.extra["cluster_config"].cluster_status == REBALANCING_STATUS:
            if not isinstance(array, BaseArray):
                raise TypeError(f"Status of an array can be checked by its instance only, got {type(array)}")
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
        return make_request(
            url, [target_node.url.raw_url], client, method=method, request_kwargs=request_kwargs, stream=stream
        )
    except InvalidConfigHash:
        updated_node = _check_status(target_node)
        return make_request(
            url, [updated_node.url.raw_url], client, method=method, request_kwargs=request_kwargs, stream=stream
        )
