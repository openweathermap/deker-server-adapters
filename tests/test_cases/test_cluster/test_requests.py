import re

import pytest

from cantok import DefaultToken
from deker.arrays import Array
from deker.ctx import CTX
from deker_tools.cancel import TokenCancelledError
from deker_tools.hash_ring import HashRing, get_hash_key
from pytest_httpx import HTTPXMock

from tests.conftest import FIRED_TOKENS, fired_token

from deker_server_adapters.cluster_config import apply_config
from deker_server_adapters.consts import REBALANCING_STATUS
from deker_server_adapters.errors import HashRingError
from deker_server_adapters.models import Status
from deker_server_adapters.utils.requests import get_node_of, request_in_cluster


def test_moved_array_is_requested_from_the_target_node(
    ctx: CTX, httpx_mock: HTTPXMock, mocked_ping: dict, nodes: list[dict], array: Array
):
    """During rebalancing an array the server reports as moved is read from the node of the target ring."""
    target = {**nodes[0], "id": "8381202B-8C95-487A-B9B5-0B5270568099", "port": 8099}
    apply_config({**mocked_ping, "cluster_status": REBALANCING_STATUS, "target": [target]}, ctx)
    httpx_mock.add_response(url=re.compile(r".*/status$"), text=Status.MOVED.value)
    httpx_mock.add_response(url=re.compile(r".*:8099/.*"), json={"moved": True})

    response = request_in_cluster("v1/anything", array, ctx, should_check_status=True, token=DefaultToken())

    assert response is not None
    assert response.url.port == 8099
    assert response.json() == {"moved": True}
    status_request = httpx_mock.get_request(url=re.compile(r".*/status$"))
    assert status_request is not None
    assert status_request.url.path == f"/v1/collection/{array.collection}/array/by-id/{array.id}/status"


@pytest.mark.parametrize(
    "description",
    [
        pytest.param({"id_": "8381202B-8C95-487A-B9B5-0B5270568099"}, id="by-id"),
        pytest.param({"primary_attributes": {"foo": "bar"}}, id="by-primary-attributes"),
    ],
)
def test_status_of_a_described_array_cannot_be_checked(
    ctx: CTX, httpx_mock: HTTPXMock, mocked_ping: dict, nodes: list[dict], description: dict
):
    """A status check needs the array instance: a dict during rebalancing is refused before any request."""
    apply_config({**mocked_ping, "cluster_status": REBALANCING_STATUS, "target": nodes}, ctx)

    with pytest.raises(TypeError, match="instance"):
        request_in_cluster("v1/anything", description, ctx, should_check_status=True, token=DefaultToken())
    assert not httpx_mock.get_requests()


@pytest.mark.parametrize(("kind", "error"), FIRED_TOKENS)
def test_a_fired_token_sends_nothing_not_even_the_status_check(
    kind: str,
    error: type[TokenCancelledError],
    ctx: CTX,
    httpx_mock: HTTPXMock,
    mocked_ping: dict,
    nodes: list[dict],
    array: Array,
):
    apply_config({**mocked_ping, "cluster_status": REBALANCING_STATUS, "target": nodes}, ctx)
    sent = len(httpx_mock.get_requests())

    with pytest.raises(error):
        request_in_cluster("v1/anything", array, ctx, should_check_status=True, token=fired_token(kind))
    assert httpx_mock.get_requests()[sent:] == []


@pytest.mark.parametrize(
    "description",
    [
        pytest.param({"id_": "8381202B-8C95-487A-B9B5-0B5270568099"}, id="by-id"),
        pytest.param({"primary_attributes": {"foo": "bar"}}, id="by-primary-attributes"),
    ],
)
def test_node_of_an_array_is_the_node_of_its_key(ctx: CTX, description: dict):
    ring = ctx.extra["hash_ring"]
    assert get_node_of(ring, description) == ring.get_node(get_hash_key(description))


@pytest.mark.parametrize(
    ("nodes_of_the_ring", "description", "message"),
    [
        pytest.param(True, {"primary_attributes": {}}, "nothing to hash", id="no-key"),
        pytest.param(False, {"id_": "8381202B-8C95-487A-B9B5-0B5270568099"}, "no nodes", id="empty-ring"),
    ],
)
def test_ring_errors_are_errors_of_the_adapters(ctx: CTX, nodes_of_the_ring: bool, description: dict, message: str):
    """The error of the ring from deker-tools comes out as the Deker error of the adapters, without its chain."""
    ring = ctx.extra["hash_ring"] if nodes_of_the_ring else HashRing([])
    with pytest.raises(HashRingError, match=message) as error:
        get_node_of(ring, description)
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


def test_request_for_an_array_without_a_key_sends_nothing(ctx: CTX, httpx_mock: HTTPXMock):
    with pytest.raises(HashRingError, match="nothing to hash"):
        request_in_cluster("v1/anything", {"primary_attributes": {}}, ctx, token=DefaultToken())
    assert not httpx_mock.get_requests()
