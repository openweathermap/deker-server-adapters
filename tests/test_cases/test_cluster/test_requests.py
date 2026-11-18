import re

import pytest

from deker.arrays import Array
from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.cluster_config import apply_config
from deker_server_adapters.consts import REBALANCING_STATUS
from deker_server_adapters.models import Status
from deker_server_adapters.utils.requests import request_in_cluster


def test_moved_array_is_requested_from_the_target_node(
    ctx: CTX, httpx_mock: HTTPXMock, mocked_ping: dict, nodes: list[dict], array: Array
):
    """During rebalancing an array the server reports as moved is read from the node of the target ring."""
    target = {**nodes[0], "id": "8381202B-8C95-487A-B9B5-0B5270568099", "port": 8099}
    apply_config({**mocked_ping, "cluster_status": REBALANCING_STATUS, "target": [target]}, ctx)
    httpx_mock.add_response(url=re.compile(r".*/status$"), text=Status.MOVED.value)
    httpx_mock.add_response(url=re.compile(r".*:8099/.*"), json={"moved": True})

    response = request_in_cluster("v1/anything", array, ctx, should_check_status=True)

    assert response is not None
    assert response.url.port == 8099
    assert response.json() == {"moved": True}


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
        request_in_cluster("v1/anything", description, ctx, should_check_status=True)
    assert not httpx_mock.get_requests()
