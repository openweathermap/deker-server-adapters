import re

import httpx
import pytest

from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.cluster_config import (
    ClusterConfig,
    apply_config,
    is_config_in_cluster_mode,
    request_and_apply_config,
)
from deker_server_adapters.errors import DekerClusterError, DekerServerError
from deker_server_adapters.hash_ring import HashRing


def test_config_without_leader_is_rejected(mocked_ping: dict):
    with pytest.raises(DekerClusterError, match="No leader"):
        ClusterConfig.from_dict({**mocked_ping, "leader_id": "missing"})


def test_target_nodes_build_a_second_ring(ctx: CTX, mocked_ping: dict, nodes: list[dict]):
    apply_config({**mocked_ping, "target": nodes[:1]}, ctx)
    assert isinstance(ctx.extra["hash_ring_target"], HashRing)
    assert ctx.extra["hash_ring_target"].nodes == ctx.extra["cluster_config"].target

    apply_config(mocked_ping, ctx)
    assert ctx.extra["hash_ring_target"] is None


def test_cluster_uri_requires_cluster_mode(ctx: CTX):
    assert is_config_in_cluster_mode({"mode": "cluster"}, ctx) is True
    with pytest.raises(DekerClusterError, match="'mode'"):
        is_config_in_cluster_mode({"mode": "single"}, ctx)
    with pytest.raises(DekerClusterError, match="'mode'"):
        is_config_in_cluster_mode(None, ctx)


def test_failed_healthcheck_closes_the_client(ctx: CTX, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=re.compile(r".*/v1/ping.*"), status_code=500)
    with pytest.raises(DekerServerError, match="answered the healthcheck with status 500"):
        request_and_apply_config(ctx)
    assert ctx.extra["httpx_client"].is_closed


def test_unreachable_servers_are_named_with_the_reason(ctx: CTX, httpx_mock: HTTPXMock):
    """The error says which servers were asked and why each one did not answer: no traceback is needed to see it."""
    httpx_mock.add_exception(
        httpx.ConnectError("[Errno -2] Name or service not known"), url=re.compile(r".*/v1/ping.*"), is_reusable=True
    )
    with pytest.raises(DekerServerError) as info:
        request_and_apply_config(ctx)
    message = str(info.value)
    assert message.startswith("No DEKER server answered the healthcheck")
    for server in ctx.uri.servers:
        assert f"{server}: [Errno -2] Name or service not known" in message
    assert ctx.extra["httpx_client"].is_closed


def test_config_without_last_modified_header_is_rejected(ctx: CTX, httpx_mock: HTTPXMock, mocked_ping: dict):
    httpx_mock.add_response(url=re.compile(r".*/v1/ping.*"), json=mocked_ping)
    with pytest.raises(DekerClusterError, match="last-modified"):
        request_and_apply_config(ctx)
