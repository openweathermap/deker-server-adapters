import re

from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.cluster_config import is_config_in_cluster_mode, request_and_apply_config


def test_single_uri_accepts_any_config(ctx: CTX):
    """Without a server list the config only says whether the server happens to be a cluster."""
    assert is_config_in_cluster_mode(None, ctx) is False
    assert is_config_in_cluster_mode({"mode": "single"}, ctx) is False
    assert is_config_in_cluster_mode({"mode": "cluster"}, ctx) is True


def test_single_server_config_is_not_applied(ctx: CTX, httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=re.compile(r".*/v1/ping.*"), json={"mode": "single"})
    request_and_apply_config(ctx)
    assert "cluster_config" not in ctx.extra
    assert ctx.extra["httpx_client"].cluster_mode is False
