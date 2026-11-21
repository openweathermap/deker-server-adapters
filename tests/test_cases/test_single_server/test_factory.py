import re

import pytest

from deker.ctx import CTX
from deker.uri import Uri
from pytest_httpx import HTTPXMock

from deker_server_adapters.factory import AdaptersFactory


def test_auth_factory(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")

    factory = AdaptersFactory(ctx, uri)
    assert factory.httpx_client.auth


def test_auth_factory_close(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")
    factory = AdaptersFactory(ctx, uri)
    factory.close()
    assert factory.httpx_client.is_closed


@pytest.mark.parametrize("body", [pytest.param(b"null", id="json-null"), pytest.param(b"", id="no-json")])
def test_a_ping_without_a_json_object_leaves_the_settings(body: bytes, ctx: CTX, httpx_mock: HTTPXMock):
    """A server that answers the ping with no JSON object tells no timeouts: the settings of the client stay."""
    httpx_mock.add_response(method="GET", url=re.compile(r".*/v1/ping.*"), content=body)

    factory = AdaptersFactory(ctx, Uri.create("http://localhost:8000"))
    try:
        assert factory.ctx.config.read_timeout == 3
        assert factory.ctx.config.write_lock_timeout == 1
        assert "timeout_limits" not in factory.ctx.extra
    finally:
        factory.close()
