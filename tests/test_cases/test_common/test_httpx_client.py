import logging
import re

import httpx
import pytest

from deker.ctx import CTX
from pytest_httpx import HTTPXMock

from deker_server_adapters.consts import KEEPALIVE_EXPIRY
from deker_server_adapters.httpx_client import HttpxClient

DROPPED = [
    pytest.param(httpx.RemoteProtocolError("Server disconnected without sending a response."), id="disconnected"),
    pytest.param(httpx.ReadError("[Errno 104] Connection reset by peer"), id="reset-on-read"),
    pytest.param(httpx.WriteError("[Errno 32] Broken pipe"), id="broken-pipe"),
    pytest.param(httpx.ConnectError("[Errno 111] Connection refused"), id="refused"),
]


@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "DELETE"])
@pytest.mark.parametrize("error", DROPPED)
def test_an_idempotent_request_is_repeated_once_on_a_dropped_connection(
    httpx_mock: HTTPXMock, ctx: CTX, caplog: pytest.LogCaptureFixture, method: str, error: httpx.TransportError
):
    client: HttpxClient = ctx.extra["httpx_client"]
    url = re.compile(r".*/v1/collection/test$")
    httpx_mock.add_exception(error, method=method, url=url)
    httpx_mock.add_response(status_code=200, method=method, url=url, json={"name": "test"})

    with caplog.at_level(logging.WARNING, logger="deker_server_adapters.httpx_client"):
        response = client.request(method, "/v1/collection/test")

    assert response.status_code == 200
    assert len(httpx_mock.get_requests(method=method)) == 2
    assert "repeated once" in caplog.text


@pytest.mark.parametrize("error", DROPPED)
def test_a_post_is_not_repeated_on_a_dropped_connection(httpx_mock: HTTPXMock, ctx: CTX, error: httpx.TransportError):
    client: HttpxClient = ctx.extra["httpx_client"]
    url = re.compile(r".*/v1/collections$")
    httpx_mock.add_exception(error, method="POST", url=url)

    with pytest.raises(type(error)):
        client.request("POST", "/v1/collections", json={"name": "test"})

    assert len(httpx_mock.get_requests(method="POST")) == 1


def test_a_request_dropped_twice_fails(httpx_mock: HTTPXMock, ctx: CTX):
    client: HttpxClient = ctx.extra["httpx_client"]
    url = re.compile(r".*/v1/collection/test$")
    httpx_mock.add_exception(httpx.RemoteProtocolError("Server disconnected without sending a response."), url=url)
    httpx_mock.add_exception(httpx.RemoteProtocolError("Server disconnected without sending a response."), url=url)

    with pytest.raises(httpx.RemoteProtocolError):
        client.request("GET", "/v1/collection/test")

    assert len(httpx_mock.get_requests(method="GET")) == 2


def test_a_streamed_request_is_repeated_too(httpx_mock: HTTPXMock, ctx: CTX):
    client: HttpxClient = ctx.extra["httpx_client"]
    url = re.compile(r".*/data$")
    httpx_mock.add_exception(httpx.ReadError("[Errno 104] Connection reset by peer"), url=url)
    httpx_mock.add_response(status_code=200, url=url, content=b"\x00\x01")

    response = client.request("GET", "/v1/collection/test/array/by-id/1/subset/[:]/data", stream=True)

    assert response.read() == b"\x00\x01"
    assert len(httpx_mock.get_requests(method="GET")) == 2


def test_the_client_lets_its_connections_go_before_the_server_does():
    """The server holds an idle connection 75 seconds; the client drops its own earlier, so it never writes into a
    connection the server has just closed."""
    assert KEEPALIVE_EXPIRY < 75


def test_the_auth_of_a_request_still_reaches_the_server(httpx_mock: HTTPXMock, ctx: CTX):
    """The request is built and sent in two steps now; the auth and the redirect arguments of httpx are kept."""
    client: HttpxClient = ctx.extra["httpx_client"]
    url = re.compile(r".*/v1/collection/test$")
    httpx_mock.add_response(status_code=200, url=url, match_headers={"Authorization": "Basic dTpw"})

    assert client.get("/v1/collection/test", auth=("u", "p"), follow_redirects=False).status_code == 200
