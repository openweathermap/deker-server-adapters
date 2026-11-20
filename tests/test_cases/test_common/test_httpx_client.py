import logging
import re

from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

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


class DroppingHandler(BaseHTTPRequestHandler):
    """An HTTP/1.1 server that answers the first request of a connection and drops the connection on the second.

    This is what a client sees when the server closed a kept-alive connection while the client was sending into it:
    the request leaves the socket fine and the connection ends without a response.
    """

    protocol_version = "HTTP/1.1"
    requests: list[tuple[str, int]] = []  # the method and the number of the request on its connection

    def do_GET(self) -> None:  # noqa: N802  # the name is the contract of the base class
        self.answer("GET")

    def do_POST(self) -> None:  # noqa: N802
        self.answer("POST")

    def answer(self, method: str) -> None:
        """Answer the first request of the connection, drop the second.

        :param method: the method of the request
        """
        number = getattr(self, "number", 0) + 1
        self.number = number
        self.requests.append((method, number))
        if number > 1:
            self.close_connection = True
            return
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        """Keep the test output clean."""


@pytest.fixture
def dropping_server() -> Generator[str, None, None]:
    """A real server that drops the second request of every connection; yields its base url."""
    DroppingHandler.requests = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), DroppingHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_a_stale_kept_alive_connection_is_replaced_against_a_real_socket(dropping_server: str, ctx: CTX):
    """The second GET over a kept-alive connection the server dropped succeeds on a new connection."""
    with HttpxClient(base_url=dropping_server) as client:
        client.ctx = ctx
        assert client.get("/v1/ping").status_code == 200
        assert client.get("/v1/ping").status_code == 200
    assert DroppingHandler.requests == [("GET", 1), ("GET", 2), ("GET", 1)]


def test_a_post_over_a_stale_kept_alive_connection_fails_against_a_real_socket(dropping_server: str, ctx: CTX):
    """A POST is not repeated: the client sees the dropped connection as the error it is."""
    with HttpxClient(base_url=dropping_server) as client:
        client.ctx = ctx
        assert client.get("/v1/ping").status_code == 200
        with pytest.raises(httpx.RemoteProtocolError):
            client.post("/v1/collections", json={})
    assert DroppingHandler.requests == [("GET", 1), ("POST", 2)]
