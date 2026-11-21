import logging
import re

from functools import partial
from unittest.mock import patch

import httpx
import pytest

from cantok import DefaultToken, SimpleToken
from deker.ctx import CTX
from deker_tools.cancel import TokenCancelledError
from pytest_httpx import HTTPXMock

from tests.conftest import FIRED_TOKENS, cancel_and_answer, fired_token

from deker_server_adapters.utils.requests import make_request


def test_make_request_returns_None_on_empty_nodes(ctx: CTX):
    with patch("deker_server_adapters.utils.requests._request") as mock:
        response = make_request(url="foo", nodes=[], client=ctx.extra["httpx_client"], token=DefaultToken())
        mock.assert_not_called()

    assert response is None


class TestMakeRequestFailures:
    """A node that cannot be reached answers None; the reason goes to the log once and to the caller on demand."""

    NODES = ["http://one.test", "http://two.test"]

    def test_the_reason_is_collected_for_the_caller(self, ctx: CTX, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(httpx.ConnectError("[Errno -2] Name or service not known"), url=re.compile(r".*/ping"))
        failures: list[str] = []
        response = make_request(
            url="/ping", nodes=self.NODES[:1], client=ctx.extra["httpx_client"], token=DefaultToken(), failures=failures
        )
        assert response is None
        assert failures == ["http://one.test: [Errno -2] Name or service not known"]

    def test_every_node_of_a_cluster_is_reported(self, ctx: CTX, httpx_mock: HTTPXMock):
        httpx_mock.add_exception(httpx.ConnectError("refused"), url=re.compile(r".*/ping"), is_reusable=True)
        failures: list[str] = []
        assert (
            make_request(
                url="/ping", nodes=self.NODES, client=ctx.extra["httpx_client"], token=DefaultToken(), failures=failures
            )
            is None
        )
        assert sorted(failures) == ["http://one.test: refused", "http://two.test: refused"]

    def test_the_log_gets_one_line_without_the_traceback(
        self, ctx: CTX, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
    ):
        httpx_mock.add_exception(httpx.ConnectError("refused"), url=re.compile(r".*/ping"))
        with caplog.at_level(logging.ERROR, logger="deker_server_adapters.utils.requests"):
            make_request(url="/ping", nodes=self.NODES[:1], client=ctx.extra["httpx_client"], token=DefaultToken())
        errors = [record for record in caplog.records if record.levelno == logging.ERROR]
        assert [record.getMessage() for record in errors] == ["Could not get a response from http://one.test: refused"]
        assert "Traceback" not in caplog.text

    def test_the_traceback_is_logged_at_debug(self, ctx: CTX, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture):
        httpx_mock.add_exception(httpx.ConnectError("refused"), url=re.compile(r".*/ping"))
        with caplog.at_level(logging.DEBUG, logger="deker_server_adapters.utils.requests"):
            make_request(url="/ping", nodes=self.NODES[:1], client=ctx.extra["httpx_client"], token=DefaultToken())
        debug = [record for record in caplog.records if record.levelno == logging.DEBUG and record.exc_info]
        assert len(debug) == 1
        assert "ConnectError" in caplog.text

    def test_a_node_that_answers_leaves_no_failure(self, ctx: CTX, httpx_mock: HTTPXMock):
        httpx_mock.add_response(url=re.compile(r".*/ping"), status_code=200)
        failures: list[str] = []
        response = make_request(
            url="/ping", nodes=self.NODES[:1], client=ctx.extra["httpx_client"], token=DefaultToken(), failures=failures
        )
        assert response is not None
        assert response.status_code == 200
        assert failures == []


@pytest.mark.parametrize(("kind", "error"), FIRED_TOKENS)
def test_make_request_with_a_fired_token_sends_nothing(
    kind: str, error: type[TokenCancelledError], ctx: CTX, httpx_mock: HTTPXMock
):
    with pytest.raises(error):
        make_request(
            url="/ping", nodes=TestMakeRequestFailures.NODES, client=ctx.extra["httpx_client"], token=fired_token(kind)
        )
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize(
    ("cancelled", "asked"),
    [
        pytest.param(True, 1, id="cancelled-asks-no-other-node"),
        pytest.param(False, 2, id="not-cancelled-asks-the-next"),
    ],
)
def test_make_request_checks_the_token_before_every_node(cancelled: bool, asked: int, ctx: CTX, httpx_mock: HTTPXMock):
    """A node that fails while the token is cancelled is the last one asked."""
    token = SimpleToken()
    if cancelled:
        httpx_mock.add_callback(partial(cancel_and_answer, token, 500, b""), url=re.compile(r".*/ping"))
    else:
        httpx_mock.add_response(url=re.compile(r".*/ping"), status_code=500, is_reusable=True)

    if cancelled:
        with pytest.raises(TokenCancelledError):
            make_request(
                url="/ping", nodes=TestMakeRequestFailures.NODES, client=ctx.extra["httpx_client"], token=token
            )
    else:
        response = make_request(
            url="/ping", nodes=TestMakeRequestFailures.NODES, client=ctx.extra["httpx_client"], token=token
        )
        assert response is not None
        assert response.status_code == 500

    assert len(httpx_mock.get_requests()) == asked
