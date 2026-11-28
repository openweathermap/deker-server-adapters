from logging import getLogger
from typing import Any

from deker.ctx import CTX
from deker.errors import DekerMemoryError
from httpx import (
    USE_CLIENT_DEFAULT,
    Client,
    ConnectError,
    ReadError,
    RemoteProtocolError,
    Request,
    Response,
    WriteError,
)
from httpx._client import UseClientDefault
from httpx._types import AuthTypes

from deker_server_adapters.cluster_config import apply_config
from deker_server_adapters.consts import (
    CONTENT_TOO_LARGE,
    EXCEPTION_CLASS_PARAM_NAME,
    LAST_MODIFIED_HEADER,
    NON_LEADER_WRITE,
    PRECONDITION_FAILED,
    RATE_ERROR_MESSAGE,
    STATUS_OK,
    TIMEOUT_CAPPED_HEADER,
    TIMEOUT_HEADER,
    TOO_LARGE_ERROR_MESSAGE,
    TOO_MANY_REQUESTS,
)
from deker_server_adapters.errors import (
    DekerBaseRateLimitError,
    DekerDataPointsLimitError,
    DekerRateLimitError,
    InvalidConfigHash,
)

logger = getLogger(__name__)

# a connection the server closed while the client kept it: the request is sent once more over a new one
DROPPED_CONNECTION_ERRORS = (ConnectError, ReadError, WriteError, RemoteProtocolError)
# the methods safe to repeat: the server ends up in the same state whether it got the request once or twice
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE"})


def rate_limit_err(response: Response, message: str, class_: type[DekerBaseRateLimitError]) -> None:
    """Raise an error with rate limit parameters.

    This function incapsulates logic of fetching rates from headers

    :param response: HttpxResponse object
    :param message: Message of the exception
    :param class_: Class (Type) of the exception
    :raises DekerBaseRateLimitError: always, the given class with the limits from the ``RateLimit-*`` headers
    """
    limit = response.headers.get("RateLimit-Limit")
    remaining = response.headers.get("RateLimit-Remaining")
    reset = response.headers.get("RateLimit-Reset")

    raise class_(
        message=message,
        limit=int(limit) if limit else None,
        remaining=int(remaining) if remaining else None,
        reset=int(reset) if reset else None,
    )


class HttpxClient(Client):
    """The http client of the adapters: an ``httpx.Client`` that follows the cluster and translates its errors.

    A response saying the cluster config changed is applied and the request repeated; a write sent to a
    non-leader node is repeated on the leader; the rate limit answers become Deker errors.
    """

    ctx: CTX
    cluster_mode: bool = False

    def _send_built(
        self,
        request: Request,
        stream: bool,
        auth: AuthTypes | UseClientDefault | None,
        follow_redirects: bool | UseClientDefault,
    ) -> Response:
        """Send a built request; the body of a failed response is read even when streaming was asked.

        A timeout the server cut down to its limit is logged as a warning, with the seconds asked and applied.

        :param request: the request
        :param stream: do not read the body of a ``200`` response
        :param auth: the auth of this request, the client's by default
        :param follow_redirects: whether to follow redirects, the client's choice by default
        :returns: server response
        """
        response = self.send(request, stream=stream, auth=auth, follow_redirects=follow_redirects)
        capped = response.headers.get(TIMEOUT_CAPPED_HEADER)
        if capped is not None:
            logger.warning(
                "The server cut the timeout of %s %s down: %s s asked, %s s applied",
                request.method,
                request.url.path,
                capped,
                response.headers.get(TIMEOUT_HEADER),
            )
        if stream and response.status_code != STATUS_OK:
            response.read()
        return response

    def _send(self, *args: Any, stream: bool, **kwargs: Any) -> Response:
        r"""Send a request, leaving the body of a successful response unread when ``stream`` is set.

        Any other response is read in full at once, so error handling and node fallback
        never meet an unread body. An idempotent request whose connection the server dropped
        is sent once more over a new connection.

        :param \*args: arguments to request
        :param stream: do not read the body of a ``200`` response
        :param \**kwargs: keyword arguments to request
        :returns: server response
        """
        auth = kwargs.pop("auth", USE_CLIENT_DEFAULT)
        follow_redirects = kwargs.pop("follow_redirects", USE_CLIENT_DEFAULT)
        request = self.build_request(*args, **kwargs)
        try:
            return self._send_built(request, stream, auth, follow_redirects)
        except DROPPED_CONNECTION_ERRORS as e:
            if request.method not in IDEMPOTENT_METHODS:
                raise
            logger.warning(
                "Connection to %s dropped on %s %s, the request is repeated once: %s",
                request.url.host,
                request.method,
                request.url.path,
                e,
            )
            return self._send_built(request, stream, auth, follow_redirects)

    def request(self, *args: Any, retry_on_hash_failure: bool = False, stream: bool = False, **kwargs: Any) -> Response:
        r"""Override httpx method to handle rate errors.

        :param \*args: arguments to request
        :param retry_on_hash_failure: If we should retry on invalid hash
        :param stream: leave the body of a successful response unread, to be consumed by the caller
        :param \**kwargs: keyword arguments to request
        :returns: server response
        :raises InvalidConfigHash: if the cluster config changed and the request is not to be retried
        :raises DekerRateLimitError: if the server answered 429
        :raises DekerDataPointsLimitError: if the server refused the request as too large
        """
        response = self._send(*args, stream=stream, **kwargs)
        if response.status_code == PRECONDITION_FAILED:
            apply_config(response.json(), self.ctx)
            if LAST_MODIFIED_HEADER in response.headers:
                self.headers.update({LAST_MODIFIED_HEADER: response.headers[LAST_MODIFIED_HEADER]})
            if retry_on_hash_failure:
                response = self._send(*args, stream=stream, **kwargs)
            else:
                raise InvalidConfigHash("Cluster hash conflict")

        if response.status_code == TOO_MANY_REQUESTS:
            rate_limit_err(response=response, message=RATE_ERROR_MESSAGE, class_=DekerRateLimitError)
        elif (
            response.status_code == CONTENT_TOO_LARGE
            and response.json().get(EXCEPTION_CLASS_PARAM_NAME) != DekerMemoryError.__name__
        ):
            rate_limit_err(response=response, message=TOO_LARGE_ERROR_MESSAGE, class_=DekerDataPointsLimitError)

        if response.status_code == NON_LEADER_WRITE:
            apply_config(response.json(), self.ctx)
            return self._send(*args, stream=stream, **kwargs)

        return response
