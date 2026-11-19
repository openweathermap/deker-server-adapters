from typing import Any

from deker.ctx import CTX
from deker.errors import DekerMemoryError
from httpx import Client, Response

from deker_server_adapters.cluster_config import apply_config
from deker_server_adapters.consts import (
    CONTENT_TOO_LARGE,
    EXCEPTION_CLASS_PARAM_NAME,
    LAST_MODIFIED_HEADER,
    NON_LEADER_WRITE,
    PRECONDITION_FAILED,
    RATE_ERROR_MESSAGE,
    STATUS_OK,
    TOO_LARGE_ERROR_MESSAGE,
    TOO_MANY_REQUESTS,
)
from deker_server_adapters.errors import (
    DekerBaseRateLimitError,
    DekerDataPointsLimitError,
    DekerRateLimitError,
    InvalidConfigHash,
)


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

    def _send(self, *args: Any, stream: bool, **kwargs: Any) -> Response:
        r"""Send a request, leaving the body of a successful response unread when ``stream`` is set.

        Any other response is read in full at once, so error handling and node fallback
        never meet an unread body.

        :param \*args: arguments to request
        :param stream: do not read the body of a ``200`` response
        :param \**kwargs: keyword arguments to request
        :returns: server response
        """
        if not stream:
            return super().request(*args, **kwargs)
        response = self.send(self.build_request(*args, **kwargs), stream=True)
        if response.status_code != STATUS_OK:
            response.read()
        return response

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
