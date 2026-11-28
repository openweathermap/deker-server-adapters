from json import JSONDecodeError

from deker.errors import DekerBaseApplicationError
from deker_tools.typings import Sentinel, SentinelType, is_sentinel
from httpx import Response, ResponseNotRead


class DekerServerError(DekerBaseApplicationError):
    """Any non-convertable-to-exception status (like 500)."""

    MAX_ERROR_TEXT_SIZE = 100

    def _make_response_from_response(
        self,
        response: Response | dict,
        message: str,
    ) -> str:
        """Make an Exception message based on response and provided text.

        :param response: Response from deker server
        :param message: Provided message
        :returns: exception message with the response status and server message
        """
        status: int | None = None
        server_message: dict | str
        if isinstance(response, dict):
            server_message = response
        else:
            status = response.status_code
            try:
                server_message = response.json()
            except JSONDecodeError:
                suffix = "..." if len(response.content) > self.MAX_ERROR_TEXT_SIZE else ""
                server_message = f"{response.content[: self.MAX_ERROR_TEXT_SIZE]!r}{suffix}"
            except ResponseNotRead:
                server_message = "<streamed body, not read>"

        return f"{message} \nResponse: {status=}, message={server_message}"

    def __init__(
        self,
        response: Response | dict | SentinelType = Sentinel,
        message: str = "",
    ) -> None:
        """Build the error from the response of the server and what the adapter was doing.

        :param response: the response of the server, or the config dict it sent; not given - no server answered
        :param message: what the adapter was doing
        """
        if is_sentinel(response):
            self.message = message
        else:
            self.message = self._make_response_from_response(response, message)
        super().__init__(self.message)


class DekerTimeoutServer(DekerServerError):
    """If we hit timeout on server."""


class DekerBaseRateLimitError(DekerBaseApplicationError):
    """If we exceed request limit or number of data points is too large."""

    limit: int | None
    remaining: int | None
    reset: int | None

    def __init__(self, message: str, limit: int | None = None, remaining: int | None = None, reset: int | None = None):
        self.limit = limit
        self.remaining = remaining
        self.reset = reset
        self.message = message
        super().__init__(message)


class DekerRateLimitError(DekerBaseRateLimitError):
    """If we exceed request limit."""


class DekerDataPointsLimitError(DekerBaseRateLimitError):
    """If number of data points is too large."""


class DekerClusterError(DekerServerError):
    """The cluster or a node cannot take the request at the moment: a 503 of the server, or a config it cannot use.

    ``retry_after`` holds the seconds the server asked to wait before a retry (its ``Retry-After`` header),
    ``None`` when it said nothing. The adapters never retry on their own: the caller decides.
    """

    retry_after: int | None

    def __init__(
        self,
        response: Response | dict | SentinelType = Sentinel,
        message: str = "",
        retry_after: int | None = None,
    ) -> None:
        """Build the error from the response of the server and the seconds it asked to wait.

        :param response: the response of the server, or the config dict it sent; not given - no server answered
        :param message: what the adapter was doing
        :param retry_after: seconds of the ``Retry-After`` header, ``None`` when the server said nothing
        """
        self.retry_after = retry_after
        super().__init__(response, message)


class HealthcheckError(DekerServerError):
    """If f there is  problem with healthcheck."""


class FilteringByIdInClusterIsForbidden(DekerBaseApplicationError):
    """If we try to filter by in in collection with primary attributes (in cluster)."""

    message = (
        "Collection has primary attributes in the schema."
        "Filtering by ID is not allowed. Use filtering by primary attributes."
    )


class HashRingError(DekerBaseApplicationError):
    """If there is a problem with HashRing."""


class InvalidConfigHash(DekerBaseApplicationError):
    """If config was updated."""
