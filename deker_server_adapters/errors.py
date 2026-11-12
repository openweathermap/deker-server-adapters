from json import JSONDecodeError

from deker.errors import DekerBaseApplicationError
from httpx import Response


class DekerServerError(DekerBaseApplicationError):
    """Any non-convertable-to-exception status (like 500)."""

    MAX_ERROR_TEXT_SIZE = 100

    def _make_response_from_response(
        self,
        response: Response | dict,
        message: str | None = None,
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

        message = message or ""
        return f"{message} \nResponse: {status=}, message={server_message}"

    def __init__(
        self,
        response: Response | dict | None = None,
        message: str | None = None,
    ) -> None:
        if response:
            self.message = self._make_response_from_response(response, message)
        else:
            self.message = message if message else ""
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
    """If there is problem with cluster."""


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
