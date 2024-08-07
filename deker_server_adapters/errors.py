from json import JSONDecodeError
from typing import Optional, Union

from deker.errors import DekerBaseApplicationError
from httpx import Response


class DekerServerError(DekerBaseApplicationError):
    """Any non-convertable-to-exception status (like 500)."""

    MAX_ERROR_TEXT_SIZE = 100

    def _make_response_from_response(
        self,
        response: Union[Response, dict],
        message: Optional[str] = None,
    ) -> str:
        """Make an Exception message based on response and provided text.

        :param response: Response from deker server
        :param message: Provided message
        """
        try:
            if isinstance(response, dict):
                status = None
                server_message = response
            else:
                status = response.status_code
                server_message = response.json()

        except JSONDecodeError:
            status = response.status_code  # type: ignore[union-attr]
            suffix = "..." if len(response.content) > self.MAX_ERROR_TEXT_SIZE else ""  # type: ignore[union-attr]
            content = response.content[: self.MAX_ERROR_TEXT_SIZE]  # type: ignore[union-attr,assignment]
            server_message = f"{content!r}" f"{suffix}"  # type: ignore[union-attr,assignment]

        message = message or ""
        return f"{message} \nResponse: {status=}, message={server_message}"

    def __init__(
        self,
        response: Optional[Union[Response, dict]] = None,
        message: Optional[str] = None,
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

    limit: Optional[int]
    remaining: Optional[int]
    reset: Optional[int]

    def __init__(
        self, message: str, limit: Optional[int] = None, remaining: Optional[int] = None, reset: Optional[int] = None
    ):
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
