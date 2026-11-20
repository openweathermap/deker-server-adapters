from enum import Enum

from deker import Array, VArray

STATUS_OK = 200
STATUS_CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404
TIMEOUT = 504
CONTENT_TOO_LARGE = 413
TOO_MANY_REQUESTS = 429
NON_LEADER_WRITE = 421
PRECONDITION_FAILED = 412  # the cluster configuration of the client is stale
LOCKED = 423  # the array is held by another process
WRITTEN_PARTLY = 409  # some Arrays of a VArray could not be written
SERVICE_UNAVAILABLE = 503  # the cluster has no leader, is rebalancing, the node is out of sync, or the server limit
RETRY_AFTER_HEADER = "Retry-After"

# Name of the parameter in the server response
COLLECTION_NAME_PARAM = "collection_name"

RATE_ERROR_MESSAGE = "Too many requests, try again later"
TOO_LARGE_ERROR_MESSAGE = "Requested object is too large, use smaller subset"

EXCEPTION_CLASS_PARAM_NAME = "class"
LAST_MODIFIED_HEADER = "last-modified"
READ_CHUNK_SIZE = 1 << 20  # bytes of a streamed array body copied at a time
# seconds an idle connection is kept; shorter than the server keeps one (75), so the client lets go first
KEEPALIVE_EXPIRY = 60
MAX_CONNECTIONS = 100  # the pool of the http client, the httpx defaults stated here
MAX_KEEPALIVE_CONNECTIONS = 20
REBALANCING_STATUS = "rebalancing"
NORMAL_STATUS = "normal"


class ArrayType(Enum):
    """Enum for array/varray values."""

    array = Array
    varray = VArray
