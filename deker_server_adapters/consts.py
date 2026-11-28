from enum import Enum

from deker import Array, VArray

STATUS_OK = 200
STATUS_CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404
TIMEOUT = 504  # the read timeout of the operation is over on the server
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

# seconds the http client waits for an answer beyond the timeout of the operation: the way there and back
NETWORK_TIMEOUT_MARGIN = 5
TIMEOUT_PARAM = "timeout"  # query parameter with the seconds of the operation, sent only when the call gives them
TIMEOUT_HEADER = "X-Deker-Timeout"  # seconds the server applied to the operation
TIMEOUT_CAPPED_HEADER = "X-Deker-Timeout-Capped"  # seconds asked, when the server cut them down to its limit
TIMEOUTS_KEY = "timeouts"  # key of the ping answer with the timeouts of the server
# the defaults and the limits of the timeouts of the server, in seconds, under TIMEOUTS_KEY
SERVER_TIMEOUTS = ("read_timeout", "write_lock_timeout", "max_read_timeout", "max_write_lock_timeout")
TIMEOUT_LIMITS = "timeout_limits"  # key of the context extra with the limits of the server, for the logs


class ArrayType(Enum):
    """Enum for array/varray values."""

    array = Array
    varray = VArray
