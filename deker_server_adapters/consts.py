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

# Name of the parameter in the server response
COLLECTION_NAME_PARAM = "collection_name"

RATE_ERROR_MESSAGE = "Too many requests, try again later"
TOO_LARGE_ERROR_MESSAGE = "Requested object is too large, use smaller subset"

EXCEPTION_CLASS_PARAM_NAME = "class"


class ArrayType(Enum):
    """Enum for array/varray values."""

    array = Array
    varray = VArray
