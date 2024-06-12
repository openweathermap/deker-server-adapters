from enum import Enum


class Status(str, Enum):
    """Status of file on the server."""

    MOVED = "exported"
    UNMOVED = "exporting"
    NORMAL = "normal"
