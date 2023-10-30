import pytest

from deker import Array
from deker.ctx import CTX

from tests.conftest import SINGLE_MODE


@pytest.fixture()
def mode() -> str:
    return SINGLE_MODE


@pytest.fixture()
def array_url(ctx: CTX, array: Array, array_url_path: str) -> str:
    return f"{ctx.uri.raw_url.rstrip('/')}{array_url_path}"
