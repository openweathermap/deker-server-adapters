import re

import httpx
import pytest

from tests.conftest import CLUSTER_MODE

from deker_server_adapters.consts import LAST_MODIFIED_HEADER


@pytest.fixture()
def mode() -> str:
    return CLUSTER_MODE
