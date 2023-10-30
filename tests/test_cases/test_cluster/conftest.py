import pytest

from tests.conftest import CLUSTER_MODE


@pytest.fixture()
def mode() -> str:
    return CLUSTER_MODE
