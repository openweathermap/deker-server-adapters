import pytest

from pytest_httpx import HTTPXMock

from deker_server_adapters.consts import RATE_ERROR_MESSAGE, TOO_LARGE_ERROR_MESSAGE
from deker_server_adapters.errors import DekerDataPointsLimitError, DekerRateLimitError
from deker_server_adapters.httpx_client import HttpxClient


@pytest.fixture()
def assert_all_responses_were_requested() -> bool:
    return False


@pytest.mark.parametrize(("method", "kwargs"), (("get", {}), ("post", {}), ("put", {})))
def test_rate_limits(httpx_mock: HTTPXMock, rate_limits_headers, method, kwargs):
    httpx_mock.add_response(status_code=429, headers=rate_limits_headers)
    with HttpxClient(base_url="http://localhost:8000") as client:
        func = getattr(client, method)
        with pytest.raises(DekerRateLimitError) as e:
            func(url="/test", **kwargs)

        assert e.value.message == RATE_ERROR_MESSAGE
        assert str(e.value.limit) == rate_limits_headers["RateLimit-Limit"]
        assert str(e.value.reset) == rate_limits_headers["RateLimit-Reset"]
        assert str(e.value.remaining) == rate_limits_headers["RateLimit-Remaining"]


@pytest.mark.parametrize(("method", "kwargs"), (("get", {}), ("post", {}), ("put", {})))
def test_data_point_limits(httpx_mock: HTTPXMock, rate_limits_headers, method, kwargs):
    httpx_mock.add_response(status_code=413, headers=rate_limits_headers, json={"class": "DekerDatapointError"})
    with HttpxClient(base_url="http://localhost:8000") as client:
        func = getattr(client, method)
        with pytest.raises(DekerDataPointsLimitError) as e:
            func(url="/test", **kwargs)

        assert e.value.message == TOO_LARGE_ERROR_MESSAGE
        assert str(e.value.limit) == rate_limits_headers["RateLimit-Limit"]
        assert str(e.value.reset) == rate_limits_headers["RateLimit-Reset"]
        assert str(e.value.remaining) == rate_limits_headers["RateLimit-Remaining"]
