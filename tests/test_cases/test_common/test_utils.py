from unittest.mock import patch

from httpx import Client

from deker_server_adapters.utils.requests import make_request


def test_make_request_returns_None_on_empty_nodes():
    with patch("deker_server_adapters.utils.requests._request") as mock:
        response = make_request(url="foo", nodes=[], client=Client())
        mock.assert_not_called()

    assert response is None
