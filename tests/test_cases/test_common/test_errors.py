import pytest

from deker_server_adapters.errors import DekerServerError


def test_message_from_a_dict_response():
    """A config dict can play the role of the response (DekerClusterError does that)."""
    error = DekerServerError({"mode": "single"}, "Wrong config")
    assert str(error) == "Wrong config \nResponse: status=None, message={'mode': 'single'}"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        pytest.param({"message": "Just a message"}, "Just a message", id="message"),
        pytest.param({}, "", id="nothing"),
    ],
)
def test_message_without_response(kwargs: dict, message: str):
    assert str(DekerServerError(**kwargs)) == message
