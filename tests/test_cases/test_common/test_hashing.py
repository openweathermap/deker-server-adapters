import pytest

from deker_server_adapters.errors import HashRingError
from deker_server_adapters.utils.hashing import get_hash_by_primary_attrs, get_hash_key


def test_v_position_is_joined_with_dashes():
    assert get_hash_by_primary_attrs({"vid": "v", "v_position": (1, 2, 3)}) == "v/1-2-3"


def test_hash_key_falls_back_to_the_id():
    assert get_hash_key({"primary_attributes": {}, "id_": "abc"}) == "abc"


def test_hash_key_requires_an_id_or_primary_attributes():
    with pytest.raises(HashRingError, match="nothing to hash"):
        get_hash_key({"primary_attributes": {}, "id_": None})
