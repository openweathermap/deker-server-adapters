from collections import Counter

import pytest

from deker_server_adapters.errors import HashRingError
from deker_server_adapters.hash_ring import HashRing


def test_empty_ring_has_no_node():
    ring: HashRing[str] = HashRing([])
    assert ring.get_node_pos("key") is None
    with pytest.raises(HashRingError, match="position"):
        ring.get_node("key")


def test_keys_past_the_last_point_wrap_to_the_first_node():
    ring = HashRing(["a", "b"])
    last_point = ring._sorted_keys[-1]
    key = next(f"key-{i}" for i in range(1_000_000) if ring.gen_key(f"key-{i}") > last_point)
    assert ring.get_node_pos(key) == 0
    assert ring.get_node(key) == ring.ring[ring._sorted_keys[0]]


def test_weights_change_the_share_of_points():
    ring = HashRing(["a", "b"], weights={"a": 3, "b": 1})
    points = Counter(ring.ring.values())
    assert points["a"] > points["b"]
    assert ring.weights == {"a": 3, "b": 1}


def test_nodes_without_weights_are_equal():
    ring = HashRing(["a", "b"])
    points = Counter(ring.ring.values())
    assert points["a"] == points["b"]
    assert ring.weights == {}
