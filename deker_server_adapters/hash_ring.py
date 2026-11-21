import hashlib
import math

from bisect import bisect
from collections.abc import Callable
from typing import Generic, TypeVar

from deker_tools.typings import Sentinel, SentinelType, is_sentinel

from deker_server_adapters.errors import HashRingError

md5_constructor = hashlib.md5

NodeT = TypeVar("NodeT")


class HashRing(Generic[NodeT]):
    """Consistent hash ring over nodes of any type with a stable ``str()`` representation."""

    def _hash_val(self, b_key: list[int], entry_fn: Callable) -> int:
        """Build a 32-bit integer from four bytes of a digest.

        :param b_key: bytes of the digest
        :param entry_fn: maps the byte number 0-3 to its index in the digest
        :returns: the integer
        """
        return (b_key[entry_fn(3)] << 24) | (b_key[entry_fn(2)] << 16) | (b_key[entry_fn(1)] << 8) | b_key[entry_fn(0)]

    def _hash_digest(self, key: str) -> list[int]:
        """Get the md5 digest of a key as a list of bytes.

        :param key: the key
        :returns: 16 bytes
        """
        m = md5_constructor()
        m.update(key.encode())
        return list(m.digest())

    def _generate_circle(self) -> None:
        """Place the nodes on the ring: 40 points per node times its share of the weights, 3 keys per point."""
        total_weight = 0
        for node in self.nodes:
            total_weight += self.weights.get(node, 1)

        for node in self.nodes:
            weight = self.weights.get(node, 1)
            factor = math.floor((40 * len(self.nodes) * weight) / total_weight)

            for j in range(0, int(factor)):
                b_key = self._hash_digest(f"{node}-{j}")

                for i in range(0, 3):
                    key = self._hash_val(b_key, lambda x: x + i * 4)  # noqa
                    self.ring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def __init__(
        self,
        nodes: list[NodeT] | tuple[NodeT, ...] | set[NodeT],
        weights: dict[NodeT, int] | SentinelType = Sentinel,
    ):
        """Generate instance of hash ring with given nodes.

        :param nodes: is a list of objects that have a proper __str__ representation.
        :param weights: is dictionary that sets weights to the nodes.  The default
        weight is that all nodes are equal.
        """
        self.ring: dict[int, NodeT] = {}
        self._sorted_keys: list[int] = []

        self.nodes: list[NodeT] | tuple[NodeT, ...] | set[NodeT] = nodes
        self.weights: dict[NodeT, int] = {} if is_sentinel(weights) else weights

        self._generate_circle()

    def get_node(self, string_key: str) -> NodeT:
        """Return the node responsible for a key.

        :param string_key: String key
        :returns: node responsible for the key
        :raises HashRingError: if the ring is empty
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            msg = f"Couldn't find a position in {self.ring}"
            raise HashRingError(msg)
        return self.ring[self._sorted_keys[pos]]

    def get_node_pos(self, string_key: str) -> int | None:
        """Return the position in the ring of the node responsible for a key.

        :param string_key: String key
        :returns: position of the node in the ring, or ``None`` if the ring is empty
        """
        if not self.ring:
            return None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        return pos

    def gen_key(self, key: str) -> int:
        """Given a string key it returns a long value.

        This long value represents a place on the hash ring.
        md5 is currently used because it mixes well.

        :param key: a string key
        :returns: integer position of the key on the hash ring
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)
