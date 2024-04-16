import hashlib
import math

from bisect import bisect
from typing import Callable, Generator, List, Optional, Set, Tuple, Union

from deker_server_adapters.errors import HashRingError


md5_constructor = hashlib.md5


class HashRing:
    """Class for hash ring."""

    def _hash_val(self, b_key: List[int], entry_fn: Callable) -> int:
        return (b_key[entry_fn(3)] << 24) | (b_key[entry_fn(2)] << 16) | (b_key[entry_fn(1)] << 8) | b_key[entry_fn(0)]

    def _hash_digest(self, key: str) -> List[int]:
        m = md5_constructor()
        m.update(key.encode())
        return [int(str(letter)) for letter in m.digest()]  # , m.digest()))

    def _generate_circle(self) -> None:
        """Generate the circle."""
        total_weight = 0
        for node in self.nodes:
            total_weight += self.weights.get(node, 1)

        for node in self.nodes:
            weight = 1

            if node in self.weights:
                weight = self.weights.get(node)  # type: ignore[assignment]

            factor = math.floor((40 * len(self.nodes) * weight) / total_weight)

            for j in range(0, int(factor)):
                b_key = self._hash_digest(f"{node}-{j}")

                for i in range(0, 3):
                    key = self._hash_val(b_key, lambda x: x + i * 4)  # noqa
                    self.ring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def __init__(self, nodes: Union[List, Tuple, Set], weights: Optional[dict] = None):
        """Generare instace of hash ring with given nodes.

        :param nodes: is a list of objects that have a proper __str__ representation.
        :param weights: is dictionary that sets weights to the nodes.  The default
        weight is that all nodes are equal.
        """
        self.ring = {}  # type: ignore[var-annotated]
        self._sorted_keys = []  # type: ignore[var-annotated]

        self.nodes = nodes

        if not weights:
            weights = {}  # type: ignore[var-annotated]
        self.weights = weights

        self._generate_circle()

    def get_node(self, string_key: str) -> str:
        """Return hash ring by given a string key a corresponding node.

        If the hash ring is empty, `None` is returned.
        :param string_key: String key
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            raise HashRingError(f"Couldn't find a position in {self.ring}")
        return self.ring[self._sorted_keys[pos]]

    def get_node_pos(self, string_key: str) -> Optional[int]:
        """Return node and position.

        Given a string key a corresponding node in the hash ring
        is returned along with it's position in the ring.
        If the hash ring is empty, (`None`, `None`) is returned.
        :param string_key: String key
        """
        if not self.ring:
            return None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        return pos

    def iterate_nodes(self, string_key: str, distinct: bool = True) -> Generator:  # noqa
        """Given a string key it returns the nodes as a generator that can hold the key.

        The generator iterates one time through the ring
        starting at the correct position.
        :param string_key: string key
        :param distinct: is set, then the nodes returned will be unique,
        i.e. no virtual copies will be returned.
        """
        if not self.ring:
            yield None, None

        returned_values = set()

        def distinct_filter(value: str) -> Optional[str]:
            """Do filtration on used values.

            :param value: Value to check
            """
            if str(value) not in returned_values:
                returned_values.add(str(value))
                return value
            return None

        pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            val = distinct_filter(self.ring[key])
            if val:
                yield val

        for i, key in enumerate(self._sorted_keys):
            if i < pos:  # type: ignore[operator]
                val = distinct_filter(self.ring[key])
                if val:
                    yield val

    def gen_key(self, key: str) -> int:
        """Given a string key it returns a long value.

        this long value represents a place on the hash ring.
        md5 is currently used because it mixes well.
        :param key: a string key
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)
