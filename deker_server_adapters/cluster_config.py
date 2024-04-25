from dataclasses import dataclass, field
from functools import cached_property
from json import JSONDecodeError
from typing import List, Optional

from deker.ctx import CTX
from deker.uri import Uri

from deker_server_adapters.consts import STATUS_OK
from deker_server_adapters.errors import DekerClusterError, DekerServerError
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.utils.requests import make_request
from deker_server_adapters.utils.version import get_api_version


CLUSTER_MODE = "cluster"


@dataclass
class Node:
    """Node of cluster."""

    id: str
    host: str
    port: str
    protocol: str = "http"

    @cached_property
    def url(self) -> Uri:
        """Make an Uri instance."""
        return Uri.create(f"{self.protocol}://{self.host}:{self.port}")

    def __hash__(self) -> int:
        """Use string repr as a Hash."""
        return hash(str(self))

    def __str__(self) -> str:
        """String representation."""
        return self.id


@dataclass
class ClusterConfig:
    """Normal mode of cluster config."""

    mode: str
    leader: Node  # UUID
    current: List[Node]
    target: Optional[List[Node]] = None  # Only appears when clust in rebalancing mode

    __hash_ring: HashRing = field(init=False)
    __hash_ring_target: HashRing = field(init=False)

    @classmethod
    def from_dict(cls, cluster_config_dict: dict) -> "ClusterConfig":
        """Create cluster config from dict.

        :param cluster_config_dict: Cluster configuration that comes from server.
        """
        leader_id = cluster_config_dict["leader_id"]
        current: List[Node] = []
        target: List[Node] = []

        def process_nodes(nodes: List[dict], out: List[Node]) -> Optional[Node]:
            _leader = None
            for node_dict in nodes:
                node = Node(**node_dict)
                out.append(node)
                if node.id == leader_id:
                    _leader = node
            out.sort(key=lambda x: str(x))
            return _leader

        leader = process_nodes(cluster_config_dict["current"], current)
        if "target" in cluster_config_dict:
            # If leader is found on the target list, and not current
            target_leader = process_nodes(cluster_config_dict["target"], target)
            leader = leader or target_leader

        if not leader:
            raise DekerClusterError("No leader has been found")

        return cls(mode=cluster_config_dict["mode"], leader=leader, current=current, target=target or None)


def request_config(ctx: CTX) -> dict:  # type: ignore[return-value]
    """Request config from server and apply it on context.

    :param ctx: App context
    """
    httpx_client = ctx.extra["httpx_client"]
    url = f"{get_api_version()}/ping"

    # If we do healthcheck in cluster
    nodes = [*ctx.uri.servers] if ctx.uri.servers else [ctx.uri.raw_url]
    response = make_request(url=url, nodes=nodes, client=httpx_client)

    if not response or response.status_code != STATUS_OK:
        httpx_client.close()
        raise DekerServerError(
            response,
            "Healthcheck failed. Deker client will be closed.",
        )

    try:
        config = response.json()  # type: ignore[union-attr]
        return config
    except JSONDecodeError:
        if ctx.uri.servers:
            raise DekerClusterError(response, "Server responded with wrong config. Couldn't parse json")


def is_config_in_cluster_mode(config: Optional[dict], ctx: CTX) -> bool:
    """Check if mode from config is set to cluster.

    :param config: Config from response
    :param ctx: Context of app
    """
    if not ctx.uri.servers:
        return config is not None and config.get("mode") == CLUSTER_MODE

    if config is None or config.get("mode") != CLUSTER_MODE:
        raise DekerClusterError(
            config,
            "Server responded with wrong config."
            " Key 'mode' either doesn't exist or its value differs from 'cluster'",
        )

    return True


def apply_config(config_dict: dict, ctx: CTX) -> None:
    """Apply config from server.

    :param config_dict: Config from server
    :param ctx: Application context
    """
    config = ClusterConfig.from_dict(config_dict)
    # Config
    ctx.extra["cluster_config"] = config

    # Httpx Client
    ctx.extra["httpx_client"].base_url = config.leader.url.raw_url
    ctx.extra["httpx_client"].cluster_mode = True

    # Hash Ring
    ctx.extra["hash_ring"] = HashRing(config.current)

    ctx.extra["hash_ring_target"] = None  # To avoid check within the dict
    if config.target:
        ctx.extra["hash_ring_target"] = HashRing(config.target)


def request_and_apply_config(ctx: CTX) -> None:
    """Request cluster config from server and apply it on current context.

    :param ctx: Application context
    """
    config_dict = request_config(ctx)
    if is_config_in_cluster_mode(config_dict, ctx):
        apply_config(config_dict, ctx)
