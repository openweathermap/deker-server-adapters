from dataclasses import dataclass, field
from functools import cached_property
from json import JSONDecodeError
from typing import List, Optional

from deker.ctx import CTX
from deker.uri import Uri

from deker_server_adapters.consts import LAST_MODIFIED_HEADER, STATUS_OK
from deker_server_adapters.errors import DekerClusterError, DekerServerError
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.utils.requests import make_request
from deker_server_adapters.utils.version import get_api_version


CLUSTER_MODE = "cluster"


@dataclass
class Node:
    """Node of cluster."""

    host: str
    port: str
    protocol: str = "http"
    id: Optional[str] = None
    storage: Optional[str] = None

    @cached_property
    def url(self) -> Uri:
        """Make an Uri instance."""
        return Uri.create(f"{self.protocol}://{self.host}:{self.port}")

    def __hash__(self) -> int:
        """Use string form of the node as a Hash."""
        return hash(str(self))

    def __str__(self) -> str:
        """String representation."""
        return self.id or ""


@dataclass
class ClusterConfig:
    """Normal mode of cluster config."""

    mode: str
    leader: Node
    current: List[Node]
    target: Optional[List[Node]] = None  # Only appears when cluster in rebalancing mode
    cluster_status: Optional[str] = ""
    __hash_ring: HashRing = field(init=False)
    __hash_ring_target: HashRing = field(init=False)

    @classmethod
    def from_dict(cls, cluster_config_dict: dict) -> "ClusterConfig":
        """Create cluster config from dict.

        :param cluster_config_dict: Cluster configuration that comes from server.
        """
        leader_id = cluster_config_dict["leader_id"]

        def process_nodes(nodes: List[dict]) -> List[Node]:
            node_list = [Node(**node_dict) for node_dict in nodes]
            node_list.sort(key=lambda x: str(x))
            return node_list

        # cluster always returns all current RAFT nodes, thus we don't need to check target config to know the leader
        # we won't need RAFT cluster config after getting the leader
        raft_nodes = process_nodes(cluster_config_dict["raft"])
        leader = next((node for node in raft_nodes if node.id == leader_id), None)

        if not leader:
            raise DekerClusterError(None, "No leader has been found")

        current = process_nodes(cluster_config_dict["current"])
        target = process_nodes(cluster_config_dict["target"]) if "target" in cluster_config_dict else None

        return cls(
            mode=cluster_config_dict["mode"],
            leader=leader,
            current=current,
            target=target,
            cluster_status=cluster_config_dict.get("cluster_status"),
        )


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
        if is_config_in_cluster_mode(config, ctx):
            # Set hash of config
            httpx_client.headers.update({LAST_MODIFIED_HEADER: response.headers[LAST_MODIFIED_HEADER]})
            apply_config(config, ctx)
    except KeyError:
        raise DekerClusterError(response, f"No {LAST_MODIFIED_HEADER} header found in response.")
    except JSONDecodeError:
        if ctx.uri.servers:
            raise DekerClusterError(response, "Server responded with wrong config. Couldn't parse json")
