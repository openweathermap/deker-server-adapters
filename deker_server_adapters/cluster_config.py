from dataclasses import dataclass, field
from functools import cached_property
from json import JSONDecodeError

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
    """Node of cluster: its address, its id and the storage it serves."""

    host: str
    port: str
    protocol: str = "http"
    id: str | None = None
    storage: str | None = None

    @cached_property
    def url(self) -> Uri:
        """Get the uri of the node: ``protocol://host:port``."""
        return Uri.create(f"{self.protocol}://{self.host}:{self.port}")

    def __hash__(self) -> int:
        """Use string form of the node as a Hash."""
        return hash(str(self))

    def __str__(self) -> str:
        """Get the id of the node, the key it is placed on the hash ring by; empty without an id."""
        return self.id or ""


def _nodes_from_dicts(nodes: list[dict]) -> list[Node]:
    """Build the nodes from their dictionaries, sorted by their ids.

    :param nodes: node dictionaries from the server
    :returns: the nodes
    """
    return sorted((Node(**node_dict) for node_dict in nodes), key=str)


@dataclass
class ClusterConfig:
    """Config of the cluster as the server reports it.

    :param mode: mode of the server, ``cluster`` for a cluster
    :param leader: the leader node, which takes the writes of the collections and the VArrays
    :param current: the nodes serving the data now
    :param target: the nodes the data is being moved to, only while the cluster is rebalancing
    :param cluster_status: status reported by the server
    """

    mode: str
    leader: Node
    current: list[Node]
    target: list[Node] | None = None  # Only appears when cluster in rebalancing mode
    cluster_status: str | None = ""
    __hash_ring: HashRing[Node] = field(init=False)
    __hash_ring_target: HashRing[Node] = field(init=False)

    @classmethod
    def from_dict(cls, cluster_config_dict: dict) -> "ClusterConfig":
        """Create cluster config from dict.

        :param cluster_config_dict: Cluster configuration that comes from server.
        :returns: ClusterConfig instance
        :raises DekerClusterError: if the leader is not among the raft nodes
        """
        leader_id = cluster_config_dict["leader_id"]

        # cluster always returns all current RAFT nodes, thus we don't need to check target config to know the leader
        # we won't need RAFT cluster config after getting the leader
        raft_nodes = _nodes_from_dicts(cluster_config_dict["raft"])
        leader = next((node for node in raft_nodes if node.id == leader_id), None)

        if not leader:
            raise DekerClusterError(None, "No leader has been found")

        current = _nodes_from_dicts(cluster_config_dict["current"])
        target = _nodes_from_dicts(cluster_config_dict["target"]) if "target" in cluster_config_dict else None

        return cls(
            mode=cluster_config_dict["mode"],
            leader=leader,
            current=current,
            target=target,
            cluster_status=cluster_config_dict.get("cluster_status"),
        )


def is_config_in_cluster_mode(config: dict | None, ctx: CTX) -> bool:
    """Check if mode from config is set to cluster.

    :param config: Config from response
    :param ctx: Context of app
    :returns: ``True`` if the server runs in cluster mode
    :raises DekerClusterError: if the uri lists several servers and the config is not a cluster one
    """
    if not ctx.uri.servers:
        return config is not None and config.get("mode") == CLUSTER_MODE

    if config is None or config.get("mode") != CLUSTER_MODE:
        raise DekerClusterError(
            config,
            "Server responded with wrong config. Key 'mode' either doesn't exist or its value differs from 'cluster'",
        )

    return True


def apply_config(config_dict: dict, ctx: CTX) -> None:
    """Apply config from server.

    The config, the leader as the base url of the client and the hash rings go into the context.

    :param config_dict: Config from server
    :param ctx: Application context
    :raises DekerClusterError: if the leader is not among the raft nodes
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
    """Ping the server (every server of a cluster uri), check it answers and apply the cluster config it returns.

    :param ctx: Application context
    :raises DekerServerError: if no server answered the ping with 200; the http client is closed then
    :raises DekerClusterError: if the config of a cluster uri is not a cluster config, or the config is not a json
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
        config = response.json()
        if is_config_in_cluster_mode(config, ctx):
            # Set hash of config
            httpx_client.headers.update({LAST_MODIFIED_HEADER: response.headers[LAST_MODIFIED_HEADER]})
            apply_config(config, ctx)
    except KeyError:
        raise DekerClusterError(response, f"No {LAST_MODIFIED_HEADER} header found in response.")
    except JSONDecodeError:
        if ctx.uri.servers:
            raise DekerClusterError(response, "Server responded with wrong config. Couldn't parse json")
