from collections import defaultdict
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from deker.ABC.base_factory import BaseAdaptersFactory
from deker.ctx import CTX
from deker.uri import Uri
from httpx import Response

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import STATUS_OK
from deker_server_adapters.errors import DekerClusterError, DekerServerError
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils import make_request
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseCollectionAdapter, BaseStorageAdapter, BaseVArrayAdapter


class AdaptersFactory(BaseAdaptersFactory):
    """Factory that produces server adapters."""

    uri_schemes = ("http", "https")

    def __init__(self, ctx: "CTX", uri: "Uri") -> None:
        # Make base url, so all urls will be relative
        kwargs = {
            "base_url": uri.raw_url,
            "verify": True,
            "http2": False,
            "timeout": None,
        }

        # If Uri contains auth params, remember them
        if uri.username:
            kwargs.update({"auth": (uri.username, uri.password)})

        # update values from context kwargs
        kwargs.update(ctx.extra.get("httpx_conf", {}))

        # We have to copy ctx to create new instance of extra
        # so client would be different for every factory
        copied_ctx = CTX(
            ctx.uri,
            config=ctx.config,
            storage_adapter=ctx.storage_adapter,
            executor=ctx.executor,
            is_closed=ctx.is_closed,
        )

        # Cluster config
        if hasattr(uri, "servers") and uri.servers:
            self.get_cluster_config_and_configure_context(copied_ctx, default_httpx_client_kwargs=kwargs)

        # Single server
        else:
            self.httpx_client = HttpxClient(**kwargs)
            ctx.extra["httpx_client"] = self.httpx_client
            self.do_healthcheck(client=self.httpx_client, ctx=copied_ctx, in_cluster=False)

        super().__init__(copied_ctx, uri)

    def close(self) -> None:
        """Shutdown executor and httpx client."""
        self.httpx_client.close()
        super().close()

    def get_array_adapter(
        self,
        collection_path: Uri,
        storage_adapter: Type["BaseStorageAdapter"],
        *args: Any,
        **kwargs: Any,
    ) -> "BaseArrayAdapter":
        """Make server array adapter.

        :param storage_adapter: Type of storage adapter
        :param collection_path: URI of collection
        :param args: Won't be passed further
        :param kwargs: Won't be passed further
        """
        return ServerArrayAdapter(
            collection_path=collection_path,
            ctx=self.ctx,
            executor=self.executor,
            storage_adapter=storage_adapter,
        )

    def get_varray_adapter(
        self,
        collection_path: Uri,
        storage_adapter: Type["BaseStorageAdapter"],
        *args: Any,
        **kwargs: Any,
    ) -> "BaseVArrayAdapter":
        """Make server varray adapter.

        :param storage_adapter: Type of storage adapter
        :param collection_path: URI of collection
        :param args: Won't be passed further
        :param kwargs: Won't be passed further
        """
        return ServerVarrayAdapter(
            collection_path=collection_path,
            ctx=self.ctx,
            executor=self.executor,
            storage_adapter=storage_adapter,
        )

    def get_collection_adapter(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "BaseCollectionAdapter":
        """Make server collection adapter.

        :param args: Won't be passed further
        :param kwargs: Won't be passed further
        """
        return ServerCollectionAdapter(self.ctx)

    def do_healthcheck(self, client: HttpxClient, ctx: CTX, in_cluster: bool = False) -> Optional[Dict]:
        """Check if server is alive.

        Fetches config as well.
        :param ctx: App context
        :param client: Httpx Client
        :param in_cluster: If we are in cluster
        """

        def check_response(response: Optional[Response], client: HttpxClient) -> None:
            if response is None or response.status_code != STATUS_OK:
                client.close()
                raise DekerServerError(
                    response,
                    "Healthcheck failed. Deker client will be closed.",
                )

        url = "v1/ping"

        # If we do healthcheck in cluster
        nodes = [*ctx.uri.servers] if in_cluster else [ctx.uri.raw_url]
        response = make_request(url=url, nodes=nodes, client=client)
        check_response(response=response, client=client)

        if in_cluster:
            try:
                config = response.json()  # type: ignore[union-attr]
                return config
            except JSONDecodeError:
                raise DekerClusterError(response, "Server responded with empty config")

    def __set_cluster_config(self, cluster_config: Dict, ctx: CTX) -> None:
        """Set cluster config in the CTX.

        :param cluster_config: Custer config json from server
        :param ctx: App cotext (Deker CTX)
        """
        # IDs used in hash ring
        ids = []
        # Mapping from ID to host
        id_to_host_mapping = defaultdict(list)
        leader_node = None
        nodes = []

        # Fill Ids and Mappings
        for node in cluster_config["current_nodes"]:
            url = f"{node['protocol']}://{node['host']}:{node['port']}"
            nodes.append(url)
            ids.append(node["id"])
            id_to_host_mapping[node["id"]].append(url)
            if node["id"] == cluster_config["leader_id"]:
                leader_node = url

        if leader_node is None:
            raise DekerServerError(None, f"Leader node cannot be setted {cluster_config=}")

        # Set variables in context
        ctx.extra["leader_node"] = Uri.create(leader_node)
        ctx.extra["nodes_mapping"] = id_to_host_mapping
        ctx.extra["hash_ring"] = HashRing(ids)
        ctx.extra["nodes"] = nodes

    def get_cluster_config_and_configure_context(self, ctx: CTX, default_httpx_client_kwargs: Dict) -> None:
        """Get info from node and set config.

        :param ctx: CTX where client and config will be injected
        :param default_httpx_client_kwargs: Default variables for Httpx client
        """
        # Instantiate an httpx client
        client = HttpxClient(**default_httpx_client_kwargs)

        # Get Cluster config
        cluster_config = self.do_healthcheck(client, ctx, in_cluster=True)

        # Set cluster config
        self.__set_cluster_config(cluster_config, ctx)  # type: ignore[arg-type]

        # Set Httpx client based on cluster config
        self.httpx_client = HttpxClient(**{**default_httpx_client_kwargs, "base_url": ctx.extra["leader_node"].raw_url})
        ctx.extra["httpx_client"] = self.httpx_client
