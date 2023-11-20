from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from deker.ABC.base_factory import BaseAdaptersFactory
from deker.ctx import CTX
from deker.uri import Uri
from httpx import Response

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import STATUS_OK
from deker_server_adapters.errors import DekerClusterError, DekerServerError, HealthcheckError
from deker_server_adapters.hash_ring import HashRing
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.utils import get_api_version, get_leader_and_nodes_mapping, make_request
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

        self.httpx_client = HttpxClient(**kwargs)
        # We have to keep reference to ctx, to be able to set new configuration
        self.httpx_client.ctx = copied_ctx
        copied_ctx.extra["httpx_client"] = self.httpx_client

        # Cluster config
        if hasattr(uri, "servers") and uri.servers:
            self.get_cluster_config_and_configure_context(copied_ctx)

        # Single server
        else:
            self.do_healthcheck(ctx=copied_ctx, in_cluster=False)

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

    def do_healthcheck(self, ctx: CTX, in_cluster: bool = False) -> Optional[Dict]:
        """Check if server is alive.

        Fetches config as well.
        :param ctx: App context
        :param in_cluster: If we are in cluster
        """

        def check_response(response: Optional[Response], client: HttpxClient) -> None:
            if response is None or response.status_code != STATUS_OK:
                client.close()
                raise DekerServerError(
                    response,
                    "Healthcheck failed. Deker client will be closed.",
                )

        url = f"{get_api_version()}/ping"

        # If we do healthcheck in cluster
        nodes = [*ctx.uri.servers] if in_cluster else [ctx.uri.raw_url]
        response = make_request(url=url, nodes=nodes, client=self.httpx_client)
        check_response(response=response, client=self.httpx_client)

        if in_cluster:
            try:
                config = response.json()  # type: ignore[union-attr]
                if config.get("mode") != "cluster":
                    raise HealthcheckError
                return config
            except JSONDecodeError:
                raise DekerClusterError(response, "Server responded with wrong config. Couldn't parse json")
            except HealthcheckError:
                raise DekerClusterError(
                    response,
                    "Server responded with wrong config."
                    " Key 'mode' either doesn't exist or its value differs from 'cluster'",
                )

    def __set_cluster_config(self, cluster_config: Dict, ctx: CTX) -> None:
        """Set cluster config in the CTX.

        :param cluster_config: Custer config json from server
        :param ctx: App cotext (Deker CTX)
        """
        leader_node, ids, id_to_host_mapping, nodes = get_leader_and_nodes_mapping(cluster_config)

        if leader_node is None:
            raise DekerServerError(None, f"Leader node cannot be setted {cluster_config=}")

        # Set variables in context
        ctx.extra["leader_node"] = Uri.create(leader_node)
        ctx.extra["nodes_mapping"] = id_to_host_mapping
        ctx.extra["hash_ring"] = HashRing(ids)
        ctx.extra["nodes"] = nodes

    def get_cluster_config_and_configure_context(self, ctx: CTX) -> None:
        """Get info from node and set config.

        :param ctx: CTX where client and config will be injected
        """
        # Get Cluster config
        cluster_config = self.do_healthcheck(ctx, in_cluster=True)

        # Set cluster config
        self.__set_cluster_config(cluster_config, ctx)  # type: ignore[arg-type]

        # Set Httpx client based on cluster config
        self.httpx_client.base_url = ctx.extra["leader_node"].raw_url
        self.httpx_client.cluster_mode = True
