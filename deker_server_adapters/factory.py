from typing import TYPE_CHECKING, Any

from deker.ABC.base_factory import BaseAdaptersFactory
from deker.ctx import CTX
from deker.tools.decorators import check_ctx_state
from deker.uri import Uri
from httpx import Limits

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.cluster_config import request_and_apply_config
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import KEEPALIVE_EXPIRY, MAX_CONNECTIONS, MAX_KEEPALIVE_CONNECTIONS
from deker_server_adapters.httpx_client import HttpxClient
from deker_server_adapters.varray_adapter import ServerVarrayAdapter

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseCollectionAdapter, BaseStorageAdapter, BaseVArrayAdapter


class AdaptersFactory(BaseAdaptersFactory):
    """Factory that produces server adapters: the ``http://`` and ``https://`` schemes."""

    uri_schemes = ("http", "https")

    def __init__(self, ctx: "CTX", uri: "Uri") -> None:
        """AdaptersFactory constructor: builds the http client, pings the server and applies its cluster config.

        The http client takes the credentials of the uri and the ``httpx_conf`` keyword arguments of the client.

        :param ctx: context of the client; the factory works with a copy of it, so every factory has its own client
        :param uri: uri of the server
        :raises DekerServerError: if the server did not answer the ping
        """
        # Make base url, so all urls will be relative
        kwargs: dict[str, Any] = {
            "base_url": uri.raw_url,
            "verify": True,
            "http2": False,
            "timeout": None,
            "limits": Limits(
                max_connections=MAX_CONNECTIONS,
                max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
                keepalive_expiry=KEEPALIVE_EXPIRY,
            ),
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
        request_and_apply_config(copied_ctx)
        super().__init__(copied_ctx, uri)

    def close(self) -> None:
        """Shutdown executor and httpx client; the factory's own context copy is marked closed.

        A factory whose server did not answer the healthcheck is dropped before its base is initialised: it has
        the http client to close and nothing else.
        """
        if not hasattr(self, "ctx"):
            self.httpx_client.close()
            return
        self.ctx.is_closed = True
        self.httpx_client.close()
        super().close()

    @check_ctx_state
    def get_array_adapter(
        self,
        collection_path: Uri,
        storage_adapter: type["BaseStorageAdapter"],
        *args: Any,
        **kwargs: Any,
    ) -> "BaseArrayAdapter":
        r"""Make server array adapter.

        :param storage_adapter: Type of storage adapter
        :param collection_path: URI of collection
        :param \*args: Won't be passed further
        :param \**kwargs: Won't be passed further
        :returns: ServerArrayAdapter instance
        """
        return ServerArrayAdapter(
            collection_path=collection_path,
            ctx=self.ctx,
            executor=self.executor,
            storage_adapter=storage_adapter,
        )

    @check_ctx_state
    def get_varray_adapter(
        self,
        collection_path: Uri,
        storage_adapter: type["BaseStorageAdapter"],
        *args: Any,
        **kwargs: Any,
    ) -> "BaseVArrayAdapter":
        r"""Make server varray adapter.

        :param storage_adapter: Type of storage adapter
        :param collection_path: URI of collection
        :param \*args: Won't be passed further
        :param \**kwargs: Won't be passed further
        :returns: ServerVarrayAdapter instance
        """
        return ServerVarrayAdapter(
            collection_path=collection_path,
            ctx=self.ctx,
            executor=self.executor,
            storage_adapter=storage_adapter,
        )

    @check_ctx_state
    def get_collection_adapter(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "BaseCollectionAdapter":
        r"""Make server collection adapter.

        :param \*args: Won't be passed further
        :param \**kwargs: Won't be passed further
        :returns: ServerCollectionAdapter instance
        """
        return ServerCollectionAdapter(self.ctx)
