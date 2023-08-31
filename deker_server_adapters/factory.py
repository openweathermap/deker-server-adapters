from typing import TYPE_CHECKING, Any, Type

from deker.ABC.base_factory import BaseAdaptersFactory
from deker.ctx import CTX
from deker.uri import Uri

from deker_server_adapters.array_adapter import ServerArrayAdapter
from deker_server_adapters.collection_adapter import ServerCollectionAdapter
from deker_server_adapters.consts import STATUS_OK
from deker_server_adapters.errors import DekerServerError
from deker_server_adapters.httpx_client import HttpxClient
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

        # Instantiate an httpx client
        self.httpx_client = HttpxClient(**kwargs)

        # We have to copy ctx to create new instance of extra
        # so client would be different for every factory
        copied_ctx = CTX(
            ctx.uri,
            config=ctx.config,
            storage_adapter=ctx.storage_adapter,
            executor=ctx.executor,
            is_closed=ctx.is_closed,
        )

        copied_ctx.extra["httpx_client"] = self.httpx_client
        self.do_healthcheck()
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

    def do_healthcheck(self) -> None:
        """Check if server is alive."""
        try:
            response = self.httpx_client.get("/v1/ping")
        except Exception:
            self.httpx_client.close()
            raise DekerServerError(
                None,
                "Healthcheck failed. Server is unavailable. Deker client will be closed.",
            )

        if response.status_code != STATUS_OK:
            self.httpx_client.close()
            raise DekerServerError(
                response,
                "Healthcheck failed. Deker client will be closed.",
            )
