from deker.uri import Uri

from deker_server_adapters.factory import AdaptersFactory


def test_auth_factory(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")

    factory = AdaptersFactory(ctx, uri)
    assert factory.httpx_client.auth


def test_auth_factory_close(ctx, mock_ping):
    uri = Uri.create("http://test:test@localhost/")
    factory = AdaptersFactory(ctx, uri)
    factory.close()
    assert factory.httpx_client.is_closed
