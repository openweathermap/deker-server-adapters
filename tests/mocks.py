from deker.ctx import CTX

from deker_server_adapters.factory import AdaptersFactory


class MockedAdaptersFactory(AdaptersFactory):
    def do_healthcheck(self, ctx: CTX) -> None:
        pass
