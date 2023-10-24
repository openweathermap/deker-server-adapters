import re

from deker import VArray
from deker.ctx import CTX
from deker.uri import Uri
from pytest_httpx import HTTPXMock

from deker_server_adapters.consts import NON_LEADER_WRITE
from deker_server_adapters.utils import get_leader_and_nodes_mapping
from deker_server_adapters.varray_adapter import ServerVarrayAdapter


def test_new_cluster_config_is_applied_after_non_leader_error(
    ctx: CTX, httpx_mock: HTTPXMock, mocked_ping, server_varray_adapter: ServerVarrayAdapter, varray: VArray
):
    new_cluster_config = {
        **mocked_ping,
        "leader_id": "8381202B-8C95-487A-B9B5-0B527056804A",
        "current_nodes": [
            {
                "id": "8381202B-8C95-487A-B9B5-0B527056804A",
                "host": "newhost.owm.io",
                "port": 80,
                "protocol": "http",
            },
        ],
    }
    # Error
    httpx_mock.add_response(
        status_code=NON_LEADER_WRITE,
        method="PUT",
        json=new_cluster_config,
        url=re.compile(f"{ctx.extra['leader_node'].raw_url}"),
    )
    # Ok
    httpx_mock.add_response(
        status_code=200, method="PUT", json=new_cluster_config, url=re.compile(f"{ctx.extra['leader_node'].raw_url}")
    )
    server_varray_adapter.clear(varray, ...)
    leader, ids, mapping, nodes = get_leader_and_nodes_mapping(new_cluster_config)
    assert server_varray_adapter.ctx.extra["leader_node"] == Uri.create(leader)
    assert server_varray_adapter.ctx.extra["nodes"] == nodes
    assert server_varray_adapter.ctx.extra["hash_ring"].nodes == ids
    assert server_varray_adapter.ctx.extra["nodes_mapping"] == mapping
