import re

from typing import Dict, List

import pytest

from deker.uri import Uri
from pytest_httpx import HTTPXMock

from deker_server_adapters.models import Status


@pytest.fixture(scope="session")
def nodes() -> List[Dict]:
    return [
        {
            "id": "8381202B-8C95-487A-B9B5-0B527056804E",
            "host": "localhost",
            "port": 8000,
            "protocol": "http",
        },
        {
            "id": "8381202B-8C95-487A-B9B5-0B5270568040",
            "host": "localhost",
            "port": 8012,
            "protocol": "http",
        },
    ]


@pytest.fixture()
def nodes_urls(nodes) -> List[str]:
    urls = []
    for node in nodes:
        url = f"{node['protocol']}://{node['host']}:{node['port']}"
        urls.append(url)
    return urls


@pytest.fixture()
def base_cluster_uri(nodes_urls):
    uri = Uri.create("http://localhost:8000,localhost:8012")
    kwargs = {key: getattr(uri, key) for key in uri._fields}
    kwargs["servers"] = nodes_urls
    return uri


@pytest.fixture()
def mocked_ping(nodes: List[Dict]) -> Dict:
    return {
        "mode": "cluster",
        "this_id": "8381202B-8C95-487A-B9B5-0B527056804E",
        "leader_id": "8381202B-8C95-487A-B9B5-0B527056804E",
        "current": nodes,
    }


@pytest.fixture()
def mock_healthcheck(httpx_mock: HTTPXMock, mocked_ping):
    httpx_mock.add_response(method="GET", url=re.compile(r".*\/v1\/ping.*"), json=mocked_ping)


@pytest.fixture()
def mocked_filestatus_check_unmoved(httpx_mock: HTTPXMock):
    httpx_mock.add_response(method="GET", url=re.compile(r".*\/status.*"), text=Status.UNMOVED.value)
