from __future__ import annotations

import pytest

from common.connectors.mx_elastic import ESClient
from common.connectors.mx_email import EmailClient
from common.connectors.mx_mongo import MongoDB, MxClient
from common.connectors.mx_mysql import MySQLClient


def test_email() -> None:
    assert EmailClient().connection()


@pytest.mark.parametrize(
    "es_index",
    [
        "dev_realestate.realestate",
        "production_api.user",
        "cdqc.person_data",
        "addressvalidation.netherlands",
    ],
)
def test_elastic(es_index: str) -> None:
    assert ESClient(es_index).ping()


@pytest.mark.parametrize(
    "host",
    [
        # "address",
        "dev",
        # "prod",
    ],
)
def test_mongo(host: str) -> None:
    client = MongoDB(host=host, client=True)
    assert isinstance(client, MxClient)
    assert client.server_info()


def test_mysql() -> None:
    assert MySQLClient().connect(conn=True)
