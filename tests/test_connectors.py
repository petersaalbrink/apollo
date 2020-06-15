import pytest
from common import EmailClient, ESClient, MongoDB, MySQLClient


def test_email():
    assert EmailClient().connection()


@pytest.mark.parametrize("es_index", [
    "dev_realestate.realestate",
    "production_api.user",
    "cdqc.person_data",
    "addressvalidation.netherlands",
])
def test_elastic(es_index: str):
    assert ESClient(es_index).ping()


@pytest.mark.parametrize("host", [
    # "address",
    "dev",
    # "prod",
])
def test_mongo(host: str):
    assert MongoDB(host=host, client=True).server_info()


def test_mysql():
    assert MySQLClient().connect(conn=True)
