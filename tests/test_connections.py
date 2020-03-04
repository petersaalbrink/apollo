from common import EmailClient, ESClient, MongoDB, MySQLClient


def test_email():
    assert EmailClient().connection()


def test_elastic():
    assert ESClient("dev_realestate.realestate").ping()
    assert ESClient("production_cdqc.person_data").ping()
    assert ESClient("addressvalidation.netherlands").ping()


def test_mongo():
    assert MongoDB(host="address", client=True).server_info()
    assert MongoDB(host="dev", client=True).server_info()
    assert MongoDB(host="prod", client=True).server_info()


def test_mysql():
    assert MySQLClient().connect(conn=True)
