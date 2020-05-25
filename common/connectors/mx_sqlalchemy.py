# Environment variables
from common.env import getenv, commondir
from common.secrets import get_secret

# Connections
from mysql.connector import ClientFlag
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# SQL Alchemy
from sqlalchemy import MetaData


class SQLClient:
    def __init__(self, database: str = None, table: str = None, **kwargs):
        self.database = database
        self.table = table
        envv = "MX_MYSQL_DEV_IP"
        usr, pwd = get_secret("MX_MYSQL_DEV")
        host = getenv(envv)
        self.__ssl = {
            "ssl_ca": f'{commondir / "server-ca.pem"}',
            "ssl_cert": f'{commondir / "client-cert.pem"}',
            "ssl_key": f'{commondir / "client-key.pem"}',
        }
        connection_string = f"mysql+mysqlconnector://{usr}:{pwd}@{host}/{self.database}"

        connect_args = {
            **self.__ssl,
            **dict(
                buffered=kwargs.get("buffered", False),
                raise_on_warnings=kwargs.get("raise_on_warnings", False),
                use_pure=kwargs.get("use_pure", True)
            )
        }
        self.engine = create_engine(connection_string, connect_args=connect_args)
        self.session = sessionmaker(bind=self.engine)()

    def count(self) -> int:
        q = f"SELECT COUNT(*) FROM {self.database}.{self.table}"
        return self.session.execute(q).scalar()

    def get_dtypes(self) -> dict:
        md = MetaData()
        md.reflect(bind=self.engine)
        dtypes = {col.name: col.type for col in md.tables[self.table].c}
        return dtypes


if __name__ == '__main__':
    sql = SQLClient(database="avix", table="region_mapping")

