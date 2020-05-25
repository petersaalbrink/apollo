# Environment variables
from common.env import getenv, commondir
from common.secrets import get_secret

# Connections
from sqlalchemy import create_engine, Table

# SQL Alchemy
from sqlalchemy import MetaData


class SQLClient:
    """
    Client for connecting to Matrixian's MySQL database.

    This client combines SQLAlchemy and MySQL Connector. For now, the class includes two additional functions:
    1. Counting the total amount of rows in a table
    2. Getting the types and length of the columns as specified in SQL

    Explanation additional arguments for configurations:
        - ssl_ arguments make sure we can connect to the Matrixian MySQL database using SSL
        - If you want warnings to be followed by exceptions, set raise_on_warnings=True
        - buffered=False makes sure we use less memory. This is important for large tables
        - use_pure=True makes sure we do not use C extension but use pure Python implementation

    Example:
        1. sql = SQLClient(database='avix', table='region_mapping')
           _dtypes = sql.get_dtypes()
           _count = sql.count()

    """
    def __init__(self, database: str = None, table: str = None, **kwargs):
        self.database = database
        self.table = table

        # Configurations
        envv = "MX_MYSQL_DEV_IP"
        usr, pwd = get_secret("MX_MYSQL_DEV")
        host = getenv(envv)

        # Create connection string
        connection_string = f"mysql+mysqlconnector://{usr}:{pwd}@{host}/{self.database}"

        # Set additional arguments
        self.__ssl = {
            "ssl_ca": f'{commondir / "server-ca.pem"}',
            "ssl_cert": f'{commondir / "client-cert.pem"}',
            "ssl_key": f'{commondir / "client-key.pem"}',
        }
        connect_args = {
            **self.__ssl,
            **dict(
                raise_on_warnings=kwargs.get("raise_on_warnings", False),
                buffered=kwargs.get("buffered", False),
                use_pure=kwargs.get("use_pure", True)
            )
        }

        # Create the engine
        self.engine = create_engine(connection_string, connect_args=connect_args)

    def count(self) -> int:
        """This function returns the total row count of an SQL table"""
        md = MetaData(bind=self.engine)
        _table = Table(self.table, meta=md, autoload=True, autoload_with=self.engine)
        return _table.count().scalar()

    def get_dtypes(self) -> dict:
        """This function returns the dtypes from SQL in dictionary form"""
        md = MetaData(bind=self.engine)
        _table = Table(self.table, meta=md, autoload=True, autoload_with=self.engine)
        dtypes = {col.name: col.type for col in _table.c}
        return dtypes


if __name__ == '__main__':
    sql = SQLClient(database="avix", table="region_mapping")
    sql.get_dtypes()

