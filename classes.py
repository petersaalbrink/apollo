import pathlib
import smtplib
import mysql.connector
from warnings import warn
from email import encoders
from socket import timeout
from base64 import b64decode
import mysql.connector.cursor
from pymongo import MongoClient
from urllib.parse import quote_plus
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pymongo.database import Database
from pymongo.database import Collection
from mysql.connector import DatabaseError
from typing import Any, List, Union
from email.mime.multipart import MIMEMultipart
from elasticsearch import Elasticsearch, ElasticsearchException


class ElasticSearch(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self, dev: bool = True, es_index: str = None):
        """Client for ElasticSearch"""
        from common.secrets import es
        if dev:
            self._host = '136.144.173.2'
            if es_index is None:
                es_index = 'dev_peter.person_data_20190606'
        else:
            self._host = '37.97.169.90'
            if es_index is None:
                es_index = 'production_realestate.realestate'
        self.es_index = es_index
        self._port = 9201
        hosts = [{'host': self._host, 'port': self._port}]
        config = {'http_auth': (es[0], b64decode(es[1]).decode()), "retry_on_timeout": True}
        super().__init__(hosts, **config)

    def __repr__(self):
        return f"{self.__class__.__name__}(host='{self._host}', port='{self._port}', index='{self.es_index}')"

    def __str__(self):
        return f"http://{self._host}:{self._port}/{self.es_index}/_stats"

    def find(self, query: Union[dict, List[dict]] = None,
             hits_only: bool = True, source_only: bool = False, first_only: bool = False,
             *args, **kwargs) -> Union[List[dict], List[List[dict]], dict]:
        """Perform an ElasticSearch query, and return the hits.

        Uses .search() method on class attribute .es_index with size=10_000. Will try again on errors.
        Accepts a single query (dict) or multiple (List[dict]).
        Returns:
            query: dict and
                not hits_only -> dict
                hits_only -> List[dict]
                source_only -> List[dict]
                first_only -> dict
            query: List[dict] and
                not hits_only -> List[dict]
                hits_only -> List[List[dict]]
                source_only -> List[List[dict]]
                first_only -> List[dict]
        """
        if isinstance(query, dict):
            query = [query]
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            warn("Returning hits only if any([source_only, first_only])")
            hits_only = True
        results = []
        for q in query:
            while True:
                try:
                    result = self.search(
                        index=self.es_index, size=10_000, body=q, *args, **kwargs)
                    break
                except (ElasticsearchException, OSError, ConnectionError, timeout):
                    warn("Retrying", ConnectionWarning)
            if hits_only:
                result = result["hits"]["hits"]
            if source_only:
                result = [doc["_source"] for doc in result]
            if first_only:
                try:
                    result = [result[0]]
                except IndexError:
                    result = []
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def query(self, field: str = None, value: Union[str, int] = None, **kwargs) -> List[dict]:
        """Perform a simple ElasticSearch query, and return the hits.

        Uses .find() method instead of regular .search()
        Substitute period . for nested fields with underscore _

        Examples:
            from common.classes import ElasticSearch
            es = ElasticSearch()
            results = es.simple(field="lastname", query="Saalbrink")

            # Add multiple search fields:
            results = es.simple(lastname="Saalbrink", address_postalCode="1014AK")
            # This results in the query:
            {"query": {"bool": {"must": [{"match": {"lastname": "Saalbrink"}},
                                         {"match": {"address.postalCode": "1014AK"}}]}}}
        """
        if kwargs:
            sort = kwargs.pop("sort", None)
            args = {}
            for k in kwargs:
                if "_" in k and not k.startswith("_"):
                    args[k.replace("_", ".")] = kwargs[k]
            if len(args) == 1:
                q = {"query": {"bool": {"must": [{"match": args}]}}}
                return self.find(q)
            else:
                q = {"query": {"bool": {"must": [{"match": {k: v}} for k, v in args.items()]}}}
                return self.find(q, sort=sort)
        return self.find({"query": {"bool": {"must": [{"match": {field: value}}]}}})


class EmailClient:
    """Client for sending plain text emails and attachments."""
    from common.secrets import mail_pass

    def __init__(self,
                 smtp_server='smtp.gmail.com:587',
                 login='dev@matrixiangroup.com',
                 password=b64decode(mail_pass).decode()):
        """Client for sending plain text emails and attachments."""
        self._smtp_server = smtp_server
        self._login = login
        self._password = password

    def send_email(self,
                   to_address: Union[str, list] = 'psaalbrink@matrixiangroup.com',
                   subject: str = 'no subject',
                   message: str = '',
                   from_address: str = 'dev@matrixiangroup.com',
                   attachment_path: Union[str, pathlib.PurePath] = None,
                   ):
        """Send an email to an email address (str) or a list of addresses.
        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory."""

        msg = MIMEMultipart()
        msg['From'] = from_address
        if isinstance(to_address, str):
            msg['To'] = to_address
        elif isinstance(to_address, list):
            msg['To'] = ','.join(to_address)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        if attachment_path is not None:
            attachment = open(attachment_path, 'rb')
            p = MIMEBase('application', 'octet-stream')
            p.set_payload(attachment.read())
            encoders.encode_base64(p)
            filename = attachment_path.name if isinstance(attachment_path, pathlib.PurePath) else attachment_path
            p.add_header('Content-Disposition', f"attachment; filename={filename}")
            msg.attach(p)

        server = smtplib.SMTP(self._smtp_server)
        server.starttls()
        server.login(self._login, self._password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()


class MongoDB(MongoClient):
    """Client for MongoDB

    Usage:
        client = MongoDB()
        db = MongoDB('dev_peter')
        coll = MongoDB('dev_peter', 'person_data_20190606')
        coll = MongoDB()['dev_peter']['person_data_20190606']
    """

    def __new__(cls, database: str = None, collection: str = None) -> Union[MongoClient, Database, Collection]:
        """Client for MongoDB

        Usage:
            client = MongoDB()
            db = MongoDB('dev_peter')
            coll = MongoDB('dev_peter', 'person_data_20190606')
            coll = MongoDB()['dev_peter']['person_data_20190606']
        """
        from common.secrets import mongo
        user = mongo[0]
        password = b64decode(mongo[1]).decode()
        host = '136.144.173.2'
        mongo_client = MongoClient(host=f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}")
        if database is not None:
            if "." in database:
                collection = database.split(".")[1]
                database = database.split(".")[0]
            if collection is not None:
                return mongo_client.__getattr__(database).__getattr__(collection)
            return mongo_client.__getattr__(database)
        return mongo_client


class Query(str):
    pass


class ConnectionWarning(Warning):
    pass


class MySQLClient:
    """Client for MySQL

    Example:
        sql = MySQLClient()
        data = sql.query(table="pc_data_final", postcode="1014AK")
    """
    def __init__(self, database: str = None, table: str = None):
        """Client for MySQL

        Example:
            sql = MySQLClient()
            data = sql.query(table="pc_data_final", postcode="1014AK")
        """
        import common
        from common.secrets import sql
        if database is not None:
            if "." in database:
                table = database.split(".")[1]
                database = database.split(".")[0]
        else:
            database = "mx_traineeship_peter"
        if table is not None:
            if "." in table:
                table = table.split(".")[1]
                database = table.split(".")[0]
        self.database = database
        self.table_name = table
        path = pathlib.Path(common.__file__).parent / "certificates"
        self.__config = {
            'user': sql[0],
            'password': b64decode(sql[1]).decode(),
            'host': '104.199.69.152',
            'database': self.database,
            'raise_on_warnings': True,
            'client_flags': [mysql.connector.ClientFlag.SSL],
            'ssl_ca': str(path / "server-ca.pem"),
            'ssl_cert': str(path / "client-cert.pem"),
            'ssl_key': str(path / "client-key.pem")}
        self.cnx = self.cursor = self._iter = None

    def connect(self, conn: bool = False) -> mysql.connector.cursor.MySQLCursorBuffered:
        """Connect to SQL server"""
        self.cnx = mysql.connector.connect(**self.__config)
        self.cnx.autocommit = True
        self.cursor = self.cnx.cursor(buffered=True)
        if conn:
            return self.cnx
        else:
            return self.cursor

    def disconnect(self):
        """Disconnect from SQL server"""
        self.cursor.close()
        self.cnx.close()

    def execute(self, query: Union[str, Query], *args, **kwargs):
        self.cursor.execute(query, *args, **kwargs)

    def executemany(self, query: Union[str, Query], data: list, *args, **kwargs):
        self.connect()
        self.cursor.executemany(query, data, *args, **kwargs)
        self.disconnect()

    def fetchall(self) -> List[tuple]:
        return self.cursor.fetchall()

    def column(self, query: Union[str, Query] = None, *args, **kwargs) -> List[str]:
        """Fetch one column from MySQL"""
        self.connect()
        if query is None:
            query = f"SHOW COLUMNS FROM {self.table_name} FROM {self.database}"
        self.execute(query, *args, **kwargs)
        column = [value[0] for value in self.fetchall()]
        self.disconnect()
        return column

    def table(self, query: Union[str, Query] = None, *args, **kwargs) -> List[list]:
        """Fetch a table from MySQL"""
        self.connect()
        if query is None:
            query = self.build()
        self.execute(query, *args, **kwargs)
        table = [list(row) for row in self.fetchall()]
        self.disconnect()
        return table

    def row(self, query: Union[str, Query] = None, *args, **kwargs) -> List[Any]:
        """Fetch one row from MySQL"""
        self.connect()
        if query is None:
            query = self.build(limit=1, offset=self._iter)
            if self._iter is None:
                self._iter = 1
            else:
                self._iter += 1
        self.execute(query, *args, **kwargs)
        row = list(self.fetchall()[0])
        self.disconnect()
        return row

    def chunk(self, query: Union[str, Query] = None, size: int = None, *args, **kwargs) -> List[list]:
        """Returns a generator for downloading a table in chunks

        Example:
            from common.classes import MySQLClient
            sql = MySQLClient()
            query = sql.build(
                table="mx_traineeship_peter.client_data",
                Province="Noord-Holland",
                select_fields=['id', 'City']
            )
            for i, row in enumerate(sql.chunk(query=query, size=10)):
                print(row)
        """
        if query is None:
            query = self.build()
        if size <= 0:
            raise ValueError("Chunk size must be > 0")
        elif size == 1:
            for i in range(0, 1_000_000_000, size):
                q = query + f" LIMIT {i}, {size}"
                try:
                    row = self.row(q, *args, **kwargs)
                except IndexError:
                    break
                yield row
        else:
            for i in range(0, 1_000_000_000, size):
                q = query + f" LIMIT {i}, {size}"
                while True:
                    try:
                        table = self.table(q, *args, **kwargs)
                        break
                    except DatabaseError:
                        warn("Retrying", ConnectionWarning)
                if len(table) == 0:
                    break
                yield table

    def create_sql_table(self, query: Union[str, Query] = None):
        self.connect()
        if query is None:
            query = self.build()
        try:
            self.execute(query)
        except DatabaseError:
            pass
        self.disconnect()

    def build(self, table: str = None, select_fields: Union[list, str] = None,
              field: str = None, value: Union[str, int] = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              **kwargs) -> Query:
        """Build a MySQL query"""
        if table is None:
            table = f"{self.database}.{self.table_name}"
        elif "." not in table:
            table = f"{self.database}.{table}"
        if select_fields is None:
            query = f"SELECT * FROM {table} "
        elif isinstance(select_fields, list):
            query = f"SELECT `{'`, `'.join(select_fields)}` FROM {table} "
        else:
            query = f"SELECT {select_fields} FROM {table} "
        if not all([field is None, value is None]):
            if isinstance(value, str):
                query += f"WHERE {field} = '{value}' "
            elif isinstance(value, int):
                query += f"WHERE {field} = {value} "
        if kwargs:
            keys = "AND ".join([f"{k} = {v} " if isinstance(v, int) else (f"{k} IS NULL " if v == "NULL" else (
                f"{k} IS NOT NULL " if v == "!NULL" else (
                    f"{k} != '{v[1:]}' " if v.startswith('!') else f"{k} = '{v}' "))) for k, v in kwargs.items()])
            if "WHERE" in query:
                query += f"AND {keys}"
            else:
                query += f"WHERE {keys}"
        if limit is not None:
            if isinstance(limit, (int, str)):
                query += f"LIMIT {limit} "
            elif isinstance(limit, (list, tuple)):
                query += f"LIMIT {limit[0]}, {limit[1]} "
        if offset is not None:
            query += f"OFFSET {offset} "
        return Query(query)

    def query(self, table: str, field: str = None, value: Union[str, int] = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              select_fields: Union[list, str] = None, **kwargs) -> List[list]:
        """Build and perform a MySQL query, and returns a data array.

        Examples:
            sql = MySQLClient()
            table = "mx_traineeship_peter.company_data"

            # Simple WHERE query:
            sql.query(table=table, field="postcode", value="1014AK")  # is the same as:
            sql.query(table=table, postcode="1014AK")

            # Filter on more fields:
            sql.query(table=table, postcode="1014AK", huisnummer=104, plaatsnaam="Amsterdam")

            # NOT filter values using '!':
            sql.query(table=table, postcode="1014AK", huisnummer=104, KvKnummer="!NULL")
            sql.query(table=table, postcode="1014AK", huisnummer="!102")

            # Using LIMIT and OFFSET:
            sql.query(table=table, postcode="1014AK", limit=10, offset=1)  # is the same as:
            sql.query(table=table, postcode="1014AK", limit=(1, 10))

            # Optionally SELECT specific fields:
            sql.query(table=table, postcode="1014AK", select_fields='KvKnummer')
            sql.query(table=table, postcode="1014AK", select_fields=['KvKnummer', 'plaatsnaam'])
            """
        if isinstance(table, Query) or table.startswith("SELECT"):
            query = table
        elif isinstance(table, str):
            query = self.build(table, field, value, limit, offset, select_fields, **kwargs)
        else:
            raise ValueError(f"Query error: '{table}'")
        self.connect()
        self.execute(query)
        if isinstance(select_fields, str) or (isinstance(select_fields, list) and len(select_fields) is 0):
            result = [value[0] for value in self.fetchall()]
        elif limit == 1:
            result = list(self.fetchall()[0])
        else:
            result = [list(row) for row in self.fetchall()]
        self.disconnect()
        return result

# TODO: sql class method for creating table
# TODO: sql class method for updating table
