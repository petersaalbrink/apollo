import pathlib
import smtplib
import mysql.connector
from os import PathLike
from email import encoders
from base64 import b64decode
import mysql.connector.cursor
from pymongo import MongoClient
from typing import Any, List, Union
from urllib.parse import quote_plus
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pymongo.database import Database
from pymongo.database import Collection
from elasticsearch import Elasticsearch
from mysql.connector import DatabaseError
from email.mime.multipart import MIMEMultipart


class ElasticSearch(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self, dev: bool = True, es_index: str = None):
        from common.secrets import es
        if dev:
            if es_index is None:
                es_index = 'dev_peter.person_data_20190606'
            host = '136.144.173.2'
        else:
            if es_index is None:
                es_index = 'production_realestate.realestate'
            host = '37.97.169.90'
        self.es_index = es_index
        super(ElasticSearch, self).__init__([{
            'host': host, 'port': 9201,
            'http_auth': (es[0], b64decode(es[1]).decode())}])

    def find(self, query: Union[dict, List[dict]] = None, *args, **kwargs):
        return self.search(index=self.es_index, size=10_000, body=query, *args, **kwargs)

    def simple(self, field: str = None, query: Union[str, int] = None, **kwargs):
        if kwargs:
            if len(kwargs) == 1:
                return self.find({"query": {"bool": {"must": [{"match": kwargs}]}}})
            else:
                keys = [{"match": {k: v}} for k, v in kwargs.items()]
                return self.find({"query": {"bool": {"must": keys}}})
        return self.find({"query": {"bool": {"must": [{"match": {field: query}}]}}})


class EmailClient:
    """Client for sending plain text emails and attachments."""
    from common.secrets import mail_pass

    def __init__(self,
                 smtp_server='smtp.gmail.com:587',
                 login='dev@matrixiangroup.com',
                 password=b64decode(mail_pass).decode()):
        """Client for sending plain text emails and attachments."""
        self.smtp_server = smtp_server
        self.login = login
        self.password = password

    def send_email(self,
                   to_address: Union[str, list] = 'psaalbrink@matrixiangroup.com',
                   subject: str = 'no subject',
                   message: str = '',
                   from_address: str = 'dev@matrixiangroup.com',
                   attachment_path: Union[str, pathlib.Path, PathLike] = None,
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
            if not isinstance(attachment_path, str):
                p.add_header('Content-Disposition', f"attachment; filename={attachment_path.name}")
            else:
                p.add_header('Content-Disposition', f"attachment; filename={attachment_path}")
            msg.attach(p)

        server = smtplib.SMTP(self.smtp_server)
        server.starttls()
        server.login(self.login, self.password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()


class MongoDB:
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
        mongo_client = MongoClient(f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}")
        if database is not None:
            if collection is not None:
                return mongo_client.__getattr__(database).__getattr__(collection)
            return mongo_client.__getattr__(database)
        return mongo_client


class Query(str):
    pass


class MySQLClient:
    """Client for MySQL

    Example:
        sql = MySQLClient()
        data = sql.query(table="pc_data_final", postcode="1014AK")
    """
    from common.secrets import sql
    config = {
        'user': sql[0],
        'password': b64decode(sql[1]).decode(),
        'host': '104.199.69.152',
        'database': 'mx_traineeship_peter',
        'raise_on_warnings': True,
        'client_flags': [mysql.connector.ClientFlag.SSL],
        'ssl_ca': str(pathlib.Path.home() / 'Python/common/certificates/server-ca.pem'),
        'ssl_cert': str(pathlib.Path.home() / 'Python/common/certificates/client-cert.pem'),
        'ssl_key': str(pathlib.Path.home() / 'Python/common/certificates/client-key.pem')}
    cnx = cursor = None

    def connect(self, conn: bool = False) -> mysql.connector.cursor.MySQLCursorBuffered:
        """Connect to SQL server"""
        self.cnx = mysql.connector.connect(**self.config)
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

    def execute(self, query: str, *args, **kwargs):
        self.cursor.execute(query, *args, **kwargs)

    def executemany(self, query: str, data: list, *args, **kwargs):
        self.connect()
        self.cursor.executemany(query, data, *args, **kwargs)
        self.disconnect()

    def fetchall(self) -> List[tuple]:
        return self.cursor.fetchall()

    def column(self, query: str = None, *args, **kwargs) -> List[str]:
        """Fetch one column from MySQL"""
        self.connect()
        if query is not None:
            self.cursor.execute(query, *args, **kwargs)
        column = [value[0] for value in self.fetchall()]
        self.disconnect()
        return column

    def table(self, query: str = None, *args, **kwargs) -> List[list]:
        """Fetch a table from MySQL"""
        self.connect()
        if query is not None:
            self.cursor.execute(query, *args, **kwargs)
        table = [list(row) for row in self.fetchall()]
        self.disconnect()
        return table

    def row(self, query: str = None, *args, **kwargs) -> List[Any]:
        """Fetch one row from MySQL"""
        self.connect()
        if query is not None:
            self.cursor.execute(query, *args, **kwargs)
        row = list(self.fetchall()[0])
        self.disconnect()
        return row

    def chunk(self, query: str = None, size: int = None, *args, **kwargs) -> List[list]:
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
        if size == 0:
            raise ValueError("Chunk size must be > 0")
        elif size == 1:
            for i in range(0, 100_000_000, size):
                q = query + f" LIMIT {i}, {size}"
                try:
                    row = self.row(q, *args, **kwargs)
                except IndexError:
                    break
                yield row
        else:
            for i in range(0, 100_000_000, size):
                q = query + f" LIMIT {i}, {size}"
                table = self.table(q, *args, **kwargs)
                if len(table) == 0:
                    break
                yield table

    def create_sql_table(self, query: str):
        self.connect()
        try:
            self.execute(query)
        except DatabaseError:
            pass
        self.disconnect()

    @staticmethod
    def build(table: str, field: str = None, value: Union[str, int] = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              select_fields: Union[list, str] = None, **kwargs) -> Query:
        """Build a MySQL query"""
        if select_fields is None:
            query = f"SELECT * FROM {table} "
        elif isinstance(select_fields, list):
            query = f"SELECT {','.join(select_fields)} FROM {table} "
        else:
            query = f"SELECT {select_fields} FROM {table} "
        if not all([field is None, value is None]):
            if isinstance(value, str):
                query += f"WHERE {field} = '{value}' "
            elif isinstance(value, int):
                query += f"WHERE {field} = {value} "
        if kwargs:
            keys = " AND ".join([f"{k} = {v} " if isinstance(v, int) else (f"{k} IS NULL " if v == "NULL" else (
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
        if type(table) == str:
            query = self.build(table, field, value, limit, offset, select_fields, **kwargs)
        elif type(table) == Query:
            query = table
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
