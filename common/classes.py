import re
import smtplib
from sys import argv
import mysql.connector
from warnings import warn
from email import encoders
from csv import DictReader
from socket import timeout
from zipfile import ZipFile
from io import TextIOWrapper
from base64 import b64decode
import mysql.connector.cursor
from pymongo import MongoClient
from subprocess import check_call
from pathlib import Path, PurePath
from urllib.parse import quote_plus
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from text_unidecode import unidecode
from pymongo.database import Database
from pymongo.database import Collection
from mysql.connector import DatabaseError
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, date
from elasticsearch import Elasticsearch, ElasticsearchException
from typing import Any, Callable, Dict, Iterator, List, Tuple, Type, Union


class ESClient(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self, dev: bool = True, es_index: str = None):
        """Client for ElasticSearch"""
        from common.secrets import es
        if dev:
            self._host = "136.144.173.2"
            if es_index is None:
                es_index = "dev_peter.person_data_20190716"
        else:
            self._host = "37.97.169.90"
            if es_index is None:
                es_index = "production_realestate.realestate"
        self.es_index = es_index
        self._port = 9201
        hosts = [{"host": self._host, "port": self._port}]
        config = {"http_auth": (es[0], b64decode(es[1]).decode()), "timeout": 60, "retry_on_timeout": True}
        super().__init__(hosts, **config)

    def __repr__(self):
        return f"{self.__class__.__name__}(host='{self._host}', port='{self._port}', index='{self.es_index}')"

    def __str__(self):
        return f"http://{self._host}:{self._port}/{self.es_index}/_stats"

    def find(self, query: Union[dict, List[dict], Iterator[dict]] = None,
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
        if not query:
            return self.search(index=self.es_index, size=1, body={}, *args, **kwargs)
        if isinstance(query, dict):
            query = [query]
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            warn("Returning hits only if any([source_only, first_only])")
            hits_only = True
        results = []
        for q in query:
            if not q:
                results.append([])
                continue
            while True:
                try:
                    result = self.search(index=self.es_index, size=10_000, body=q, *args, **kwargs)
                    break
                except (ElasticsearchException, OSError, ConnectionError, timeout):
                    warn("Retrying", ConnectionWarning)
            if hits_only:
                result = result["hits"]["hits"]
            if source_only:
                result = [doc["_source"] for doc in result]
            if first_only:
                try:
                    result = result[0]
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
            track_scores = kwargs.pop("track_scores", None)
            args = {}
            for k in kwargs:
                if "_" in k and not k.startswith("_"):
                    args[k.replace("_", ".")] = kwargs[k]
            if len(args) == 1:
                q = {"query": {"bool": {"must": [{"match": args}]}}}
                return self.find(q, sort=sort, track_scores=track_scores)
            else:
                q = {"query": {"bool": {"must": [{"match": {k: v}} for k, v in args.items()]}}}
                return self.find(q, sort=sort, track_scores=track_scores)
        if field and value:
            q = {"query": {"bool": {"must": [{"match": {field: value}}]}}}
            return self.find(q)
        else:
            return self.find()


class EmailClient:
    """Client for sending plain text emails and attachments."""
    from common.secrets import mail_pass

    def __init__(self,
                 smtp_server="smtp.gmail.com:587",
                 login="dev@matrixiangroup.com",
                 password=b64decode(mail_pass).decode()):
        """Client for sending plain text emails and attachments."""
        self._smtp_server = smtp_server
        self._login = login
        self._password = password

    def send_email(self,
                   to_address: Union[str, list] = "psaalbrink@matrixiangroup.com",
                   subject: str = None,
                   message: Union[str, Exception] = None,
                   from_address: str = "dev@matrixiangroup.com",
                   attachment_path: Union[str, PurePath] = None,
                   error_message: bool = False,
                   ):
        """Send an email to an email address (str) or a list of addresses.
        To attach a file, include the Path to the file
        or just the filename (str) if it's in the current working directory."""

        msg = MIMEMultipart()
        msg["From"] = from_address
        if isinstance(to_address, str):
            msg["To"] = to_address
        elif isinstance(to_address, list):
            msg["To"] = ",".join(to_address)
        if not subject:
            subject = Path(argv[0]).stem
        msg["Subject"] = subject
        if not message:
            message = ""
        elif isinstance(message, Exception):
            message = str(message)
        if error_message:
            from sys import exc_info
            from traceback import format_exception
            message = f"{message}\n\n{''.join(format_exception(*exc_info()))}" \
                if message else "".join(format_exception(*exc_info()))
        msg.attach(MIMEText(message, "plain"))

        if attachment_path:
            p = MIMEBase("application", "octet-stream")
            with open(attachment_path, "rb") as attachment:
                p.set_payload(attachment.read())
            encoders.encode_base64(p)
            filename = attachment_path.name if isinstance(attachment_path, PurePath) \
                else attachment_path.split("/")[-1]
            p.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(p)

        server = smtplib.SMTP(self._smtp_server)
        server.starttls()
        server.login(self._login, self._password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()


class MongoDB(MongoClient):
    """Client for MongoDB. Uses MongoClient as superclass."""

    def __new__(cls, database: str = None, collection: str = None, client: bool = False, host: str = None) \
            -> Union[MongoClient, Database, Collection]:
        """Client for MongoDB

        Usage:
            client = MongoDB()
            db = MongoDB('dev_peter')
            coll = MongoDB('dev_peter', 'person_data_20190606')
            coll = MongoDB()['dev_peter']['person_data_20190606']
        """
        if host in {None, "136.144.173.2", "dev"}:
            from common.secrets import mongo
            host = "136.144.173.2"
        elif host in {"149.210.164.50", "address"}:
            from common.secrets import addr as mongo
            host = "149.210.164.50"
        # noinspection PyUnboundLocalVariable
        user, password = mongo[0], b64decode(mongo[1]).decode()
        mongo_client = MongoClient(host=f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}",
                                   connectTimeoutMS=None)
        if not client and not database:
            database = "dev_peter.person_data_20190716"
        if database:
            if "." in database:
                collection = database.split(".")[1]
                database = database.split(".")[0]
            if collection:
                return mongo_client.__getattr__(database).__getattr__(collection)
            return mongo_client.__getattr__(database)
        return mongo_client

    def find_last(self) -> dict:
        """Return the last document in a collection.

        Usage:
            from common.classes import MongoDB
            db = MongoDB("dev_peter.person_data_20190716")
            doc = MongoDB.find_last(db)
            print(doc)
        """
        if isinstance(self, Collection):
            return next(self.find().sort([("_id", -1)]).limit(1))

    def find_duplicates(self) -> List[dict]:
        """Return duplicated documents in a collection.

        Usage:
            from common.classes import MongoDB
            db = MongoDB("dev_peter.person_data_20190716")
            docs = MongoDB.find_duplicates(db)
            print(docs)
        """
        return list(self.aggregate([
            {"$unwind": "$birth"},
            {"$unwind": "$address"},
            {"$unwind": "$address.current"},
            {"$group": {"_id": {
                "lastname": "$lastname",
                "dateOfRecord": "$dateOfRecord",
                "birth": "$birth.date",
                "address": "$address.current.postalCode",
            },
                "uniqueIds": {"$addToSet": "$_id"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ], allowDiskUse=True))


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
        from common import __file__
        from common.secrets import sql
        if database:
            if "." in database:
                table = database.split(".")[1]
                database = database.split(".")[0]
        else:
            database = "mx_traineeship_peter"
        if table:
            if "." in table:
                table = table.split(".")[1]
                database = table.split(".")[0]
        self.database = database
        self.table_name = table
        path = Path(__file__).parent / "certificates"
        self.__config = {
            "user": sql[0],
            "password": b64decode(sql[1]).decode(),
            "host": "104.199.69.152",
            "database": self.database,
            "raise_on_warnings": True,
            "client_flags": 2048,
            "ssl_ca": str(path / "server-ca.pem"),
            "ssl_cert": str(path / "client-cert.pem"),
            "ssl_key": str(path / "client-key.pem")}
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

    def fetchone(self) -> List[tuple]:
        return self.cursor.fetchone()

    def column(self, query: Union[str, Query] = None, *args, **kwargs) -> List[str]:
        """Fetch one column from MySQL"""
        self.connect()
        if query is None:
            query = f"SHOW COLUMNS FROM {self.table_name} FROM {self.database}"
        self.execute(query, *args, **kwargs)
        column = [value[0] for value in self.fetchall()]
        self.disconnect()
        return column

    def table(self,
              query: Union[str, Query] = None,
              fieldnames: Union[bool, List[str]] = False,
              *args, **kwargs) -> Union[List[list], List[dict]]:
        """Fetch a table from MySQL"""
        if not self.table_name and query and "." in query:
            for word in query.split():
                if "." in word:
                    self.database, self.table_name = word.split(".")
                    break
        if not isinstance(fieldnames, list) and fieldnames:
            fieldnames = self.column()
        if query is None:
            query = self.build()
        self.connect()
        self.execute(query, *args, **kwargs)
        table = [dict(zip(fieldnames, row)) for row in self.fetchall()] if fieldnames \
            else [list(row) for row in self.fetchall()]
        self.disconnect()
        return table

    def row(self,
            query: Union[str, Query] = None,
            fieldnames: Union[bool, List[str]] = False,
            *args, **kwargs) -> Union[Dict[str, Any], List[Any]]:
        """Fetch one row from MySQL"""
        if isinstance(fieldnames, list):
            pass
        elif fieldnames:
            fieldnames = self.column()
        if query is None:
            query = self.build(limit=1, offset=self._iter)
            if self._iter is None:
                self._iter = 1
            else:
                self._iter += 1
        self.connect()
        self.execute(query, *args, **kwargs)
        row = dict(zip(fieldnames, list(self.fetchall()[0]))) if fieldnames else list(self.fetchall()[0])
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
            for row in sql.chunk(query=query, size=10):
                print(row)
        """
        if query is None:
            query = self.build()
        if size <= 0:
            raise ValueError("Chunk size must be > 0")
        elif size is None:
            size = 1
        if size == 1:
            for i in range(0, 1_000_000_000, size):
                q = f"{query} LIMIT {i}, {size}"
                try:
                    row = self.row(q, *args, **kwargs)
                except IndexError:
                    break
                yield row
        else:
            for i in range(0, 1_000_000_000, size):
                q = f"{query} LIMIT {i}, {size}"
                while True:
                    try:
                        table = self.table(q, *args, **kwargs)
                        break
                    except DatabaseError:
                        warn("Retrying", ConnectionWarning)
                if len(table) == 0:
                    break
                yield table

    @staticmethod
    def create_definition(data: List[Union[list, tuple, dict]], fieldnames: List[str]) -> dict:
        """Use this method to provide data for the fields argument in create_table.

        Example:
            sql = MySQLClient()
            data = [[1, "Peter"], [2, "Paul"]]
            fieldnames = ["id", "name"]
            fields = sql.create_definition(data=data, fieldnames=fieldnames)
            sql.create_table(table="employees", fields=fields)
        """
        i = 0
        test_row = data[i].values() if isinstance(data[i], dict) else data[i]
        try:
            while None in test_row:
                i += 1
                test_row = data[i].values() if isinstance(data[i], dict) else data[i]
        except IndexError:
            test_row = ["" if row is None else row for row in test_row]
        if isinstance(data[0], (list, tuple)):
            types = list(zip([type(value) for value in test_row],
                             list(map(max, zip(*[[len(str(value)) for value in row] for row in data])))))
        elif isinstance(data[0], dict):
            types = list(zip([type(value) for value in test_row],
                             list(map(max, zip(*[[len(str(value)) for value in row.values()] for row in data])))))
        else:
            raise ValueError(f"Data array should contain `list`, `tuple`, or `dict`, not {type(data[0])}")
        types = [(type_, float(f"{prec}.{round(prec / 2)}")) if type_ is float else (type_, prec)
                 for type_, prec in types]
        if len(types) != len(fieldnames):
            raise ValueError("Lengths don't match; does every data row have the same number of fields?")
        return dict(zip(fieldnames, types))

    def create_table(self,
                     table: str,
                     fields: Dict[str, Tuple[Type[Union[str, int, float, bool]], Union[int, float]]],
                     drop_existing: bool = False):
        """Create a SQL table in MySQLClient.database.

        :param table: The name of the table to be created.
        :param fields: A dictionary with field names for keys and tuples for values, containing a pair of class type
        and precision. For example:
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}
        :param drop_existing: If the table already exists, delete it (default: False).
        """
        types = {str: "CHAR", int: "INT", float: "DECIMAL", bool: "TINYINT",
                 timedelta: "TIMESTAMP", datetime: "DATETIME", date: "DATE", datetime.date: "DATE"}
        fields = [f"{name} {types[type_]}({str(length).replace('.', ',')})"
                  if type_ not in [date, datetime.date] else f"{name} {types[type_]}"
                  for name, (type_, length) in fields.items()]
        self.connect()
        if drop_existing:
            try:
                self.execute(f"DROP TABLE {table}")
            except DatabaseError:
                pass
        try:
            self.execute(f"CREATE TABLE {table} ({', '.join(fields)})")
        except DatabaseError:
            pass
        self.disconnect()

    def insert(self, table: str, data: List[Union[list, tuple, dict]]) -> int:
        """Insert a data array into a SQL table.

        The data is split into chunks of appropriate size before upload."""
        self.connect()
        try:
            query = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(data[0]))})"
        except IndexError:
            raise ValueError("Your data might be empty.")
        limit = 10_000
        for offset in range(0, 1_000_000_000, limit):
            chunk = data[offset:offset + limit]
            if len(chunk) == 0:
                break
            if isinstance(chunk[0], dict):
                chunk = [list(d.values()) for d in chunk]
            try:
                self.executemany(query, chunk)
            except DatabaseError:
                print(chunk)
                raise
        self.disconnect()
        return len(data)

    def add_index(self, table: str = None, fieldnames: Union[List[str], str] = None):
        """Add indexes to a MySQL table."""
        query = f"ALTER TABLE {self.database}.{table if table else self.table_name}"
        if not fieldnames:
            fieldnames = self.column(f"SHOW COLUMNS FROM {table if table else self.table_name} FROM {self.database}")
        for index in fieldnames if isinstance(fieldnames, list) else [fieldnames]:
            query = f"{query} ADD INDEX `{index}` (`{index}`) USING BTREE,"
        self.connect()
        self.execute(query.rstrip(","))
        self.disconnect()

    def insert_new(self,
                   table: str,
                   fields: Dict[str, Tuple[Type[Union[str, int, float, bool]], Union[int, float]]],
                   data: List[Union[list, tuple, dict]]) -> int:
        """Create a new SQL table in MySQLClient.database, and insert a data array into it.

        :param table: The name of the table to be created.
        :param fields: A dictionary with field names for keys and tuples for values, containing a pair of class type
        and precision. For example:
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}
        :param data: A two-dimensional array containing data corresponding to fields.

        The data is split into chunks of appropriate size before upload.
        """
        self.create_table(table, fields, drop_existing=True)
        self.add_index(table, list(fields))
        return self.insert(table, data)

    def build(self, table: str = None, select_fields: Union[list, str] = None,
              field: str = None, value: Union[str, int] = None, distinct: str = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              **kwargs) -> Query:
        """Build a MySQL query"""
        if not distinct:
            distinct = ""
        if table is None:
            table = f"{self.database}.{self.table_name}"
        elif "." not in table:
            table = f"{self.database}.{table}"
        if select_fields is None:
            query = f"SELECT {distinct} * FROM {table} "
        elif isinstance(select_fields, list):
            query = f"SELECT {distinct} `{'`, `'.join(select_fields)}` FROM {table}"
        else:
            query = f"SELECT {distinct} {select_fields} FROM {table}"
        if not all([field is None, value is None]):
            if isinstance(value, str):
                query = f"{query} WHERE {field} = '{value}'"
            elif isinstance(value, int):
                query = f"{query} WHERE {field} = {value}"
        if kwargs:
            keys = " AND ".join([f"{k} = {v}" if isinstance(v, int) else (f"{k} IS NULL" if v == "NULL" else (
                f"{k} IS NOT NULL" if v == "!NULL" else (
                    f"{k} != '{v[1:]}'" if v.startswith('!') else f"{k} = '{v}'"))) for k, v in kwargs.items()])
            if "WHERE" in query:
                query = f"{query} AND {keys}"
            else:
                query = f"{query} WHERE {keys}"
        if limit:
            if isinstance(limit, (int, str)):
                query = f"{query} LIMIT {limit}"
            elif isinstance(limit, (list, tuple)):
                query = f"{query} LIMIT {limit[0]}, {limit[1]}"
        if offset:
            query = f"{query} OFFSET {offset} "
        return Query(query)

    def query(self, table: str = None, field: str = None, value: Union[str, int] = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              fieldnames: Union[bool, List[str]] = False, select_fields: Union[list, str] = None,
              query: Union[str, Query] = None, **kwargs) -> Union[List[list], List[dict], None]:
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
        if query:
            self.connect()
            self.execute(query)
            self.disconnect()
            return
        elif not query:
            query = table if table and (isinstance(table, Query) or table.startswith("SELECT")) \
                else self.build(table=table, field=field, value=value, limit=limit, offset=offset,
                                select_fields=select_fields, **kwargs)
        if isinstance(fieldnames, list):
            pass
        elif fieldnames:
            fieldnames = self.column()
        self.connect()
        self.execute(query)
        if isinstance(select_fields, str) or (isinstance(select_fields, list) and len(select_fields) is 0):
            result = [{select_fields if isinstance(select_fields, str) else select_fields[0]: value[0]}
                      for value in self.fetchall()] if fieldnames else [value[0] for value in self.fetchall()]
        elif limit == 1:
            result = dict(zip(fieldnames, list(self.fetchall()[0]))) if fieldnames else list(self.fetchall()[0])
        else:
            result = [dict(zip(fieldnames, row)) for row in self.fetchall()] if fieldnames \
                else [list(row) for row in self.fetchall()]
        self.disconnect()
        return result


class ZipData:
    """Class for processing zip archives containing csv data files."""
    def __init__(self, file_path: PurePath, data_as_dicts: bool = False, **kwargs):
        """Create a ZipData class instance.

        Examples:
            path = Path.cwd() / "test.zip"
            zipt = ZipData(path)
            zipt.open(remove=True)
            zipt.transform(function=custom_func)
            zipt.write(replace=("_In", "_Uit"))
        """
        assert file_path.suffix == ".zip"
        self.file_path = file_path
        self._dicts = data_as_dicts
        self.data = {}
        self._encoding = kwargs.get("encoding", "utf-8")
        self._delimiter = kwargs.get("delimiter", ";")

    def open(self, remove: bool = False):
        """Load (and optionally remove) data from zip archive."""
        if remove:
            from sys import platform
            assert "x" in platform
        with ZipFile(self.file_path) as zipfile:
            for file in zipfile.namelist():
                if file.endswith(".csv"):
                    with TextIOWrapper(zipfile.open(file), encoding=self._encoding) as csv:
                        csv = DictReader(csv, delimiter=self._delimiter)
                        self.data[file] = [row for row in csv] if self._dicts else \
                            [csv.fieldnames] + [list(row.values()) for row in csv]
                    if remove:
                        check_call(["zip", "-d", zipfile.filename, file])
        if len(self.data) == 1:
            self.data = self.data[list(self.data)[0]]

    def transform(self, function: Callable, skip_fieldnames: bool = True, *args, **kwargs):
        """Perform a custom function on all data files.

        Optionally, (keyword) arguments are passed through. By default, skips the file header."""
        for file, data in self.data.items():
            self.data[file] = function(data, *args, **kwargs) if self._dicts else \
                function(data[1:] if skip_fieldnames else data, *args, **kwargs)

    def write(self, replace: Tuple[str, str] = ("", "")):
        """Archives and deflates all data files."""
        with ZipFile(self.file_path, "w", compression=8) as zipfile:
            for file, values in self.data.items():
                if self._dicts:
                    zipfile.writestr(file.replace(*replace), "\n".join(
                        values[0].keys() + [",".join(row.values()) for row in values]))
                else:
                    zipfile.writestr(file.replace(*replace), "\n".join([",".join(row) for row in values]))


class NoMatch(Exception):
    pass


class Match:
    """Base Match class"""
    drop_fields = {"source", "previous", "common", "country", "place", "fax", "id", "year",
                   "postalCodeMin", "purposeOfUse", "latitude", "longitude", "title", "state", "dateOfRecord"}
    title_data = {"ac", "ad", "ba", "bc", "bi", "bsc", "d", "de", "dr", "drs", "het", "ing",
                  "ir", "jr", "llb", "llm", "ma", "mr", "msc", "o", "phd", "sr", "t", "van"}

    def __init__(self, strictness: int = 3):
        self.data = self.query = self.result = None
        self._strictness = strictness
        self.es = ESClient()

    def match(self, data: dict, strictness: int = None):
        if strictness:
            self._strictness = strictness
        self.data = data
        self.build_query(strictness=strictness)
        self.find_match()
        if self.result:
            return self.result
        else:
            self.build_query(strictness=strictness, fuzzy=True)
            self.find_match()
            return self.result

    def build_query(self, strictness: int, fuzzy: bool = False):
        if not self.data.get("lastname"):
            raise NoMatch

        self.query = {"query": {"bool": {"must": [
            {"match": {"lastname": self._clean_lastname(self.data["lastname"])}},
        ]}}} if not fuzzy else {"query": {"bool": {"must": [
            {"match": {"lastname": {"query": self._clean_lastname(self.data["lastname"]), "fuzziness": 1}}},
        ]}}}

        should_list = []
        if self.data.get("initials"):
            should_list.append({"match": {"initials": self._clean_initials(self.data["initials"])}})
        if self.data.get("gender"):
            should_list.append({"match": {"gender": self._clean_gender(self.data["gender"])}})
        if self.data.get("date_of_birth"):
            should_list.append({"match": {"birth.date": self._clean_dob(self.data["date_of_birth"])}})
        if self.data.get("postalCode"):
            should_list.append({"match": {"address.current.postalCode": self.data["postalCode"]}})
        if self.data.get("telephone") and self.data["telephone"] != "0":
            phone_number, phone_type = self._clean_phone(self.data["telephone"])
            should_list.append({"match": {f"phoneNumber.{phone_type}": phone_number}})
        if should_list:
            self.query["query"]["bool"]["should"] = should_list
            # noinspection PyTypeChecker
            self.query["query"]["bool"]["minimum_should_match"] = min(strictness, len(should_list))

    def find_match(self):
        self.result = self.es.find(self.query, sort="dateOfRecord:desc")
        if not self.result:
            raise NoMatch
        self.result = self.result[0]["_source"]
        # Run twice to flatten nested address dicts
        self._flatten_result()
        self._flatten_result()

    def _clean_lastname(self, name: str) -> str:
        """Keep only letters; hyphens become spaces. Remove all special characters and titles"""
        if name:
            name = unidecode(re.sub(r"[^\sA-Za-z\u00C0-\u017F]", "", re.sub(r"-", " ", name)).strip()) \
                .replace("÷", "o").replace("y", "ij")
            if name != "" and name.split()[-1].lower() in self.title_data:
                name = " ".join(name.split()[:-1])
        return name

    @staticmethod
    def _clean_initials(initials: str) -> str:
        return re.sub(r"[^A-Za-z\u00C0-\u017F]", "", initials.upper())

    @staticmethod
    def _clean_gender(gender: str) -> str:
        return gender.upper().replace("MAN", "M").replace("VROUW", "V") if gender in [
            "Man", "Vrouw", "MAN", "VROUW", "man", "vrouw", "m", "v", "M", "V"] else ""

    @staticmethod
    def _clean_dob(dob: str) -> datetime:
        try:
            return datetime.strptime(dob, "%Y-%m-%d")
        except ValueError:
            return datetime.strptime(dob, "%d/%m/%Y")

    @staticmethod
    def _clean_phone(phone: Union[str, int]) -> (int, str):
        if isinstance(phone, str):
            phone = phone.replace("-", "").replace(" ", "").lstrip("0")
        phone_type = "mobile" if f"{phone}".startswith("6") else "number"
        return int(phone), phone_type

    def _flatten_result(self):
        new_result = {}
        for key, maybe_nested in self.result.items():
            if not key.startswith("_") and key not in self.drop_fields:
                if isinstance(maybe_nested, dict):
                    for sub, value in maybe_nested.items():
                        if key in {"birth", "death"} and sub == "date":
                            sub = f"date_of_{key}"
                        if sub not in self.drop_fields:
                            new_result[sub] = value
                else:
                    new_result[key] = maybe_nested
        self.result = new_result


class NamesData:
    @staticmethod
    def first_names() -> dict:
        """Import a file with first names and gender occurrence, and return a {first_name: gender} dictionary.

        This function returns if any given Dutch first name has more male or female bearers. If the number is equal,
        None is returned. Names are cleaned before output.

        The output can be used to fill missing gender data."""
        return {doc["firstname"]: doc["gender"] for doc in
                MongoDB("dev_peter.names_data").find({"data": "firstnames"}, {"firstname": True, "gender": True})}

    @staticmethod
    def titles() -> set:
        """Imports a file with titles and returns them as a set. The output can be used to clean last name data."""
        return set(doc["title"] for doc in MongoDB("dev_peter.names_data").find({"data": "titles"}, {"title": True}))

    @staticmethod
    def surnames(common_only: bool = True) -> set:
        """Imports a database with surnames frequencies and returns common surnames as a list.

        Only surnames that occur more than 200 times in the Netherlands are regarded as common.
        If a name occurs twice in the file, the largest number is taken.

        The output can be used for data and matching quality calculations."""
        db = MongoDB("dev_peter.names_data")
        if common_only:
            # Return only names that occur commonly
            names_data = set(doc["surname"] for doc in
                             db.find({"data": "surnames", "number": {"$gt": 200}}, {"surname": True}))
        else:
            names_data = set(doc["surname"] for doc in db.find({"data": "surnames"}, {"surname": True}))
        return names_data
