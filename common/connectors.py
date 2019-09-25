import smtplib
from sys import argv
import mysql.connector
from tqdm import trange
from warnings import warn
from email import encoders
from socket import timeout
import mysql.connector.cursor
from pymongo import MongoClient
from pathlib import Path, PurePath
from urllib.parse import quote_plus
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from pymongo.database import Database
from pymongo.database import Collection
from pymongo.operations import UpdateOne
from mysql.connector import DatabaseError
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, date
from elasticsearch import Elasticsearch, ElasticsearchException
from typing import Any, Dict, Iterator, List, Tuple, Type, Union


class ESClient(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self, dev: bool = True, es_index: str = None):
        """Client for ElasticSearch"""
        from common.secrets import get_secret
        es = get_secret("es")
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
        config = {"http_auth": (es.usr, es.pwd), "timeout": 60, "retry_on_timeout": True}
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
        if "index" in kwargs:
            index = kwargs.pop("index")
        else:
            index = self.es_index
        if not query:
            size = kwargs.pop("size") if "size" in kwargs else 1
            return self.search(index=index, size=size, body={}, *args, **kwargs)
        if isinstance(query, dict):
            query = [query]
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            warn("Returning hits only if any([source_only, first_only])")
            hits_only = True
        size = kwargs.pop("size") if "size" in kwargs else 10_000
        results = []
        for q in query:
            if not q:
                results.append([])
                continue
            while True:
                try:
                    result = self.search(index=self.es_index, size=size, body=q, *args, **kwargs)
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
    from common.secrets import get_secret
    mail = get_secret("mail_pass")

    def __init__(self,
                 smtp_server="smtp.gmail.com:587",
                 login="dev@matrixiangroup.com",
                 password=mail.pwd):
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

    def __new__(cls, database: str = None, collection: str = None, host: str = None, client: bool = False) \
            -> Union[MongoClient, Database, Collection]:
        """Client for MongoDB

        Usage:
            client = MongoDB()
            db = MongoDB('dev_peter')
            coll = MongoDB('dev_peter', 'person_data_20190606')
            coll = MongoDB()['dev_peter']['person_data_20190606']
        """
        from common.secrets import get_secret
        if collection and not database:
            raise ValueError("Please provide a database name as well.")
        if not host:
            host = "address" if database and "addressvalidation" in database else "dev"
        if host == "address":
            mongo = get_secret("addr")
            host = "149.210.164.50"
        elif host == "dev":
            mongo = get_secret("mongo")
            host = "136.144.173.2"
        elif host == "stg":
            mongo = get_secret("mongo_stg")
            host = "136.144.189.123"
        # noinspection PyUnboundLocalVariable
        mongo_client = MongoClient(host=f"mongodb://{quote_plus(mongo.usr)}:{quote_plus(mongo.pwd)}@{host}",
                                   connectTimeoutMS=None)
        if not client and not database:
            database, collection = "dev_peter", "person_data_20190716"
        if database:
            if "." in database:
                database, collection = database.split(".")
            if collection:
                return mongo_client.__getattr__(database).__getattr__(collection)
            return mongo_client.__getattr__(database)
        return mongo_client

    # noinspection PyPep8Naming, PyShadowingBuiltins
    @staticmethod
    def UpdateOne(filter, update, upsert=False, collation=None, array_filters=None):
        return UpdateOne(filter, update, upsert, collation, array_filters)

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
        """Return duplicated documents in the person_data collection.

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
        from common.secrets import get_secret
        sql = get_secret("sql")
        if database:
            if "." in database:
                database, table = database.split(".")
        else:
            database = "mx_traineeship_peter"
        if table:
            if "." in table:
                database, table = database.split(".")
        self.database = database
        self.table_name = table
        path = Path(__file__).parent / "certificates"
        self.__config = {
            "user": sql.usr,
            "password": sql.pwd,
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

    def count(self, table: str = None, *args, **kwargs) -> int:
        """Fetch row count from MySQL"""
        self.connect()
        if not self.table_name:
            self.table_name = table
        query = f"SELECT COUNT(*) FROM {self.database}.{self.table_name}"
        self.execute(query, *args, **kwargs)
        count = self.fetchone()
        if isinstance(count, list):
            count = count[0]
        if isinstance(count, tuple):
            count = count[0]
        self.disconnect()
        return count

    def table(self, query: Union[str, Query] = None,
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

    def chunk(self, query: Union[str, Query] = None, size: int = None,
              use_tqdm: bool = False, retry_on_error: bool = False,
              *args, **kwargs) -> List[list]:
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
        range_func = trange if use_tqdm else range
        count = self.count() if use_tqdm else 1_000_000_000
        if query is None:
            query = self.build()
        if size <= 0:
            raise ValueError("Chunk size must be > 0")
        elif size is None:
            size = 1
        if size == 1:
            for i in range_func(0, count, size):
                q = f"{query} LIMIT {i}, {size}"
                try:
                    row = self.row(q, *args, **kwargs)
                except IndexError:
                    break
                yield row
        else:
            for i in range_func(0, count, size):
                q = f"{query} LIMIT {i}, {size}"
                if retry_on_error:
                    while True:
                        try:
                            table = self.table(q, *args, **kwargs)
                            break
                        except DatabaseError:
                            warn("Retrying", ConnectionWarning)
                else:
                    table = self.table(q, *args, **kwargs)
                if len(table) == 0:
                    break
                yield table

    @staticmethod
    def create_definition(data: List[Union[list, tuple, dict]], fieldnames: List[str] = None) -> dict:
        """Use this method to provide data for the fields argument in create_table.

        Example:
            sql = MySQLClient()
            data = [[1, "Peter"], [2, "Paul"]]
            fieldnames = ["id", "name"]
            fields = sql.create_definition(data=data, fieldnames=fieldnames)
            sql.create_table(table="employees", fields=fields)
        """
        if not fieldnames and not isinstance(data[0], dict):
            raise ValueError("Provide fieldnames if you don't have data dicts!")
        elif not fieldnames:
            fieldnames = list(data[0].keys())
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

    def create_table(self, table: str,
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

    def _increase_max_field_len(self, e: str, chunk: List[Union[list, tuple]]):
        field = e.split("'")[1]
        field_type, position = self.row(f"SELECT COLUMN_TYPE FROM information_schema.COLUMNS"
                                        f" WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME"
                                        f" = '{self.table_name}' AND COLUMN_NAME = '{field}'")[0]
        field_type, field_len = field_type.split("(")
        field_len = int(field_len.strip(")"))
        position -= 1
        new_len = max(len(str(value)) for value in [row[position] for row in chunk])
        assert new_len > field_len
        field_type = f"{field_type}({new_len})"
        self.execute(f"ALTER TABLE {self.database}.{self.table_name} MODIFY COLUMN `{field}` {field_type}")

    def insert(self, table: str, data: List[Union[list, tuple, dict]], _limit: int = 10_000,
               use_tqdm: bool = True) -> int:
        """Insert a data array into a SQL table.

        The data is split into chunks of appropriate size before upload."""
        range_func = trange if use_tqdm else range
        self.connect()
        try:
            query = f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(data[0]))})"
        except IndexError:
            raise ValueError("Your data might be empty.")
        for offset in range_func(0, len(data), _limit):
            chunk = data[offset:offset + _limit]
            if len(chunk) == 0:
                break
            if isinstance(chunk[0], dict):
                chunk = [list(d.values()) for d in chunk]
            while True:
                try:
                    self.executemany(query, chunk)
                    break
                except DatabaseError as e:
                    e = f"{e}"
                    if "truncated" in e:
                        self._increase_max_field_len(e, chunk)
                    else:
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

    def insert_new(self, table: str, data: List[Union[list, tuple, dict]],
                   fields: Dict[str, Tuple[Type[Union[str, int, float, bool]], Union[int, float]]] = None) -> int:
        """Create a new SQL table in MySQLClient.database, and insert a data array into it.

        :param table: The name of the table to be created.
        :param data: A two-dimensional array containing data corresponding to fields.
        :param fields: A dictionary with field names for keys and tuples for values, containing a pair of class type
        and precision. For example:
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}

        The data is split into chunks of appropriate size before upload.
        """
        if not fields:
            fields = self.create_definition(data)
        self.create_table(table, fields, drop_existing=True)
        self.add_index(table, list(fields))
        return self.insert(table, data)

    def build(self, table: str = None, select_fields: Union[List[str], str] = None,
              field: str = None, value: Union[str, int] = None, distinct: str = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              **kwargs) -> Query:
        """Build a MySQL query"""

        def search_for(k, v):
            key = f"{k} = '{v}'"
            if isinstance(v, int):
                key = f"{k} = {v}"
            elif isinstance(v, str):
                if v == "NULL":
                    key = f"{k} = {v}"
                elif v == "IS NULL":
                    key = f"{k} IS NULL"
                elif v == "!NULL":
                    key = f"{k} IS NOT NULL"
                elif v.startswith("!"):
                    key = f"{k} != '{v[1:]}'"
                elif v.startswith((">", "<")):
                    key = f"{k} {v[0]} '{v[1:]}'"
                elif v.startswith((">=", "<=")):
                    key = f"{k} {v[:2]} '{v[2:]}'"
                elif "%" in v:
                    key = f"{k} LIKE '{v}'"
                elif "IN " in v:
                    key = f"{k} {v}"
            return key

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
            keys = []
            for field, value in kwargs.items():
                if isinstance(value, list):
                    skey = " AND ".join([search_for(field, skey) for skey in value])
                else:
                    skey = search_for(field, value)
                keys.append(skey)
            keys = " AND ".join(keys)
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
              *, limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              fieldnames: Union[bool, List[str]] = False, select_fields: Union[list, str] = None,
              query: Union[str, Query] = None, **kwargs) -> Union[List[dict], List[list], None]:
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
        if table and not self.table_name:
            self.table_name = table
        if isinstance(fieldnames, list):
            pass
        elif fieldnames and not select_fields:
            fieldnames = self.column()
        elif fieldnames and select_fields:
            fieldnames = select_fields
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
