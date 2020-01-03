from contextlib import suppress
from datetime import datetime, timedelta, date
from email.encoders import encode_base64
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path, PurePath
from smtplib import SMTP
from socket import timeout
from sys import argv
from typing import (Any,
                    Dict,
                    Iterator,
                    List,
                    Mapping,
                    Sequence,
                    Tuple,
                    Type,
                    Union)
from urllib.parse import quote_plus
from warnings import warn

from elasticsearch.client import Elasticsearch
from elasticsearch.exceptions import (ElasticsearchException,
                                      AuthenticationException,
                                      AuthorizationException)
from mysql.connector import connect as mysqlconnect
from mysql.connector.cursor import MySQLCursorBuffered
from mysql.connector.errors import DatabaseError, InterfaceError
from pandas.core.arrays.datetimelike import (NaTType,
                                             Timestamp,
                                             Timedelta)
from pandas.core.dtypes.missing import isna
from pymongo.database import Collection, Database
from pymongo.mongo_client import MongoClient
from pymongo.operations import UpdateOne, UpdateMany
from tqdm.std import trange


class ESClient(Elasticsearch):
    """Client for ElasticSearch"""

    def __init__(self,
                 es_index: str = None,
                 dev: bool = True,
                 **kwargs
                 ):
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
        self.size = kwargs.pop("size", 20)

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
            size = kwargs.pop("size", 1)
            return self.search(index=index, size=size, body={}, *args, **kwargs)
        if isinstance(query, dict):
            query = [query]
        if first_only and not source_only:
            source_only = True
        if source_only and not hits_only:
            warn("Returning hits only if any([source_only, first_only])")
            hits_only = True
        size = kwargs.pop("size", self.size)
        results = []
        for q in query:
            if not q:
                results.append([])
                continue
            while True:
                try:
                    result = self.search(index=self.es_index, size=size, body=q, *args, **kwargs)
                    break
                except (ElasticsearchException, OSError, ConnectionError, timeout) as e:
                    raise ElasticsearchException(q) from e
            if size != 0:
                if hits_only:
                    result = result["hits"]["hits"]
                if source_only:
                    result = [doc["_source"] for doc in result]
                if first_only:
                    with suppress(IndexError):
                        result = result[0]
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def geo_distance(self, *,
                     address_id: str = None,
                     location: Union[
                         Sequence[Union[str, float]],
                         Mapping[str, Union[str, float]]] = None,
                     distance: str = None
                     ) -> Sequence[dict]:
        """Find all real estate objects within :param distance: of
        :param address_id: or :param location:.

        :param address_id: Address ID in format
            "postalCode houseNumber houseNumberExt"
        :param location: A tuple, list, or dict of
            a latitude-longitude pair.
        :param distance: Distance (in various units) in format "42km".
        :return: List of results that are :param distance: away.

        Example:
            es = ESClient("dev_realestate.realestate")
            res = es.geo_distance(
                address_id="1071XB 71 B", distance="10m")
            for d in res:
                print(d["avmData"]["locationData"]["address_id"])
        """

        if not any((address_id, location)) or all((address_id, location)):
            raise ValueError("Provide either an address_id or a location")

        if address_id:
            query = {"query": {"bool": {"must": [
                {"match": {"avmData.locationData.address_id.keyword": address_id}}]}}}
            result: Dict[str, Any] = self.find(query=query, first_only=True)
            location = (result["geometry"]["latitude"], result["geometry"]["longitude"])

        location = dict(zip(("latitude", "longitude"), location.values())) \
            if isinstance(location, Mapping) else \
            dict(zip(("latitude", "longitude"), location))

        query = {"query": {"bool": {"filter": {
                        "geo_distance": {
                            "distance": distance,
                            "geometry.geoPoint": {
                                "lat": location["latitude"],
                                "lon": location["longitude"]}}}}},
                        "sort": [{
                            "_geo_distance": {
                                "geometry.geoPoint": {
                                    "lat": location["latitude"],
                                    "lon": location["longitude"]},
                                "order": "asc"}}]}

        results = self.findall(query=query)

        if results:
            results = [result["_source"] for result in results]

        return results

    def findall(self,
                query: Dict[Any, Any],
                index: str = None,
                **kwargs,
                ) -> List[Dict[Any, Any]]:
        """Used for elastic search queries that are larger than the max
        window size of 10,000.
        :param query: Dict[Any, Any]
        :param index: str
        :param kwargs: scroll: str
        :return: List[Dict[Any, Any]]
        """

        scroll = kwargs.pop("scroll", "10m")

        if not index:
            index = "dev_realestate.realestate"

        data = self.search(index=index, scroll=scroll, size=10_000, body=query)

        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])
        results = data["hits"]["hits"]

        # We scroll over the results until nothing is returned
        while scroll_size > 0:
            data = self.scroll(scroll_id=sid, scroll=scroll)
            results.extend(data["hits"]["hits"])
            sid = data["_scroll_id"]
            scroll_size = len(data["hits"]["hits"])

        return results

    def query(self, field: str = None, value: Union[str, int] = None,
              **kwargs) -> Union[List[dict], Dict[str, Union[Any, dict]]]:
        """Perform a simple ElasticSearch query, and return the hits.

        Uses .find() method instead of regular .search()
        Substitute period . for nested fields with underscore _

        Examples:
            from common.classes import ElasticSearch
            es = ElasticSearch()
            results = es.query(field="lastname", query="Saalbrink")

            # Add multiple search fields:
            results = es.query(lastname="Saalbrink", address_postalCode="1014AK")
            # This results in the query:
            {"query": {"bool": {"must": [{"match": {"lastname": "Saalbrink"}},
                                         {"match": {"address.postalCode": "1014AK"}}]}}}
        """
        size = kwargs.pop("size", self.size)
        sort = kwargs.pop("sort", None)
        track_scores = kwargs.pop("track_scores", None)
        if field and value:
            q = {"query": {"bool": {"must": [{"match": {field: value}}]}}}
            return self.find(q, sort=sort, size=size, track_scores=track_scores)
        args = {}
        for k in kwargs:
            if "_" in k and not k.startswith("_"):
                args[k.replace("_", ".")] = kwargs[k]
        if len(args) == 1:
            q = {"query": {"bool": {"must": [{"match": args}]}}}
            return self.find(q, sort=sort, size=size, track_scores=track_scores)
        elif len(args) > 1:
            q = {"query": {"bool": {"must": [{"match": {k: v}} for k, v in args.items()]}}}
            return self.find(q, sort=sort, size=size, track_scores=track_scores)
        else:
            return self.find(sort=sort, size=size, track_scores=track_scores)


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
            encode_base64(p)
            filename = attachment_path.name if isinstance(attachment_path, PurePath) \
                else attachment_path.split("/")[-1]
            p.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(p)

        server = SMTP(self._smtp_server)
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
            client = MongoDB(client=True)
            db = MongoDB("dev_peter")
            coll = MongoDB("dev_peter", "person_data_20190606")
            coll = MongoDB("dev_peter.person_data_20190606")
            coll = MongoDB()["dev_peter"]["person_data_20190606"]
        """
        if collection and not database:
            raise ValueError("Please provide a database name as well.")
        if not host:
            host = "address" if database and "addressvalidation" in database else "dev"
        if not client and not database:
            database, collection = "dev_peter", "person_data_20190716"
        hosts = {
            "address": ("149.210.164.50", "addr"),
            "dev": ("136.144.173.2", "mongo"),
            "stg": ("136.144.189.123", "mongo_stg"),
            "prod": ("37.97.169.90", "mongo_prod"),
        }
        if host not in hosts:
            raise ValueError(f"Host `{host}` not recognized")
        elif host == "stg":
            raise DeprecationWarning("Staging database is not used anymore.")
        host, secret = hosts[host]
        from common.secrets import get_secret
        cred = get_secret(secret)
        uri = f"mongodb://{quote_plus(cred.usr)}:{quote_plus(cred.pwd)}@{host}"
        mongo_client = MongoClient(host=uri, connectTimeoutMS=None)
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

    # noinspection PyPep8Naming, PyShadowingBuiltins
    @staticmethod
    def UpdateMany(filter, update, upsert=False, collation=None, array_filters=None):
        return UpdateMany(filter, update, upsert, collation, array_filters)

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
        if table and "." in table:
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
            "ssl_ca": f'{path / "server-ca.pem"}',
            "ssl_cert": f'{path / "client-cert.pem"}',
            "ssl_key": f'{path / "client-key.pem"}'}
        self.cnx = self.cursor = self._iter = None
        self._max_errors = 100
        self._types = {
            str: "CHAR",
            int: "INT",
            float: "DECIMAL",
            bool: "TINYINT",
            timedelta: "TIMESTAMP",
            Timedelta: "DECIMAL",
            Timestamp: "DATETIME",
            NaTType: "DATETIME",
            datetime: "DATETIME",
            date: "DATE",
            datetime.date: "DATE"
        }

    def connect(self, conn: bool = False) -> MySQLCursorBuffered:
        """Connect to SQL server"""
        self.cnx = mysqlconnect(**self.__config)
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
        self.cnx.commit()

    def executemany(self, query: Union[str, Query], data: list, *args, **kwargs):
        self.connect()
        self.cursor.executemany(query, data, *args, **kwargs)
        self.cnx.commit()
        self.disconnect()

    def fetchall(self) -> List[tuple]:
        return self.cursor.fetchall()

    def fetchone(self) -> List[tuple]:
        return self.cursor.fetchone()

    def exists(self) -> bool:
        try:
            self.query(select_fields="1", limit=1)
            return True
        except DatabaseError:
            return False

    def truncate(self):
        self.query(query=f"TRUNCATE TABLE {self.database}.{self.table_name}")

    def column(self, query: Union[str, Query] = None, *args, **kwargs) -> List[str]:
        """Fetch one column from MySQL"""
        self.connect()
        if query is None:
            query = f"SHOW COLUMNS FROM {self.table_name} FROM {self.database}"
        try:
            self.execute(query, *args, **kwargs)
            column = [value[0] for value in self.fetchall()]
        except DatabaseError as e:
            raise DatabaseError(query) from e
        self.disconnect()
        return column

    def count(self, table: str = None, *args, **kwargs) -> int:
        """Fetch row count from MySQL"""
        self.connect()
        if table and "." in table:
            self.database, table = table.split(".")
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
              *args, **kwargs) -> Union[List[dict], List[list]]:
        """Fetch a table from MySQL"""
        if not self.table_name and query and "." in query:
            for word in query.split():
                if "." in word:
                    self.database, self.table_name = word.split(".")
                    break
        if query is None:
            query = self.build()
        if fieldnames is True:
            fieldnames = self._get_fieldnames(query)
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
        if fieldnames is True:
            if query:
                fieldnames = self._get_fieldnames(query)
            else:
                fieldnames = self.column()
        if query is None:
            query = self.build(
                limit=1,
                offset=self._iter,
                *args, **kwargs)
            if self._iter is None:
                self._iter = 1
            else:
                self._iter += 1
        self.connect()
        try:
            self.execute(query)
            row = dict(zip(fieldnames, list(self.fetchall()[0]))) if fieldnames else list(self.fetchall()[0])
        except DatabaseError as e:
            raise DatabaseError(query) from e
        except IndexError:
            row = {} if fieldnames else []
        self.disconnect()
        return row

    def chunk(self, query: Union[str, Query] = None, size: int = None,
              use_tqdm: bool = False, retry_on_error: bool = False,
              *args, **kwargs) -> List[Union[dict, list]]:
        """Returns a generator for downloading a table in chunks

        Example:
            from common.classes import MySQLClient
            sql = MySQLClient()
            query = sql.build(
                table="mx_traineeship_peter.client_data",
                Province="Noord-Holland",
                select_fields=['id', 'City']
            )
            for rows in iter(sql.chunk(query=query, size=10)):
                print(rows)
        """
        range_func = trange if use_tqdm else range
        count = self.count() if use_tqdm else 1_000_000_000
        select_fields = kwargs.pop("select_fields", None)
        order_by = kwargs.pop("order_by", None)
        fieldnames = kwargs.pop("fieldnames", None)
        if query is None:
            query = self.build(select_fields=select_fields,
                               order_by=order_by,
                               *args, **kwargs)
        if size is None:
            size = 1
        elif size <= 0:
            raise ValueError("Chunk size must be > 0")
        if size == 1:
            for i in range_func(0, count, size):
                q = f"{query} LIMIT {i}, {size}"
                try:
                    row = self.row(q, fieldnames=fieldnames, *args, **kwargs)
                except IndexError:
                    break
                yield row
        else:
            for i in range_func(0, count, size):
                q = f"{query} LIMIT {i}, {size}"
                if retry_on_error:
                    while True:
                        try:
                            table = self.table(q, fieldnames=fieldnames, *args, **kwargs)
                            break
                        except DatabaseError as e:
                            raise DatabaseError(q) from e
                else:
                    table = self.table(q, fieldnames=fieldnames, *args, **kwargs)
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

        # Setup
        if not data:
            raise ValueError("Provide non-empty data")
        elif not fieldnames and not isinstance(data[0], dict):
            raise ValueError("Provide fieldnames if you don't have data dicts!")
        elif not fieldnames:
            fieldnames = data[0].keys()
        elif isinstance(data[0], (list, tuple)):
            data = [dict(zip(fieldnames, row)) for row in data]
        else:
            raise ValueError(f"Data array should contain `list`, `tuple`, or `dict`, not {type(data[0])}")

        # Create the type dict
        type_dict = {}
        for row in data:
            for field in row:
                if field not in type_dict and not isna(row[field]):
                    type_dict[field] = type(row[field])
            if len(type_dict) == len(row):
                break
        else:
            for field in data[0]:
                if field not in type_dict:
                    type_dict[field] = str
        type_dict = {field: type_dict[field] for field in data[0]}

        # Get the field lenghts for each type
        types = list(zip(
            type_dict.values(),
            list(map(max, zip(
                *[[len(str(value)) for value in row.values()] for row in data])))
        ))
        # Change default precision for floats and datetimes
        types = [
            (type_, 6) if type_ in (
                timedelta, datetime, Timedelta, Timestamp, NaTType) else (
                (type_, float(f"{prec}.2")) if type_ in (float, Timedelta) else (
                    type_, prec
                ))
            for type_, prec in types
        ]
        if len(types) != len(fieldnames):
            raise ValueError("Lengths don't match; does every data row have the same number of fields?")
        return dict(zip(fieldnames, types))

    def _fields(self, fields: dict) -> str:
        fields = [f"`{name}` {self._types[type_]}({str(length).replace('.', ',')})"
                  if type_ not in [date, datetime.date] else f"`{name}` {self._types[type_]}"
                  for name, (type_, length) in fields.items()]
        return ", ".join(fields)

    def create_table(self, table: str,
                     fields: Dict[str, Tuple[Type[Union[str, int, float, bool]], Union[int, float]]],
                     drop_existing: bool = False,
                     raise_on_error: bool = True):
        """Create a SQL table in MySQLClient.database.

        :param table: The name of the table to be created.
        :param fields: A dictionary with field names for keys and tuples for values, containing a pair of class type
        and precision. For example:
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}
        :param drop_existing: If the table already exists, delete it (default: False).
        :param raise_on_error: Raise on error during creating (default: True).
        """
        if "." in table:
            self.database, table = table.split(".")
        self.connect()
        if drop_existing:
            query = f"DROP TABLE {self.database}.{table}"
            if raise_on_error:
                self.execute(query)
            else:
                with suppress(DatabaseError):
                    self.execute(query)
        query = f"CREATE TABLE {table} ({self._fields(fields)})"
        self.execute(query)
        # if raise_on_error:
        #     self.execute(query)
        # else:
        #     with suppress(DatabaseError):
        #         self.execute(query)
        self.disconnect()

    def _increase_max_field_len(self, e: str, table: str, chunk: List[Union[list, tuple]]):
        field = e.split("'")[1]
        field_type, position = self.row(f"SELECT COLUMN_TYPE, ORDINAL_POSITION FROM information_schema.COLUMNS"
                                        f" WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME"
                                        f" = '{table}' AND COLUMN_NAME = '{field}'")
        field_type, field_len = field_type.strip(")").split("(")
        position -= 1  # MySQL starts counting at 1, Python at 0
        if "," in field_len:
            field_len = max(sum(map(len, f"{row[position]}")) for row in chunk)
            new_len = []
            for row in chunk:
                if "." in f"{row[position]}":
                    new_len.append(len(f"{row[position]}".split(".")[1]))
            new_len = max(new_len)
            field_type = f"{field_type}({field_len + 1},{new_len})"
        else:
            new_len = max(len(f"{row[position]}") for row in chunk)
            field_type = f"{field_type}({new_len})"
        self.connect()
        self.execute(f"ALTER TABLE {self.database}.{table} MODIFY COLUMN `{field}` {field_type}")
        self.disconnect()

    def insert(self,
               table: str = None,
               data: List[Union[list, tuple, dict]] = None,
               ignore: bool = False,
               _limit: int = 10_000,
               use_tqdm: bool = False,
               fields: Union[list, tuple] = None,
               ) -> int:
        """Insert a data array into a SQL table.

        The data is split into chunks of appropriate size before upload."""
        if not data:
            raise ValueError("No data provided.")
        if not table:
            if not self.table_name:
                raise ValueError("Provide a table name.")
            table = self.table_name
        if "." in table:
            self.database, table = table.split(".")
        if fields:
            fields = [f"`{f}`" for f in fields]
            fields = f"({', '.join(fields)})"
        else:
            fields = ""
        query = (f"INSERT {'IGNORE' if ignore else ''} INTO "
                 f"{self.database}.{table} {fields} VALUES "
                 f"({', '.join(['%s'] * len(data[0]))})")
        range_func = trange if use_tqdm else range
        errors = 0
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
                except (DatabaseError, InterfaceError) as e:
                    errors += 1
                    if errors >= self._max_errors:
                        raise
                    e = f"{e}"
                    if "truncated" in e or "Out of range value" in e:
                        self._increase_max_field_len(e, table, chunk)
                    elif ("Column count doesn't match value count" in e
                          and isinstance(data[0], dict)):
                        cols = {col: self.create_definition([{col: data[0][col]}])[col]
                                for col in set(self.column()).symmetric_difference(set(data[0]))}
                        afters = [list(data[0])[list(data[0]).index(col) - 1]
                                  for col in cols]
                        cols = self._fields(cols)
                        cols = ", ".join([f"ADD COLUMN {col} AFTER {after}"
                                          for col, after
                                          in zip(cols.split(", "), afters)])
                        self.connect()
                        self.execute(f"ALTER TABLE {table} {cols}")
                        self.disconnect()
                    elif ("Timestamp" in e
                          or "Timedelta" in e
                          or "NaTType" in e):
                        for row in chunk:
                            for field in row:
                                if ("date" in field
                                        or "time" in field
                                        or "datum" in field):
                                    if isinstance(row[field],
                                                  (Timestamp,
                                                   NaTType)):
                                        row[field] = row[field].to_pydatetime()
                                    elif isinstance(row[field], Timedelta):
                                        row[field] = row[field].total_seconds()
                    elif "Unknown column 'nan'" in e:
                        chunk = [[None if value == "" or isna(value)
                                  else value for value in row]
                                 for row in chunk]
                    else:
                        print(chunk)
                        raise
        return len(data)

    def add_index(self, table: str = None, fieldnames: Union[List[str], str] = None):
        """Add indexes to a MySQL table."""
        if table and "." in table:
            self.database, table = table.split(".")
        query = f"ALTER TABLE {self.database}.{table if table else self.table_name}"
        if not fieldnames:
            fieldnames = self.column(f"SHOW COLUMNS FROM {table if table else self.table_name} FROM {self.database}")
        for index in fieldnames if isinstance(fieldnames, list) else [fieldnames]:
            query = f"{query} ADD INDEX `{index}` (`{index}`) USING BTREE,"
        self.connect()
        self.execute(query.rstrip(","))
        self.disconnect()

    def insert_new(self,
                   table: str = None,
                   data: List[Union[list, tuple, dict]] = None,
                   fields: Dict[str, Tuple[type, Union[int, float]]] = None
                   ) -> int:
        """Create a new SQL table in MySQLClient.database, and insert a data array into it.

        :param table: The name of the table to be created.
        :param data: A two-dimensional array containing data corresponding to fields.
        :param fields: A dictionary with field names for keys and tuples for values, containing a pair of class type
        and precision. For example:
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}

        The data is split into chunks of appropriate size before upload.
        """
        if not data:
            raise ValueError("No data provided.")
        if not table:
            if not self.table_name:
                raise ValueError("Provide a table name.")
            table = self.table_name
        elif "." in table:
            self.database, table = table.split(".")
        if not fields:
            fields = self.create_definition(data)
        self.create_table(table, fields,
                          drop_existing=True,
                          raise_on_error=False)
        with suppress(DatabaseError):
            self.add_index(table, list(fields))
        return self.insert(table, data)

    def _get_fieldnames(self, query: Union[str, Query]) -> List[str]:
        if "*" in query:
            fieldnames = self.column()
        else:
            for select_ in {"SELECT", "Select", "select"}:
                if select_ in query:
                    break
            fieldnames = query.split(select_)[1]
            for from_ in {"FROM", "From", "from"}:
                if from_ in query:
                    break
            fieldnames = fieldnames.split(from_)[0]
            fieldnames = [fieldname.replace("`", "").strip() for fieldname in fieldnames.split(",")]
        return fieldnames

    def build(self, table: str = None, select_fields: Union[List[str], str] = None,
              field: str = None, value: Union[str, int] = None, distinct: Union[bool, str] = None,
              limit: Union[str, int, list, tuple] = None, offset: Union[str, int] = None,
              order_by: Union[str, List[str]] = None, and_or: str = None, **kwargs) -> Query:
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
                elif "!IN " in v:
                    v = v.replace("!", "NOT ")
                    key = f"{k} {v}"
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

        if not and_or:
            and_or = "AND"
        elif and_or not in {"AND", "OR"}:
            raise ValueError(f"`and_or` should be either AND or OR, not {and_or}.")

        if distinct is True:
            distinct = "DISTINCT"
        elif not distinct:
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
            query = f"{query} WHERE {search_for(field, value)}"
        if kwargs:
            keys = []
            for field, value in kwargs.items():
                if isinstance(value, list):
                    skey = f" {and_or} ".join([search_for(field, skey) for skey in value])
                else:
                    skey = search_for(field, value)
                keys.append(skey)
            keys = f" {and_or} ".join(keys)
            if "WHERE" in query:
                query = f"{query} {and_or} {keys}"
            else:
                query = f"{query} WHERE {keys}"
        if order_by:
            if isinstance(order_by, str):
                order_by = [order_by]
            order_by = ", ".join(order_by)
            query = f"{query} ORDER BY {order_by}"
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
        if table and "." in table:
            self.database, table = table.split(".")
        if table and not self.table_name:
            self.table_name = table
        if fieldnames is True:
            fieldnames = select_fields or self.column()
        self.connect()
        try:
            self.execute(query)
            if isinstance(select_fields, str) or (isinstance(select_fields, list) and len(select_fields) == 0):
                result = [{select_fields if isinstance(select_fields, str) else select_fields[0]: value[0]}
                          for value in self.fetchall()] if fieldnames else [value[0] for value in self.fetchall()]
            elif limit == 1:
                result = dict(zip(fieldnames, list(self.fetchall()[0]))) if fieldnames else list(self.fetchall()[0])
            else:
                result = [dict(zip(fieldnames, row)) for row in self.fetchall()] if fieldnames \
                    else [list(row) for row in self.fetchall()]
        except DatabaseError as e:
            raise DatabaseError(query) from e
        except IndexError:
            result = {} if fieldnames else []
        self.disconnect()
        return result
