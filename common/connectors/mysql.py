from contextlib import suppress
from datetime import datetime, timedelta, date
from decimal import Decimal
from functools import partial
from logging import info
from pathlib import Path
from random import sample
from typing import (Any,
                    Dict,
                    Generator,
                    Iterable,
                    Iterator,
                    List,
                    Mapping,
                    MutableMapping,
                    Optional,
                    Pattern,
                    Sequence,
                    Tuple,
                    Type,
                    Union)

from mysql.connector import connect, MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.constants import ClientFlag
from mysql.connector.errors import (DatabaseError,
                                    InterfaceError,
                                    OperationalError)
from pandas.core.arrays.datetimelike import (NaTType,
                                             Timestamp,
                                             Timedelta)
from pandas.core.dtypes.missing import isna
from ..handlers import tqdm, trange


class Query(str):
    pass


# noinspection SqlInjection
class MySQLClient:
    """Client for connecting to Matrixian's MySQL database.

    Basic methods:
        The following three methods can be used to directly execute a query,
        and return the resulting data of that query. The :param query:
        parameter accepts both string and :class:`Query` objects (which can be
        build using :meth:`MySQLClient.build`; see below).
        :meth:`MySQLClient.table` can be used for queries that return two-
        dimensional data, and :meth:`MySQLClient.row` and
        :meth:`MySQLClient.column` can be used for queries that return one-
        dimensional data.
        When used without :param query:, these three methods will return data
        for :attr:`MySQLClient.table_name`. :meth:`MySQLClient.table` will
        return all data in the table, :meth:`MySQLClient.row` will return the
        first row of data, and :meth:`MySQLClient.column` will return the
        column names.

    Advanced methods:
        The following three methods are considered more Pythonic, and can be
        used to automatically build a MySQL :class:`Query` and subsequently
        retrieve the data for that query.
        :meth:`MySQLClient.query` can be used to build an advanced query using
        the method's keyword arguments and then execute it. This is the main
        method for using :class:`MySQLClient`.
        In addition, :meth:`MySQLClient.query` can be used to execute a query
        that returns data, if that query is provided as the first
        positional-only argument; and to execute a query that does not return
        data, using :param query:.
        :meth:`MySQLClient.chunk` and :meth:`MySQLClient.iter` both return
        generators, and can thus be used to iterate over larger datasets.
        While :meth:`MySQLClient.chunk` returns chunks of data, of which the
        size can be set by using :param size:; :meth:`MySQLClient.iter`
        returns each row one by one.

    Creating tables and inserting data:
        :meth:`MySQLClient.create_table` can be used to create a new table.
        :meth:`MySQLClient.create_definition` can be used as input to
        :meth:`MySQLClient.create_table`.
        :meth:`MySQLClient.add_index` can be used to add one or more indices.
        :meth:`MySQLClient.insert` is the main method for inserting data into
        MySQL using Python.
        :meth:`MySQLClient.insert_new` is a wrapper method for all of the
        above.

    Helper methods:
        :meth:`MySQLClient.exists` can be used to check whether a table with
        :attr table_name: exists in :attr database:.
        :meth:`MySQLClient.count` can be used to return the total row count
        for :attr table_name:.
        :meth:`MySQLClient.truncate` can be used to truncate all data in
        :attr table_name:.
        :meth:`MySQLClient.build` with :rtype: :class:`Query` is used
        internally to build queries.

    Other important internal methods:
        :meth:`MySQLClient.connect` and :meth:`MySQLClient.disconnect` can be
        used for (dis)connecting the instance. This might be useful when
        working directly with the :attr:`MySQLClient.cursor` attribute.
        :meth:`MySQLClient.execute` and :meth:`MySQLClient.executemany` can be
        used for executing a query. :meth:`MySQLClient.fetchall` and
        :meth:`MySQLClient.fetchone` can be used for fetching results from a
        query. These methods are used internally throughout the class's methods.

    Attributes:
        :attr:`MySQLClient.database` and :attr:`MySQLClient.table_name` hold
        the database name that the instance connects to, and optionally the
        name of the table that will be used by default for queries or that has
        been used for the most recent query.
        :attr:`MySQLClient.cnx` and :attr:`MySQLClient.cursor` hold the
        :class:`CMySQLConnection` and :class:`CMySQLCursorBuffered` objects
        for this instance.
        :attr:`MySQLClient.executed_query` holds the most recently executed
        query. Note that column names can be retrieved together with the data
        by initializing with ``:param dictionary:=True``, setting
        ``:attr dictionary:=True`` manually, or by using a method with
        ``:param fieldnames:=True``.

    Example::
        sql = MySQLClient("webspider_nl_google")
        data = sql.query(table="pc_data_final", postcode="1014AK")

    This class may be subclassed to enable connectivity to another server.
    If subclassing, specify a database and optionally a table after the
    :func:`super().__init__()` call, and provide login credentials using the
    :attr:`MySQLClient.__config` dictionary (**:attr:`MySQLClient.__config`
    will be used to connect the instance).
    """
    def __init__(self,
                 database: str = None,
                 table: str = None,
                 buffered: bool = False,
                 dictionary: bool = False
                 ):
        """Create client for MySQL, and connect to a specific database.
        You can provide a database and optionally a table name.
        The default database is `mx_traineeship_peter`.

        :param database: Database to connect to. After connecting,
            different databases may be queried simply by overriding the
            database using a method's `table` parameter.
        :type database: str
        :param table: Optionally, provide a table that will be stored
            and used as the default table when making queries while not
            specifying a table using one of the class's methods.
        :type table: str
        :param buffered: Whether or not to use :class:`CMySQLCursorBuffered`
        (default: False)
        :type buffered: bool
        :param dictionary: Whether or not to use :class:`CMySQLCursorDict`
        (default: False)
        :type dictionary: bool

        Examples::
            sql = MySQLClient()
            sql = MySQLClient(database="client_work_google")
            sql = MySQLClient(database="webspider_nl_google",
                              table="pc_data_final")
            sql = MySQLClient("august_2017_google.shop_data_nl_main")
        Default::
            sql = MySQLClient("mx_traineeship_peter")
        """
        import common
        from common.env import getenv
        from common.secrets import get_secret
        usr, pwd = get_secret("MX_MYSQL_DEV")
        if database:
            if "." in database:
                database, table = database.split(".")
        else:
            database = "mx_traineeship_peter"
        if table and "." in table:
            database, table = database.split(".")
        self.database = database
        self.table_name = table
        path = Path(common.__file__).parent / "certificates"
        self.__config = {
            "user": usr,
            "password": pwd,
            "host": getenv("MX_MYSQL_DEV_IP"),
            "database": self.database,
            "raise_on_warnings": True,
            "client_flags": [ClientFlag.SSL],
            "ssl_ca": f'{path / "server-ca.pem"}',
            "ssl_cert": f'{path / "client-cert.pem"}',
            "ssl_key": f'{path / "client-key.pem"}',
        }
        self.cnx = None
        self.cursor = None
        self._iter = None
        self._cursor_columns = None
        self.executed_query = None
        self._cursor_row_count = None
        self.buffered = buffered
        self.dictionary = dictionary
        self._max_errors = 100
        self._types = {
            str: "CHAR",
            int: "INT",
            float: "DECIMAL",
            Decimal: "DECIMAL",
            bool: "TINYINT",
            timedelta: "TIMESTAMP",
            Timedelta: "DECIMAL",
            Timestamp: "DATETIME",
            NaTType: "DATETIME",
            datetime: "DATETIME",
            date: "DATE",
            datetime.date: "DATE"
        }

    def __repr__(self):
        args = ", ".join(f"{k}={v}" for k, v in self.__dict__.items())
        return f"MySQLClient({args})"

    def connect(self,
                conn: bool = False
                ) -> Union[MySQLCursor,
                           MySQLConnection]:
        """Connect to MySQL server.

        :param conn: Whether or not to return a connection object
        (default: False)
        :type conn: bool
        :return: Either a :class:`CMySQLConnection` or a (subclass of)
        :class:`CMySQLCursor`, dependent on :param conn:.
        """
        while True:
            with suppress(OperationalError):
                self.cnx = connect(**self.__config)
                self.cursor = self.cnx.cursor(buffered=self.buffered,
                                              dictionary=self.dictionary)
                break
        if conn:
            return self.cnx
        else:
            return self.cursor

    def disconnect(self):
        """Disconnect from MySQL server."""
        self.cursor.close()
        self.cnx.close()

    def _set_cursor_properties(self):
        """Property setter for cursor-related attributes."""
        try:
            self._cursor_columns = self.cursor.column_names
        except AttributeError:
            self._cursor_columns = None
        try:
            self.executed_query = self.cursor.statement
        except AttributeError:
            self.executed_query = None
        if self.buffered:
            try:
                self._cursor_row_count = self.cursor.rowcount
            except AttributeError:
                self._cursor_row_count = None

    def execute(self,
                query: Union[Query, str],
                *args, **kwargs):
        """Execute and (if necessary) commit a query on the MySQL instance.

        :param query: Statement to execute in the connected cursor.
        :param args: and :param kwargs: will be passed onto
        :meth:`MySQLClient.cursor.execute`.
        """
        self.cursor.execute(query, *args, **kwargs)
        if any(query.strip().upper().startswith(s) for s in
               ("INSERT", "UPDATE", "DELETE")):
            self.cnx.commit()
        self._set_cursor_properties()

    def executemany(self,
                    query: Union[Query, str],
                    data: Sequence[Sequence[Any]],
                    *args, **kwargs):
        """Execute and (if necessary) commit a query many times in MySQL.

        This method can be used to insert a data set into MySQL.

        :param query: Statement to execute in the connected cursor.
        :param data: The data array (or "sequence of parameters") to insert.
        :param args: and :param kwargs: will be passed onto
        :meth:`MySQLClient.cursor.execute`.
        """
        self.cursor.executemany(query, data, *args, **kwargs)
        if any(query.strip().upper().startswith(s) for s in
               ("INSERT", "UPDATE", "DELETE")):
            self.cnx.commit()
        self._set_cursor_properties()

    def fetchall(self) -> List[Tuple[Any]]:
        return self.cursor.fetchall()

    def fetchone(self) -> Tuple[Any]:
        return self.cursor.fetchone()

    def exists(self) -> bool:
        try:
            self.query(select_fields="1", limit=1)
            return True
        except DatabaseError:
            return False

    def truncate(self):
        self.query(
            query=Query(
                f"TRUNCATE TABLE {self.database}.{self.table_name}"))

    def column(self,
               query: Union[Query, str] = None,
               *args, **kwargs
               ) -> List[str]:
        """Fetch one column from MySQL"""
        if not self.table_name:
            raise ValueError("Provide a table.")
        self.connect()
        if not query:
            query = Query(
                f"SHOW COLUMNS FROM {self.table_name} FROM {self.database}")
        try:
            self.execute(query, *args, **kwargs)
            column = [value[0] for value in self.fetchall()]
        except DatabaseError as e:
            raise DatabaseError(query) from e
        self.disconnect()
        return column

    def _count(self,
               query: Union[Query, str],
               *args, **kwargs
               ) -> int:
        self.connect()
        self.execute(query, *args, **kwargs)
        count = self.fetchone()
        self.disconnect()
        while True:
            if isinstance(count, dict):
                count = list(count.values())
            if isinstance(count, list):
                count = count[0]
            if isinstance(count, tuple):
                count = count[0]
            if isinstance(count, int):
                break
        return count

    def count(self,
              table: str = None,
              *args, **kwargs
              ) -> int:
        """Fetch row count from MySQL"""
        if table is None and self.table_name is None:
            raise ValueError("No table name provided.")
        if table and "." in table:
            self.database, table = table.split(".")
        elif table is None and self.table_name is not None:
            table = self.table_name
        if self.table_name is None:
            self.table_name = table
        query = Query(f"SELECT COUNT(*) FROM {self.database}.{table}")
        count = self._count(query, *args, **kwargs)
        return count

    def table(self,
              query: Union[Query, str] = None,
              fieldnames: bool = None,
              *args, **kwargs
              ) -> Union[List[Dict[str, Any]], List[List[Any]]]:
        """Fetch a table from MySQL"""
        if not self.table_name and query and "." in query:
            for word in query.split():
                if "." in word:
                    self.database, self.table_name = word.split(".")
                    break
        if not query:
            query = self.build()
        if fieldnames is not None:
            self.dictionary = fieldnames
        self.connect()
        self.execute(query, *args, **kwargs)
        table = (list(self.fetchall())
                 if self.dictionary else
                 [list(row) for row in self.fetchall()])
        self.disconnect()
        return table

    def row(self,
            query: Union[Query, str] = None,
            fieldnames: bool = None,
            *args, **kwargs
            ) -> Union[Dict[str, Any], List[Any]]:
        """Fetch one row from MySQL"""
        if not query:
            query = self.build(
                limit=1,
                offset=self._iter,
                *args, **kwargs)
            if self._iter is None:
                self._iter = 1
            else:
                self._iter += 1
        if fieldnames is not None:
            self.dictionary = fieldnames
        self.connect()
        try:
            self.execute(query)
            row = self.fetchall()[0] if self.dictionary else list(self.fetchall()[0])
        except DatabaseError as e:
            raise DatabaseError(query) from e
        except IndexError:
            row = {} if self.dictionary else []
        self.disconnect()
        return row

    def chunk(self,
              query: Union[Query, str] = None,
              size: int = None,
              use_tqdm: bool = False,
              retry_on_error: bool = False,
              *args, **kwargs
              ) -> Iterator[Union[List[Dict[str, Any]], List[List[Any]]]]:
        """Returns a generator for downloading a table in chunks

        Example::
            from common.classes import MySQLClient
            sql = MySQLClient()
            query = sql.build(
                table="mx_traineeship_peter.client_data",
                Province="Noord-Holland",
                select_fields=['id', 'City']
            )
            for rows in sql.chunk(query=query, size=10):
                print(rows)
        """
        range_func = partial(trange, desc="chunking", disable=not use_tqdm)
        count = self.count() if use_tqdm else 1_000_000_000
        select_fields = kwargs.pop("select_fields", None)
        order_by = kwargs.pop("order_by", None)
        fieldnames = kwargs.pop("fieldnames", True)
        if not query:
            query = self.build(select_fields=select_fields,
                               order_by=order_by,
                               *args, **kwargs)
        if size is None:
            size = 1
        elif size <= 0:
            raise ValueError("Chunk size must be > 0")

        def _chunk() -> Generator[Union[dict, list], None, None]:
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

        # Avoid memory leak
        self.__config["use_pure"] = True
        yield from iter(_chunk())
        self.__config["use_pure"] = False

    def iter(self,
             query: Union[Query, str] = None,
             use_tqdm: bool = False,
             *args, **kwargs
             ) -> Iterator[Union[Dict[str, Any], List[Any]]]:
        """Returns a generator for retrieving query data row by row.

        Example::
            from common.classes import MySQLClient
            sql = MySQLClient()
            query = sql.build(
                table="mx_traineeship_peter.client_data",
                Province="Noord-Holland",
                select_fields=['id', 'City']
            )
            for row in sql.iter(query=query):
                print(row)
        """
        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)
        select_fields = kwargs.pop("select_fields", None)
        order_by = kwargs.pop("order_by", None)
        self.dictionary = kwargs.pop("fieldnames", True)
        if not query:
            query = self.build(select_fields=select_fields,
                               order_by=order_by,
                               *args, **kwargs)

        if use_tqdm:
            count = self._count(Query(f"SELECT COUNT(*) FROM ({query}) AS x"),
                                *args, **kwargs)
        else:
            count = None

        # Create a local cursor to avoid ReferenceError
        cnx = connect(**self.__config)
        cursor = cnx.cursor(buffered=False,
                            dictionary=self.dictionary)
        cursor.execute(query, *args, **kwargs)

        while True:
            try:
                for row in _tqdm(cursor, total=count):
                    yield row
                break
            except OperationalError as e:
                info("Attempting reconnect: %s", e)
                cnx.ping(reconnect=True)

        cursor.close()
        cnx.close()

    @staticmethod
    def create_definition(
            data: Sequence[Union[Mapping[str, Any], Sequence[Any]]],
            fieldnames: Sequence[str] = None
    ) -> dict:
        """Use this method to provide data for the fields argument in create_table.

        Example:
            sql = MySQLClient()
            data = [[1, "Peter"], [2, "Paul"]]
            fieldnames = ["id", "name"]
            fields = sql.create_definition(data=data, fieldnames=fieldnames)
            sql.create_table(table="employees", fields=fields)
        """

        # Setup
        if not data or not data[0]:
            raise ValueError("Provide non-empty data.")
        elif fieldnames and isinstance(data[0], dict):
            pass
        elif not fieldnames and not isinstance(data[0], dict):
            raise ValueError("Provide fieldnames if you don't have data dicts!")
        elif not fieldnames:
            fieldnames = data[0].keys()
        elif isinstance(data[0], (list, tuple)):
            data = [dict(zip(fieldnames, row)) for row in data]
        else:
            raise ValueError(f"Data array should contain `list`, `tuple`, or `dict`, not {type(data[0])}")

        # Try taking a sample
        with suppress(ValueError):
            data = sample(data, 1000)

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
        type_dict: dict = {field: type_dict[field] for field in data[0]}

        # Get the field lenghts for each type
        date_types: set = {timedelta, datetime, Timedelta, Timestamp, NaTType}
        float_types: set = {float, Decimal}
        union: set = date_types.union(float_types)
        dates: dict = {field: (_type, 6) for field, _type in type_dict.items() if _type in date_types}
        floats_dict: dict = {field: _type for field, _type in type_dict.items() if _type in float_types}
        floats_list: list = list(zip(
            floats_dict.values(),
            list(map(max, zip(
                *[[tuple(map(len, f"{value}".split("."))) for key, value in row.items()
                   if key in floats_dict.keys()]
                  for row in data])))
        ))
        floats_list: list = [(_type, float(".".join((f"{l + r}", f"{r}")))) for _type, (l, r) in floats_list]
        floats: dict = dict(zip(floats_dict, floats_list))
        normals_dict: dict = {field: _type for field, _type in type_dict.items() if _type not in union}
        normals_list: list = list(zip(
            normals_dict.values(),
            list(map(max, zip(
                *[[len(f"{value}") for key, value in row.items()
                   if key in normals_dict.keys()]
                  for row in data])))
        ))
        normals: dict = dict(zip(normals_dict, normals_list))
        all_types: dict = {**dates, **floats, **normals}
        type_dict: dict = {field: all_types[field] for field in type_dict}

        if len(type_dict) != len(fieldnames):
            raise ValueError("Lengths don't match; does every data row have the same number of fields?")

        return type_dict

    def _fields(self, fields: Mapping[str, Tuple[Type, Union[int, float]]]) -> str:
        fields = [f"`{name}` {self._types[type_]}({str(length).replace('.', ',')})"
                  if type_ not in [date, datetime.date] else f"`{name}` {self._types[type_]}"
                  for name, (type_, length) in fields.items()]
        return ", ".join(fields)

    def create_table(self,
                     table: str,
                     fields: Mapping[str, Tuple[Type, Union[int, float]]],
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
            self.database, self.table_name = table.split(".")
        self.connect()
        if drop_existing:
            query = Query(f"DROP TABLE {self.database}.{self.table_name}")
            with suppress(DatabaseError):
                self.execute(query)
        query = Query(f"CREATE TABLE {self.database}.{self.table_name}"
                      f" ({self._fields(fields)})")
        if raise_on_error:
            self.execute(query)
        else:
            with suppress(DatabaseError):
                self.execute(query)
        self.disconnect()

    def _increase_max_field_len(self,
                                e: str,
                                table: str = None,
                                chunk: Sequence[Sequence[Any]] = None):
        field = e.split("'")[1]
        if table is None:
            table = self.table_name
        result = self.row(Query(
            f"SELECT COLUMN_TYPE, ORDINAL_POSITION FROM information_schema.COLUMNS"
            f" WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME"
            f" = '{table}' AND COLUMN_NAME = '{field}'"))
        if result:
            if self.dictionary:
                field_type, position = result.values()
            else:
                field_type, position = result
        else:
            raise
        field_type, field_len = field_type.strip(")").split("(")

        if chunk is not None:
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
        else:
            if "," in field_len:
                field_len, decimal_part = field_len.split(",")
                field_type = f"{field_type}({int(field_len) + 1},{decimal_part})"
            else:
                field_type = f"{field_type}({int(field_len) + 1})"

        self.connect()
        self.execute(Query(
            f"ALTER TABLE {self.database}.{table} MODIFY COLUMN `{field}` {field_type}"))
        self.disconnect()

    def insert(self,
               table: str = None,
               data: Sequence[Union[Mapping[str, Any], Sequence[Any]]] = None,
               ignore: bool = False,
               _limit: int = 10_000,
               use_tqdm: bool = False,
               fields: Sequence[str] = None,
               ) -> int:
        """Insert a data array into a SQL table.

        The data is split into chunks of appropriate size before upload."""
        if not data or not data[0]:
            raise ValueError("No data provided.")
        if not table:
            if not self.table_name:
                raise ValueError("Provide a table name.")
            table = self.table_name
        if "." in table:
            self.database, table = table.split(".")
        if fields is None and isinstance(data[0], Mapping):
            fields = list(data[0].keys())
        if isinstance(fields, Iterable):
            fields = [f"`{f}`" for f in fields]
            fields = f"({', '.join(fields)})"
        else:
            fields = ""
        query = Query(f"INSERT {'IGNORE' if ignore else ''} INTO "
                      f"{self.database}.{table} {fields} VALUES "
                      f"({', '.join(['%s'] * len(data[0]))})")
        errors = 0
        for offset in trange(0, len(data), _limit, desc="inserting", disable=not use_tqdm):
            chunk = data[offset:offset + _limit]
            if len(chunk) == 0:
                break
            if isinstance(chunk[0], dict):
                chunk = [list(d.values()) for d in chunk]
            while True:
                try:
                    self.connect()
                    self.executemany(query, chunk)
                    self.disconnect()
                    break
                except (DatabaseError, InterfaceError) as e:
                    errors += 1
                    if errors >= self._max_errors:
                        raise
                    e = f"{e}"
                    info("%s", e)
                    if "truncated" in e or "Out of range value" in e:
                        self._increase_max_field_len(e, table=table, chunk=chunk)
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
                        self.execute(Query(f"ALTER TABLE {table} {cols}"))
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
                        print(query)
                        raise
        return len(data)

    def add_index(self,
                  table: str = None,
                  fieldnames: Union[Sequence[str], str] = None):
        """Add indexes to a MySQL table."""
        if table:
            if "." in table:
                self.database, self.table_name = table.split(".")
            else:
                self.table_name = table
        query = f"ALTER TABLE {self.database}.{self.table_name}"
        if not fieldnames:
            fieldnames = self.column()
        if isinstance(fieldnames, str):
            fieldnames = [fieldnames]
        for index in fieldnames:
            query = f"{query} ADD INDEX `{index}` (`{index}`) USING BTREE,"
        self.connect()
        self.execute(Query(query.rstrip(",")))
        self.disconnect()

    def insert_new(self,
                   table: str = None,
                   data: Sequence[Union[Mapping[str, Any], Sequence[Any]]] = None,
                   fields: Mapping[str, Tuple[Type, Union[int, float]]] = None
                   ) -> int:
        """Create a new SQL table in MySQLClient.database,
         and insert a data array into it.

        The data is split into chunks of appropriate size before upload.

        :param table: The name of the table to be created.
        :param data: A two-dimensional array containing data corresponding to fields.
        :param fields: A dictionary with field names for keys and tuples for values,
        containing a pair of class type and precision. For example::
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}
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
                          raise_on_error=True)
        with suppress(DatabaseError):
            self.add_index(table, list(fields))
        return self.insert(table, data)

    def _get_fieldnames(self,
                        query: Union[Query, str]
                        ) -> List[str]:
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

    def build(self,
              table: str = None,
              *,
              select_fields: Union[Sequence[str], str] = None,
              fields_as: MutableMapping[str, str] = None,
              field: str = None,
              value: Any = None,
              distinct: Union[bool, str] = None,
              limit: Union[str, int, Sequence[Union[str, int]]] = None,
              offset: Union[str, int] = None,
              group_by: Union[str, Sequence[str]] = None,
              order_by: Union[str, Sequence[str]] = None,
              and_or: str = None,
              **kwargs
              ) -> Query:
        """Build a MySQL query"""

        def search_for(k, v):
            key = rf"""{k} = "{v}" """
            if isinstance(v, int):
                key = rf"{k} = {v}"
            elif isinstance(v, str):
                if v == "NULL":
                    key = rf"{k} = {v}"
                elif v == "IS NULL":
                    key = rf"{k} IS NULL"
                elif v == "!NULL":
                    key = rf"{k} IS NOT NULL"
                elif "!IN " in v:
                    v = v.replace("!", "NOT ")
                    key = rf"{k} {v}"
                elif v.startswith("!"):
                    key = rf"""{k} != "{v[1:]}" """
                elif v.startswith((">", "<")):
                    key = rf"{k} {v[0]} {v[1:]}"
                elif v.startswith((">=", "<=")):
                    key = rf"""{k} {v[:2]} {v[2:]} """
                elif "%" in v:
                    key = rf"""{k} LIKE "{v}" """
                elif v.startswith("IN "):
                    key = rf"{k} {v}"
                if '"' in v and r'\"' not in v:
                    v = v.replace('"', r'\"')
                    key = rf"""{k} = "{v}" """
                if "'" in v and r"\'" not in v:
                    v = v.replace("'", r"\'")
                    key = rf"""{k} = "{v}" """
            elif isinstance(v, Pattern):
                key = f"""{k} REGEXP "{v.pattern}" """
            return key

        if not and_or:
            and_or = "AND"
        elif and_or not in {"AND", "OR"}:
            raise ValueError(f"`and_or` should be either AND or OR, not {and_or}.")

        if distinct is True:
            distinct = "DISTINCT"
        elif not distinct:
            distinct = ""
        if not table:
            if not self.table_name:
                raise ValueError("Provide a table name.")
            table = f"{self.database}.{self.table_name}"
        elif "." not in table:
            table = f"{self.database}.{table}"
        elif not self.table_name:
            if "." in table:
                self.database, self.table_name = table.split(".")
            else:
                self.table_name = table

        if fields_as:
            if isinstance(select_fields, str):
                query = rf"SELECT {distinct} {select_fields} AS {fields_as[select_fields]} FROM {table}"
            else:
                if not select_fields:
                    select_fields = self.column()
                if len(select_fields) > len(fields_as):
                    for field in select_fields:
                        if field not in fields_as:
                            fields_as[field] = field
                fields_as = [f"{a} AS {b}" for a, b in fields_as.items()]
                query = rf"SELECT {distinct} {', '.join(fields_as)} FROM {table} "
        else:
            if not select_fields:
                query = rf"SELECT {distinct} * FROM {table} "
            elif isinstance(select_fields, str):
                query = rf"SELECT {distinct} {select_fields} FROM {table}"
            else:
                query = rf"SELECT {distinct} {', '.join(select_fields)} FROM {table}"

        if not all([field is None, value is None]):
            query = rf"{query} WHERE {search_for(field, value)}"
        if kwargs:
            keys = []
            for field, value in kwargs.items():
                if isinstance(value, list):
                    skey = rf" {and_or} ".join([search_for(field, skey) for skey in value])
                else:
                    skey = search_for(field, value)
                keys.append(skey)
            keys = rf" {and_or} ".join(keys)
            if "WHERE" in query:
                query = rf"{query} {and_or} {keys}"
            else:
                query = rf"{query} WHERE {keys}"
        if group_by:
            if isinstance(group_by, str):
                group_by = [group_by]
            group_by = ", ".join(group_by)
            query = rf"{query} GROUP BY {group_by}"
        if order_by:
            if isinstance(order_by, str):
                order_by = [order_by]
            order_by = ", ".join(order_by)
            query = rf"{query} ORDER BY {order_by}"
        if limit:
            if isinstance(limit, (int, str)):
                query = rf"{query} LIMIT {limit}"
            elif isinstance(limit, (list, tuple)):
                query = rf"{query} LIMIT {limit[0]}, {limit[1]}"
        if offset:
            query = rf"{query} OFFSET {offset} "
        return Query(query)

    def _execute_query(self, query: Union[Query, str]):
        """Helper method to execute a query."""
        info("Executing query. If you meant to retrieve data, use"
             " .query() without the `query=` parameter instead.")
        errors = 0
        while True:
            try:
                self.connect()
                self.execute(query)
                self.disconnect()
                break
            except DatabaseError as e:
                errors += 1
                if errors >= self._max_errors:
                    raise
                e = f"{e}"
                if ("truncated" in e or "Out of range value" in e
                        and query.strip().upper().startswith("INSERT")):
                    self._increase_max_field_len(e)
                else:
                    raise

    def query(self,
              # q: Union[Query, str] = None, /,
              table: Union[Query, str] = None,
              field: str = None,
              value: Any = None,
              *,
              limit: Union[str, int, Sequence[Union[str, int]]] = None,
              offset: Union[str, int] = None,
              fieldnames: bool = None,
              select_fields: Union[Sequence[str], str] = None,
              query: Union[Query, str] = None,
              **kwargs
              ) -> Optional[Union[List[Dict[str, Any]],
                                  List[List[Any]],
                                  Dict[str, Any],
                                  List[Any]]]:
        """Build and perform a MySQL query, and returns a data array.

        :param table:
        :type table:
        :param field:
        :type field:
        :param value:
        :type value:
        :param limit:
        :type limit:
        :param offset:
        :type offset:
        :param fieldnames:
        :type fieldnames:
        :param select_fields:
        :type select_fields:
        :param query:
        :type query: Union[Query, str]
        :param kwargs: Any additional keyword arguments that will be passed
        through to :meth:`MySQLClient.build`, which will then be transformed
        into additional where-statements.
        :return: Data array.
        :rtype: Optional[Union[List[dict], List[list], dict, list]]

        Examples::
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
            return self._execute_query(query)
        if select_fields and len(select_fields) == 0:
            raise TypeError(f"Empty {type(select_fields)} not accepted.")
        if table and (isinstance(table, Query)
                      or table.strip().upper().startswith("SELECT")):
            query = table
        else:
            query = self.build(table=table,
                               field=field,
                               value=value,
                               limit=limit,
                               offset=offset,
                               select_fields=select_fields,
                               **kwargs)
        if table and "." in table:
            self.database, table = table.split(".")
        if table and not self.table_name:
            self.table_name = table
        if fieldnames is not None:
            self.dictionary = fieldnames
        self.connect()
        try:
            self.execute(query)
            if isinstance(select_fields, str):
                if self.dictionary:
                    if limit == 1:
                        result = self.fetchall()[0]
                    else:
                        result = list(self.fetchall())
                else:
                    if limit == 1:
                        result = self.fetchall()[0][0]
                    else:
                        result = [value[0] for value in self.fetchall()]
            else:
                if self.dictionary:
                    if limit == 1:
                        result = self.fetchall()[0]
                    else:
                        result = list(self.fetchall())
                else:
                    if limit == 1:
                        result = list(self.fetchall()[0])
                    else:
                        result = [list(row) for row in self.fetchall()]
        except DatabaseError as e:
            raise DatabaseError(query) from e
        except IndexError:
            result = {} if self.dictionary else []
        self.disconnect()
        return result
