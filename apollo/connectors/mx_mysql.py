"""Connect to Matrixian's MySQL database."""

from __future__ import annotations

__all__ = (
    "MySQLClient",
    "Query",
)

from ast import literal_eval
from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import partial
from logging import info
from pathlib import Path
from random import sample
from typing import Any, Pattern

from mysql.connector import (
    HAVE_CEXT,
    ClientFlag,
    DatabaseError,
    InterfaceError,
    OperationalError,
    connect,
)
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from pandas import NaT, Timedelta, Timestamp, isna

from ..env import _write_pem, commondir, envfile, getenv  # noqa
from ..exceptions import MySQLClientError
from ..handlers import tqdm, trange
from ..parsers import count_bytes
from ..secrets import get_secret

_MAX_ERRORS = 10_000
_MYSQL_TYPES = {
    str: "CHAR",
    int: {
        1: "TINYINT",
        2: "SMALLINT",
        3: "MEDIUMINT",
        4: "INT",
        8: "BIGINT",
    },
    float: "DECIMAL",
    Decimal: "DECIMAL",
    bool: "TINYINT",
    timedelta: "TIMESTAMP",
    Timedelta: "DECIMAL",
    Timestamp: "DATETIME",
    NaT: "DATETIME",
    datetime: "DATETIME",
    date: "DATE",
}


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

    Please be aware that MySQLClient does not provide any protection
    against SQL injection.
    """

    _after_execute_statements = (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "LOAD DATA ",
        "LOAD DATA\n"
        "INSERT\n",
        "UPDATE\n",
        "DELETE\n",
    )

    def __init__(
        self,
        database: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ):
        """Create client for MySQL, and connect to a specific database.
        You can provide a database and optionally a table name.

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
        (default: True)
        :type dictionary: bool
        :param raise_on_warnings: Whether or not to raise on warnings
        (default: True)
        :type raise_on_warnings: bool
        :param use_pure: Whether or not to use pure Python or C extension
        (default: False)
        :type use_pure: bool

        Examples::
            sql = MySQLClient()
            sql = MySQLClient(database="client_work_google")
            sql = MySQLClient(database="webspider_nl_google",
                              table="pc_data_final")
            sql = MySQLClient("august_2017_google.shop_data_nl_main")
        """
        global commondir  # noqa

        buffered = kwargs.pop("buffered", False)
        dictionary = kwargs.pop("dictionary", True)
        raise_on_warnings = kwargs.pop("raise_on_warnings", True)
        use_pure = kwargs.pop("use_pure", False)

        if database and "." in database:
            database, table = database.split(".")
        elif table and "." in table:
            database, table = table.split(".")
        self.database = database
        self.table_name = table

        usr, pwd = get_secret("MX_MYSQL_DEV")
        envv = "MX_MYSQL_DEV_IP"
        host = getenv(envv)
        if not host:
            raise MySQLClientError(
                f"Make sure a host is configured for variable"
                f" name '{envv}' in file '{envfile}'"
            )
        if not list(commondir.glob("*.pem")):
            try:
                _write_pem()
                commondir = Path.cwd()
            except Exception:
                raise MySQLClientError(
                    f"Please make sure all '.pem' SSL certificate "
                    f"files are placed in directory '{commondir}'"
                )

        self.buffered = buffered
        self.dictionary = dictionary
        self.use_pure = use_pure if HAVE_CEXT else True

        self.cnx: MySQLConnectionAbstract | None = None
        self.cursor: MySQLCursorAbstract | None = None
        self.executed_query: str | None = None
        self._cursor_columns: list[str] | None = None
        self._cursor_row_count: int | None = None
        self._iter: int | None = None

        self.__config = {
            "user": usr,
            "password": pwd,
            "host": host,
            "database": self.database,
            "raise_on_warnings": raise_on_warnings,
            "client_flags": [ClientFlag.SSL],
            "ssl_ca": f'{commondir / "server-ca.pem"}',
            "ssl_cert": f'{commondir / "client-cert.pem"}',
            "ssl_key": f'{commondir / "client-key.pem"}',
            "use_pure": self.use_pure,
        }

    def __repr__(self) -> str:
        args = f"{self.database}{f'.{self.table_name}' if self.table_name else ''}"
        return f"MySQLClient({args})"

    def connect(
        self, conn: bool = False
    ) -> MySQLCursorAbstract | MySQLConnectionAbstract:
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
                self.cursor = self.cnx.cursor(
                    buffered=self.buffered,
                    dictionary=self.dictionary,
                )
                self.set_session_variables()
                break
        if conn:
            return self.cnx
        else:
            return self.cursor

    def disconnect(self) -> None:
        """Disconnect from MySQL server."""
        assert isinstance(self.cursor, MySQLCursorAbstract)
        assert isinstance(self.cnx, MySQLConnectionAbstract)
        self.cursor.close()
        self.cnx.close()

    def _set_cursor_properties(self) -> None:
        """Property setter for cursor-related attributes."""
        if isinstance(self.cursor, MySQLCursorAbstract):
            self._cursor_columns = self.cursor.column_names
            self.executed_query = self.cursor.statement
            if self.buffered:
                self._cursor_row_count = self.cursor.rowcount
        else:
            self._cursor_columns = None
            self.executed_query = None
            self._cursor_row_count = None

    def execute(self, query: Query | str, *args: Any, **kwargs: Any) -> None:
        """Execute and (if necessary) commit a query on the MySQL instance.

        :param query: Statement to execute in the connected cursor.
        :param args: and :param kwargs: will be passed onto
        :meth:`MySQLClient.cursor.execute`.
        """
        assert isinstance(self.cursor, MySQLCursorAbstract)
        self.cursor.execute(query, *args, **kwargs)
        self._after_execute(query)

    def executemany(
        self,
        query: Query | str,
        data: Iterable[Iterable[Any]],
    ) -> None:
        """Execute and (if necessary) commit a query many times in MySQL.

        This method can be used to insert a data set into MySQL.

        :param query: Statement to execute in the connected cursor.
        :param data: The data array (or "sequence of parameters") to insert.
        """
        assert isinstance(self.cursor, MySQLCursorAbstract)
        self.cursor.executemany(query, data)
        self._after_execute(query)

    def _after_execute(self, query: Query | str) -> None:
        query = query.upper()
        if any(st in query for st in self._after_execute_statements):
            assert isinstance(self.cnx, MySQLConnectionAbstract)
            self.cnx.commit()
        self._set_cursor_properties()

    def fetchall(self) -> list[dict[str, Any] | tuple[Any, ...]]:
        """Returns all rows of a query result set."""
        assert isinstance(self.cursor, MySQLCursorAbstract)
        return self.cursor.fetchall()

    def fetchmany(
        self,
        size: int | None = None,
    ) -> list[dict[str, Any] | tuple[Any, ...]]:
        """Returns the next set of rows of a query result."""
        assert isinstance(self.cursor, MySQLCursorAbstract)
        return self.cursor.fetchmany(size)

    def fetchone(self) -> dict[str, Any] | tuple[Any, ...]:
        """Returns next row of a query result set."""
        assert isinstance(self.cursor, MySQLCursorAbstract)
        return self.cursor.fetchone()

    def exists(self) -> bool:
        """Check if the current table exists."""
        try:
            q = self.build(select_fields="1", limit=1)
            self._execute_query(q)
            return True
        except (DatabaseError, MySQLClientError):
            return False

    def truncate(self) -> None:
        """Truncate the current table. ATTENTION: REMOVES ALL DATA!"""
        self.query(query=Query(f"TRUNCATE TABLE {self.database}.{self.table_name}"))

    def column(
        self,
        query: Query | str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> list[str]:
        """Fetch one column from MySQL."""
        if not self.table_name:
            raise MySQLClientError("Provide a table.")
        self.connect()
        if not query:
            query = Query(f"SHOW COLUMNS FROM {self.table_name} FROM {self.database}")
        try:
            self.execute(query, *args, **kwargs)
            column = [
                row["Field"] if isinstance(row, dict) else row[0]
                for row in self.fetchall()
            ]
        except DatabaseError as e:
            raise MySQLClientError(query) from e
        self.disconnect()
        return column

    def _count(self, query: Query | str, *args: Any, **kwargs: Any) -> int:
        self.connect()
        self.execute(query, *args, **kwargs)
        data = self.fetchone()
        self.disconnect()
        if isinstance(data, dict):
            data = tuple(data.values())
        assert isinstance(data, tuple)
        count = data[0]
        assert isinstance(count, int)
        return count

    def count(self, table: str | None = None, *args: Any, **kwargs: Any) -> int:
        """Fetch row count from MySQL."""
        if table is None and self.table_name is None:
            raise MySQLClientError("No table name provided.")
        if table and "." in table:
            self.database, table = table.split(".")
        elif table is None and self.table_name is not None:
            table = self.table_name
        if self.table_name is None:
            self.table_name = table
        query = Query(f"SELECT COUNT(*) FROM {self.database}.{table}")
        count = self._count(query, *args, **kwargs)
        return count

    def table(
        self,
        query: Query | str | None = None,
        fieldnames: bool | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> list[dict[str, Any] | tuple[Any, ...]]:
        """Fetch a table from MySQL."""
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
        table = self.fetchall()
        self.disconnect()
        return table

    def row(
        self,
        query: Query | str | None = None,
        fieldnames: bool | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any] | tuple[Any, ...]:
        """Fetch one row from MySQL."""
        if not query:
            query = self.build(limit=1, offset=self._iter, *args, **kwargs)
            if self._iter is None:
                self._iter = 1
            else:
                self._iter += 1
        if fieldnames is not None:
            self.dictionary = fieldnames
        self.connect()
        try:
            self.execute(query)
            row = self.fetchone()
        except DatabaseError as e:
            raise MySQLClientError(query) from e
        except IndexError:
            row = {} if self.dictionary else ()
        self.disconnect()
        return row

    def set_session_variables(
        self,
        cursor: MySQLCursorAbstract | None = None,
        *,
        variables: dict[str, str] | None = None,
        maximum_timeouts: bool = False,
    ) -> None:
        """Set session variables, for example to avoid error 2013 (Lost connection)."""
        if maximum_timeouts:
            variables = {
                # "CONNECT_TIMEOUT"; "31536000",  # s, this is the maximum
                "INTERACTIVE_TIMEOUT": "31536000",  # s, can be higher
                "MAX_EXECUTION_TIME": "0",  # disable execution timeouts
                "NET_READ_TIMEOUT": "31536000",  # s, can be higher
                "NET_WRITE_TIMEOUT": "31536000",  # s, can be higher
                "WAIT_TIMEOUT": "31536000",  # s, this is the maximum
                "innodb_lock_wait_timeout": "1073741824",  # s, this is the maximum
                **(variables or {}),
            }
        if not variables:
            variables = {
                "INTERACTIVE_TIMEOUT": "28800",  # s
                "MAX_EXECUTION_TIME": "7200000",  # ms
                "NET_READ_TIMEOUT": "7200",  # s, can be higher
                "NET_WRITE_TIMEOUT": "7200",  # s, can be higher
                "WAIT_TIMEOUT": "7200",  # s
                "innodb_lock_wait_timeout": "7200",  # s
            }
        query = "SET SESSION " + ", ".join(
            f"{var}={val}" for var, val in variables.items()
        )
        if cursor:
            cursor.execute(query)
        else:
            assert isinstance(self.cursor, MySQLCursorAbstract)
            self.cursor.execute(query)

    def chunk(
        self,
        query: Query | str | None = None,
        size: int | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[list[dict[str, Any]] | list[list[Any]] | None]:
        """Returns a generator for downloading a table in chunks.

        Example::
            from apollo.connectors import MySQLClient
            sql = MySQLClient("real_estate.real_estate")
            for rows in sql.chunk():
                for row in rows:
                    print(row)
        """
        tqdm_func = partial(
            tqdm,
            desc="query",
            disable=not kwargs.pop("use_tqdm", False),
        )
        select_fields = kwargs.pop("select_fields", None)
        order_by = kwargs.pop("order_by", None)
        fieldnames = kwargs.pop("fieldnames", None)
        yield_execution = kwargs.pop("yield_execution", False)
        if fieldnames is not None:
            self.dictionary = fieldnames

        if not query:
            query = self.build(
                select_fields=select_fields,
                order_by=order_by,
                *args,
                **kwargs,
            )
        if size is None:
            size = kwargs.pop("chunk_size", 10_000)
        elif size <= 0:
            raise MySQLClientError("Chunk size must be > 0")

        self.__config["use_pure"] = True
        self.connect()
        cnx, cursor = self.cnx, self.cursor
        assert isinstance(cnx, MySQLConnectionAbstract)
        assert isinstance(cursor, MySQLCursorAbstract)
        self.set_session_variables(cursor, maximum_timeouts=True)

        try:
            cursor.execute(query, *args, **kwargs)
        except DatabaseError as e:
            raise MySQLClientError(query) from e

        try:
            count = cursor.row_count
        except AttributeError:
            count = None

        if yield_execution:
            yield None

        bar = tqdm_func(total=count)
        while True:
            try:
                data = cursor.fetchmany(size)
                if not data:
                    break
                yield data
                bar.update(len(data))
            except OperationalError as e:
                if e.errno == 2013:
                    info("Attempting reconnect: %s", e)
                    # Try to revive the connection
                    cnx.ping(reconnect=True)
                else:
                    raise

        cursor.close()
        cnx.close()
        bar.close()
        self.__config["use_pure"] = self.use_pure

    def iter(
        self,
        query: Query | str | None = None,
        use_tqdm: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any] | list[Any]]:
        """Returns a generator for retrieving query data row by row.

        Example::
            from apollo.connectors import MySQLClient
            sql = MySQLClient()
            query = sql.build(
                table="real_estate.real_estate",
                provincie="Noord-Holland",
                select_fields=['bag_nummeraanduidingid', 'plaatsnaam']
            )
            for row in sql.iter(query=query):
                print(row)
        """
        _tqdm = partial(tqdm, desc="iterating", disable=not use_tqdm)
        select_fields = kwargs.pop("select_fields", None)
        order_by = kwargs.pop("order_by", None)
        self.dictionary = kwargs.pop("fieldnames", True)
        if not query:
            query = self.build(
                select_fields=select_fields,
                order_by=order_by,
                *args,
                **kwargs,
            )

        count: int | None
        if use_tqdm:
            count = self._count(
                Query(f"SELECT COUNT(*) FROM ({query}) AS x"),
                *args,
                **kwargs,
            )
        else:
            count = None

        # Create a local cursor to avoid ReferenceError
        cnx = connect(**self.__config)
        cursor = cnx.cursor(buffered=False, dictionary=self.dictionary)
        self.set_session_variables(cursor, maximum_timeouts=True)

        cursor.execute(query, *args, **kwargs)
        yield from _tqdm(cursor, total=count)

        cursor.close()
        cnx.close()

    @staticmethod
    def create_definition(
        data: Sequence[dict[str, Any] | list[Any] | tuple[Any, ...]],
        fieldnames: list[str] | None = None,
    ) -> dict[str, tuple[type, int | float]]:
        """Use this method to provide data for the fields argument in create_table.

        Example:
            from apollo.connectors import MySQLClient
            sql = MySQLClient()
            data = [[1, "Peter"], [2, "Paul"]]
            fieldnames = ["id", "name"]
            fields = sql.create_definition(data=data, fieldnames=fieldnames)
            sql.create_table(table="employees", fields=fields)
        """

        if not data or not data[0]:
            raise MySQLClientError("Provide non-empty data.")
        elif not fieldnames and not isinstance(data[0], dict):
            raise MySQLClientError("Provide fieldnames if you don't have data dicts!")
        elif isinstance(data[0], dict) and not fieldnames:
            fieldnames = list(data[0].keys())
        elif fieldnames and isinstance(data[0], (list, tuple)):
            data = [dict(zip(fieldnames, row)) for row in data]
        else:
            raise MySQLClientError(
                f"Data array should contain `list`, `tuple`, or `dict`, not {type(data[0])}"
            )

        # Try taking a sample
        with suppress(ValueError):
            data = sample(data, 1000)

        # Create the type dict
        type_dict1 = {}
        for row in data:
            assert isinstance(row, dict)
            for field in row:
                if field not in type_dict1 and row[field]:
                    type_dict1[field] = type(row[field])
            if len(type_dict1) == len(row):
                break
        else:
            for field in data[0]:
                if field not in type_dict1:
                    type_dict1[field] = str
        type_dict2 = {field: type_dict1[field] for field in data[0]}

        # Get the field lenghts for each type
        date_types = {timedelta, datetime, Timedelta, Timestamp, NaT, date}
        float_types = {float, Decimal}
        union = date_types.union(float_types)
        union.add(int)
        dates = {
            field: (_type, 6)
            for field, _type in type_dict2.items()
            if _type in date_types
        }
        floats_dict = {
            field: _type for field, _type in type_dict2.items() if _type in float_types
        }
        floats_list1 = [
            [
                [len(x) for x in f"{value or ''}".split(".")]
                for key, value in row.items()
                if key in floats_dict.keys()
            ]
            for row in data
            if isinstance(row, dict)
        ]
        floats_list2 = zip(
            floats_dict.values(),
            (max(field) for field in zip(*floats_list1)),
        )
        floats_list3 = (
            (_type, float(".".join((f"{l + r}", f"{r}"))))
            for _type, (l, r) in floats_list2
        )
        floats = dict(zip(floats_dict, floats_list3))

        ints_dict = {
            field: _type for field, _type in type_dict2.items() if _type is int
        }
        ints_list1 = (
            (
                count_bytes(value) if value else 0
                for key, value in row.items()
                if key in ints_dict.keys()
            )
            for row in data
            if isinstance(row, dict)
        )
        ints_list2 = zip(
            ints_dict.values(),
            (max(field) for field in zip(*ints_list1)),
        )
        ints = dict(zip(ints_dict, ints_list2))

        normals_dict = {
            field: _type for field, _type in type_dict2.items() if _type not in union
        }
        normals_list1 = (
            (
                len(f"{value}")
                for key, value in row.items()
                if key in normals_dict.keys()
            )
            for row in data
            if isinstance(row, dict)
        )
        normals_list2 = zip(
            normals_dict.values(),
            (max(field) for field in zip(*normals_list1)),
        )
        normals = dict(zip(normals_dict, normals_list2))

        all_types: dict[str | Any, tuple[type[Any], Any]] = {  # noqa
            **dates,
            **floats,
            **ints,
            **normals,
        }
        type_dict3 = {field: all_types[field] for field in type_dict2}

        if len(type_dict3) != len(fieldnames):
            raise MySQLClientError(
                "Lengths don't match; does every data row have the same number of fields?"
            )

        return type_dict3

    @staticmethod
    def _fields(fields: dict[str, tuple[type, int | float]]) -> str:
        definitions = []
        for name, (type_, length) in fields.items():
            if type_ in {date, datetime}:
                definitions.append(f"`{name}` {_MYSQL_TYPES[type_]}")
            elif type_ is int:
                ints = _MYSQL_TYPES[type_]
                assert isinstance(ints, dict)
                definitions.append(f"`{name}` {ints.get(length, 'BIGINT')}")
            else:
                definitions.append(
                    f"`{name}` {_MYSQL_TYPES[type_]}({str(length).replace('.', ',')})"
                )
        return ", ".join(definitions)

    def create_table(
        self,
        table: str,
        fields: dict[str, tuple[type, int | float]],
        drop_existing: bool = False,
        raise_on_error: bool = True,
    ) -> None:
        """Create a SQL table in MySQLClient.database.

        :param table: The name of the table to be created.
        :param fields: A dictionary with field names for keys and tuples for values,
        containing a pair of class type and precision. For example:
            fields = {
                "string_column": (str, 25),
                "integer_column": (int, 6),
                "decimal_column": (float, 4.2),
            }
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
        query = Query(
            f"CREATE TABLE {self.database}.{self.table_name}"
            f" ({self._fields(fields)})"
        )
        if raise_on_error:
            self.execute(query)
        else:
            with suppress(DatabaseError):
                self.execute(query)
        self.disconnect()

    def _increase_max_field_len(
        self,
        e: str,
        table: str | None = None,
        chunk: list[list[Any]] | None = None,
    ) -> None:
        """If an error occurred, tries to increase the field length."""
        field = e.split("'")[1]
        if table is None:
            table = self.table_name
        result = self.row(
            Query(
                f"SELECT COLUMN_TYPE, ORDINAL_POSITION FROM information_schema.COLUMNS"
                f" WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME"
                f" = '{table}' AND COLUMN_NAME = '{field}'"
            )
        )
        if isinstance(result, dict):
            field_type, position = result.values()
        elif isinstance(result, tuple):
            field_type, position = result
        else:
            raise TypeError(type(result))
        field_type, field_len = field_type.strip(")").split("(")

        if "INT" in field_type.upper():
            ints = _MYSQL_TYPES[int]
            assert isinstance(ints, dict)
            int_types = list(ints.values())
            field_type = int_types[int_types.index(field_type.upper()) + 1]

        elif chunk is not None:
            position -= 1  # MySQL starts counting at 1, Python at 0
            if "," in field_len:
                field_len = max(sum(map(len, f"{row[position]}")) for row in chunk)
                new_len_list = []
                for row in chunk:
                    if "." in f"{row[position]}":
                        new_len_list.append(len(f"{row[position]}".split(".")[1]))
                new_len = max(new_len_list)
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
        self.execute(
            Query(
                f"ALTER TABLE {self.database}.{table} MODIFY COLUMN `{field}` {field_type}"
            )
        )
        self.disconnect()

    def insert(
        self,
        table: str | None = None,
        data: Sequence[dict[str, Any] | list[Any] | tuple[Any, ...]] | None = None,
        ignore: bool = False,
        _limit: int = 10_000,
        use_tqdm: bool = False,
        fields: list[str] | None = None,
    ) -> int:
        """Insert a data array into a SQL table.

        The data is split into chunks of appropriate size before upload.
        """
        if not data or not data[0]:
            raise MySQLClientError("No data provided.")
        if not table:
            if not self.table_name:
                raise MySQLClientError("Provide a table name.")
            table = self.table_name
        if "." in table:
            self.database, table = table.split(".")
        if fields is None and isinstance(data[0], dict):
            fields = list(data[0].keys())
        if isinstance(fields, Iterable):
            fields_str = f"({', '.join(f'`{f}`' for f in fields)})"
        else:
            fields_str = ""
        query = Query(
            f"INSERT {'IGNORE' if ignore else ''} INTO "
            f"{self.database}.{table} {fields_str} VALUES "
            f"({', '.join(['%s'] * len(data[0]))})"
        )
        errors = 0
        for offset in trange(
            0,
            len(data),
            _limit,
            desc="inserting",
            disable=not use_tqdm,
        ):
            chunk = [
                list(d.values()) if isinstance(d, dict) else d
                for d in data[offset : offset + _limit]
            ]
            if len(chunk) == 0:
                break
            while True:
                try:
                    self.connect()
                    self.executemany(query, chunk)
                    break
                except (DatabaseError, InterfaceError) as e:
                    errors += 1
                    if errors >= _MAX_ERRORS:
                        raise MySQLClientError(query) from e
                    info("%s", e)
                    if "truncated" in e.args[1] or "Out of range value" in e.args[1]:
                        self._increase_max_field_len(
                            e.args[1],
                            table=table,
                            chunk=chunk,  # type: ignore
                        )
                    elif "Column count doesn't match value count" in e.args[
                        1
                    ] and isinstance(data[0], dict):
                        cols = {
                            col: self.create_definition([{col: data[0][col]}])[col]
                            for col in set(self.column()).symmetric_difference(
                                set(data[0])
                            )
                        }
                        afters = [
                            list(data[0])[list(data[0]).index(col) - 1] for col in cols
                        ]
                        cols_str = ", ".join(
                            [
                                f"ADD COLUMN {col} AFTER {after}"
                                for col, after in zip(
                                    self._fields(cols).split(", "), afters
                                )
                            ]
                        )
                        self.connect()
                        self.execute(Query(f"ALTER TABLE {table} {cols_str}"))
                        self.disconnect()
                    elif (
                        "Timestamp" in e.args[1]
                        or "Timedelta" in e.args[1]
                        or "NaTType" in e.args[1]
                    ):
                        for row in chunk:
                            for field in row:
                                if (
                                    "date" in field
                                    or "time" in field
                                    or "datum" in field
                                ):
                                    assert isinstance(row, dict)
                                    if (
                                        isinstance(row[field], Timestamp)
                                        or row[field] is NaT
                                    ):
                                        row[field] = row[field].to_pydatetime()
                                    elif isinstance(row[field], Timedelta):
                                        row[field] = row[field].total_seconds()
                    elif "Unknown column 'nan'" in e.args[1]:
                        chunk = [
                            [
                                None if value == "" or isna(value) else value
                                for value in row
                            ]
                            for row in chunk
                        ]
                    else:
                        raise MySQLClientError(query) from e
                finally:
                    self.disconnect()
        return len(data)

    def add_index(
        self,
        table: str | None = None,
        fieldnames: list[str] | str | None = None,
    ) -> None:
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

    def insert_new(
        self,
        table: str | None = None,
        data: Sequence[dict[str, Any] | list[Any] | tuple[Any, ...]] | None = None,
        fields: dict[str, tuple[type, int | float]] | None = None,
    ) -> int:
        """Create a new SQL table in MySQLClient.database, and insert a data array into it.

        The data is split into chunks of appropriate size before upload.

        :param table: The name of the table to be created.
        :param data: A two-dimensional array containing data corresponding to fields.
        :param fields: A dictionary with field names for keys and tuples for values,
        containing a pair of class type and precision. For example::
            fields={"string_column": (str, 25), "integer_column": (int, 6), "decimal_column": (float, 4.2)}
        """
        if not data:
            raise MySQLClientError("No data provided.")
        if not table:
            if not self.table_name:
                raise MySQLClientError("Provide a table name.")
            table = self.table_name
        elif "." in table:
            self.database, table = table.split(".")
        if not fields:
            fields = self.create_definition(data)
        self.create_table(table, fields, drop_existing=True, raise_on_error=True)
        with suppress(DatabaseError):
            self.add_index(table, list(fields))
        return self.insert(table, data)

    def _get_fieldnames(self, query: Query | str) -> list[str]:
        if "*" in query:
            fieldnames = self.column()
        else:
            for select_ in ("SELECT", "Select", "select"):
                if select_ in query:
                    fieldnames1 = query.split(select_)[1]
                    break
            else:
                raise MySQLClientError
            for from_ in ("FROM", "From", "from"):
                if from_ in query:
                    fieldnames2 = fieldnames1.split(from_)[0]
                    break
            else:
                raise MySQLClientError
            fieldnames = [
                fieldname.replace("`", "").strip()
                for fieldname in fieldnames2.split(",")
            ]
        return fieldnames

    def build(
        self,
        table: str | None = None,
        *,
        select_fields: list[str] | str | None = None,
        fields_as: dict[str, str] | None = None,
        field: str | None = None,
        value: Any = None,
        distinct: bool | str | None = None,
        limit: str | int | list[str | int] | None = None,
        offset: str | int | None = None,
        group_by: str | list[str] | None = None,
        order_by: str | list[str] | None = None,
        and_or: str | None = None,
        **kwargs: Any,
    ) -> Query:
        """Build a MySQL query.

        For kwargs values, pass a list to create AND/OR statements,
        and pass a tuple to create IN statement.
        """

        def search_for(k: str, v: Any) -> str:
            def replace_quote(
                _k: str,
                _v: Any,
                _key: str | None = None,
            ) -> tuple[str, Any, str | None]:
                if _key:
                    op = _key.replace(_k, "").replace(_v, "").strip('" ')
                else:
                    op = "="
                if '"' in _v and "'" in _v:
                    for _s in ('"', "'"):
                        if rf"\{_s}" not in _v:
                            _v = _v.replace(_s, rf"\{_s}")
                            if _v[0] == "(":
                                _key = rf"""{_k} {op} {_v} """
                            else:
                                q = '"' if "'" in _v else "'"
                                _key = rf"""{_k} {op} {q}{_v}{q} """
                else:
                    for _s in ('"', "'"):
                        if _s in _v:
                            q = '"' if "'" in _v else "'"
                            _key = rf"""{_k} {op} {q}{_v}{q} """
                return _k, _v, _key

            key = rf"""{k} = "{v}" """
            if f"{v}".startswith(("IN ", "!IN ")) or isinstance(v, (tuple, list)):
                _not = "NOT" if f"{v}".startswith("!IN ") else ""
                if isinstance(v, str):
                    v = v.lstrip("!IN ")
                    if "NULL" in v and '"NULL"' not in v:
                        v = v.replace("NULL", '"NULL"')
                    if "SELECT" not in v:
                        v = literal_eval(v)
                if isinstance(v, (tuple, list)):
                    v = tuple(sv for _, sv, _ in (replace_quote(k, sv) for sv in v))
                key = rf"{k} {_not} IN {v}"
                key = key.replace('"NULL"', "NULL")
                key = key.replace("'NULL'", "NULL")
            elif isinstance(v, int):
                key = rf"{k} = {v}"
            elif isinstance(v, str):
                if v == "NULL":
                    key = rf"{k} = {v}"
                elif v == "IS NULL":
                    key = rf"{k} IS NULL"
                elif v == "!NULL":
                    key = rf"{k} IS NOT NULL"
                elif v.startswith("!"):
                    key = rf"""{k} != "{v[1:]}" """
                elif v.startswith((">", "<")):
                    key = rf"{k} {v[0]} {v[1:]}"
                elif v.startswith((">=", "<=")):
                    key = rf"""{k} {v[:2]} {v[2:]} """
                elif "%" in v:
                    key = rf"""{k} LIKE "{v}" """
                if '"' in v or "'" in v:
                    *_, new_key = replace_quote(k, v, key)
                    assert isinstance(new_key, str)
                    key = new_key
            elif isinstance(v, Pattern):
                key = f"""{k} REGEXP "{v.pattern}" """
            elif v is None:
                key = rf"{k} IS NULL"
            return key

        if not and_or:
            and_or = "AND"
        elif and_or not in {"AND", "OR"}:
            raise MySQLClientError(
                f"`and_or` should be either AND or OR, not {and_or}."
            )

        def search_key(_field: str, _value: Any) -> str:
            if isinstance(_value, list):
                _skey = rf" {and_or} ".join(
                    [search_for(_field, _skey) for _skey in _value]
                )
            else:
                _skey = search_for(_field, _value)
            return _skey

        if distinct is True:
            distinct = "DISTINCT"
        elif not distinct:
            distinct = ""
        if not table:
            if not self.table_name:
                raise MySQLClientError("Provide a table name.")
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
                fields_as_g = (f"{a} AS {b}" for a, b in fields_as.items())
                query = rf"SELECT {distinct} {', '.join(fields_as_g)} FROM {table} "
        else:
            if not select_fields:
                query = rf"SELECT {distinct} * FROM {table} "
            elif isinstance(select_fields, str):
                query = rf"SELECT {distinct} {select_fields} FROM {table}"
            else:
                query = rf"SELECT {distinct} {', '.join(select_fields)} FROM {table}"

        if field is not None and value is not None:
            query = rf"{query} WHERE {search_key(field, value)}"
        if kwargs:
            keys = rf" {and_or} ".join(
                search_key(field, value) for field, value in kwargs.items()
            )
            if "WHERE" in query:
                query = rf"{query} {and_or} {keys}"
            else:
                query = rf"{query} WHERE {keys}"
        if group_by:
            if not isinstance(group_by, str):
                group_by = ", ".join(group_by)
            query = rf"{query} GROUP BY {group_by}"
        if order_by:
            if not isinstance(order_by, str):
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

    def _execute_query(self, query: Query | str) -> None:
        """Helper method to execute a query."""
        info(
            "Executing query. If you meant to retrieve data, use"
            " .query() without the `query=` parameter instead."
        )
        errors = 0
        buffered, self.buffered = self.buffered, True
        while True:
            try:
                self.connect()
                self.execute(query)
                self.disconnect()
                break
            except DatabaseError as e:
                errors += 1
                if errors >= _MAX_ERRORS:
                    raise MySQLClientError(query) from e
                if (
                    "truncated" in e.args[1]
                    or "Out of range value" in e.args[1]
                    and query.strip().upper().startswith("INSERT")
                ):
                    self._increase_max_field_len(e.args[1])
                else:
                    raise MySQLClientError(query) from e
        self.buffered = buffered

    def query(
        self,
        # q: Union[Query, str] = None, /,
        table: Query | str | None = None,
        field: str | None = None,
        value: Any = None,
        *,
        limit: str | int | list[str | int] | None = None,
        offset: str | int | None = None,
        fieldnames: bool | None = None,
        select_fields: list[str] | str | None = None,
        query: Query | str | None = None,
        **kwargs: Any,
    ) -> None | (list[dict[str, Any]] | list[list[Any]] | dict[str, Any] | list[Any]):
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
        :rtype: Optional[Union[list[dict], list[list], dict, list]]

        Examples::
            sql = MySQLClient()
            table = "real_estate.real_estate"

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
        if table and (
            isinstance(table, Query) or table.strip().upper().startswith("SELECT")
        ):
            query = table
        if query:
            self._execute_query(query)
            return None
        if select_fields and len(select_fields) == 0:
            raise MySQLClientError(f"Empty {type(select_fields)} not accepted.")
        if not query:
            query = self.build(
                table=table,
                field=field,
                value=value,
                limit=limit,
                offset=offset,
                select_fields=select_fields,
                **kwargs,
            )
        if table and "." in table:
            self.database, table = table.split(".")
        if table and not self.table_name:
            self.table_name = table
        if fieldnames is not None:
            self.dictionary = fieldnames
        self.connect()
        try:
            self.execute(query)
            one_field = isinstance(select_fields, str)
            if limit == 1:
                result_dict_or_tuple = self.fetchone()
                if isinstance(result_dict_or_tuple, tuple) and one_field:
                    result = result_dict_or_tuple[0]
                else:
                    result = result_dict_or_tuple
            else:
                result = [
                    row[0] if (one_field and not isinstance(row, dict)) else row
                    for row in self.fetchall()
                ]
        except DatabaseError as e:
            raise MySQLClientError(query) from e
        except IndexError:
            result = None
        self.disconnect()
        return result
