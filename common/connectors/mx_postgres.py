from __future__ import annotations

__all__ = ("PgSql",)

from collections.abc import Collection, Iterable, Iterator, Sequence, Sized
from functools import partial
from typing import Any

import psycopg2.extras
from psycopg2 import sql
from psycopg2.sql import Composed as _Composed

from ..exceptions import PgSqlError
from ..secrets import get_secret


class PgSql:
    """Connector for PostgreSQL database.

    Example:
        from common.connectors.mx_postgres import PgSql
        with PgSql("vgm") as pg:
            data = list(pg.select_all("vgm_account"))

    Example:
        from common.connectors.mx_postgres import PgSql
        query = "SELECT * FROM {} WHERE {} LIKE %s"
        table, column, value = "vgm_building", "vgo_building", "WTC%"
        with PgSql("vgm") as pg:
            data = list(pg.select(query, table, **{column: value}))
    """

    Error = psycopg2.Error  # noqa
    sql = sql

    def __init__(self, database: str, server_side_cursor: bool = False):
        self.connection: psycopg2.extras.DictConnection | None = None
        self.cursor: psycopg2.extras.DictCursor | None = None
        self.database = database
        self.server_side_cursor = server_side_cursor
        self.query: bytes | None = None

    def __enter__(self) -> PgSql:
        return self.connect()

    def __exit__(self, *args: Any, **kwargs: Any) -> bool:
        assert isinstance(self.connection, psycopg2.extras.DictConnection)
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        self.cursor.__exit__(*args, **kwargs)
        self.connection.__exit__(*args, **kwargs)
        if any((args, kwargs)):
            return False
        return True

    def connect(self) -> PgSql:
        usr, pwd = get_secret("MX_PSQL")
        self.connection = psycopg2.connect(
            host="37.97.209.246",
            port=5432,
            database=self.database,
            user=usr,
            password=pwd,
            connection_factory=psycopg2.extras.DictConnection,
        ).__enter__()
        self._connect_cursor()
        return self

    def close(self) -> None:
        assert isinstance(self.connection, psycopg2.extras.DictConnection)
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        try:
            self.cursor.close()
        except self.Error:
            pass
        self.connection.close()

    def _connect_cursor(self) -> None:
        assert isinstance(self.connection, psycopg2.extras.DictConnection)
        if self.server_side_cursor:
            self.cursor = self.connection.cursor(
                "NamedCursor",
                withhold=True,
            ).__enter__()
        else:
            self.cursor = self.connection.cursor().__enter__()

    def _reconnect_cursor(self) -> None:
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        self.cursor.close()
        self._connect_cursor()

    def compose(self, query: str, *args: Any, **kwargs: Any) -> Composed:
        composed = sql.SQL(query).format(*args, **kwargs)
        composed.execute = partial(self.execute, composed)
        return composed

    def count(self, table: str) -> int:
        return next(self.select("SELECT COUNT(*) FROM {}", table))["count"]

    def create(self, table: str, args: Iterable[str]) -> None:
        self.execute(
            self.compose("CREATE TABLE {} ", sql.Identifier(table))
            + sql.SQL("(")
            + _Composed(map(sql.SQL, args)).join(", ")
            + sql.SQL(")")
        )

    def create_and_insert(
        self,
        table: str,
        args: Collection[str],
        data: Iterable[dict[str, Any]],
    ) -> None:
        self.create(table, args)
        self.insert(
            table,
            (tuple(row.values()) for row in data),  # noqa
            n_values=len(args),
        )

    def execute(
        self,
        query: _Composed,
        args: Iterable[str] | None = None,
    ) -> None:
        assert isinstance(self.connection, psycopg2.extras.DictConnection)
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        try:
            self.cursor.execute(query, args)
        except self.Error:
            self._reconnect_cursor()
            try:
                self.cursor.execute(query, args)
            except self.Error:
                self.connection.rollback()
                raise
        self.query = self.cursor.query
        assert isinstance(self.query, bytes)
        if self.query.split()[0].upper() != b"SELECT":
            self.connection.commit()

    def fetch(
        self,
        query: _Composed,
        args: Iterable[str] | None = None,
    ) -> Iterator[psycopg2.extras.DictRow]:
        self.execute(query, args)
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        yield from self.cursor

    def index(self, table: str, field: str, unique: bool = False) -> None:
        self.execute(
            self.compose(
                f"CREATE{' UNIQUE ' if unique else ' '}INDEX " "{} ON {} ({})",
                sql.Identifier(field),
                sql.Identifier(table),
                sql.Identifier(field),
            )
        )

    def insert(
        self,
        table: str,
        args: Sequence[Sized] | Iterable[Iterable[Any]],
        n_values: int | None = None,
        ignore: bool = False,
    ) -> None:
        assert isinstance(self.connection, psycopg2.extras.DictConnection)
        assert isinstance(self.cursor, psycopg2.extras.DictCursor)
        if n_values:
            pass
        elif isinstance(args, Sequence) and isinstance(args[0], Sized):
            n_values = len(args[0])
        elif not n_values:
            raise PgSqlError(
                "Could not read number of values from args; provide n_values."
            )
        composed = self.compose(
            "INSERT INTO {} VALUES ({})"
            f"{' ON CONFLICT DO NOTHING' if ignore else ''}",
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Placeholder() * n_values),
        )
        try:
            self.cursor.executemany(composed, args)
        except self.Error:
            self.connection.rollback()
            raise
        self.connection.commit()

    def select(
        self,
        query: str,
        *args: Any,
        **kwargs: Any,
    ) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(
            self.compose(query, *map(sql.Identifier, args + tuple(kwargs.keys()))),
            tuple(kwargs.values()),
        )

    def select_all(self, table: str) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(self.compose("SELECT * FROM {}", sql.Identifier(table)))

    def truncate(self, table: str) -> None:
        self.execute(self.compose("TRUNCATE TABLE {} ", sql.Identifier(table)))


class Composed(_Composed):
    def execute(self) -> None:
        """Execute a composed query."""
