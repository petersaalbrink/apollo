from __future__ import annotations

__all__ = "PgSql",

from collections.abc import Collection, Iterable, Iterator, Sequence, Sized
from functools import partial
from typing import Any, Optional, Union

import psycopg2.extras
from psycopg2 import sql

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
    Error = psycopg2.Error
    sql = sql

    def __init__(self, database: str, server_side_cursor: bool = False):
        self.connection: Optional[psycopg2.extras.DictConnection] = None
        self.cursor: Optional[psycopg2.extras.DictCursor] = None
        self.database = database
        self.server_side_cursor = server_side_cursor
        self.query: Optional[bytes] = None

    def __enter__(self) -> PgSql:
        return self.connect()

    def __exit__(self, *args, **kwargs):
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

    def close(self):
        try:
            self.cursor.close()
        except self.Error:
            pass
        self.connection.close()

    def _connect_cursor(self):
        if self.server_side_cursor:
            self.cursor = self.connection.cursor("NamedCursor").__enter__()
        else:
            self.cursor = self.connection.cursor().__enter__()

    def _reconnect_cursor(self):
        self.cursor.close()
        self._connect_cursor()

    def compose(self, query: str, *args, **kwargs) -> sql.Composed:
        composed = sql.SQL(query).format(*args, **kwargs)
        composed.execute = partial(self.execute, composed)
        return composed

    def create(self, table: str, args: Iterable[str]):
        self.execute(
            self.compose("CREATE TABLE {} ", sql.Identifier(table))
            + sql.SQL("(") + sql.Composed(map(sql.SQL, args)).join(", ") + sql.SQL(")")
        )

    def create_and_insert(self, table: str, args: Collection[str], data: Iterable[dict]):
        self.create(table, args)
        self.insert(table, (tuple(row.values()) for row in data), n_values=len(args))

    def execute(self, query: sql.Composed, args: Iterable[str] = None):
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
        if self.query.split()[0].upper() != b"SELECT":
            self.connection.commit()

    def fetch(self, query: sql.Composed, args: Iterable[str] = None) -> Iterator[psycopg2.extras.DictRow]:
        self.execute(query, args)
        yield from self.cursor

    def index(self, table: str, field: str, unique: bool = False):
        self.execute(self.compose(
            f"CREATE{' UNIQUE ' if unique else ' '}INDEX " "{} ON {} ({})",
            sql.Identifier(field), sql.Identifier(table), sql.Identifier(field),
        ))

    def insert(
            self,
            table: str,
            args: Union[Sequence[Sized[Any]], Iterable[Iterable[Any]]],
            n_values: int = None,
            ignore: bool = False,
    ):
        if isinstance(args, Sequence) and isinstance(args[0], Sized):
            n_values = len(args[0])
        elif not n_values:
            raise PgSqlError("Could not read number of values from args; provide n_values.")
        composed = self.compose(
            "INSERT INTO {} VALUES ({})" f"{' ON CONFLICT DO NOTHING' if ignore else ''}",
            sql.Identifier(table), sql.SQL(", ").join(sql.Placeholder() * n_values),
        )
        try:
            self.cursor.executemany(composed, args)
        except self.Error:
            self.connection.rollback()
            raise
        self.connection.commit()

    def select(self, query: str, *args, **kwargs) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(
            self.compose(query, *map(sql.Identifier, args + tuple(kwargs.keys()))),
            tuple(kwargs.values()),
        )

    def select_all(self, table: str) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(self.compose("SELECT * FROM {}", sql.Identifier(table)))

    def truncate(self, table: str):
        self.execute(self.compose("TRUNCATE TABLE {} ", sql.Identifier(table)))
