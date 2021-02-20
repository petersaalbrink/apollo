from __future__ import annotations

__all__ = "PgSql",

from collections.abc import Iterable, Iterator, Sequence
from typing import Any, Optional

import psycopg2.extras
from psycopg2 import sql

from ..secrets import get_secret


class PgSql:
    """Connector for PostgreSQL database.

    Example:
        from common.connectors.mx_postgres import PgSql
        with PgSql() as pg:
            data = list(pg.select_all("vgm_account"))

    Example:
        from common.connectors.mx_postgres import PgSql
        query = "SELECT * FROM {} WHERE {} LIKE %s"
        table, column, value = "vgm_building", "vgo_building", "WTC%"
        with PgSql() as pg:
            data = list(pg.select(query, table, **{column: value}))
    """
    def __init__(self, database: str = "vgm", server_side_cursor: bool = False):
        self.connection: Optional[psycopg2.extras.DictConnection] = None
        self.cursor: Optional[psycopg2.extras.DictCursor] = None
        self.database = database
        self.server_side_cursor = server_side_cursor
        self.query: Optional[bytes] = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        usr, pwd = get_secret("MX_PSQL")
        self.connection = psycopg2.connect(
            host="37.97.209.246",
            port=5432,
            database=self.database,
            user=usr,
            password=pwd,
            connection_factory=psycopg2.extras.DictConnection,
        )
        self._connect_cursor()
        return self

    def close(self):
        try:
            self.cursor.close()
        except psycopg2.InterfaceError:
            pass
        self.connection.close()

    def _connect_cursor(self):
        if self.server_side_cursor:
            self.cursor = self.connection.cursor("NamedCursor")
        else:
            self.cursor = self.connection.cursor()

    def _reconnect_cursor(self):
        self.cursor.close()
        self._connect_cursor()

    @staticmethod
    def compose(query: str, *args, **kwargs) -> sql.Composed:
        return sql.SQL(query).format(*args, **kwargs)

    def create(self, table: str, args: Sequence[str]):
        self.execute(
            self.compose("CREATE TABLE {} ", sql.Identifier(table))
            + sql.SQL("(") + sql.Composed(map(sql.SQL, args)).join(", ") + sql.SQL(")")
        )

    def create_and_insert(self, table: str, args: Sequence[str], data: Iterable[dict]):
        self.create(table, args)
        self.insert(table, [tuple(row.values()) for row in data])

    def execute(self, query: sql.Composed, args: Sequence[str] = None):
        try:
            self.cursor.execute(query, args)
        except (psycopg2.ProgrammingError, psycopg2.InterfaceError):
            self._reconnect_cursor()
            self.cursor.execute(query, args)
        self.query = self.cursor.query
        if self.query.split()[0].upper() != b"SELECT":
            self.connection.commit()

    def fetch(self, query: sql.Composed, args: Sequence[str] = None) -> Iterator[psycopg2.extras.DictRow]:
        self.execute(query, args)
        yield from self.cursor

    def insert(self, table: str, args: Sequence[Sequence[Any]]):
        self.cursor.executemany(self.compose(
            "INSERT INTO {} VALUES ({})", sql.Identifier(table),
            sql.SQL(", ").join(sql.Placeholder() * len(args[0]))
        ), args)
        self.connection.commit()

    def select(self, query: str, *args, **kwargs) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(
            self.compose(query, *map(sql.Identifier, args + tuple(kwargs.keys()))),
            tuple(kwargs.values()),
        )

    def select_all(self, table: str) -> Iterator[psycopg2.extras.DictRow]:
        yield from self.fetch(self.compose("SELECT * FROM {}", sql.Identifier(table)))
