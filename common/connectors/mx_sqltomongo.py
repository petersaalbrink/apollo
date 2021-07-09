from __future__ import annotations

__all__ = (
    "MappingsBase",
    "SQLtoMongo",
)

from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import suppress
from typing import Any, NewType

from pandas import DataFrame, read_sql
from pymongo import IndexModel, UpdateMany, UpdateOne
from pymongo.errors import OperationFailure
from sqlalchemy import create_engine
from tqdm import tqdm

from ..connectors.mx_email import EmailClient
from ..connectors.mx_mongo import MongoDB, MxCollection
from ..connectors.mx_mysql import MySQLClient
from ..exceptions import ConnectorError


class MappingsBase(ABC):
    @abstractmethod
    def document(self, d: dict[str, Any]) -> dict[str, Any]:
        pass

    @abstractmethod
    def delete(self, d: dict[str, Any]) -> str:
        pass


Mappings = NewType("Mappings", MappingsBase)


class SQLtoMongo:
    def __init__(
        self,
        *,
        mongo_database: str,
        mongo_collection: str,
        sql_database: str,
        sql_table: str,
        mappings: Mappings | None = None,
        **kwargs: Any,
    ):
        """Create an object to perform MySQL-to-MongoDB operations.

        Provide a MySQL from database and table, and a MongoDB to database
        and collection. Additionally, provide a mappings object, which should
        at least have a :meth: `Mappings.document`"""
        self.coll = MongoDB(
            database=mongo_database,
            collection=mongo_collection,
        )
        self.sql = MySQLClient(
            database=sql_database,
            table=sql_table,
        )
        config = self.sql.__dict__["_MySQLClient__config"]
        conn_args = {
            k: v for k, v in config.items() if k in ["ssl_ca", "ssl_cert", "ssl_key"]
        }
        self.engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{sql_database}",
            connect_args={**conn_args, "buffered": False},
        )
        self.mappings = mappings
        self.query: str | None = None
        self.matched_count = (
            self.number_of_insertions
        ) = self.number_of_updates = self.number_of_deletions = 0
        self.chunksize = kwargs.pop("chunksize", 1_000)
        self.date_columns = kwargs.pop("date_columns", None)
        self.index_columns = kwargs.pop("index_columns", None)

    def create_indexes(self, names: list[str]) -> None:
        """Create indexes in the MongoDB collection.

        The name of the (nested) field is set as the index name.
        """
        assert isinstance(self.coll, MxCollection)
        indexes = [
            IndexModel([(name, 1)], name=name.split(".")[-1])
            for name in names
            if name.split(".")[-1] not in self.coll.index_information().keys()
        ]
        with suppress(OperationFailure):
            self.coll.create_indexes(indexes=indexes)

    def delete(
        self,
        *,
        filter: dict[str, Any] | None = None,
        field: str | None = None,
        preprocessing: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        if (
            (filter and field)
            or (filter and preprocessing)
            or (not filter and not field)
        ):
            raise ConnectorError("Use filter OR field/processing.")
        assert isinstance(self.coll, MxCollection)
        if filter:
            result = self.coll.delete_many(filter=filter)
            self.number_of_deletions += result.deleted_count
        else:
            assert isinstance(self.mappings, MappingsBase)
            for chunk in self.generator_df:
                if preprocessing:
                    chunk = preprocessing(chunk)
                chunk = [self.mappings.delete(d) for d in chunk.to_dict("records")]
                result = self.coll.delete_many(filter={field: {"$in": chunk}})
                self.number_of_deletions += result.deleted_count

    def _set_session_variables(self) -> None:
        # We set these session variables to avoid error 2013 (Lost connection)
        for var, val in (
            ("MAX_EXECUTION_TIME", "31536000000"),  # ms, can be higher
            # ("CONNECT_TIMEOUT", "31536000"),  # s, this is the maximum
            ("WAIT_TIMEOUT", "31536000"),  # s, this is the maximum
            ("INTERACTIVE_TIMEOUT", "31536000"),  # s, can be higher
            ("NET_WRITE_TIMEOUT", "31536000"),  # s, can be higher
        ):
            self.engine.execute(f"SET SESSION {var}={val}")

    @property
    def generator_df(self) -> DataFrame:
        self._set_session_variables()
        try:
            return read_sql(
                sql=self.query,
                con=self.engine,
                chunksize=self.chunksize,
                parse_dates=self.date_columns,
                index_col=self.index_columns,
            )
        except Exception as e:
            if not self.query:
                raise ConnectorError("Use `.set_query()` to set a query first.") from e
            raise

    def insert(
        self,
        *,
        preprocessing: Callable[[DataFrame], DataFrame] | None = None,
    ) -> None:
        """Insert new documents from MySQL into MongoDB.

        Documents are selected from MySQL using :attr: `SQLtoMongo.query`,
        and then mapped using :attr: `SQLtoMongo.mappings.document`.
        This assumes the :class: `Mappings` object has a :meth: `Mappings.document`.
        """
        assert isinstance(self.coll, MxCollection)
        assert isinstance(self.mappings, MappingsBase)
        for chunk in self.generator_df:
            if preprocessing:
                chunk = preprocessing(chunk)
            chunk = [self.mappings.document(d) for d in chunk.to_dict("records")]
            result = self.coll.insert_many(chunk)
            self.number_of_insertions += len(result.inserted_ids)

    def notify(
        self,
        to_address: list[str] | str,
        title: str = "Update succeeded",
        name: str = "",
    ) -> None:
        if name:
            name = f"{name}\n\n"
        total = sum(
            self.__getattribute__(n) for n in dir(self) if n.startswith("number")
        )
        EmailClient().send_email(
            to_address=to_address,
            subject=title,
            message=f"MongoDB update ran successfully!\n\n"
            f"{name}"
            f"Number of matched documents: {self.matched_count}\n"
            f"Number of updated documents: {self.number_of_updates}\n"
            f"Number of deleted documents: {self.number_of_deletions}\n"
            f"Number of inserted documents: {self.number_of_insertions}\n"
            f"Total number of documents affected: {total}",
        )

    def set_query(
        self,
        *,
        query_name: str | None = None,
        query: str | None = None,
    ) -> None:
        """Set the query for the MySQL SELECT operation.

        You can set a query directly via :param query:.
        If it is None, the query will default to all rows and all columns.
        """
        if query_name:
            raise ConnectorError("Using `query_name` is deprecated.")
        if query:
            self.query = query
        else:
            self.query = f"SELECT * FROM {self.sql.database}.{self.sql.table_name}"

    def update(
        self,
        *,
        filter: Callable[[dict[str, Any]], dict[str, Any]],
        update: Callable[[dict[str, Any]], dict[str, Any]],
        preprocessing: Callable[[DataFrame], DataFrame] | None = None,
        progress_bar: bool = False,
        update_cls: type[UpdateMany] | type[UpdateOne] = UpdateOne,  # noqa
        upsert: bool = False,
    ) -> None:
        assert isinstance(self.coll, MxCollection)
        for chunk in tqdm(self.generator_df, disable=not progress_bar):
            if preprocessing:
                chunk = preprocessing(chunk)
            chunk = [
                update_cls(
                    filter(d),
                    update(d),
                    upsert=upsert,
                )
                for d in chunk.to_dict("records")
            ]
            result = self.coll.bulk_write(requests=chunk)
            self.matched_count += result.matched_count
            self.number_of_updates += result.modified_count
