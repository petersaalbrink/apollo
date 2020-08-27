from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Callable, List, NewType
from tqdm import tqdm

from pandas import read_sql
from pymongo import IndexModel
from pymongo.errors import OperationFailure
from sqlalchemy import create_engine

from ..connectors import EmailClient, MongoDB, MySQLClient
from ..exceptions import ConnectorError


class MappingsBase(ABC):
    @abstractmethod
    def document(self, d: dict) -> dict:
        pass

    @abstractmethod
    def delete(self, d: dict) -> str:
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
            mappings: Mappings = None,
            **kwargs,
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
        conn_args = {k: v for k, v in config.items() if k in ["ssl_ca", "ssl_cert", "ssl_key"]}
        self.engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{sql_database}",
            connect_args={**conn_args, "buffered": False})
        self.mappings = mappings
        self.query = None
        self.matched_count = self.number_of_insertions = self.number_of_updates = self.number_of_deletions = 0
        self.chunksize = kwargs.pop("chunksize", 1_000)
        self.date_columns = kwargs.pop("date_columns", None)
        self.index_columns = kwargs.pop("index_columns", None)

    def create_indexes(self, names: List[str]):
        """Create indexes in the MongoDB collection.

        The name of the (nested) field is set as the index name.
        """
        indexes = [
            IndexModel([(name, 1)], name=name.split('.')[-1])
            for name in names
            if name.split(".")[-1] not in self.coll.index_information().keys()
        ]
        with suppress(OperationFailure):
            self.coll.create_indexes(indexes=indexes)

    def delete(
            self,
            *,
            filter: dict = None,
            field: str = None,
            preprocessing: Callable = None,
    ):
        if (filter and field) or (filter and preprocessing) or (not filter and not field):
            raise ConnectorError("Use filter OR field/processing.")
        if filter:
            result = self.coll.delete_many(filter=filter)
            self.number_of_deletions += result.deleted_count
        else:
            for chunk in self.generator_df:
                if preprocessing:
                    chunk = preprocessing(chunk)
                chunk = [self.mappings.delete(d) for d in chunk.to_dict("records")]
                result = self.coll.delete_many(filter={field: {"$in": chunk}})
                self.number_of_deletions += result.deleted_count

    @property
    def generator_df(self):
        try:
            return read_sql(
                sql=self.query,
                con=self.engine,
                chunksize=self.chunksize,
                parse_dates=self.date_columns,
                index_col=self.index_columns
            )
        except Exception as e:
            if not self.query:
                raise ConnectorError("Use `.set_query()` to set a query first.") from e
            raise

    def insert(
            self,
            *,
            preprocessing: Callable = None,
    ):
        """Insert new documents from MySQL into MongoDB.

        Documents are selected from MySQL using :attr: `SQLtoMongo.query`,
        and then mapped using :attr: `SQLtoMongo.mappings.document`.
        This assumes the :class: `Mappings` object has a :meth: `Mappings.document`.
        """
        for chunk in self.generator_df:
            if preprocessing:
                chunk = preprocessing(chunk)
            chunk = [self.mappings.document(d) for d in chunk.to_dict("records")]
            result = self.coll.insert_many(chunk)
            self.number_of_insertions += len(result.inserted_ids)

    def notify(self, to_address: str, title: str = "Update succeeded"):
        total = sum(self.__getattribute__(n) for n in dir(self) if n.startswith("number"))
        EmailClient().send_email(
            to_address=to_address,
            subject=title,
            message=f"MongoDB update ran successfully!\n\n"
                    f"Number of matched documents: {self.matched_count}\n"
                    f"Number of updated documents: {self.number_of_updates}\n"
                    f"Number of deleted documents: {self.number_of_deletions}\n"
                    f"Number of inserted documents: {self.number_of_insertions}\n"
                    f"Total number of documents affected: {total}"
        )

    def set_query(
            self,
            *,
            query_name: str = None,
            query: str = None,
    ):
        """Set the query for the MySQL SELECT operation.

        If :param query_name: is None, this will default to all rows and all columns.
        You can set a query directly via :param query:.
        You can also create a document in the `queries` collection in your database.
        The format for this should be::
            {
                "name": name,
                "query": query
            }
        """
        if query:
            self.query = query
        elif not query_name:
            self.query = f"SELECT * FROM {self.sql.database}.{self.sql.table_name}"
        else:
            try:
                self.query = MongoDB(f"{self.coll.database.name}.queries").find_one({"name": query_name})["query"]
            except KeyError as e:
                raise ConnectorError(f"Could not find query '{query_name}'") from e

    def update(
            self,
            *,
            filter: Callable,
            update: Callable,
            preprocessing: Callable = None,
            progress_bar: bool = False,
    ):
        for chunk in tqdm(self.generator_df, disable=not progress_bar):
            if preprocessing:
                chunk = preprocessing(chunk)
            chunk = [MongoDB.UpdateOne(
                filter(d),
                update(d),
            )
                for d in chunk.to_dict("records")
            ]
            result = self.coll.bulk_write(requests=chunk)
            self.matched_count += result.matched_count
            self.number_of_updates += result.modified_count
