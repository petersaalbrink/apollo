"""Connect to Matrixian's MongoDB databases."""

from __future__ import annotations

__all__ = (
    "Count",
    "MongoDB",
    "MxClient",
    "MxCollection",
    "MxDatabase",
)

from collections import namedtuple
from collections.abc import Generator, Iterable
from threading import Lock
from typing import Any
from urllib.parse import quote_plus

from bson import CodecOptions
from pymongo.database import Collection, Database
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne, UpdateMany, UpdateOne
from pymongo.results import InsertManyResult, InsertOneResult

from ..exceptions import MongoDBError

_hosts = {
    "address": "MX_MONGO_ADDR",
    "address_dev": "MX_MONGO_ADDR_DEV",
    "cdqc": "MX_MONGO_CDQC",
    "dev": "MX_MONGO_DEV",
    "prod": "MX_MONGO_PROD",
}

Count = namedtuple("Count", ("db", "es"))


class MxClient(MongoClient):
    ...


class MxDatabase(Database):
    ...


class MxCollection(Collection):
    def test_connection(self) -> None:
        """Test connection by getting a document."""
        try:
            self.find_one({}, {"_id": True})
        except ServerSelectionTimeoutError as e:
            raise MongoDBError("Are you contected with a Matrixian network?") from e

    def find_last(self) -> dict[str, Any]:
        """Return the last document in a collection.

        Usage::
            from apollo.connectors.mx_mongo import MongoDB
            db = MongoDB("cdqc.person_data")
            doc = MongoDB.find_last(db)
            print(doc)
        """
        return next(self.find().sort([("_id", -1)]).limit(1))

    def find_duplicates(self) -> list[dict[str, Any]]:
        """Return duplicated documents in a collection.

        Usage::
            from apollo.connectors.mx_mongo import MongoDB
            db = MongoDB("dev_peter.person_data_20190716")
            docs = MongoDB.find_duplicates(db)
            print(docs)
        """
        return list(
            self.aggregate(
                [
                    {"$unwind": "$birth"},
                    {"$unwind": "$address"},
                    {"$unwind": "$address.current"},
                    {
                        "$group": {
                            "_id": {
                                "lastname": "$lastname",
                                "dateOfRecord": "$dateOfRecord",
                                "birth": "$birth.date",
                                "address": "$address.current.postalCode",
                            },
                            "uniqueIds": {"$addToSet": "$_id"},
                            "count": {"$sum": 1},
                        }
                    },
                    {"$match": {"count": {"$gt": 1}}},
                ],
                allowDiskUse=True,
            )
        )

    def remove_duplicates(
        self,
        field: str,
        use_tqdm: bool = False,
    ) -> int:
        """Remove duplicated documents in a collection, based on `field`.

        Provide `field` as a dot-separated string for nested fields.

        Returns the number of deleted documents.

        Usage::
            from apollo.connectors.mx_mongo import MongoDB
            coll = MongoDB("dev_realestate.real_estate_v10")
            coll.remove_duplicates("address.identification.addressId")
        """
        from ..handlers import tqdm
        from ..parsers import flatten
        from ..requests import thread

        bar = tqdm(total=self.estimated_document_count(), disable=not use_tqdm)
        batch_size = 10_000
        count = 0
        lock = Lock()

        def count_and_delete(doc: dict[str, Any]) -> None:
            if self.count_documents({field: flatten(doc)[field]}) > 1:
                self.delete_one({"_id": doc["_id"]})
                nonlocal count
                with lock:
                    count += 1
                    bar.update()
            else:
                with lock:
                    bar.update()

        thread(
            function=count_and_delete,
            data=self.find({}, {field: True}).batch_size(batch_size),
            process_chunk_size=batch_size,
        )
        bar.close()
        return count

    def insert_many(
        self,
        documents: list[dict[str, Any]],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: Any = None,
    ) -> InsertManyResult:
        _documents: list[dict[str, Any]] | Generator[dict[str, Any], None, None]
        if isinstance(documents, list) and documents and isinstance(documents[0], dict):
            # If `documents` is a list, we have to do the type and key checking only once
            if documents[0].get("geometry"):
                _documents = (
                    self.correct_geoshape(doc, "geometry") for doc in documents
                )
            elif documents[0].get("location"):
                _documents = (
                    self.correct_geoshape(doc, "location") for doc in documents
                )
            else:
                _documents = documents
        elif isinstance(documents, Iterable):
            # Otherwise, do it doc-wise but lazily
            _documents = (
                self.correct_geoshape(doc, "geometry")
                if (isinstance(doc, dict) and doc.get("geometry"))
                else (
                    self.correct_geoshape(doc, "location")
                    if (isinstance(doc, dict) and doc.get("location"))
                    else doc
                )
                for doc in documents
            )
        else:
            raise MongoDBError("Provide non-empty documents.")
        return super().insert_many(
            _documents, ordered, bypass_document_validation, session
        )

    def insert_one(
        self,
        document: dict[str, Any],
        bypass_document_validation: bool = False,
        session: Any = None,
    ) -> InsertOneResult:
        if isinstance(document, dict) and document.get("geometry"):
            document = self.correct_geoshape(document)
        return super().insert_one(document, bypass_document_validation, session)

    @staticmethod
    def correct_geoshape(doc: dict[str, Any], key: str = "geometry") -> dict[str, Any]:

        # TODO: add to InsertOne and InsertMany
        # TODO: add to update_one, update_many, UpdateOne, UpdateMany (in the future)
        # TODO: add possibility for nested keys (e.g., key="geometry.geoPoint")

        import shapely.geometry

        try:
            geom_cls = getattr(shapely.geometry, doc[key]["type"])

            if geom_cls is shapely.geometry.MultiPolygon:
                geom_doc = geom_cls(
                    [shapely.geometry.Polygon(x[0]) for x in doc[key]["coordinates"]]
                )
            elif geom_cls is shapely.geometry.Polygon:
                geom_doc = geom_cls(doc[key]["coordinates"][0])
            else:
                return doc

            if not geom_doc.is_valid:
                doc[key] = shapely.geometry.mapping(geom_doc.buffer(0))

        except KeyError:
            pass

        return doc

    def es(self) -> tuple[int, int]:
        """Returns a named two-tuple with the document count
        of this collection and the corresponding Elasticsearch index."""
        from .mx_elastic import ESClient

        return Count(
            self.estimated_document_count(), ESClient(self.full_name.lower()).count()
        )


class MongoDB:
    """Factory for a Matrixian MongoDB client, database, or collection object."""

    def __new__(  # type: ignore
        cls,
        database: str | None = None,
        collection: str | None = None,
        host: str | None = None,
        client: bool = False,
        tz_aware: bool = False,
        **kwargs: Any,
    ) -> MxClient | MxDatabase | MxCollection:
        """Factory for Matrixian's MongoDB database access.

        Creates a MxClient, MxDatabase, or MxCollection object.

        Usage::
            # Create a MxClient object
            client = MongoDB(client=True)

            # Create a MxDatabase object
            db = MongoDB("cdqc")

            # Create a MxCollection object (pick one)
            coll = MongoDB("cdqc", "person_data")  # first method
            coll = MongoDB("cdqc.person_data")  # second method
            coll = MongoDB()["cdqc"]["person_data"]  # third method
        """
        if collection and not database:
            raise MongoDBError("Please provide a database name as well.")
        elif database and "." in database:
            if collection:
                raise MongoDBError(
                    "Provide database.collection paired or individually, not both."
                )
            database, collection = database.split(".")
        elif not database and not collection and not client:
            client = True

        if kwargs.pop("local", False) or host == "localhost":
            uri = "mongodb://localhost"
        else:
            if host == "dev" and database and database.startswith("addressvalidation"):
                host = "address_dev"
            elif not host:
                if database:
                    if database.startswith("addressvalidation"):
                        host = "address"
                    elif database.startswith("production"):
                        host = "prod"
                    elif database.startswith("cdqc"):
                        host = "cdqc"
                    else:
                        host = "dev"
                else:
                    host = "dev"
            elif host not in _hosts:
                raise MongoDBError(f"Host `{host}` not recognized")
            host = _hosts[host]
            from ..env import getenv
            from ..secrets import get_secret

            usr, pwd = get_secret(host)
            envv = f"{host}_IP"
            host = getenv(envv)
            if not host:
                from ..env import envfile

                raise MongoDBError(
                    f"Make sure a host is configured for variable"
                    f" name '{envv}' in file '{envfile}'"
                )
            uri = f"mongodb://{quote_plus(usr)}:{quote_plus(pwd)}@{host}"

        if tz_aware:
            from pendulum.tz import timezone

            codec_options = CodecOptions(
                tz_aware=True,
                tzinfo=timezone("Europe/Amsterdam"),
            )
        else:
            codec_options = None

        mongo_client = MxClient(host=uri, connectTimeoutMS=None)
        if client:
            return mongo_client
        mongo_db = MxDatabase(
            client=mongo_client, name=database, codec_options=codec_options
        )
        if collection:
            mongo_coll = MxCollection(database=mongo_db, name=collection)
            mongo_coll.test_connection()
            return mongo_coll
        else:
            return mongo_db

    @staticmethod
    def InsertOne(document: dict[str, Any]) -> InsertOne:  # noqa
        """Convience method for `InsertOne`."""
        return InsertOne(document)

    @staticmethod
    def UpdateOne(  # noqa
        filter: dict[str, Any],  # noqa
        update: dict[str, Any],
        upsert: bool = False,
        collation: Any = None,
        array_filters: list[dict[str, Any]] | None = None,
    ) -> UpdateOne:
        """Convience method for `UpdateOne`."""
        return UpdateOne(filter, update, upsert, collation, array_filters)

    @staticmethod
    def UpdateMany(  # noqa
        filter: dict[str, Any],  # noqa
        update: dict[str, Any],
        upsert: bool = False,
        collation: Any = None,
        array_filters: list[dict[str, Any]] | None = None,
    ) -> UpdateMany:
        """Convience method for `UpdateMany`."""
        return UpdateMany(filter, update, upsert, collation, array_filters)
