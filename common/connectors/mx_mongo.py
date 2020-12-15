"""Connect to Matrixian's MongoDB databases."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Union
from urllib.parse import quote_plus

from pymongo.database import Collection, Database
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne, UpdateOne, UpdateMany

from ..exceptions import MongoDBError

_hosts = {
    "address": "MX_MONGO_ADDR",
    "cdqc": "MX_MONGO_CDQC",
    "dev": "MX_MONGO_DEV",
    "prod": "MX_MONGO_PROD",
}


class MxClient(MongoClient):
    ...


class MxDatabase(Database):
    ...


class MxCollection(Collection):
    def test_connection(self):
        """Test connection by getting a document."""
        try:
            self.find_one({}, {"_id": True})
        except ServerSelectionTimeoutError as e:
            raise MongoDBError("Are you contected with a Matrixian network?") from e

    def find_last(self) -> dict:
        """Return the last document in a collection.

        Usage::
            from common.connectors import MongoDB
            db = MongoDB("cdqc.person_data")
            doc = MongoDB.find_last(db)
            print(doc)
        """
        return next(self.find().sort([("_id", -1)]).limit(1))

    def find_duplicates(self) -> list[dict]:
        """Return duplicated documents in a collection.

        Usage::
            from common.connectors import MongoDB
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

    def insert_many(self, documents, ordered=True, bypass_document_validation=False, session=None):
        if not documents:
            raise MongoDBError("Provide non-empty documents.")
        elif isinstance(documents, list):
            # If `documents` is a list, we have to do the type and key checking only once
            if isinstance(documents[0], dict) and documents[0].get("geometry"):
                documents = [self.correct_geoshape(doc) for doc in documents]
        elif isinstance(documents, Iterable):
            # Otherwise do it doc-wise but lazily
            documents = (
                self.correct_geoshape(doc)
                if (isinstance(doc, dict) and doc.get("geometry"))
                else doc for doc in documents
            )
        super().insert_many(documents, ordered, bypass_document_validation, session)

    def insert_one(self, document, bypass_document_validation=False, session=None):
        if isinstance(document, dict) and document.get("geometry"):
            document = self.correct_geoshape(document)
        super().insert_many(document, bypass_document_validation, session)

    @staticmethod
    def correct_geoshape(doc: dict) -> dict:
        import shapely.geometry

        geom_cls = getattr(shapely.geometry, doc["geometry"]["type"])

        if geom_cls is shapely.geometry.MultiPolygon:
            geom_doc = geom_cls([
                shapely.geometry.Polygon(x[0]) for x in doc["geometry"]["coordinates"]])
        elif geom_cls is shapely.geometry.Polygon:
            geom_doc = geom_cls(doc["geometry"]["coordinates"][0])
        else:
            return doc

        if not geom_doc.is_valid:
            doc["geometry"] = shapely.geometry.mapping(geom_doc.buffer(0))

        return doc


class MongoDB:
    """Client for Matrixian's MongoDB databases.

    Inherits from the official `MongoClient`.
    """

    def __new__(cls,
                database: str = None,
                collection: str = None,
                host: str = None,
                client: bool = False,
                **kwargs
                ) -> Union[MxClient, MxDatabase, MxCollection]:
        """Client for Matrixian's MongoDB databases.

        Creates a MongoClient, Database, or Collection object.

        Usage::
            # Create a MongoClient object
            client = MongoDB(client=True)

            # Create a Database object
            db = MongoDB("cdqc")

            # Create a Collection object
            coll = MongoDB("cdqc", "person_data")  # first method
            coll = MongoDB("cdqc.person_data")  # second method
            coll = MongoDB()["cdqc"]["person_data"]  # third method
        """
        if collection and not database:
            raise MongoDBError("Please provide a database name as well.")
        elif database and "." in database:
            if collection:
                raise MongoDBError("Provide database.collection paired or individually, not both.")
            database, collection = database.split(".")
        elif not database and not collection and not client:
            client = True

        if kwargs.pop("local", False) or host == "localhost":
            uri = "mongodb://localhost"
        else:
            if not host:
                host = "dev"
                if database:
                    if "addressvalidation" in database:
                        host = "address"
                    elif "production" in database:
                        host = "prod"
                    elif database.startswith("cdqc"):
                        host = "cdqc"
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
                raise MongoDBError(f"Make sure a host is configured for variable"
                                   f" name '{envv}' in file '{envfile}'")
            uri = f"mongodb://{quote_plus(usr)}:{quote_plus(pwd)}@{host}"

        mongo_client = MxClient(host=uri, connectTimeoutMS=None)
        if client:
            return mongo_client
        mongo_db = MxDatabase(client=mongo_client, name=database)
        if collection:
            mongo_coll = MxCollection(database=mongo_db, name=collection)
            mongo_coll.test_connection()
            return mongo_coll
        else:
            return mongo_db

    @staticmethod
    def InsertOne(document):  # noqa
        """Convience method for `InsertOne`."""
        return InsertOne(document)

    @staticmethod
    def UpdateOne(filter, update, upsert=False, collation=None, array_filters=None):  # noqa
        """Convience method for `UpdateOne`."""
        return UpdateOne(filter, update, upsert, collation, array_filters)

    @staticmethod
    def UpdateMany(filter, update, upsert=False, collation=None, array_filters=None):  # noqa
        """Convience method for `UpdateMany`."""
        return UpdateMany(filter, update, upsert, collation, array_filters)
