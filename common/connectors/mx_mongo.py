"""Connect to Matrixian's MongoDB databases."""

from __future__ import annotations

__all__ = (
    "Count",
    "MongoDB",
)

from collections import namedtuple
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
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

Count = namedtuple("Count", ("db", "es"))


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
            from common.connectors.mx_mongo import MongoDB
            db = MongoDB("cdqc.person_data")
            doc = MongoDB.find_last(db)
            print(doc)
        """
        return next(self.find().sort([("_id", -1)]).limit(1))

    def find_duplicates(self) -> list[dict]:
        """Return duplicated documents in a collection.

        Usage::
            from common.connectors.mx_mongo import MongoDB
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

    def remove_duplicates(
            self,
            field: str,
            use_tqdm: bool = False,
    ) -> int:
        """Remove duplicated documents in a collection, based on `field`.

        Provide `field` as a dot-separated string for nested fields.

        Returns the number of deleted documents.

        Usage::
            from common.connectors.mx_mongo import MongoDB
            coll = MongoDB("dev_realestate.real_estate_v10")
            coll.remove_duplicates("address.identification.addressId")
        """
        from ..handlers import tqdm
        from ..parsers import flatten

        bar = tqdm(total=self.estimated_document_count(), disable=not use_tqdm)
        count = 0
        lock = Lock()

        def count_and_delete(doc):
            n_docs = self.count_documents({field: flatten(doc)[field]})
            if n_docs > 1:
                self.delete_one({"_id": doc["_id"]})
                nonlocal count
                with lock:
                    count += 1
                    bar.update(n_docs)
            else:
                with lock:
                    bar.update(n_docs)

        with ThreadPoolExecutor() as executor:
            for future in as_completed(
                    executor.submit(count_and_delete, d) for d in self.find({}, {field: True})
            ):
                future.result()
        bar.close()
        return count

    def insert_many(self, documents, ordered=True, bypass_document_validation=False, session=None):
        if not documents:
            raise MongoDBError("Provide non-empty documents.")
        elif isinstance(documents, list) and isinstance(documents[0], dict):
            # If `documents` is a list, we have to do the type and key checking only once
            if documents[0].get("geometry"):
                documents = [self.correct_geoshape(doc, "geometry") for doc in documents]
            elif documents[0].get("location"):
                documents = [self.correct_geoshape(doc, "location") for doc in documents]
        elif isinstance(documents, Iterable):
            # Otherwise do it doc-wise but lazily
            documents = (
                self.correct_geoshape(doc, "geometry")
                if (isinstance(doc, dict) and doc.get("geometry"))
                else (
                    self.correct_geoshape(doc, "location")
                    if (isinstance(doc, dict) and doc.get("location"))
                    else doc
                ) for doc in documents
            )
        super().insert_many(documents, ordered, bypass_document_validation, session)

    def insert_one(self, document, bypass_document_validation=False, session=None):
        if isinstance(document, dict) and document.get("geometry"):
            document = self.correct_geoshape(document)
        super().insert_one(document, bypass_document_validation, session)

    @staticmethod
    def correct_geoshape(doc: dict, key: str = "geometry") -> dict:

        # TODO: add to InsertOne and InsertMany
        # TODO: add to update_one, update_many, UpdateOne, UpdateMany (in the future)

        import shapely.geometry

        geom_cls = getattr(shapely.geometry, doc[key]["type"])

        if geom_cls is shapely.geometry.MultiPolygon:
            geom_doc = geom_cls([shapely.geometry.Polygon(x[0]) for x in doc[key]["coordinates"]])
        elif geom_cls is shapely.geometry.Polygon:
            geom_doc = geom_cls(doc[key]["coordinates"][0])
        else:
            return doc

        if not geom_doc.is_valid:
            doc[key] = shapely.geometry.mapping(geom_doc.buffer(0))

        return doc

    def es(self) -> tuple[int, int]:
        """Returns a named two-tuple with the document count
        of this collection and the corresponding Elasticsearch index."""
        from .mx_elastic import ESClient
        return Count(self.estimated_document_count(), ESClient(self.full_name).count())


class MongoDB:
    """Factory for a Matrixian MongoDB client, database, or collection object."""

    def __new__(cls,
                database: str = None,
                collection: str = None,
                host: str = None,
                client: bool = False,
                **kwargs
                ) -> Union[MxClient, MxDatabase, MxCollection]:
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
