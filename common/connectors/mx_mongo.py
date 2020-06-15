"""Connect to Matrixian's MongoDB databases."""
from typing import List, Union
from urllib.parse import quote_plus

from pymongo.database import Collection, Database
from pymongo.errors import ServerSelectionTimeoutError
from pymongo.mongo_client import MongoClient
from pymongo.operations import InsertOne, UpdateOne, UpdateMany

from ..exceptions import MongoDBError


class MongoDB(MongoClient):
    """Client for MongoDB. Uses MongoClient as superclass."""

    def __new__(cls,
                database: str = None,
                collection: str = None,
                host: str = None,
                client: bool = False,
                **kwargs
                ) -> Union[MongoClient, Database, Collection]:
        """Client for MongoDB

        Usage:
            client = MongoDB(client=True)
            db = MongoDB("cdqc")
            coll = MongoDB("cdqc", "person_data")
            coll = MongoDB("cdqc.person_data")
            coll = MongoDB()["cdqc"]["person_data"]
        """
        if kwargs.pop("local", False) or host == "localhost":
            uri = "mongodb://localhost"
        else:
            if collection and not database:
                raise MongoDBError("Please provide a database name as well.")
            if not host:
                host = "dev"
                if database:
                    if "addressvalidation" in database:
                        host = "address"
                    elif "production" in database:
                        host = "prod"
            elif host == "stg":
                raise MongoDBError("Staging database is not used anymore.")
            if not client and not database:
                database = "admin"
            hosts = {
                "address": "MX_MONGO_ADDR",
                "dev": "MX_MONGO_DEV",
                "prod": "MX_MONGO_PROD",
            }
            if host not in hosts:
                raise MongoDBError(f"Host `{host}` not recognized")
            host = hosts[host]
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

        mongo_client = MongoClient(host=uri, connectTimeoutMS=None)
        if database:
            if "." in database:
                database, collection = database.split(".")
            if collection:
                collection = mongo_client.__getattr__(database).__getattr__(collection)
                cls.test_connection(collection)
                return collection
            return mongo_client.__getattr__(database)
        return mongo_client

    @staticmethod
    def test_connection(collection: Collection):
        try:
            collection.find_one()
        except ServerSelectionTimeoutError as e:
            raise MongoDBError(
                "Are you contected with a Matrixian network?") from e

    @staticmethod
    def InsertOne(document):  # noqa
        return InsertOne(document)

    @staticmethod
    def UpdateOne(filter, update, upsert=False, collation=None, array_filters=None):  # noqa
        return UpdateOne(filter, update, upsert, collation, array_filters)

    @staticmethod
    def UpdateMany(filter, update, upsert=False, collation=None, array_filters=None):  # noqa
        return UpdateMany(filter, update, upsert, collation, array_filters)

    def find_last(self) -> dict:
        """Return the last document in a collection.

        Usage::
            from common.connectors import MongoDB
            db = MongoDB("dev_peter.person_data_20190716")
            doc = MongoDB.find_last(db)
            print(doc)
        """
        if isinstance(self, Collection):
            return next(self.find().sort([("_id", -1)]).limit(1))

    def find_duplicates(self) -> List[dict]:
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
