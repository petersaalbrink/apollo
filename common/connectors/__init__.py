"""This module contains database connectors for Matrixian Group.

Connect to MySQL with MySQLClient
Connect to MongoDB with MongoDB
Connect to Elasticsearch with ESClient

There is also an EmailClient, which can be used to send emails.

There are two alternative connectors for MySQL: PandasSQL and SQLClient

Finally, there is a SQLtoMongo class for moving data from MySQL to MongoDB
"""
from importlib import import_module

__mapping__ = {
    "mx_elastic": "ESClient",
    "mx_email": "EmailClient",
    "mx_mongo": "MongoDB",
    "mx_mysql": "MySQLClient",
    "mx_pandassql": "PandasSQL",
    "mx_sqlalchemy": "SQLClient",
    "mx_sqltomongo": "SQLtoMongo",
}
__modules__ = list(__mapping__)
__all__ = list(__mapping__.values()) + __modules__


def __getattr__(name):
    if name in __modules__:
        return import_module(f".{name}", __name__)
    for module, symbol in __mapping__.items():
        if name == symbol:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
