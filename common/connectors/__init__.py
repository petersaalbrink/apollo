"""This module contains database connectors for Matrixian Group.

Connect to MySQL with MySQLClient
Connect to MongoDB with MongoDB
Connect to Elasticsearch with ESClient

There is also an EmailClient, which can be used to send emails.

There are two alternative connectors for MySQL: PandasSQL and SQLClient

Finally, there is a SQLtoMongo class for moving data from MySQL to MongoDB
"""

from __future__ import annotations

__all__ = (
    "ESClient",
    "EmailClient",
    "MongoDB",
    "MySQLClient",
    "PandasSQL",
    "PgSql",
    "SQLClient",
    "SQLtoMongo",
)

from importlib import import_module

_module_mapping = {
    "mx_elastic": "ESClient",
    "mx_email": "EmailClient",
    "mx_mongo": "MongoDB",
    "mx_mysql": "MySQLClient",
    "mx_pandassql": "PandasSQL",
    "mx_postgres": "PgSql",
    "mx_sqlalchemy": "SQLClient",
    "mx_sqltomongo": "SQLtoMongo",
}


def __getattr__(name):
    if name in _module_mapping:
        return import_module(f".{name}", __name__)
    for module, symbol in _module_mapping.items():
        if name == symbol:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
