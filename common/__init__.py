"""Common classes and functions for the Matrixian Group Data Team.

Read the documentation on Confluence:
https://matrixiangroup.atlassian.net/wiki/spaces/DBR/pages/1584693297/common+classes+mx
"""

from __future__ import annotations

__all__ = (
    "Address",
    "ApiError",
    "Checks",
    "Cleaner",
    "CommonError",
    "ConnectorError",
    "Credentials",
    "Data",
    "DataError",
    "DistributionPlot",
    "ESClient",
    "ESClientError",
    "EmailClient",
    "FileTransfer",
    "FileTransferError",
    "FunctionTimer",
    "Log",
    "Match",
    "MatchError",
    "MatchMetrics",
    "MongoDB",
    "MongoDBError",
    "MySQLClient",
    "MySQLClientError",
    "Names",
    "NamesData",
    "NoMatch",
    "NoMatch",
    "PandasSQL",
    "ParseError",
    "Person",
    "PersonData",
    "PersonsError",
    "PhoneApiError",
    "PlotMx",
    "Query",
    "RadarPlot",
    "RequestError",
    "SQLClient",
    "SQLtoMongo",
    "Statistics",
    "ThreadSafeIterator",
    "TicToc",
    "Timeout",
    "Timer",
    "TimerError",
    "ZipData",
    "ZipDataError",
    "api",
    "assert_never",
    "calculate_bandwith",
    "change_secret",
    "check_email",
    "check_phone",
    "chunker",
    "connectors",
    "count_bytes",
    "csv_read",
    "csv_read_from_zip",
    "csv_write",
    "customer_communication",
    "data_delivery_tool",
    "dateformat",
    "download_file",
    "drop_empty_columns",
    "env",
    "exceptions",
    "expand",
    "flatten",
    "get",
    "get_logger",
    "get_proxies",
    "get_secret",
    "get_session",
    "get_token",
    "getenv",
    "google_sign_url",
    "handlers",
    "keep_trying",
    "levenshtein",
    "parse",
    "parse_name",
    "parsers",
    "partition",
    "persondata",
    "persons",
    "pip_upgrade",
    "platform",
    "plot_stacked_bar",
    "post",
    "preload_db",
    "read_json",
    "read_json_line",
    "read_txt",
    "remove_adjacent",
    "request",
    "requests",
    "reverse_geocode",
    "secrets",
    "send_email",
    "set_alpha",
    "set_clean_email",
    "set_must_have_address",
    "set_population_size",
    "set_search_size",
    "set_years_ago",
    "thread",
    "threadsafe",
    "timer",
    "tqdm",
    "trange",
    "validate",
    "visualizations",
)

from importlib import import_module
from .etc.version import (
    __version__,
)

_module_mapping = {
    "api": [
        "check_email",
        "check_phone",
        "parse",
        "validate",
    ],
    "connectors": [
        "ESClient",
        "EmailClient",
        "MongoDB",
        "MySQLClient",
        "PandasSQL",
        "SQLClient",
        "SQLtoMongo",
    ],
    "customer_communication": [
        "data_delivery_tool",
    ],
    "env": [
        "getenv",
    ],
    "exceptions": [
        "ApiError",
        "CommonError",
        "ConnectorError",
        "DataError",
        "ESClientError",
        "FileTransferError",
        "MatchError",
        "MongoDBError",
        "MySQLClientError",
        "NoMatch",
        "ParseError",
        "PersonsError",
        "PhoneApiError",
        "RequestError",
        "Timeout",
        "TimerError",
        "ZipDataError",
    ],
    "handlers": [
        "FunctionTimer",
        "Log",
        "TicToc",
        "Timer",
        "ZipData",
        "assert_never",
        "chunker",
        "csv_read",
        "csv_read_from_zip",
        "csv_write",
        "get_logger",
        "keep_trying",
        "pip_upgrade",
        "read_json",
        "read_json_line",
        "read_txt",
        "remove_adjacent",
        "send_email",
        "timer",
        "tqdm",
        "trange",
    ],
    "parsers": [
        "Checks",
        "count_bytes",
        "dateformat",
        "drop_empty_columns",
        "expand",
        "flatten",
        "levenshtein",
        "partition",
        "reverse_geocode",
    ],
    "persondata": [
        "Cleaner",
        "Data",
        "MatchMetrics",
        "NamesData",
        "NoMatch",
        "PersonData",
    ],
    "persons": [
        "Address",
        "Match",
        "Names",
        "Person",
        "Query",
        "Statistics",
        "parse_name",
        "preload_db",
        "set_alpha",
        "set_clean_email",
        "set_must_have_address",
        "set_population_size",
        "set_search_size",
        "set_years_ago",
    ],
    "platform": [
        "FileTransfer",
    ],
    "requests": [
        "ThreadSafeIterator",
        "calculate_bandwith",
        "download_file",
        "google_sign_url",
        "get",
        "get_proxies",
        "get_session",
        "post",
        "request",
        "thread",
        "threadsafe",
    ],
    "secrets": [
        "Credentials",
        "change_secret",
        "get_secret",
        "get_token",
    ],
    "visualizations": [
        "DistributionPlot",
        "PlotMx",
        "RadarPlot",
        "plot_stacked_bar",
    ],
}


def __getattr__(name):
    if name in _module_mapping:
        return import_module(f".{name}", __name__)
    for module, symbols in _module_mapping.items():
        if name in symbols:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
