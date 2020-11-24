"""Common classes and functions for the Matrixian Group Data Team.

Read the documentation on Confluence:
https://matrixiangroup.atlassian.net/wiki/spaces/DBR/pages/1584693297/common+classes+mx
"""
from importlib import import_module
from .etc.version import (
    __version__,
)

__mapping__ = {
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
        "Timer",
        "TicToc",
        "ZipData",
        "chunker",
        "csv_write",
        "csv_read",
        "get_logger",
        "keep_trying",
        "pip_upgrade",
        "remove_adjacent",
        "send_email",
        "timer",
        "tqdm",
        "trange",
    ],
    "parsers": [
        "Checks",
        "dateformat",
        "drop_empty_columns",
        "expand",
        "flatten",
        "levenshtein",
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
        "set_alpha",
        "set_clean_email",
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
__modules__ = list(__mapping__)
__all__ = [symbol for symbols in __mapping__.values() for symbol in symbols] + __modules__


def __getattr__(name):
    if name in __modules__:
        return import_module(f".{name}", __name__)
    for module, symbols in __mapping__.items():
        if name in symbols:
            return getattr(import_module(f".{module}", __name__), name)
    raise ImportError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
