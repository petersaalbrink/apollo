"""Common classes and functions for the Matrixian Group Data Team.

Read the documentation on Confluence:
https://matrixiangroup.atlassian.net/wiki/spaces/DBR/pages/1584693297/common+classes+mx
"""
from .address import (
    parse,
    validate,
)
from .connectors import (
    ESClient,
    EmailClient,
    MongoDB,
    MySQLClient,
    PandasSQL,
    SQLClient,
    SQLtoMongo,
)
from .handlers import (
    FunctionTimer,
    Log,
    Timer,
    TicToc,
    ZipData,
    csv_write,
    csv_read,
    get_logger,
    pip_upgrade,
    send_email,
    timer,
)
from .parsers import (
    Checks,
    dateformat,
    drop_empty_columns,
    expand,
    flatten,
    levenshtein,
)
from .persondata import (
    NamesData,
    NoMatch,
    PersonData,
)
from .platform import (
    FileTransfer,
)
from .requests import (
    ThreadSafeIterator,
    get,
    post,
    request,
    thread,
    threadsafe,
    get_proxies,
    get_session,
)
from .etc._version import (
    __version__,
)

__all__ = [
    "Checks",
    "ESClient",
    "EmailClient",
    "FileTransfer",
    "FunctionTimer",
    "Log",
    "MongoDB",
    "MySQLClient",
    "NamesData",
    "NoMatch",
    "PersonData",
    "ThreadSafeIterator",
    "TicToc",
    "Timer",
    "ZipData",
    "__version__",
    "csv_read",
    "csv_write",
    "dateformat",
    "drop_empty_columns",
    "expand",
    "flatten",
    "get",
    "get_logger",
    "get_proxies",
    "get_session",
    "levenshtein",
    "parse",
    "post",
    "pip_upgrade",
    "request",
    "send_email",
    "thread",
    "threadsafe",
    "timer",
    "validate",
]
