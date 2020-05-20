"""Common classes and functions for the Matrixian Group Data Team."""

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
    get,
    thread,
    get_proxies,
    get_session,
)
from ._version import (
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
    "TicToc",
    "Timer",
    "ZipData",
    "__version__",
    "csv_read",
    "csv_write",
    "dateformat",
    "flatten",
    "get",
    "get_logger",
    "get_proxies",
    "get_session",
    "levenshtein",
    "parse",
    "pip_upgrade",
    "send_email",
    "thread",
    "timer",
    "validate",
]
