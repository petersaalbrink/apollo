class CommonError(Exception):
    pass


class ConnectorError(Exception):
    pass


class ESClientError(ConnectorError):
    pass


class MongoDBError(ConnectorError):
    pass


class MySQLClientError(ConnectorError):
    pass


class FileTransferError(CommonError):
    pass


class MatchError(CommonError):
    pass


class NoMatch(CommonError):
    pass


class ParseError(CommonError):
    pass


class TimerError(CommonError):
    """Exception used to report errors in use of Timer class."""


class ZipDataError(CommonError):
    """Exception used to report errors in use of ZipData class."""
