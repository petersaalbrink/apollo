"""Module that contains exceptions used by other common modules."""


class CommonError(Exception):
    """Base exception for all common modules."""


class ConnectorError(Exception):
    """Base exception for all common.connectors modules."""


class ESClientError(ConnectorError):
    """Exception for ESClient."""


class MongoDBError(ConnectorError):
    """Exception for MongoDB."""


class MySQLClientError(ConnectorError):
    """Exception for MySQLClient."""


class DataError(CommonError):
    """Exception for data handlers."""


class FileTransferError(CommonError):
    """Exception for FileTransfer."""


class MatchError(CommonError):
    """Exception for Person Matching."""


class NoMatch(CommonError):
    """Exception for Person Matching."""


class ParseError(CommonError):
    """Exception for data parsers."""


class TimerError(CommonError):
    """Exception for Timer."""


class ZipDataError(CommonError):
    """Exception for ZipData."""
