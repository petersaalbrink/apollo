"""Module that contains exceptions used by other apollo modules."""


from __future__ import annotations


class ApolloError(Exception):
    """Base exception for all apollo modules."""


class ApiError(ApolloError):
    """Base exception for all apollo.api modules."""


class ConnectorError(ApolloError):
    """Base exception for all apollo.connectors modules."""


class EmailClientError(ApolloError):
    """Exception for EmailClient."""


class ESClientError(ConnectorError):
    """Exception for ESClient."""


class PgSqlError(ConnectorError):
    """Exception for PgSql."""


class PhoneApiError(ApiError):
    """Exception for Phone Checker API."""


class MongoDBError(ConnectorError):
    """Exception for MongoDB."""


class MySQLClientError(ConnectorError):
    """Exception for MySQLClient."""


class DataError(ApolloError):
    """Exception for data handlers."""


class FileTransferError(ApolloError):
    """Exception for FileTransfer."""


class MatchError(ApolloError):
    """Exception for Person Matching."""


class NoMatch(ApolloError):
    """Exception for Person Matching."""


class ParseError(ApolloError):
    """Exception for data parsers."""


class PersonsError(ApolloError):
    """Exception for apollo.persondata"""


class RequestError(ApolloError):
    """Exception for requests."""


class Timeout(ApolloError):
    """Timout."""


class TimerError(ApolloError):
    """Exception for Timer."""


class ZipDataError(ApolloError):
    """Exception for ZipData."""
