"""Exceptions shared by the ingestion clients."""


class IngestionError(Exception):
    """Base class for ingestion errors that are not HTTP transport problems."""


class NoDataError(IngestionError):
    """The source answered correctly but returned no rows for the requested day."""


class UnexpectedResponseError(IngestionError):
    """The response does not look like what the API contract promises."""
