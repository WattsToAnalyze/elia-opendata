"""Exception classes for the Elia OpenData API client."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from requests import Response


class EliaError(Exception):
    """Base class for Elia OpenData API errors."""

    def __init__(self, message: str, response: Optional["Response"] = None) -> None:
        super().__init__(message)
        self.response = response


class RateLimitError(EliaError):
    """Raised when API rate limit is exceeded."""

    def __init__(
        self,
        message: str,
        error_code: str = "RateLimitError",
        response: Optional["Response"] = None,
    ) -> None:
        super().__init__(message, response)
        self.error_code = error_code

class APIError(EliaError):
    """Raised when API returns an error response."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        response: Optional["Response"] = None,
    ) -> None:
        super().__init__(message, response)
        self.error_code = error_code


class ODSQLError(APIError):
    """Raised when ODSQL query is malformed."""

    def __init__(
        self,
        message: str,
        error_code: str = "ODSQLError",
        response: Optional["Response"] = None,
    ) -> None:
        super().__init__(message, error_code, response)
