"""
Exception classes for the Elia OpenData API client.
"""


class EliaError(Exception):
    """Base class for Elia OpenData API errors"""
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class RateLimitError(EliaError):
    """Raised when API rate limit is exceeded"""
    def __init__(self, message, error_code="RateLimitError", response=None):
        super().__init__(message, response)
        self.error_code = error_code

class APIError(EliaError):
    """Raised when API returns an error response"""
    def __init__(self, message, error_code=None, response=None):
        super().__init__(message, response)
        self.error_code = error_code


class ODSQLError(APIError):
    """Raised when ODSQL query is malformed"""
    def __init__(self, message, error_code="ODSQLError", response=None):
        super().__init__(message, error_code, response)
