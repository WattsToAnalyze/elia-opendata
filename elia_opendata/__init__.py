"""
Elia OpenData API Client Library
~~~~~~~~~~~~~~~~~~~

A library for accessing the Elia Open Data Portal API.

Basic usage:

    >>> from elia_opendata import EliaClient
    >>> client = EliaClient()
    >>> datasets = client.get_datasets()

Full documentation is available at [docs link].
"""

from .client import EliaClient
from .error import EliaError, RateLimitError, AuthError
from .datasets import Dataset, DatasetCategory

__version__ = "0.1.0"
__author__ = "Gargy Agokhale"

__all__ = [
    'EliaClient',
    'EliaError',
    'RateLimitError',
    'AuthError',
    'Dataset',
    'DatasetCategory',
]