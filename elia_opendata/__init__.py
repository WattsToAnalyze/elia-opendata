"""
Elia OpenData API Client Library
~~~~~~~~~~~~~~~~~~~

A library for accessing the Elia Open Data Portal API.

Basic usage:

    >>> from elia_opendata import EliaClient, EliaDataProcessor, Dataset
    >>> # Basic client usage
    >>> client = EliaClient()
    >>> datasets = client.get_datasets()
    >>> 
    >>> # Advanced data processing
    >>> processor = EliaDataProcessor(client)
    >>> complete_data = processor.fetch_complete_dataset(Dataset.SOLAR_GENERATION)
    >>> df = processor.to_dataframe(complete_data, output_format="pandas")

Full documentation is available at [docs link].
"""

from .client import EliaClient
from .error import EliaError, RateLimitError, AuthError
from .datasets import Dataset, DatasetCategory
from .data_processor import EliaDataProcessor

__version__ = "0.2.1"
__author__ = "WattsToAnalyze"

__all__ = [
    'EliaClient',
    'EliaDataProcessor',
    'EliaError',
    'RateLimitError',
    'AuthError',
    'Dataset',
    'DatasetCategory',
]
