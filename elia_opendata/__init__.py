"""
Elia OpenData API Client Library
~~~~~~~~~~~~~~~~~~~

A library for accessing the Elia Open Data Portal API.

Basic usage:

    ```python
    from elia_opendata import EliaClient, EliaDataProcessor

    # Basic client usage
    client = EliaClient()
    data = client.get_records("ods032", limit=100)

    # Advanced data processing
    processor = EliaDataProcessor(client)
    complete_data = processor.fetch_current_value("ods032")
    ```

Full documentation is available at https://wattstoanalyze.github.io/elia-opendata/.
"""

from .client import EliaClient
from .data_processor import EliaDataProcessor
from .error import APIError, RateLimitError

__version__ = "1.1.0"
__author__ = "WattsToAnalyze"

__all__ = [
    'EliaClient',
    'EliaDataProcessor',
    'RateLimitError',
    'APIError',
]
