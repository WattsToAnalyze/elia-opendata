"""
Elia OpenData API Client Library
~~~~~~~~~~~~~~~~~~~

A library for accessing the Elia Open Data Portal API.

Basic usage:

    ```python
    from elia_opendata import EliaClient, EliaDataProcessor

    # Basic client usage
    from elia_opendata.dataset_catalog import PV_PRODUCTION

    client = EliaClient()
    data = client.get_records(PV_PRODUCTION, limit=100)

    # Advanced data processing
    processor = EliaDataProcessor(client)
    complete_data = processor.fetch_current_value(PV_PRODUCTION)
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
