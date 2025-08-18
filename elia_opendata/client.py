"""Core client implementation for the Elia OpenData API.

This module provides the main client class for interacting with the Elia
OpenData API. It handles authentication, request formatting, error handling,
and response parsing.

The client focuses on the records endpoint which provides access to dataset
records with support for filtering, pagination, and various query parameters.

Example:
    Basic usage of the Elia client:

    ```python
    from elia_opendata.client import EliaClient
    client = EliaClient()
    data = client.get_records("ods032", limit=100)
    print(f"Retrieved {len(data)} records")
    ```

"""
import logging
from typing import Dict, List, Optional, Any, NoReturn
from urllib.parse import urljoin

import requests
from .error import (
    RateLimitError,
    AuthError,
    APIError,
    ConnectionError as EliaConnectionError,
)

# Configure logging
logger = logging.getLogger(__name__)


class EliaClient:
    """Client for interacting with the Elia Open Data Portal API.

    This client provides a simple interface to access the Elia OpenData API
    records endpoint. It handles authentication, request formatting, and error
    handling for dataset queries.

    The client supports various query parameters including filtering,
    pagination, and sorting options as provided by the Elia OpenData API.

    Attributes:
        BASE_URL (str): The base URL for the Elia OpenData API.
        api_key (Optional[str]): API key for authenticated requests.
        timeout (int): Request timeout in seconds.

    Example:
        Basic usage:

        ```python
        client = EliaClient()
        data = client.get_records("ods032", limit=100)
        ```

    """

    BASE_URL = "https://opendata.elia.be/api/explore/v2.1/"

    def __init__(
        self,
        timeout: int = 30
    ) -> None:
        """Initialize the Elia API client.

        Args:
            timeout: Request timeout in seconds. Defaults to 30 seconds.
                Increase this value for large dataset queries.


        """
        self.timeout = timeout

    def get_records(
        self,
        dataset_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where: Optional[str] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get records from a specific dataset.

        This method queries the Elia OpenData API records endpoint to retrieve
        dataset records with optional filtering and pagination.

        Args:
            dataset_id: The unique identifier for the dataset to query.
                Examples include "ods032" for PV production data or "ods001"
                for total load data.
            limit: Maximum number of records to return in a single request.
                If None, the API default limit applies (typically 10).
                Maximum value is usually 10000 per request.
            offset: Number of records to skip before starting to return
                results. Useful for pagination. Defaults to 0 if not
                specified.
            where: Filter expression in OData format to limit results.
                Examples: "datetime>'2023-01-01'" or "value>100".
            **kwargs: Additional query parameters supported by the API.
                Common options include 'order_by', 'select', 'group_by'.

        Returns:
            A list of records

        Raises:
            APIError: If the API request fails due to server error, invalid
                dataset ID, or malformed query parameters.
            AuthError: If authentication is required but invalid/missing
                API key is provided.
            RateLimitError: If API rate limits are exceeded.
            EliaConnectionError: If network connection fails or times out.

        Example:
            Basic usage:

            ```python
            client = EliaClient()
            data = client.get_records("ods032", limit=100)
            ```

            With filtering:

            ```python
            filtered_data = client.get_records(
                "ods001",
                where="datetime>='2023-01-01' AND datetime<'2023-02-01'",
                limit=1000,
                order_by="datetime"
            )
            ```

            Pagination:

            ```python
            page1 = client.get_records("ods032", limit=50, offset=0)
            page2 = client.get_records("ods032", limit=50, offset=50)
            ```
        """
        url = urljoin(self.BASE_URL, f"catalog/datasets/{dataset_id}/records")

        headers = {
            "accept": "application/json; charset=utf-8"
        }

        # Build parameters
        params: Dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if where is not None:
            params["where"] = where

        default_params = {
            'timezone': 'UTC',
            'include_links': 'false',
            'include_app_metas': 'false',
        }

        params.update(default_params)

        try:

            req = requests.Request('GET', url, params=params, headers=headers)
            prepared = req.prepare()

            # Print the exact URL being used
            logger.debug(f"Requesting: {prepared.url}")

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )

            response.raise_for_status()

            raw_data = response.json()

            records = raw_data.get("results")
            return records

        except requests.exceptions.HTTPError as e:
            self._handle_http_error(e)
        except requests.exceptions.RequestException as e:
            raise EliaConnectionError(f"Connection failed: {str(e)}") from e

    def export(
        self,
        dataset_id: str,
        limit: Optional[int] = None,
        where: Optional[str] = None,
        export_format: str = "json",
        **kwargs: Any
    ) -> Any:
        """Export dataset records in a specific format.

        This method uses the Elia OpenData API export endpoint to download
        complete dataset records in various formats (JSON, CSV, or Parquet).
        Unlike get_records, this endpoint is optimized for bulk data export.

        Args:
            dataset_id: The unique identifier for the dataset to export.
                Examples include "ods032" for PV production data or "ods001"
                for total load data.
            limit: Maximum number of records to export. If None, exports
                all available records in the dataset.
            where: Filter expression in OData format to limit results.
                Examples: "datetime>'2023-01-01'" or "value>100".
            export_format: The format for the exported data. Supported
                formats are "json", "csv", and "parquet". Defaults to "json".
            **kwargs: Additional parameters supported by the export endpoint.
                Common options include:
                - lang (str): Language for labels, defaults to 'en'
                - timezone (str): Timezone for datetime fields, defaults to
                  'UTC'
                - use_labels (str): Whether to use human-readable labels,
                  defaults to 'false'
                - compressed (str): Whether to compress the output,
                  defaults to 'false'

        Returns:
            The exported data in the requested format:
            - For "json": Dict containing the parsed JSON response
            - For "csv": str containing the CSV data
            - For "parquet": bytes containing the Parquet file content

        Raises:
            ValueError: If an unsupported export format is specified.
            APIError: If the API request fails due to server error, invalid
                dataset ID, or malformed query parameters.
            AuthError: If authentication is required but invalid/missing
                API key is provided.
            RateLimitError: If API rate limits are exceeded.
            EliaConnectionError: If network connection fails or times out.

        Example:
            Basic JSON export:

            ```python
            client = EliaClient()
            data = client.export("ods032", limit=1000)
            ```

            CSV export with filtering:

            ```python
            csv_data = client.export(
                "ods001",
                where="datetime>='2023-01-01'",
                export_format="csv",
                use_labels="true"
            )
            ```

            Parquet export:

            ```python
            parquet_data = client.export(
                "ods032",
                export_format="parquet",
                compressed="true"
            )
            ```
        """

        # Build the export URL based on format
        if export_format == "json":
            url = urljoin(
                self.BASE_URL, f"catalog/datasets/{dataset_id}/exports/json"
            )
        elif export_format == "csv":
            url = urljoin(
                self.BASE_URL, f"catalog/datasets/{dataset_id}/exports/csv"
            )
        elif export_format == "parquet":
            url = urljoin(
                self.BASE_URL, f"catalog/datasets/{dataset_id}/exports/parquet"
            )
        else:
            raise ValueError(
                f"Unsupported export format: {export_format}. "
                "Supported formats are 'json', 'csv', and 'parquet'."
            )

        # Build parameters, filtering out None values
        params: Dict[str, Any] = {}
        if where is not None:
            params['where'] = where
        if limit is not None:
            params['limit'] = limit

        # Add optional parameters with defaults
        params.update({
            'lang': kwargs.get('lang', 'en'),
            'timezone': kwargs.get('timezone', 'UTC'),
            'use_labels': kwargs.get('use_labels', 'false'),
            'compressed': kwargs.get('compressed', 'false'),
            'epsg': kwargs.get('epsg', 4326)
        })

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            if export_format == "json":
                return response.json()
            elif export_format == "csv":
                return response.text
            else:  # parquet
                return response.content

        except requests.exceptions.HTTPError as e:
            self._handle_http_error(e)
        except requests.exceptions.RequestException as e:
            raise EliaConnectionError(f"Connection failed: {str(e)}") from e

    def _handle_http_error(self, e: requests.exceptions.HTTPError) -> NoReturn:
        """Handle HTTP errors and raise appropriate custom exceptions.

        This private method processes HTTP error responses from the API and
        raises specific exception types based on the HTTP status code.

        Args:
            e: The HTTPError exception from the requests library containing
                the failed HTTP response.

        Raises:
            RateLimitError: If the response status code is 429 (Too Many
                Requests), indicating API rate limits have been exceeded.
            AuthError: If the response status code is 401 (Unauthorized),
                indicating authentication failure or invalid API key.
            APIError: For all other HTTP error status codes, wrapping the
                original error with additional context.

        Note:
            This method never returns normally - it always raises an
            exception. The NoReturn type annotation indicates this behavior.
        """
        response = e.response
        if response.status_code == 429:
            raise RateLimitError(
                "API rate limit exceeded", response=response
            ) from e
        elif response.status_code == 401:
            raise AuthError(
                "Authentication failed", response=response
            ) from e
        else:
            raise APIError(
                f"API request failed: {str(e)}", response=response
            ) from e
