"""Data processing utilities for Elia OpenData API.

This module provides high-level data processing capabilities for working with
This module provides high-level data processing capabilities for working with
Elia OpenData datasets. It offers convenient methods for fetching and
formatting data from the API, with support for multiple output formats
including JSON, Pandas DataFrames, and Polars DataFrames.

The main class, EliaDataProcessor, handles common data retrieval patterns
such as fetching the most recent values or retrieving data within specific
date ranges. It automatically handles pagination for large datasets and
provides consistent output formatting.

Example:
    Basic usage with different return types:

    >>> from elia_opendata.data_processor import EliaDataProcessor
    >>> from elia_opendata.dataset_catalog import TOTAL_LOAD

    >>> # JSON output (default)
    >>> processor = EliaDataProcessor()
    >>> data = processor.fetch_current_value(TOTAL_LOAD)
    >>> print(type(data))  # <class 'list'>

    >>> # Pandas DataFrame output
    >>> processor = EliaDataProcessor(return_type="pandas")
    >>> df = processor.fetch_current_value(TOTAL_LOAD)
    >>> print(type(df))  # <class 'pandas.core.frame.DataFrame'>

    >>> # Date range query
    >>> from datetime import datetime
    >>> start = datetime(2023, 1, 1)
    >>> end = datetime(2023, 1, 31)
    >>> monthly_data = processor.fetch_data_between(TOTAL_LOAD, start, end)
"""
from typing import Optional, Any, Union, List
from datetime import datetime
import logging
import pandas as pd
import polars as pl

from .client import EliaClient

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = logging.getLogger(__name__)


class EliaDataProcessor:
    """High-level data processor for Elia OpenData datasets.

    This class provides convenient methods for fetching and processing data
    from the Elia OpenData API. It supports multiple output formats and handles
    common data retrieval patterns automatically.

    The processor can return data in three formats:
    - JSON: Raw list of dictionaries (default)
    - Pandas: pandas.DataFrame for data analysis
    - Polars: polars.DataFrame for high-performance data processing

    Attributes:
        client (EliaClient): The underlying API client for making requests.
        return_type (str): The format for returned data ("json", "pandas",
            or "polars").

    Example:
        Basic usage:

        >>> processor = EliaDataProcessor()
        >>> current_data = processor.fetch_current_value("ods001")

        With custom client and return type:

        >>> from elia_opendata.client import EliaClient
        >>> client = EliaClient(api_key="your_key")
        >>> processor = EliaDataProcessor(client=client, return_type="pandas")
        >>> df = processor.fetch_current_value("ods032")
        >>> print(df.head())

        Date range queries:

        >>> from datetime import datetime
        >>> start = datetime(2023, 1, 1)
        >>> end = datetime(2023, 1, 31)
        >>> data = processor.fetch_data_between("ods001", start, end)
    """

    def __init__(
        self,
        client: Optional[EliaClient] = None,
        return_type: str = "json"
    ):
        """Initialize the data processor.

        Args:
            client: EliaClient instance for making API requests. If None,
                a new client with default settings will be created
                automatically.
            return_type: Output format for processed data. Must be one of:
                - "json": Returns raw list of dictionaries (default)
                - "pandas": Returns pandas.DataFrame
                - "polars": Returns polars.DataFrame

        Raises:
            ValueError: If return_type is not one of the supported formats.

        Example:
            Default initialization:

            >>> processor = EliaDataProcessor()

            With custom client:

            >>> from elia_opendata.client import EliaClient
            >>> client = EliaClient(api_key="your_key", timeout=60)
            >>> processor = EliaDataProcessor(client=client)

            With pandas output:

            >>> processor = EliaDataProcessor(return_type="pandas")
        """
        self.client = client or EliaClient()
        if return_type not in ["json", "pandas", "polars"]:
            raise ValueError(
                f"Invalid return_type: {return_type}. "
                f"Must be 'json', 'pandas', or 'polars'"
            )
        self.return_type = return_type

    def fetch_current_value(
        self,
        dataset_id: str,
        **kwargs
    ) -> Any:
        """Fetch the most recent value from a dataset.

        This method retrieves the single most recent record from the specified
        dataset by automatically setting limit=1 and ordering by datetime in
        descending order.

        Args:
            dataset_id: Unique identifier for the dataset to query. Use
                constants from dataset_catalog module (e.g., TOTAL_LOAD).
            **kwargs: Additional query parameters to pass to the API:
                - where: Filter condition in OData format
                - select: Comma-separated list of fields to retrieve
                - Any other parameters supported by the API

        Returns:
            The most recent record(s) in the format specified by return_type:
            - If return_type="json": List containing one dictionary
            - If return_type="pandas": pandas.DataFrame with one row
            - If return_type="polars": polars.DataFrame with one row

        Example:
            Get current total load:

            >>> from elia_opendata.dataset_catalog import TOTAL_LOAD
            >>> processor = EliaDataProcessor()
            >>> current = processor.fetch_current_value(TOTAL_LOAD)
            >>> print(current[0]['datetime'])  # Most recent timestamp

            With filtering:

            >>> current_measured = processor.fetch_current_value(
            ...     TOTAL_LOAD,
            ...     where="type='measured'"
            ... )

            As pandas DataFrame:

            >>> processor = EliaDataProcessor(return_type="pandas")
            >>> df = processor.fetch_current_value(TOTAL_LOAD)
            >>> print(df.iloc[0]['value'])  # Most recent value
        """
        logger.info("Fetching current value for dataset %s", dataset_id)

        # Get the most recent record by limiting to 1 and ordering by
        # datetime desc
        kwargs["limit"] = 1
        if "order_by" not in kwargs:
            kwargs["order_by"] = "-datetime"

        records = self.client.get_records(dataset_id, **kwargs)

        return self._format_output(records)

    def fetch_data_between(
        self,
        dataset_id: str,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        **kwargs
    ) -> Any:
        """Fetch data between two dates with automatic pagination.

        This method retrieves all records from the specified dataset within
        the given date range. It automatically handles pagination to fetch
        large datasets completely, combining multiple API requests as needed.

        Args:
            dataset_id: Unique identifier for the dataset to query. Use
                constants from dataset_catalog module.
            start_date: Start date for the query range. Can be either:
                - datetime object
                - ISO format string (e.g., "2023-01-01T00:00:00")
            end_date: End date for the query range. Can be either:
                - datetime object
                - ISO format string (e.g., "2023-01-31T23:59:59")
            **kwargs: Additional query parameters:
                - where: Additional filter conditions (combined with date
                  filter)
                - limit: Batch size for pagination (default: 100)
                - order_by: Sort order for results
                - select: Comma-separated fields to retrieve
                - Any other API-supported parameters

        Returns:
            All matching records in the format specified by return_type:
            - If return_type="json": List of dictionaries
            - If return_type="pandas": pandas.DataFrame
            - If return_type="polars": polars.DataFrame

        Note:
            The method automatically paginates through all results. For very
            large date ranges, consider using smaller batch sizes by setting
            the 'limit' parameter in kwargs.

        Example:
            Fetch data for January 2023:

            >>> from datetime import datetime
            >>> from elia_opendata.dataset_catalog import TOTAL_LOAD
            >>> processor = EliaDataProcessor()
            >>> start = datetime(2023, 1, 1)
            >>> end = datetime(2023, 1, 31, 23, 59, 59)
            >>> data = processor.fetch_data_between(TOTAL_LOAD, start, end)
            >>> print(f"Retrieved {len(data)} records")

            With string dates:

            >>> data = processor.fetch_data_between(
            ...     TOTAL_LOAD,
            ...     "2023-01-01T00:00:00",
            ...     "2023-01-31T23:59:59"
            ... )

            With additional filtering:

            >>> measured_data = processor.fetch_data_between(
            ...     TOTAL_LOAD,
            ...     start,
            ...     end,
            ...     where="type='measured'",
            ...     limit=500  # Larger batch size
            ... )

            As pandas DataFrame:

            >>> processor = EliaDataProcessor(return_type="pandas")
            >>> df = processor.fetch_data_between(TOTAL_LOAD, start, end)
            >>> print(df.describe())  # Statistical summary
        """
        
        if isinstance(start_date, datetime):
            start_date = start_date.strftime(DATETIME_FORMAT)
            
        if isinstance(end_date, datetime):
            end_date = end_date.strftime(DATETIME_FORMAT)

        logger.info(
            "Fetching data for dataset %s between %s and %s",
            dataset_id, start_date, end_date
        )

        # Build the date filter condition
        where_condition = (
            f"datetime IN [date'{start_date}'..date'{end_date}']"
        )
        if "where" in kwargs:
            kwargs["where"] = f"({kwargs['where']}) AND ({where_condition})"
        else:
            kwargs["where"] = where_condition

        # Fetch all records with pagination
        all_records = []
        offset = 0
        # Remove limit from kwargs to avoid duplication
        limit = kwargs.pop("limit", 100)

        while True:

            batch_records = self.client.get_records(
                dataset_id,
                limit=limit,
                offset=offset,
                **kwargs
            )

            if not batch_records:
                break

            all_records.extend(batch_records)

            # Check if we got fewer records than requested (end of data)
            if len(batch_records) < limit:
                break

            offset += limit
            
            if limit + offset > 10000:
                break

        return self._format_output(all_records)

    def _format_output(self, records: List[dict]) -> Any:
        """Format the output according to the specified return type.

        This private method converts the raw list of record dictionaries
        into the format specified by the processor's return_type setting.

        Args:
            records: List of record dictionaries from the API response.
                Each dictionary represents a single data record with
                fields like 'datetime', 'value', etc.

        Returns:
            Formatted data in the specified return type:
            - If return_type="json": Returns the input list unchanged
            - If return_type="pandas": Returns pandas.DataFrame
            - If return_type="polars": Returns polars.DataFrame

        Raises:
            ValueError: If the return_type is not supported (should not occur
                if properly initialized).

        Note:
            This is a private method intended for internal use only. The
            conversion handles empty record lists gracefully by returning
            empty DataFrames for pandas/polars formats.
        """
        if self.return_type == "json":
            return records
        elif self.return_type == "pandas":
            return pd.DataFrame(records)
        elif self.return_type == "polars":
            return pl.DataFrame(records)
        else:
            raise ValueError(f"Unsupported return type: {self.return_type}")
