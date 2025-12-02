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

    ```python
    from elia_opendata.data_processor import EliaDataProcessor
    from elia_opendata.dataset_catalog import TOTAL_LOAD

    # JSON output (default)
    processor = EliaDataProcessor()
    data = processor.fetch_current_value(TOTAL_LOAD)
    print(type(data))  # <class 'list'>

    # Pandas DataFrame output
    processor = EliaDataProcessor(return_type="pandas")
    df = processor.fetch_current_value(TOTAL_LOAD)
    print(type(df))  # <class 'pandas.core.frame.DataFrame'>

    # Date range query
    from datetime import datetime
    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 31)
    monthly_data = processor.fetch_data_between(TOTAL_LOAD, start, end)
    ```
"""
from typing import Optional, Any, Union, List
from datetime import datetime
import logging
import io
import pandas as pd
import polars as pl

from .client import EliaClient
from .dataset_catalog import DATASET_NAME_MAPPING

DATE_FORMAT = "%Y-%m-%d"
MARI_TRANSITION_DATE = datetime(2024, 5, 22)

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

        ```python
        processor = EliaDataProcessor()
        current_data = processor.fetch_current_value("ods001")
        ```

        With custom client and return type:

        ```python
        from elia_opendata.client import EliaClient
        client = EliaClient(api_key="your_key")
        processor = EliaDataProcessor(client=client, return_type="pandas")
        df = processor.fetch_current_value("ods032")
        print(df.head())
        ```

        Date range queries:

        ```python
        from datetime import datetime
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)
        data = processor.fetch_data_between("ods001", start, end)
        ```
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

            ```python
            processor = EliaDataProcessor()
            ```

            With custom client:

            ```python
            from elia_opendata.client import EliaClient
            client = EliaClient(api_key="your_key", timeout=60)
            processor = EliaDataProcessor(client=client)
            ```

            With pandas output:

            ```python
            processor = EliaDataProcessor(return_type="pandas")
            ```
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

            ```python
            from elia_opendata.dataset_catalog import TOTAL_LOAD
            processor = EliaDataProcessor()
            current = processor.fetch_current_value(TOTAL_LOAD)
            print(current[0]['datetime'])  # Most recent timestamp
            ```

            With filtering:

            ```python
            current_measured = processor.fetch_current_value(
                TOTAL_LOAD,
                where="type='measured'"
            )
            ```

            As pandas DataFrame:

            ```python
            processor = EliaDataProcessor(return_type="pandas")
            df = processor.fetch_current_value(TOTAL_LOAD)
            print(df.iloc[0]['value'])  # Most recent value
            ```
        """
        logger.debug("Fetching current value for dataset %s", dataset_id)

        # Get the most recent record by limiting to 1 and ordering by
        # datetime desc
        if "order_by" not in kwargs:
            kwargs["order_by"] = "datetime desc"

        records = self.client.get_records(dataset_id,  limit=1, **kwargs)

        return self._format_output(records)

    def fetch_data_between(
        self,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        dataset_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        **kwargs
    ) -> Any:
        """Fetch data between two dates with automatic pagination.

        Includes automatic MARI transition handling for imbalance datasets.

        This method retrieves all records from the specified dataset within
        the given date range. It supports two modes:
        1. Pagination mode (default): Uses multiple API requests with
           pagination
        2. Export mode: Uses the bulk export endpoint for large datasets

        For datasets with MARI transition (imbalance-related datasets),
        this method automatically handles the transition date (May 22,
        2024) by selecting the appropriate dataset ID(s) or merging data
        from both pre-MARI and post-MARI datasets when the date range
        spans the transition.

        Args:
            start_date: Start date for the query range. Can be either:
                - datetime object
                - ISO date string (e.g., "2025-01-01")
            end_date: End date for the query range. Can be either:
                - datetime object
                - ISO date string (e.g., "2025-01-31")
            dataset_id: Unique identifier for the dataset to query. Use
                constants from dataset_catalog module. Optional if dataset_name
                is provided.
            dataset_name: Friendly name for datasets with MARI transition.
                If provided, automatically selects the correct dataset ID(s)
                based on the date range. Examples: "IMBALANCE_PRICES_QH",
                "SYSTEM_IMBALANCE". Takes precedence over dataset_id.
            **kwargs: Additional query parameters:
                - export_data (bool): If True, uses the export endpoint for
                  bulk data retrieval. If False (default), uses pagination.
                - where: Additional filter conditions (combined with date
                  filter)
                - select: Comma-separated fields to retrieve
                - limit: Batch size for pagination (default: 100) or maximum
                  records for export
                - order_by: Sort order for results
                - Any other API-supported parameters

        Returns:
            All matching records in the format specified by return_type:
            - If return_type="json": List of dictionaries
            - If return_type="pandas": pandas.DataFrame
            - If return_type="polars": polars.DataFrame

        Raises:
            ValueError: If both dataset_id and dataset_name are None, or if
                dataset_name is not found in DATASET_NAME_MAPPING.

        Note:
            For large date ranges (>10,000 records), consider setting
            export_data=True to use the more efficient export endpoint.
            The export endpoint automatically uses the optimal format:
            - JSON for json return_type
            - Parquet for pandas/polars return_types

        Example:
            Using dataset_name with MARI transition handling:

            ```python
            from datetime import datetime
            processor = EliaDataProcessor()
            
            # Query before MARI - automatically uses PRE_MARI dataset
            data = processor.fetch_data_between(
                dataset_name="IMBALANCE_PRICES_QH",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 3, 31)
            )
            
            # Query after MARI - automatically uses POST_MARI dataset
            data = processor.fetch_data_between(
                dataset_name="IMBALANCE_PRICES_QH",
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 30)
            )
            
            # Query spanning MARI - automatically merges both datasets
            data = processor.fetch_data_between(
                dataset_name="IMBALANCE_PRICES_QH",
                start_date=datetime(2025, 4, 1),
                end_date=datetime(2025, 5, 31)
            )
            ```

            Traditional usage with dataset_id:

            ```python
            from elia_opendata.dataset_catalog import TOTAL_LOAD
            processor = EliaDataProcessor()
            start = datetime(2025, 1, 1)
            end = datetime(2025, 1, 31)
            data = processor.fetch_data_between(TOTAL_LOAD, start, end)
            print(f"Retrieved {len(data)} records")
            ```

            Using export endpoint for large datasets:

            ```python
            data = processor.fetch_data_between(
                TOTAL_LOAD,
                start,
                end,
                export_data=True
            )
            ```
        """
        # Validate that at least one of dataset_id or dataset_name is provided
        if dataset_id is None and dataset_name is None:
            raise ValueError(
                "Either dataset_id or dataset_name must be provided"
            )

        # Convert string dates to datetime objects for comparison
        if isinstance(start_date, str):
            start_dt = datetime.fromisoformat(start_date)
        else:
            start_dt = start_date

        if isinstance(end_date, str):
            end_dt = datetime.fromisoformat(end_date)
        else:
            end_dt = end_date

        # Process timing info - convert to string format for API calls
        if isinstance(start_date, datetime):
            start_date_str = start_date.strftime(DATE_FORMAT)
        else:
            start_date_str = start_date

        if isinstance(end_date, datetime):
            end_date_str = end_date.strftime(DATE_FORMAT)
        else:
            end_date_str = end_date

        # Handle dataset_name if provided (takes precedence)
        if dataset_name is not None:
            if dataset_name not in DATASET_NAME_MAPPING:
                raise ValueError(
                    f"Dataset name '{dataset_name}' not found in "
                    f"DATASET_NAME_MAPPING. Available names: "
                    f"{list(DATASET_NAME_MAPPING.keys())}"
                )

            logger.debug(
                "Using dataset_name '%s' with MARI transition handling",
                dataset_name
            )

            # Get the dataset mapping
            dataset_mapping = DATASET_NAME_MAPPING[dataset_name]
            pre_mari_id = dataset_mapping["pre_mari"]
            post_mari_id = dataset_mapping["post_mari"]

            # Case 1: Both dates are before the MARI transition
            if end_dt < MARI_TRANSITION_DATE:
                logger.debug(
                    "Date range is entirely before MARI transition, "
                    "using PRE_MARI dataset: %s", pre_mari_id
                )
                return self._fetch_data_for_period(
                    pre_mari_id, start_date_str, end_date_str, **kwargs
                )

            # Case 2: Both dates are on or after the transition
            if start_dt >= MARI_TRANSITION_DATE:
                logger.debug(
                    "Date range is entirely after MARI transition, "
                    "using POST_MARI dataset: %s", post_mari_id
                )
                return self._fetch_data_for_period(
                    post_mari_id, start_date_str, end_date_str, **kwargs
                )

            # Case 3: Date range spans the transition
            # Fetch from both and merge
            logger.debug(
                "Date range spans MARI transition, fetching from both datasets"
            )

            # Fetch pre-MARI data (from start_date to day before transition)
            transition_date_minus_one = MARI_TRANSITION_DATE - pd.Timedelta(days=1)
            pre_mari_end = transition_date_minus_one.strftime(DATE_FORMAT)
            
            logger.debug(
                "Fetching pre-MARI data from %s to %s using dataset %s",
                start_date_str, pre_mari_end, pre_mari_id
            )
            pre_mari_data = self._fetch_data_for_period(
                pre_mari_id, start_date_str, pre_mari_end, **kwargs
            )

            # Fetch post-MARI data (from transition date to end_date)
            mari_transition_str = MARI_TRANSITION_DATE.strftime(DATE_FORMAT)
            
            logger.debug(
                "Fetching post-MARI data from %s to %s using dataset %s",
                mari_transition_str, end_date_str, post_mari_id
            )
            post_mari_data = self._fetch_data_for_period(
                post_mari_id, mari_transition_str, end_date_str, **kwargs
            )

            # Merge the results based on return type
            return self._merge_data(pre_mari_data, post_mari_data)

        # Use dataset_id if dataset_name is not provided
        else:
            if dataset_id is None:
                raise ValueError(
                    "dataset_id must be provided when dataset_name is not used"
                )
            else:
                logger.debug(
                    "Fetching data for dataset %s between %s and %s",
                    dataset_id, start_date_str, end_date_str
                )
                return self._fetch_data_for_period(
                    dataset_id, start_date_str, end_date_str, **kwargs
                )

    def _fetch_data_for_period(
        self,
        dataset_id: str,
        start_date_str: str,
        end_date_str: str,
        **kwargs
    ) -> Any:
        """Internal method to fetch data for a specific period.

        Args:
            dataset_id: Unique identifier for the dataset to query.
            start_date_str: Start date as string in DATE_FORMAT.
            end_date_str: End date as string in DATE_FORMAT.
            **kwargs: Additional query parameters.

        Returns:
            Formatted data according to return_type.
        """
        # Build the date filter condition
        where_condition = (
            f"datetime IN [date'{start_date_str}'..date'{end_date_str}']"
        )
        if "where" in kwargs:
            kwargs["where"] = f"({kwargs['where']}) AND ({where_condition})"
        else:
            kwargs["where"] = where_condition

        # Check if export endpoint should be used
        export_data = kwargs.pop("export_data", False)

        if export_data:
            return self._fetch_via_export(dataset_id, **kwargs)
        else:
            return self._fetch_via_pagination(dataset_id, **kwargs)

    def _fetch_via_pagination(
        self,
        dataset_id: str,
        **kwargs
    ) -> Any:
        """Fetch data using pagination through the records endpoint.

        Args:
            dataset_id: Unique identifier for the dataset to query.
            **kwargs: Additional query parameters including where conditions.

        Returns:
            Formatted data according to return_type.
        """
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
                logger.warning(
                    "Reached maximum pagination limit. "
                    "If you expect more data, consider setting the "
                    "export_data flag to True."
                )
                break

        return self._format_output(all_records)

    def _fetch_via_export(
        self,
        dataset_id: str,
        **kwargs
    ) -> Any:
        """Fetch data using the export endpoint.

        Args:
            dataset_id: Unique identifier for the dataset to query.
            **kwargs: Additional query parameters including where conditions.

        Returns:
            Formatted data according to return_type.
        """
        # Determine export format based on return type
        if self.return_type in ["pandas", "polars"]:
            export_format = "parquet"
        else:
            export_format = "json"

        logger.debug(
            "Using export endpoint for dataset %s with format %s",
            dataset_id, export_format
        )

        # Export the data
        exported_data = self.client.export(
            dataset_id,
            export_format=export_format,
            **kwargs
        )

        # Process the exported data based on format
        if export_format == "json":
            # For JSON export, the response structure might be different
            # Extract the records if they're nested
            if isinstance(exported_data, dict) and 'results' in exported_data:
                records = exported_data['results']
            elif isinstance(exported_data, list):
                records = exported_data
            else:
                records = [exported_data]

            return self._format_output(records)

        if export_format == "parquet":
            # For parquet, we need to read the bytes and convert
            if self.return_type == "pandas":
                return pd.read_parquet(io.BytesIO(exported_data))

            if self.return_type == "polars":
                return pl.read_parquet(io.BytesIO(exported_data))

        return exported_data

    def _merge_data(self, pre_mari_data: Any, post_mari_data: Any) -> Any:
        """Merge pre-MARI and post-MARI datasets.

        This private method combines data from both datasets according to
        the return_type setting.

        Args:
            pre_mari_data: Data from the pre-MARI dataset.
            post_mari_data: Data from the post-MARI dataset.

        Returns:
            Merged data in the format specified by return_type:
            - If return_type="json": Concatenated list
            - If return_type="pandas": Concatenated DataFrame
            - If return_type="polars": Concatenated DataFrame

        Raises:
            ValueError: If the return_type is not supported.
        """
        if self.return_type == "json":
            # Simply concatenate the lists
            return pre_mari_data + post_mari_data

        elif self.return_type == "pandas":
            # Concatenate pandas DataFrames
            if pre_mari_data.empty:
                return post_mari_data
            if post_mari_data.empty:
                return pre_mari_data
            return pd.concat(
                [pre_mari_data, post_mari_data], ignore_index=True
            )

        elif self.return_type == "polars":
            # Concatenate polars DataFrames
            if pre_mari_data.is_empty():
                return post_mari_data
            if post_mari_data.is_empty():
                return pre_mari_data
            return pl.concat([pre_mari_data, post_mari_data])

        else:
            raise ValueError(f"Unsupported return type: {self.return_type}")

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
