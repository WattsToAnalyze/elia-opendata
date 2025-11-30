"""Specialized data processor for imbalance datasets with MARI transition handling.

This module provides a specialized data processor that handles the transition
between legacy and post-MARI imbalance datasets. The transition occurred on
April 22, 2024, when Elia migrated to the MARI/ICAROS platform.

The ImbalanceDataProcessor automatically selects the correct dataset ID based
on the date range being queried and seamlessly merges data across the
transition period when necessary.

Example:
    Basic usage for imbalance prices:

    ```python
    from elia_opendata.imbalance_data_processor import ImbalanceDataProcessor
    from elia_opendata.dataset_catalog import (
        IMBALANCE_PRICES_QH_PRE_MARI,
        IMBALANCE_PRICES_QH_POST_MARI
    )
    from datetime import datetime

    # Initialize processor with PRE and POST MARI dataset IDs
    processor = ImbalanceDataProcessor(
        pre_mari_dataset_id=IMBALANCE_PRICES_QH_PRE_MARI,
        post_mari_dataset_id=IMBALANCE_PRICES_QH_POST_MARI,
        return_type="pandas"
    )

    # Query data before transition - uses PRE_MARI dataset
    start = datetime(2024, 1, 1)
    end = datetime(2024, 3, 31)
    df = processor.fetch_data_between(start, end)

    # Query data after transition - uses POST_MARI dataset
    start = datetime(2024, 5, 1)
    end = datetime(2024, 6, 30)
    df = processor.fetch_data_between(start, end)

    # Query across transition - automatically merges both datasets
    start = datetime(2024, 4, 1)
    end = datetime(2024, 5, 31)
    df = processor.fetch_data_between(start, end)
    ```
"""
from typing import Optional, Union, Any
from datetime import datetime
import logging
import pandas as pd
import polars as pl

from .client import EliaClient
from .data_processor import EliaDataProcessor

logger = logging.getLogger(__name__)

# MARI/ICAROS transition date
MARI_TRANSITION_DATE = datetime(2024, 4, 22)


class ImbalanceDataProcessor(EliaDataProcessor):
    """Specialized processor for imbalance datasets with MARI transition handling.

    This class extends EliaDataProcessor to handle the transition between
    legacy imbalance datasets (pre-MARI) and new datasets (post-MARI). It
    automatically selects the appropriate dataset ID based on the queried
    date range and merges data when the range spans the transition date.

    The MARI/ICAROS platform went live on April 22, 2024, which changed
    how imbalance data is structured and stored in Elia's systems.

    Attributes:
        client (EliaClient): The underlying API client for making requests.
        return_type (str): The format for returned data ("json", "pandas",
            or "polars").
        pre_mari_dataset_id (str): Dataset ID for legacy data (before April 22, 2024).
        post_mari_dataset_id (str): Dataset ID for new data (from April 22, 2024 onwards).

    Example:
        For imbalance prices (quarter-hourly):

        ```python
        from elia_opendata.dataset_catalog import (
            IMBALANCE_PRICES_QH_PRE_MARI,
            IMBALANCE_PRICES_QH_POST_MARI
        )

        processor = ImbalanceDataProcessor(
            pre_mari_dataset_id=IMBALANCE_PRICES_QH_PRE_MARI,
            post_mari_dataset_id=IMBALANCE_PRICES_QH_POST_MARI,
            return_type="pandas"
        )
        ```

        For system imbalance:

        ```python
        from elia_opendata.dataset_catalog import (
            SYSTEM_IMBALANCE_PRE_MARI,
            SYSTEM_IMBALANCE_POST_MARI
        )

        processor = ImbalanceDataProcessor(
            pre_mari_dataset_id=SYSTEM_IMBALANCE_PRE_MARI,
            post_mari_dataset_id=SYSTEM_IMBALANCE_POST_MARI
        )
        ```
    """

    def __init__(
        self,
        pre_mari_dataset_id: str,
        post_mari_dataset_id: str,
        client: Optional[EliaClient] = None,
        return_type: str = "json"
    ):
        """Initialize the imbalance data processor.

        Args:
            pre_mari_dataset_id: Dataset ID for legacy data (before April 22, 2024).
                Use constants from dataset_catalog ending with _PRE_MARI.
            post_mari_dataset_id: Dataset ID for new data (from April 22, 2024 onwards).
                Use constants from dataset_catalog ending with _POST_MARI.
            client: EliaClient instance for making API requests. If None,
                a new client with default settings will be created.
            return_type: Output format for processed data. Must be one of:
                - "json": Returns raw list of dictionaries (default)
                - "pandas": Returns pandas.DataFrame
                - "polars": Returns polars.DataFrame

        Raises:
            ValueError: If return_type is not one of the supported formats.

        Example:
            ```python
            from elia_opendata.dataset_catalog import (
                IMBALANCE_PRICES_QH_PRE_MARI,
                IMBALANCE_PRICES_QH_POST_MARI
            )

            processor = ImbalanceDataProcessor(
                pre_mari_dataset_id=IMBALANCE_PRICES_QH_PRE_MARI,
                post_mari_dataset_id=IMBALANCE_PRICES_QH_POST_MARI,
                return_type="pandas"
            )
            ```
        """
        super().__init__(client=client, return_type=return_type)
        self.pre_mari_dataset_id = pre_mari_dataset_id
        self.post_mari_dataset_id = post_mari_dataset_id

    def fetch_data_between(
        self,
        dataset_id: Optional[str],
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        **kwargs
    ) -> Any:
        """Fetch imbalance data between two dates with automatic MARI transition handling.

        This method overrides the parent fetch_data_between to automatically
        handle the MARI transition date. It determines which dataset(s) to
        query based on the date range:

        - If both dates are before April 22, 2024: Uses pre_mari_dataset_id
        - If both dates are on/after April 22, 2024: Uses post_mari_dataset_id
        - If the range spans the transition: Queries both datasets and merges results

        Args:
            start_date: Start date for the query range. Can be either:
                - datetime object
                - ISO date string (e.g., "2024-01-01")
            end_date: End date for the query range. Can be either:
                - datetime object
                - ISO date string (e.g., "2024-03-31")
            **kwargs: Additional query parameters:
                - export_data (bool): If True, uses the export endpoint
                - where: Additional filter conditions
                - select: Comma-separated fields to retrieve
                - limit: Batch size for pagination
                - order_by: Sort order for results
                - Any other API-supported parameters

        Returns:
            All matching records in the format specified by return_type:
            - If return_type="json": List of dictionaries
            - If return_type="pandas": pandas.DataFrame
            - If return_type="polars": polars.DataFrame

        Example:
            Query data before transition:

            ```python
            from datetime import datetime
            processor = ImbalanceDataProcessor(...)
            
            # Uses PRE_MARI dataset only
            start = datetime(2024, 1, 1)
            end = datetime(2024, 3, 31)
            data = processor.fetch_data_between(start, end)
            ```

            Query data after transition:

            ```python
            # Uses POST_MARI dataset only
            start = datetime(2024, 5, 1)
            end = datetime(2024, 6, 30)
            data = processor.fetch_data_between(start, end)
            ```

            Query across transition:

            ```python
            # Automatically queries both datasets and merges
            start = datetime(2024, 4, 1)
            end = datetime(2024, 5, 31)
            data = processor.fetch_data_between(start, end)
            ```
        """
        # Convert string dates to datetime objects for comparison
        if isinstance(start_date, str):
            start_dt = datetime.fromisoformat(start_date)
        else:
            start_dt = start_date

        if isinstance(end_date, str):
            end_dt = datetime.fromisoformat(end_date)
        else:
            end_dt = end_date

        logger.debug(
            "Fetching imbalance data between %s and %s (transition: %s)",
            start_dt, end_dt, MARI_TRANSITION_DATE
        )

        # Case 1: Both dates are before the transition
        if end_dt < MARI_TRANSITION_DATE:
            logger.debug("Date range is entirely before MARI transition, using PRE_MARI dataset")
            return super().fetch_data_between(
                self.pre_mari_dataset_id,
                start_date,
                end_date,
                **kwargs
            )

        # Case 2: Both dates are on or after the transition
        if start_dt >= MARI_TRANSITION_DATE:
            logger.debug("Date range is entirely after MARI transition, using POST_MARI dataset")
            return super().fetch_data_between(
                self.post_mari_dataset_id,
                start_date,
                end_date,
                **kwargs
            )

        # Case 3: Date range spans the transition - fetch from both and merge
        logger.debug("Date range spans MARI transition, fetching from both datasets")
        
        # Fetch pre-MARI data (from start_date to day before transition)
        transition_date_minus_one = datetime(2024, 4, 21)
        pre_mari_data = super().fetch_data_between(
            self.pre_mari_dataset_id,
            start_date,
            transition_date_minus_one,
            **kwargs
        )

        # Fetch post-MARI data (from transition date to end_date)
        post_mari_data = super().fetch_data_between(
            self.post_mari_dataset_id,
            MARI_TRANSITION_DATE,
            end_date,
            **kwargs
        )

        # Merge the results based on return type
        return self._merge_data(pre_mari_data, post_mari_data)

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
            return pd.concat([pre_mari_data, post_mari_data], ignore_index=True)

        elif self.return_type == "polars":
            # Concatenate polars DataFrames
            if pre_mari_data.is_empty():
                return post_mari_data
            if post_mari_data.is_empty():
                return pre_mari_data
            return pl.concat([pre_mari_data, post_mari_data])

        else:
            raise ValueError(f"Unsupported return type: {self.return_type}")
