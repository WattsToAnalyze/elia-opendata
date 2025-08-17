"""
Data processing utilities for Elia OpenData API.
"""
from typing import Optional, Any, Union, List
from datetime import datetime
import logging
import pandas as pd
import polars as pl

from .client import EliaClient

logger = logging.getLogger(__name__)


class EliaDataProcessor:
    """
    Simplified data processor for Elia OpenData datasets.
    Returns data in the specified format (json, pandas, or polars).
    """

    def __init__(
        self,
        client: Optional[EliaClient] = None,
        return_type: str = "json"
    ):
        """
        Initialize the data processor.

        Args:
            client: EliaClient instance. If not provided, creates a new one.
            return_type: Format for returned data - "json", "pandas", "polars"
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
        """
        Fetch the most recent value from a dataset.

        Args:
            dataset_id: Dataset ID string
            **kwargs: Additional query parameters

        Returns:
            Data in the format specified by return_type
        """
        logger.info("Fetching current value for dataset %s", dataset_id)

        # Get the most recent record by limiting to 1 and ordering by
        # datetime desc
        kwargs["limit"] = 1
        if "order_by" not in kwargs:
            kwargs["order_by"] = "-datetime"

        response = self.client.get_records(dataset_id, **kwargs)
        records = response.get("records", [])

        return self._format_output(records)

    def fetch_data_between(
        self,
        dataset_id: str,
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        **kwargs
    ) -> Any:
        """
        Fetch data between two dates.

        Args:
            dataset_id: Dataset ID string
            start_date: Start date (ISO format string or datetime)
            end_date: End date (ISO format string or datetime)
            **kwargs: Additional query parameters

        Returns:
            Data in the format specified by return_type
        """
        # Convert datetime objects to ISO format strings
        if isinstance(start_date, datetime):
            start_date = start_date.isoformat()
        if isinstance(end_date, datetime):
            end_date = end_date.isoformat()

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
        limit = kwargs.get("limit", 100)  # Default batch size

        while True:
            response = self.client.get_records(
                dataset_id,
                limit=limit,
                offset=offset,
                **kwargs
            )
            batch_records = response.get("results", [])

            if not batch_records:
                break

            all_records.extend(batch_records)

            # Check if we got fewer records than requested (end of data)
            if len(batch_records) < limit:
                break

            offset += limit

        return self._format_output(all_records)

    def _format_output(self, records: List[dict]) -> Any:
        """
        Format the output according to the specified return type.

        Args:
            records: List of record dictionaries

        Returns:
            Data in the specified format
        """
        if self.return_type == "json":
            return records
        elif self.return_type == "pandas":
            return pd.DataFrame(records)
        elif self.return_type == "polars":
            return pl.DataFrame(records)
        else:
            raise ValueError(f"Unsupported return type: {self.return_type}")
