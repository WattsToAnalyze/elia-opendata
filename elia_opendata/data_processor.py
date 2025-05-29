"""
Data processing utilities for Elia OpenData API.
"""
from typing import List, Union, Optional, Dict, Any, Iterator
from datetime import datetime
import logging
from .client import EliaClient
from .models import Records
from .datasets import Dataset

logger = logging.getLogger(__name__)

class EliaDataProcessor:
    """
    Data processing utilities for working with Elia OpenData datasets.
    Handles pagination, data fetching, and aggregation operations.
    """
    
    def __init__(self, client: Optional[EliaClient] = None):
        """
        Initialize the data processor.
        
        Args:
            client: EliaClient instance. If not provided, creates a new one.
        """
        self.client = client or EliaClient()
    
    def fetch_complete_dataset(
        self,
        dataset: Union[Dataset, str],
        batch_size: int = 100,  # Set to API maximum limit (100)
        **kwargs
    ) -> Records:
        """
        Fetch all records from a dataset, handling pagination automatically.
        
        Args:
            dataset: Dataset enum or ID string
            batch_size: Number of records per batch (default: 100, max allowed by API)
            max_batches: Maximum number of batches to fetch (default: 1000)
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing all records from the dataset
        """
        logger.info(f"Fetching complete dataset {dataset}")
        all_records = []
        total_count = 0
        
        # Ensure batch_size doesn't exceed API limit
        batch_size = min(batch_size, 100)  # API limit is 100
        
        # Set a maximum number of batches to prevent infinite loops
        max_batches = kwargs.pop("max_batches", 1000)  # Default to 1000 batches max
        batch_count = 0
        empty_batch_count = 0
        max_empty_batches = 3  # Stop after 3 consecutive empty batches

        try:
            for batch in self.client.iter_records(dataset, batch_size=batch_size, **kwargs):
                # Keep track of total count for early termination
                if total_count == 0 and batch.total_count > 0:
                    total_count = batch.total_count
                
                if not batch.records:
                    empty_batch_count += 1
                    if empty_batch_count >= max_empty_batches:
                        logger.warning(f"Received {max_empty_batches} consecutive empty batches, stopping.")
                        break
                    continue
                else:
                    empty_batch_count = 0  # Reset empty batch counter
                
                all_records.extend(batch.records)
                batch_count += 1
                logger.debug(f"Fetched {len(all_records)}/{total_count} records (batch {batch_count})")
                
                # Safety check to prevent infinite loops
                if batch_count >= max_batches:
                    logger.warning(f"Reached maximum batch count ({max_batches}), stopping.")
                    break
                
                # Early termination if we have all data
                if total_count > 0 and len(all_records) >= total_count:
                    logger.debug("Fetched all available records based on count.")
                    break
                
                # Check if we've reached the end of pagination
                if not batch.has_next:
                    logger.debug("No more pages available.")
                    break
        except Exception as e:
            # Don't lose data if we hit an error after fetching some records
            logger.error(f"Error during data fetching: {str(e)}")
            if not all_records:
                # Re-raise if we didn't get any records
                raise

        # Create a new Records object with all data
        return Records({
            "total_count": len(all_records),  # Use actual count of records fetched
            "records": all_records,
            "links": []  # No pagination links needed for complete dataset
        })

    def fetch_date_range(
        self,
        dataset: Union[Dataset, str],
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        batch_size: int = 100,  # Use API maximum as default
        **kwargs
    ) -> Records:
        """
        Fetch all records between two dates, handling pagination automatically.
        
        Args:
            dataset: Dataset enum or ID string
            start_date: Start date (ISO format string or datetime)
            end_date: End date (ISO format string or datetime) 
            batch_size: Number of records per batch (default: 100, which is API maximum)
            max_batches: Maximum number of batches to fetch
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing all records in the date range
        """
        # Ensure we're using proper ISO format strings for the API query
        if isinstance(start_date, datetime):
            # Keep milliseconds for more precise filtering
            start_date = start_date.isoformat()
        if isinstance(end_date, datetime):
            end_date = end_date.isoformat()
            
        logger.info(f"Fetching dataset {dataset} between {start_date} and {end_date}")
        
        # Ensure batch_size doesn't exceed API limit
        batch_size = min(batch_size, 100)  # API limit is 100
            
        # Build the date filter condition
        where_condition = f"datetime >= '{start_date}' AND datetime <= '{end_date}'"
        if "where" in kwargs:
            kwargs["where"] = f"({kwargs['where']}) AND ({where_condition})"
        else:
            kwargs["where"] = where_condition
            
        # Calculate expected record count for time range to optimize fetching
        try:
            # Try to parse dates to estimate record count
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            time_span_hours = (end_dt - start_dt).total_seconds() / 3600
            
            # Most datasets have 15-minute intervals (4 records per hour)
            est_records = int(time_span_hours * 4)
            
            # If estimated record count is very large, increase the batch size 
            # to the maximum allowed to reduce API calls
            if est_records > 1000:
                batch_size = 100  # API maximum
                
            # If it's a small dataset, reduce default max_batches to avoid unnecessary API calls
            if est_records < 500 and "max_batches" not in kwargs:
                kwargs["max_batches"] = max(10, est_records // batch_size + 2)
                
        except (ValueError, TypeError):
            # If we can't parse the dates, use the default settings
            pass
            
        # Pass the optimized batch size to fetch_complete_dataset
        return self.fetch_complete_dataset(dataset, batch_size=batch_size, **kwargs)

    def aggregate_by_field(
        self,
        records: Records,
        field: str,
        agg_fields: Dict[str, str]
    ) -> Records:
        """
        Aggregate records by a specific field using specified aggregation functions.
        
        Args:
            records: Records object to aggregate
            field: Field to group by
            agg_fields: Dictionary mapping field names to aggregation functions
                       (e.g., {"value": "sum", "time": "max"})
                       or lists of aggregation functions 
                       (e.g., {"value": ["sum", "mean", "max"], "time": "max"})
                       
        Returns:
            Records object with aggregated data
            
        Example:
            >>> processor = EliaDataProcessor()
            >>> data = processor.fetch_complete_dataset(Dataset.SOLAR_GENERATION)
            >>> daily_sum = processor.aggregate_by_field(
            ...     data,
            ...     "date",
            ...     {"solar_power": "sum", "datetime": "max"}
            ... )
        """
        pd = records._ensure_dependencies("pandas")
        
        # Convert to pandas for aggregation
        df = records.to_pandas()
        
        # Make a copy to avoid warnings and verify the field exists
        if field not in df.columns:
            raise ValueError(f"Field '{field}' not found in records. Available fields: {list(df.columns)}")
            
        # Perform groupby and aggregation
        grouped = df.groupby(field).agg(agg_fields)
        
        # Reset index to make the groupby field a column again
        result_df = grouped.reset_index()
        
        # Convert back to Records format
        records_data = []
        for _, row in result_df.iterrows():
            # Convert Series to dict and handle NaN values
            fields_dict = {}
            for col_name, value in row.items():
                if pd.isna(value):  # Handle NaN values
                    fields_dict[col_name] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    # Convert timestamps to ISO format strings
                    fields_dict[col_name] = value.isoformat()
                else:
                    fields_dict[col_name] = value
                    
            records_data.append({"record": {"fields": fields_dict}})
        
        return Records({
            "total_count": len(records_data),
            "records": records_data,
            "links": []
        })

    def to_dataframe(
        self,
        records: Records,
        output_format: str = "pandas"
    ) -> Any:
        """
        Convert Records to the specified DataFrame format.
        
        Args:
            records: Records object to convert
            output_format: Target format ("pandas", "polars", or "numpy")
            
        Returns:
            DataFrame in the specified format
        """
        formats = {
            "pandas": records.to_pandas,
            "polars": records.to_polars,
            "numpy": records.to_numpy
        }
        
        if output_format not in formats:
            raise ValueError(
                f"Unsupported output format: {output_format}. "
                f"Supported formats: {list(formats.keys())}"
            )
            
        return formats[output_format]()