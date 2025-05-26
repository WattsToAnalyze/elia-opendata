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
        batch_size: int = 1000,
        **kwargs
    ) -> Records:
        """
        Fetch all records from a dataset, handling pagination automatically.
        
        Args:
            dataset: Dataset enum or ID string
            batch_size: Number of records per batch
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing all records from the dataset
        """
        logger.info(f"Fetching complete dataset {dataset}")
        all_records = []
        total_count = 0

        for batch in self.client.iter_records(dataset, batch_size=batch_size, **kwargs):
            all_records.extend(batch.records)
            total_count = batch.total_count
            logger.debug(f"Fetched {len(all_records)}/{total_count} records")

        # Create a new Records object with all data
        return Records({
            "total_count": total_count,
            "records": all_records,
            "links": []  # No pagination links needed for complete dataset
        })

    def fetch_date_range(
        self,
        dataset: Union[Dataset, str],
        start_date: Union[str, datetime],
        end_date: Union[str, datetime],
        batch_size: int = 1000,
        **kwargs
    ) -> Records:
        """
        Fetch all records between two dates, handling pagination automatically.
        
        Args:
            dataset: Dataset enum or ID string
            start_date: Start date (ISO format string or datetime)
            end_date: End date (ISO format string or datetime)
            batch_size: Number of records per batch
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing all records in the date range
        """
        # Convert datetime objects to ISO strings if needed
        if isinstance(start_date, datetime):
            start_date = start_date.date().isoformat()
        if isinstance(end_date, datetime):
            end_date = end_date.date().isoformat()
            
        logger.info(f"Fetching dataset {dataset} between {start_date} and {end_date}")
        
        # Use client's date range method with pagination
        where_condition = f"datetime >= '{start_date}' AND datetime <= '{end_date}'"
        if "where" in kwargs:
            kwargs["where"] = f"({kwargs['where']}) AND ({where_condition})"
        else:
            kwargs["where"] = where_condition
            
        return self.fetch_complete_dataset(dataset, batch_size=batch_size, **kwargs)

    def merge_records(self, records_list: List[Records]) -> Records:
        """
        Merge multiple Records objects into a single Records object.
        
        Args:
            records_list: List of Records objects to merge
            
        Returns:
            Combined Records object
        """
        if not records_list:
            return Records({"total_count": 0, "records": [], "links": []})
            
        merged_records = []
        total_count = 0
        
        for records in records_list:
            merged_records.extend(records.records)
            total_count += records.total_count
            
        return Records({
            "total_count": total_count,
            "records": merged_records,
            "links": []
        })

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
        
        # Perform groupby and aggregation
        grouped = df.groupby(field).agg(agg_fields)
        
        # Convert back to Records format
        records_data = []
        for idx, row in grouped.reset_index().iterrows():
            record_dict = row.to_dict()
            records_data.append({"record": {"fields": record_dict}})
            
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