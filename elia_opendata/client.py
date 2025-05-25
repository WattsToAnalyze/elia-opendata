"""
Core client implementation for the Elia OpenData API.
"""
import requests
import logging
from typing import Dict, List, Optional, Union, Iterator
from urllib.parse import urljoin

from .models import CatalogEntry, DatasetMetadata, Records, BaseModel
from .datasets import Dataset

from .error import (
    EliaError,
    RateLimitError,
    AuthError,
    APIError,
    ValidationError,
    ConnectionError,
    ODSQLError,
)

# Configure logging
logger = logging.getLogger(__name__)

class EliaClient:
    """
    Client for interacting with the Elia Open Data Portal API.
    
    Basic usage:
        >>> from elia_opendata import EliaClient, Dataset
        >>> client = EliaClient()
        >>> # Get all datasets
        >>> catalog = client.get_catalog()
        >>> # Get specific dataset using enum
        >>> data = client.get_records(Dataset.SOLAR_GENERATION)

    Error handling:
        >>> try:
        ...     client.get_records(Dataset.SOLAR_GENERATION, where="invalid_function()")
        ... except ODSQLError as e:
        ...     print(f"Query error: {e.message}")  # Query error: ODSQL query is malformed...
        ... except RateLimitError as e:
        ...     print(f"Rate limit exceeded. Reset at {e.reset_time}")
        ...     print(f"Limit: {e.call_limit} calls per {e.limit_time_unit}")
    """
    
    BASE_URL = "https://opendata.elia.be/api/v2/"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """
        Initialize the Elia API client.
        
        Args:
            api_key: Optional API key for authenticated requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
        """
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        logger.debug(f"Initializing EliaClient with timeout={timeout}, max_retries={max_retries}")
        
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})
            logger.debug("API key provided, authorization header set")
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
    ) -> Dict:
        """
        Make an HTTP request to the API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            params: Query parameters
            data: Request body data
            
        Returns:
            API response data
            
        Raises:
            EliaError: If the request fails
        """
        logger.debug(f"Making {method} request to {endpoint}")
        url = urljoin(self.BASE_URL, endpoint)
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            error_response = e.response.json() if e.response.content else {}
            
            if e.response.status_code == 429:
                logger.error("API rate limit exceeded")
                raise RateLimitError(
                    message=error_response.get('error', "API rate limit exceeded"),
                    call_limit=error_response.get('call_limit'),
                    reset_time=error_response.get('reset_time'),
                    limit_time_unit=error_response.get('limit_time_unit'),
                    response=e.response
                )
            elif e.response.status_code == 401:
                logger.error("Authentication failed - invalid or missing API key")
                raise AuthError("Authentication failed", response=e.response)
            elif e.response.status_code == 400 and error_response.get('error_code') == 'ODSQLError':
                logger.error(f"ODSQL Error: {error_response.get('message')}")
                raise ODSQLError(
                    message=error_response.get('message', "ODSQL query is malformed"),
                    response=e.response
                )
            else:
                logger.error(f"API request failed with status {e.response.status_code}: {str(e)}")
                raise APIError(
                    message=error_response.get('message', f"API request failed: {str(e)}"),
                    error_code=error_response.get('error_code', str(e.response.status_code)),
                    response=e.response
                )
        except requests.exceptions.RequestException as e:
            logger.error(f"Connection error occurred: {str(e)}")
            raise ConnectionError(f"Connection failed: {str(e)}")

    def get_catalog(self, **kwargs) -> List[CatalogEntry]:
        """
        Get list of available datasets from the catalog.
        Endpoint: GET /api/v2/catalog/datasets
        
        Args:
            **kwargs: Additional query parameters
            
        Returns:
            List of CatalogEntry objects representing available datasets
        """
        logger.info("Fetching catalog entries")
        data = self._make_request("GET", "catalog/datasets", params=kwargs)
        logger.debug(f"Raw catalog response: {data}")

        # Correct extraction of dataset list from API response (use 'datasets' key)
        items = data.get('datasets', []) if isinstance(data, dict) else data
        # Pass only the 'dataset' dict to CatalogEntry
        entries = [CatalogEntry(item) for item in items if "dataset" in item]
        logger.debug(f"Retrieved {len(entries)} catalog entries")
        return entries

    def get_dataset(self, dataset: Union[Dataset, str], **kwargs) -> DatasetMetadata:
        """
        Get detailed metadata for a specific dataset.
        Endpoint: GET /api/v2/catalog/datasets/{dataset_id}
        
        Args:
            dataset: Dataset enum or ID string
            **kwargs: Additional query parameters
            
        Returns:
            DatasetMetadata object with detailed dataset information
        """
        dataset_id = dataset.value if isinstance(dataset, Dataset) else dataset
        logger.info(f"Fetching metadata for dataset {dataset_id}")
        data = self._make_request("GET", f"catalog/datasets/{dataset_id}", params=kwargs)
        metadata = DatasetMetadata(data)
        logger.debug(f"Retrieved metadata for dataset {metadata.title}")
        return metadata

    def get_records(
        self,
        dataset: Union[Dataset, str],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where: Optional[str] = None,
        **kwargs
    ) -> Records:
        """
        Get records from a specific dataset.
        Endpoint: GET /api/v2/catalog/datasets/{dataset_id}/records
        
        Args:
            dataset: Dataset enum or ID string
            limit: Maximum number of records to return
            offset: Number of records to skip
            where: Filter condition
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing the dataset records and metadata
        """
        dataset_id = dataset.value if isinstance(dataset, Dataset) else dataset
        params = {
            "limit": limit,
            "offset": offset,
            "where": where,
            **kwargs
        }
        params = {k: v for k, v in params.items() if v is not None}
        
        logger.info(f"Fetching records for dataset {dataset_id}")
        data = self._make_request(
            "GET",
            f"catalog/datasets/{dataset_id}/records",
            params=params
        )
        records = Records(data)
        logger.debug(f"Retrieved {records.total_count} records")
        return records

    def iter_records(
        self,
        dataset: Union[Dataset, str],
        batch_size: int = 1000,
        **kwargs
    ) -> Iterator[Records]:
        """
        Iterate through all records in a dataset using pagination.
        
        Args:
            dataset: Dataset enum or ID string
            batch_size: Number of records per batch
            **kwargs: Additional query parameters
            
        Yields:
            Records objects in batches
        """
        offset = kwargs.pop("offset", 0)
        while True:
            logger.debug(f"Fetching batch at offset {offset} with size {batch_size}")
            data = self.get_records(
                dataset,
                offset=offset,
                limit=batch_size,
                **kwargs
            )
            yield data
            if not data.has_next:
                logger.debug("No more records available")
                break
            offset += batch_size

    def search_catalog(self, query: str, **kwargs) -> List[CatalogEntry]:
        """
        Search for datasets in the catalog.
        Endpoint: GET /api/v2/catalog/datasets/search
        
        Args:
            query: Search query string
            **kwargs: Additional query parameters
            
        Returns:
            List of matching CatalogEntry objects
        """
        params = {"q": query, **kwargs}
        data = self._make_request("GET", "catalog/datasets/search", params=params)
        return [CatalogEntry(item) for item in data]

    def search_datasets(
        self,
        query: str,
        **kwargs
    ) -> List[Dict]:
        """
        Search for datasets.
        
        Args:
            query: Search query string
            **kwargs: Additional query parameters
            
        Returns:
            List of matching datasets
        """
        params = {"q": query, **kwargs}
        return self._make_request("GET", "catalog/datasets/search", params=params)

    def get_dataset_between(
        self,
        dataset: Union[Dataset, str],
        start_date: str,
        end_date: str,
        **kwargs
    ) -> Records:
        """
        Get dataset records between two dates.
        
        Args:
            dataset: Dataset enum or ID string
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD)
            **kwargs: Additional query parameters
            
        Returns:
            Records object containing the filtered dataset records
            
        Example:
            >>> client = EliaClient()
            >>> data = client.get_dataset_between(
            ...     Dataset.SOLAR_GENERATION,
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31"
            ... )
        """
        # Build the date filter condition
        where_condition = f"datetime >= '{start_date}' AND datetime <= '{end_date}'"
        
        # If there's an existing where condition in kwargs, combine them
        if "where" in kwargs:
            kwargs["where"] = f"({kwargs['where']}) AND ({where_condition})"
        else:
            kwargs["where"] = where_condition
            
        logger.info(f"Fetching records for dataset {dataset} between {start_date} and {end_date}")
        return self.get_records(dataset, **kwargs)