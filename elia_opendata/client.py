"""
Core client implementation for the Elia OpenData API.
"""
import logging
from typing import Dict, Optional, Any, NoReturn
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
    """
    Simple client for the Elia Open Data Portal API records endpoint.

    Basic usage:
        >>> client = EliaClient()
        >>> data = client.get_records("ods032", limit=100)
    """
    
    BASE_URL = "https://opendata.elia.be/api/v2/"
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30
    ) -> None:
        """
        Initialize the Elia API client.

        Args:
            api_key: Optional API key for authenticated requests
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.timeout = timeout
    
    def get_records(
        self,
        dataset_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Get records from a specific dataset.
        Implements the records endpoint directly.

        Args:
            dataset_id: Dataset ID string
            limit: Maximum number of records to return
            offset: Number of records to skip
            where: Filter condition
            **kwargs: Additional query parameters

        Returns:
            Raw JSON response from the API

        Raises:
            APIError: If the request fails
        """
        url = urljoin(self.BASE_URL, f"catalog/datasets/{dataset_id}/records")
        
        headers = {
            "accept": "application/json; charset=utf-8"
        }

        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        # Build parameters
        params: Dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if where is not None:
            params["where"] = where
        
        # Add any additional parameters
        params.update(kwargs)
        
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            self._handle_http_error(e)
        except requests.exceptions.RequestException as e:
            raise EliaConnectionError(f"Connection failed: {str(e)}") from e

    def _handle_http_error(self, e: requests.exceptions.HTTPError) -> NoReturn:
        """Handle HTTP errors and raise appropriate exceptions."""
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
