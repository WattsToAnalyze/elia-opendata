"""
Simple tests for the Elia OpenData data processor.
"""
import responses
from datetime import datetime
from elia_opendata.data_processor import EliaDataProcessor
from elia_opendata.client import EliaClient
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_REALTIME


def test_data_processor_initialization():
    """Test data processor initialization."""
    # Test with default values
    processor = EliaDataProcessor()
    assert isinstance(processor.client, EliaClient)
    assert processor.return_type == "json"
    
    # Test with custom return type
    processor = EliaDataProcessor(return_type="pandas")
    assert processor.return_type == "pandas"
    
    # Test with custom client
    custom_client = EliaClient(timeout=60)
    processor = EliaDataProcessor(client=custom_client, return_type="polars")
    assert processor.client == custom_client
    assert processor.return_type == "polars"


@responses.activate
def test_fetch_current_value():
    """Test fetching the current/most recent value from a dataset."""
    # Mock response data for current value (uses "records" key)
    mock_response = {
        "total_count": 1,
        "records": [
            {
                "datetime": "2025-08-17T12:00:00+00:00",
                "resolutioncode": "PT1M",
                "imbalanceprice": 45.5,
                "systemimbalance": 150.0
            }
        ]
    }
    
    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )
    
    # Test with JSON return type
    processor = EliaDataProcessor(return_type="json")
    result = processor.fetch_current_value(IMBALANCE_PRICES_REALTIME)
    
    # Verify the response
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["datetime"] == "2025-08-17T12:00:00+00:00"
    assert result[0]["imbalanceprice"] == 45.5


@responses.activate
def test_fetch_data_between():
    """Test fetching data between two specific dates."""
    # Mock response data for date range (uses "results" key)
    mock_response = {
        "total_count": 2,
        "results": [
            {
                "datetime": "2025-08-17T10:00:00+00:00",
                "resolutioncode": "PT1M",
                "imbalanceprice": 40.0,
                "systemimbalance": 120.0
            },
            {
                "datetime": "2025-08-17T11:00:00+00:00",
                "resolutioncode": "PT1M",
                "imbalanceprice": 50.0,
                "systemimbalance": 180.0
            }
        ]
    }
    
    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )
    
    # Test with datetime objects
    processor = EliaDataProcessor(return_type="json")
    start_date = datetime(2025, 8, 17, 10, 0, 0)
    end_date = datetime(2025, 8, 17, 12, 0, 0)
    
    result = processor.fetch_data_between(
        IMBALANCE_PRICES_REALTIME,
        start_date,
        end_date
    )
    
    # Verify the response
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["datetime"] == "2025-08-17T10:00:00+00:00"
    assert result[1]["datetime"] == "2025-08-17T11:00:00+00:00"


@responses.activate
def test_json_conversion():
    """Test JSON output format (default)."""
    # Mock response
    mock_response = {
        "total_count": 1,
        "records": [
            {
                "datetime": "2025-08-17T12:00:00+00:00",
                "imbalanceprice": 25.5
            }
        ]
    }
    
    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )
    
    # Test JSON format
    processor = EliaDataProcessor(return_type="json")
    result = processor.fetch_current_value(IMBALANCE_PRICES_REALTIME)
    
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], dict)
    assert result[0]["datetime"] == "2025-08-17T12:00:00+00:00"


@responses.activate
def test_pandas_conversion():
    """Test Pandas DataFrame conversion."""
    # Mock response
    mock_response = {
        "total_count": 1,
        "records": [
            {
                "datetime": "2025-08-17T12:00:00+00:00",
                "imbalanceprice": 25.5,
                "systemimbalance": 100.0
            }
        ]
    }
    
    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )
    
    # Test pandas format
    try:
        import pandas as pd
        processor = EliaDataProcessor(return_type="pandas")
        result = processor.fetch_current_value(IMBALANCE_PRICES_REALTIME)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "datetime" in result.columns
        assert "imbalanceprice" in result.columns
        assert result.iloc[0]["imbalanceprice"] == 25.5
    except ImportError:
        # Skip test if pandas is not available
        pass


@responses.activate
def test_polars_conversion():
    """Test Polars DataFrame conversion."""
    # Mock response
    mock_response = {
        "total_count": 1,
        "records": [
            {
                "datetime": "2025-08-17T12:00:00+00:00",
                "imbalanceprice": 30.0,
                "systemimbalance": 120.0
            }
        ]
    }
    
    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )
    
    # Test polars format
    try:
        import polars as pl
        processor = EliaDataProcessor(return_type="polars")
        result = processor.fetch_current_value(IMBALANCE_PRICES_REALTIME)
        
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        assert "datetime" in result.columns
        assert "imbalanceprice" in result.columns
        assert result.row(0, named=True)["imbalanceprice"] == 30.0
    except ImportError:
        # Skip test if polars is not available
        pass
