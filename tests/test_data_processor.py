"""
Tests for the data processor module.
"""
import pytest
import time
from datetime import datetime, timedelta
from elia_opendata.data_processor import EliaDataProcessor
from elia_opendata.client import EliaClient
from elia_opendata.models import Records
from elia_opendata.datasets import Dataset
from elia_opendata.error import ConnectionError

@pytest.fixture
def processor():
    """Fixture for EliaDataProcessor instance with longer timeout."""
    client = EliaClient(timeout=120)  # Increase timeout to 120 seconds
    return EliaDataProcessor(client=client)

@pytest.fixture
def sample_records():
    """Fixture for sample Records object."""
    return Records({
        "total_count": 2,
        "records": [
            {"record": {"fields": {"datetime": "2025-01-01", "value": 100}}},
            {"record": {"fields": {"datetime": "2025-01-02", "value": 200}}}
        ],
        "links": []
    })

def test_init_with_default_client():
    """Test initialization with default client."""
    processor = EliaDataProcessor()
    assert isinstance(processor.client, EliaClient)

def test_init_with_custom_client():
    """Test initialization with custom client."""
    custom_client = EliaClient()
    processor = EliaDataProcessor(client=custom_client)
    assert processor.client == custom_client

def test_merge_records_empty_list(processor):
    """Test merging empty list of records."""
    result = processor.merge_records([])
    assert result.total_count == 0
    assert len(result.records) == 0

def test_merge_records(processor, sample_records):
    """Test merging multiple Records objects."""
    merged = processor.merge_records([sample_records, sample_records])
    assert merged.total_count == 4
    assert len(merged.records) == 4

def test_to_dataframe_invalid_format(processor, sample_records):
    """Test to_dataframe with invalid format."""
    with pytest.raises(ValueError) as exc_info:
        processor.to_dataframe(sample_records, output_format="invalid")
    assert "Unsupported output format" in str(exc_info.value)

def test_fetch_date_range_datetime_conversion(processor):
    """Test date range with datetime objects."""
    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 2)
    
    # Mock the fetch_complete_dataset to verify the converted dates
    original_fetch = processor.fetch_complete_dataset
    def mock_fetch(dataset, **kwargs):
        where = kwargs.get('where', '')
        assert '2025-01-01' in where
        assert '2025-01-02' in where
        return Records({"total_count": 0, "records": [], "links": []})
    
    processor.fetch_complete_dataset = mock_fetch
    processor.fetch_date_range(Dataset.PV_PRODUCTION, start, end)
    processor.fetch_complete_dataset = original_fetch

def test_aggregate_by_field(processor, sample_records):
    """Test aggregation by field."""
    try:
        agg_fields = {"value": "sum"}
        result = processor.aggregate_by_field(sample_records, "datetime", agg_fields)
        assert result.total_count == 2
        # Note: Full assertion of aggregated values would require pandas
    except ImportError:
        pytest.skip("Pandas not installed")

@pytest.mark.integration
def test_fetch_complete_dataset_integration(processor):
    """Integration test for fetching complete dataset."""
    # Use a very small time window (1 hour) to minimize data volume
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(hours=1)
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            result = processor.fetch_date_range(
                Dataset.PV_PRODUCTION,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                batch_size=5
            )
            assert isinstance(result, Records)
            assert hasattr(result, 'total_count')
            assert hasattr(result, 'records')
            return  # Test passed
            
        except ConnectionError as e:
            if attempt == max_retries - 1:  # Last attempt
                raise  # Re-raise the last error
            time.sleep(retry_delay)  # Wait before retrying
            retry_delay *= 2  # Exponential backoff