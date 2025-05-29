"""
Tests for measuring performance of fetch operations.
"""
import pytest
import logging
import time
from datetime import datetime, timedelta
from elia_opendata.data_processor import EliaDataProcessor
from elia_opendata.client import EliaClient
from elia_opendata.datasets import Dataset

# Configure logging
logging.basicConfig(level=logging.INFO)  # INFO level to reduce output
logger = logging.getLogger(__name__)

@pytest.fixture
def processor():
    """Fixture for EliaDataProcessor instance with longer timeout."""
    client = EliaClient(timeout=120)
    return EliaDataProcessor(client=client)

@pytest.mark.parametrize("batch_size", [25, 50, 100])
def test_fetch_complete_dataset_performance(processor, batch_size):
    """Test performance of fetch_complete_dataset with different batch sizes."""
    print(f"\nTesting fetch_complete_dataset with batch_size={batch_size}:")
    start_time = time.time()
    
    result = processor.fetch_complete_dataset(
        Dataset.PV_PRODUCTION,
        batch_size=batch_size,
        where="datetime >= '2025-01-01' AND datetime <= '2025-01-02'",
        max_batches=10  # Limit to prevent too many API calls
    )
    
    end_time = time.time()
    elapsed = end_time - start_time
    record_count = len(result.records)
    rps = record_count/elapsed if elapsed > 0 else 0
    
    print(f"Fetched {record_count} records in {elapsed:.2f} seconds")
    print(f"Records per second: {rps:.2f}")
    print(f"Total records according to API: {result.total_count}")
    
    # Basic assertions to verify the test ran properly
    assert record_count > 0, "Should fetch at least some records"
    assert result.total_count >= record_count, "Total count should be at least as many as fetched records"

@pytest.mark.parametrize("label,delta", [
    ("1 hour", timedelta(hours=1)),
    ("6 hours", timedelta(hours=6)),
    ("1 day", timedelta(days=1))
])
def test_fetch_date_range_performance(processor, label, delta):
    """Test performance of fetch_date_range with different time ranges."""
    print(f"\nTesting fetch_date_range with time range: {label}")
    end_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_date = end_date - delta
    
    start_time = time.time()
    
    result = processor.fetch_date_range(
        Dataset.PV_PRODUCTION,
        start_date=start_date,
        end_date=end_date,
        max_batches=10  # Limit to prevent too many API calls
    )
    
    end_time = time.time()
    elapsed = end_time - start_time
    record_count = len(result.records)
    rps = record_count/elapsed if record_count > 0 and elapsed > 0 else 0
    
    print(f"Fetched {record_count} records in {elapsed:.2f} seconds")
    print(f"Records per second: {rps:.2f}")
    print(f"Total records according to API: {result.total_count}")
    
    # Basic assertions
    assert record_count >= 0, "Should have a valid record count"
    
    # Verify record dates if we have results
    if result.records and len(result.records) > 0:
        sample_size = min(10, len(result.records))
        count_in_range = 0
        
        for record in result.records[:sample_size]:
            fields = record["record"]["fields"]
            record_date = datetime.fromisoformat(fields["datetime"].replace('Z', '+00:00'))
            # Make start_date and end_date timezone-aware for comparison
            aware_start_date = start_date.replace(tzinfo=record_date.tzinfo)
            aware_end_date = end_date.replace(tzinfo=record_date.tzinfo)
            if aware_start_date <= record_date <= aware_end_date:
                count_in_range += 1
                
        print(f"Records in correct date range: {count_in_range}/{sample_size} checked")
        assert count_in_range > 0, "At least some records should be in the specified date range"
