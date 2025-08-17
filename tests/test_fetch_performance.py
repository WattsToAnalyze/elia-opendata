"""
Performance tests for the Elia OpenData API client.
"""
import time
from datetime import datetime, timedelta
from elia_opendata.data_processor import EliaDataProcessor
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_QH


def test_single_batch_performance():
    """Test performance of fetching a single batch of 100 records."""
    print("\n=== Single Batch Performance Test ===")
    
    processor = EliaDataProcessor(return_type="json")
    
    # Record start time
    start_time = time.time()
    
    # Use the underlying client to get 100 records directly
    records = processor.client.get_records(
        IMBALANCE_PRICES_QH,
        limit=100
    )
    print(records)
    # Format the output using the processor's format method
    result = processor._format_output(records)
    
    # Record end time
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Get results and calculate metrics
    records_fetched = len(result)
    
    # Display performance metrics
    print("Records requested: 100")
    print(f"Records fetched: {records_fetched}")
    print(f"Time elapsed: {elapsed_time:.3f} seconds")
    if elapsed_time > 0:
        print(f"Records per second: {records_fetched / elapsed_time:.2f}")
    
    # Verify we got data
    assert records_fetched > 0, "Should fetch at least some records"
    assert records_fetched <= 100, "Should not exceed requested limit"
    assert isinstance(result, list), "Results should be a list"
    
    # Verify record structure (direct client records are flattened)
    if result:
        first_record = result[0]
        assert "datetime" in first_record, "Record should have datetime field"
        assert "imbalanceprice" in first_record, (
            "Record should have imbalanceprice field"
        )
        print(f"Sample record datetime: {first_record['datetime']}")
        print(f"Sample imbalance price: {first_record['imbalanceprice']}")


def test_pagination_performance():
    """Test performance of pagination from 2025-01-01 to 2025-01-07."""
    print("\n=== Pagination Performance Test ===")
    
    processor = EliaDataProcessor(return_type="json")
    
    # Date range: one week in January 2025
    # start_date = datetime(2025, 1, 1)
    # end_date = datetime(2025, 1, 8)
    start_date = "2025-01-01"
    end_date = "2025-01-31"
    
    # Record start time
    start_time = time.time()
    
    # Use fetch_data_between which handles pagination automatically
    print(f"Fetching data from {start_date} to {end_date}")
    
    result = processor.fetch_data_between(
        IMBALANCE_PRICES_QH,
        start_date=start_date,
        end_date=end_date,
        limit=100  # Batch size of 100
    )
    print(result)
    # Record end time
    end_time = time.time()
    total_elapsed = end_time - start_time
    total_records = len(result)
    
    # Display performance metrics
    print("\n=== Pagination Results ===")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Total records fetched: {total_records}")
    print(f"Total time elapsed: {total_elapsed:.3f} seconds")
    if total_elapsed > 0:
        print(f"Records per second: {total_records / total_elapsed:.2f}")
    
    # Verify we got data
    assert total_records > 0, (
        "Should fetch at least some records for the date range"
    )
    assert isinstance(result, list), "All records should be in a list"
    
    # Verify record structure and date range
    if result:
        first_record = result[0]
        last_record = result[-1]
        
        assert "datetime" in first_record, (
            "First record should have datetime field"
        )
        assert "imbalanceprice" in first_record, (
            "First record should have imbalanceprice field"
        )
        
        print(f"First record datetime: {first_record['datetime']}")
        print(f"Last record datetime: {last_record['datetime']}")
        print(f"Sample imbalance price: {first_record['imbalanceprice']}")
        
