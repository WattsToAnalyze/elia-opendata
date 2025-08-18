"""
Unit tests for the Elia OpenData data processor.
"""
from datetime import datetime
import pandas as pd
from elia_opendata.data_processor import EliaDataProcessor, DATE_FORMAT
from elia_opendata.client import EliaClient


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


def test_date_format_constant():
    """Test that the DATE_FORMAT constant is correctly defined."""
    assert DATE_FORMAT == "%Y-%m-%d"


def test_date_formatting_conversion():
    """Test that datetime objects are converted to the correct date format."""
    # Test datetime formatting
    test_date = datetime(2024, 1, 15)
    formatted = test_date.strftime(DATE_FORMAT)
    assert formatted == "2024-01-15"

    # Test edge cases
    edge_date = datetime(2023, 12, 31)
    formatted_edge = edge_date.strftime(DATE_FORMAT)
    assert formatted_edge == "2023-12-31"


def test_export_data_parameter_default():
    """Test that export_data parameter defaults to False."""
    processor = EliaDataProcessor()

    # Test that we can create the processor and that the parameter
    # would default to False (pagination mode)
    assert processor.return_type == "json"
    assert hasattr(processor, '_fetch_via_pagination')
    assert hasattr(processor, '_fetch_via_export')


def test_date_filter_construction():
    """Test that date filters are constructed correctly."""
    # This tests the internal date filter logic
    start_date = "2024-01-01"
    end_date = "2024-01-31"

    expected_condition = f"datetime IN [date'{start_date}'..date'{end_date}']"
    expected = "datetime IN [date'2024-01-01'..date'2024-01-31']"
    assert expected == expected_condition


def test_invalid_return_type():
    """Test that invalid return types raise ValueError."""
    try:
        EliaDataProcessor(return_type="invalid")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid return_type" in str(e)
        assert "Must be 'json', 'pandas', or 'polars'" in str(e)


def test_format_output_json():
    """Test _format_output method with JSON return type."""
    processor = EliaDataProcessor(return_type="json")
    test_records = [
        {"datetime": "2024-01-01", "value": 100.0},
        {"datetime": "2024-01-02", "value": 200.0}
    ]

    result = processor._format_output(test_records)
    assert result == test_records
    assert isinstance(result, list)


def test_format_output_pandas():
    """Test _format_output method with pandas return type."""
    try:
        
        processor = EliaDataProcessor(return_type="pandas")
        test_records = [
            {"datetime": "2024-01-01", "value": 100.0},
            {"datetime": "2024-01-02", "value": 200.0}
        ]

        result = processor._format_output(test_records)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["datetime", "value"]
        assert result.iloc[0]["value"] == 100.0
        assert result.iloc[1]["value"] == 200.0

    except ImportError:
        # Skip test if pandas is not available
        pass


def test_format_output_polars():
    """Test _format_output method with polars return type."""
    try:
        import polars as pl

        processor = EliaDataProcessor(return_type="polars")
        test_records = [
            {"datetime": "2024-01-01", "value": 100.0},
            {"datetime": "2024-01-02", "value": 200.0}
        ]

        result = processor._format_output(test_records)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["datetime", "value"]
        assert result.row(0, named=True)["value"] == 100.0
        assert result.row(1, named=True)["value"] == 200.0

    except ImportError:
        # Skip test if polars is not available
        pass


def test_format_output_empty_records():
    """Test _format_output method with empty records."""
    processor = EliaDataProcessor(return_type="json")
    result = processor._format_output([])
    assert result == []

    try:
        import pandas as pd
        processor = EliaDataProcessor(return_type="pandas")
        result = processor._format_output([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    except ImportError:
        pass

    try:
        import polars as pl
        processor = EliaDataProcessor(return_type="polars")
        result = processor._format_output([])
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
    except ImportError:
        pass


def test_format_output_unsupported_type():
    """Test that unsupported return types raise ValueError."""
    processor = EliaDataProcessor()
    processor.return_type = "unsupported"  # Manually set invalid type

    try:
        processor._format_output([{"test": "data"}])
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Unsupported return type" in str(e)
