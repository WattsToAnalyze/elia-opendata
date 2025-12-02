"""
Unit tests for the Elia OpenData data processor.
"""
from datetime import datetime
import pytz
import pandas as pd
from elia_opendata.data_processor import EliaDataProcessor, DATE_FORMAT
from elia_opendata.client import EliaClient
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_REALTIME


def test_current_imbalance_fetching():
    """
    Test if the current imbalance published prices is working correctly.

    """

    processor = EliaDataProcessor(return_type="pandas")

    df = processor.fetch_current_value(IMBALANCE_PRICES_REALTIME)

    assert isinstance(df, pd.DataFrame)
    
    now = datetime.now(pytz.utc)
    time_col = pd.to_datetime(df["datetime"]).dt.tz_convert("UTC").iloc[0]
    delta = now - time_col.to_pydatetime()
    assert abs(delta.total_seconds()) < 1800  # within the last 30-minutes
