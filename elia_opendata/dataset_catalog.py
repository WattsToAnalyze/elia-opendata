"""Dataset Catalog for Elia OpenData API.

This module provides a comprehensive catalog of all available dataset IDs from
the Elia OpenData API as simple constants. It serves as a central registry for
dataset identifiers, making it easy to discover and use the correct IDs when
querying the API.

The constants are organized by category (Load/Consumption, Generation,
Transmission, Balancing, Congestion Management, Capacity, and Bidding/Market)
to help users find relevant datasets quickly.


Example:
    Import specific dataset constants:

    ```python
    from elia_opendata.dataset_catalog import TOTAL_LOAD, IMBALANCE_PRICES_QH  # noqa: E501
    from elia_opendata.client import EliaClient

    client = EliaClient()
    load_data = client.get_records(TOTAL_LOAD, limit=10)
    price_data = client.get_records(IMBALANCE_PRICES_QH, limit=10)
    ```

    Import all constants:

    ```python
    from elia_opendata.dataset_catalog import *
    from elia_opendata.data_processor import EliaDataProcessor

    processor = EliaDataProcessor(return_type="pandas")
    wind_df = processor.fetch_current_value(WIND_PRODUCTION)
    pv_df = processor.fetch_current_value(PV_PRODUCTION)
    ```

    Use with date range queries:

    ```python
    from datetime import datetime
    from elia_opendata.dataset_catalog import SYSTEM_IMBALANCE

    start = datetime(2023, 1, 1)
    end = datetime(2023, 1, 31)
    data = processor.fetch_data_between(SYSTEM_IMBALANCE, start, end)
    ```

Note:
    All dataset IDs are strings that correspond to the official Elia OpenData
    API dataset identifiers. These constants provide a convenient and
    type-safe way to reference datasets without memorizing numeric IDs.
"""

# Load/Consumption
TOTAL_LOAD = "ods001"
LOAD = "ods003"
TOTAL_LOAD_NRT = "ods002"

# Generation
INSTALLED_POWER = "ods036"
WIND_PRODUCTION = "ods031"
PV_PRODUCTION = "ods032"
PV_PRODUCTION_NRT = "ods087"
CO2_INTENSITY = "ods192"
CO2_INTENSITY_NRT = "ods191"

# Transmission
Q_AHEAD_NTC = "ods006"
M_AHEAD_NTC = "ods007"
WEEK_AHEAD_NTC = "ods008"
DAY_AHEAD_NTC = "ods009"
INTRADAY_NTC = "ods011"
PHYSICAL_FLOWS = "ods124"

# Balancing
IMBALANCE_PRICES_QH = "ods134"
IMBALANCE_PRICES_MIN = "ods133"
IMBALANCE_PRICES_REALTIME = "ods161"
SYSTEM_IMBALANCE = "ods126"
ACTIVATED_BALANCING_PRICES = "ods064"
ACTIVATED_BALANCING_VOLUMES = "ods063"
ACTIVATED_VOLUMES = "ods132"
AVAILABLE_BALANCING_PRICES = "ods153"
AVAILABLE_BALANCING_VOLUMES = "ods152"

# Congestion Management
REDISPATCH_INTERNAL = "ods071"
REDISPATCH_CROSSBORDER = "ods072"
CONGESTION_COSTS = "ods074"
CONGESTION_RISKS = "ods076"
CRI = "ods183"

# Capacity
TRANSMISSION_CAPACITY = "ods006"
INSTALLED_CAPACITY = "ods036"

# Bidding/Market
INTRADAY_AVAILABLE_CAPACITY = "ods013"
LONG_TERM_AVAILABLE_CAPACITY = "ods014"
