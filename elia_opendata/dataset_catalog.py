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

# Real-time information
REALTIME_SYSTEM_IMBALANCE = "ods169"
REALTIME_IMBALANCE_PRICES_MIN = "ods161"
REALTIME_IMBALANCE_PRICES_QH = "ods162"
REALTIME_ACTIVATED_VOLUMES = "ods135"

REALTIME_INCREMENTAL_BALANCING_ENERGY_BIDS = "ods163"
REALTIME_DECREMENTAL_BALANCING_ENERGY_BIDS = "ods164"

REALTIME_BALANCING_ENERGY_VOLUME_COMPONENTS_MIN = "ods174"
REALTIME_BALANCING_ENERGY_VOLUME_COMPONENTS_QH = "ods167"

REALTIME_BALANCING_ENERGY_PRICE_COMPONENTS_MIN = "ods175"
REALTIME_BALANCING_ENERGY_PRICE_COMPONENTS_QH = "ods168"

# Historical data after MARI/ICAROS go-live (22/05/2024)
IMBALANCE_PRICES_MIN_POST_MARI = "ods133"
IMBALANCE_PRICES_QH_POST_MARI = "ods134"
SYSTEM_IMBALANCE_POST_MARI = "ods126"
ACTIVATED_BALANCING_PRICES_POST_MARI = "ods064"
ACTIVATED_BALANCING_VOLUMES_POST_MARI = "ods063"
AVAILABLE_BALANCING_PRICES_POST_MARI = "ods153"
AVAILABLE_BALANCING_VOLUMES_POST_MARI = "ods152"

# Historical data before MARI/ICAROS go-live (22/05/2024)
IMBALANCE_PRICES_MIN_PRE_MARI = "ods046"
IMBALANCE_PRICES_QH_PRE_MARI = "ods047"
SYSTEM_IMBALANCE_PRE_MARI = "ods045"
ACTIVATED_BALANCING_VOLUMES_PRE_MARI = "ods061"
ACTIVATED_BALANCING_PRICES_PRE_MARI = "ods062"


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

# MARI/ICAROS transition date constant
MARI_TRANSITION_DATE_STR = "2024-05-22"

# Dataset name to ID mapping for MARI transition handling
# Maps friendly dataset names to their pre-MARI and post-MARI dataset IDs
DATASET_NAME_MAPPING = {
    "IMBALANCE_PRICES_QH": {
        "pre_mari": IMBALANCE_PRICES_QH_PRE_MARI,
        "post_mari": IMBALANCE_PRICES_QH_POST_MARI
    },
    "IMBALANCE_PRICES_MIN": {
        "pre_mari": IMBALANCE_PRICES_MIN_PRE_MARI,
        "post_mari": IMBALANCE_PRICES_MIN_POST_MARI
    },
    "SYSTEM_IMBALANCE": {
        "pre_mari": SYSTEM_IMBALANCE_PRE_MARI,
        "post_mari": SYSTEM_IMBALANCE_POST_MARI
    },
    "ACTIVATED_BALANCING_VOLUMES": {
        "pre_mari": ACTIVATED_BALANCING_VOLUMES_PRE_MARI,
        "post_mari": ACTIVATED_BALANCING_VOLUMES_POST_MARI
    },
    "ACTIVATED_BALANCING_PRICES": {
        "pre_mari": ACTIVATED_BALANCING_PRICES_PRE_MARI,
        "post_mari": ACTIVATED_BALANCING_PRICES_POST_MARI
    },
}
