"""Dataset Catalog for Elia OpenData API.

This module provides a comprehensive catalog of all available dataset IDs from
the Elia OpenData API as simple constants. It serves as a central registry for
dataset identifiers, making it easy to discover and use the correct IDs when
querying the API.

The constants are organized by category (Load/Consumption, Generation,
Transmission, Balancing, Congestion Management, Capacity, and Bidding/Market)
to help users find relevant datasets quickly.

Constants:
    Load/Consumption datasets:
        TOTAL_LOAD (str): "ods001" - Measured and forecasted total load on the
            Belgian grid (Historical data)
        LOAD (str): "ods003" - Load on the Elia grid
        TOTAL_LOAD_NRT (str): "ods002" - Measured and forecasted total load on
            the Belgian grid (Near-real-time)

    Generation datasets:
        INSTALLED_POWER (str): "ods036" - Actual installed power by unit and
            fuel type
        WIND_PRODUCTION (str): "ods031" - Wind power production estimation and
            forecast on Belgian grid (Historical)
        PV_PRODUCTION (str): "ods032" - Photovoltaic power production
            estimation and forecast on Belgian grid (Historical)
        PV_PRODUCTION_NRT (str): "ods087" - Photovoltaic power production
            estimation and forecast on Belgian grid (Near real-time)
        CO2_INTENSITY (str): "ods192" - Production-Based CO2 Intensity and
            Consumption-Based CO2 Intensity Belgium (Historical)
        CO2_INTENSITY_NRT (str): "ods191" - Production-Based CO2 Intensity and
            Consumption-Based CO2 Intensity Belgium (Near real-time)

    Transmission datasets:
        Q_AHEAD_NTC (str): "ods006" - Quarter-ahead forecast net transfer
            capacity and capacity for auction by border
        M_AHEAD_NTC (str): "ods007" - Month-ahead forecast net transfer
            capacity and capacity for auction by border
        WEEK_AHEAD_NTC (str): "ods008" - Week-ahead forecast net transfer
            capacity by border
        DAY_AHEAD_NTC (str): "ods009" - Day-ahead forecast net transfer
            capacity between Belgium and United Kingdom
        INTRADAY_NTC (str): "ods011" - Intraday net transfer capacity between
            Belgium and United Kingdom
        PHYSICAL_FLOWS (str): "ods124" - Physical flows on the Belgian
            high-voltage grid

    Balancing datasets:
        IMBALANCE_PRICES_QH (str): "ods134" - Imbalance prices per
            quarter-hour (Historical)
        IMBALANCE_PRICES_MIN (str): "ods133" - Imbalance price per minute
            (Historical)
        IMBALANCE_PRICES_REALTIME (str): "ods161" - Real-time imbalance prices
            with system data (Near real-time)
        SYSTEM_IMBALANCE (str): "ods126" - Current system imbalance
            (Historical)
        ACTIVATED_BALANCING_PRICES (str): "ods064" - Activated balancing energy
            prices per quarter hour (Historical)
        ACTIVATED_BALANCING_VOLUMES (str): "ods063" - Activated balancing
            energy volumes per quarter-hour (Historical)
        ACTIVATED_VOLUMES (str): "ods132" - Activated Volumes in Belgium
            (Historical)
        AVAILABLE_BALANCING_PRICES (str): "ods153" - Available balancing energy
            prices per quarter hour in Belgium (Historical)
        AVAILABLE_BALANCING_VOLUMES (str): "ods152" - Available balancing
            energy volumes per quarter-hour (Historical)

    Congestion Management datasets:
        REDISPATCH_INTERNAL (str): "ods071" - Congestion management activations
            - Internal redispatching
        REDISPATCH_CROSSBORDER (str): "ods072" - Congestion management
            activations - Cross-border redispatching
        CONGESTION_COSTS (str): "ods074" - Congestion management costs
        CONGESTION_RISKS (str): "ods076" - Congestion risks 'Red Zones' per
            electrical zone
        CRI (str): "ods183" - Congestion Risk Indicator (CRI) per electrical
            zone

    Capacity datasets:
        TRANSMISSION_CAPACITY (str): "ods006" - Quarter-ahead forecast net
            transfer capacity
        INSTALLED_CAPACITY (str): "ods036" - Actual installed power by unit and
            fuel type

    Bidding/Market datasets:
        INTRADAY_AVAILABLE_CAPACITY (str): "ods013" - Intraday available
            capacity at last closed gate by border
        LONG_TERM_AVAILABLE_CAPACITY (str): "ods014" - Long term available
            capacity and use it or sell it allocated capacity by border

Example:
    Import specific dataset constants:

    >>> from elia_opendata.dataset_catalog import TOTAL_LOAD, IMBALANCE_PRICES_QH  # noqa: E501
    >>> from elia_opendata.client import EliaClient
    >>>
    >>> client = EliaClient()
    >>> load_data = client.get_records(TOTAL_LOAD, limit=10)
    >>> price_data = client.get_records(IMBALANCE_PRICES_QH, limit=10)

    Import all constants:

    >>> from elia_opendata.dataset_catalog import *
    >>> from elia_opendata.data_processor import EliaDataProcessor
    >>>
    >>> processor = EliaDataProcessor(return_type="pandas")
    >>> wind_df = processor.fetch_current_value(WIND_PRODUCTION)
    >>> pv_df = processor.fetch_current_value(PV_PRODUCTION)

    Use with date range queries:

    >>> from datetime import datetime
    >>> from elia_opendata.dataset_catalog import SYSTEM_IMBALANCE
    >>>
    >>> start = datetime(2023, 1, 1)
    >>> end = datetime(2023, 1, 31)
    >>> data = processor.fetch_data_between(SYSTEM_IMBALANCE, start, end)

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
