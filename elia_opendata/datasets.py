"""
Mapping of human-readable dataset names to Elia OpenData dataset IDs.
"""
from enum import Enum

class DatasetCategory(str, Enum):
    """Categories of Elia OpenData datasets."""
    CONSUMPTION = "consumption"
    GENERATION = "generation"
    TRANSMISSION = "transmission"
    BALANCING = "balancing"
    CONGESTION = "congestion"
    CAPACITY = "capacity"
    BIDDING = "bidding"

class Dataset(str, Enum):
    """
    Human-readable mapping to Elia OpenData dataset IDs.
    Use these values when querying specific datasets.
    """
    # Load/Consumption
    TOTAL_LOAD = "ods001"  # Measured and forecasted total load on the Belgian grid (Historical data)
    LOAD = "ods003"        # Load on the Elia grid
    TOTAL_LOAD_NRT = "ods002"  # Measured and forecasted total load on the Belgian grid (Near-real-time)

    # Generation
    INSTALLED_POWER = "ods036"  # Actual installed power - by unit and by fuel type
    WIND_PRODUCTION = "ods031"  # Wind power production estimation and forecast on Belgian grid (Historical)
    PV_PRODUCTION = "ods032"    # Photovoltaic power production estimation and forecast on Belgian grid (Historical)
    PV_PRODUCTION_NRT = "ods087" # Photovoltaic power production estimation and forecast on Belgian grid (Near real-time)
    CO2_INTENSITY = "ods192"    # Production-Based CO2 Intensity and Consumption-Based CO2 Intensity Belgium (Historical)
    CO2_INTENSITY_NRT = "ods191" # Production-Based CO2 Intensity and Consumption-Based CO2 Intensity Belgium (Near real-time)

    # Transmission
    Q_AHEAD_NTC = "ods006"      # Quarter-ahead forecast net transfer capacity and capacity for auction - by border
    M_AHEAD_NTC = "ods007"      # Month-ahead forecast net transfer capacity and capacity for auction - by border
    WEEK_AHEAD_NTC = "ods008"   # Week-ahead forecast net transfer capacity - by border
    DAY_AHEAD_NTC = "ods009"    # Day-ahead forecast net transfer capacity - between Belgium and United Kingdom
    INTRADAY_NTC = "ods011"     # Intraday net transfer capacity - between Belgium and United Kingdom
    PHYSICAL_FLOWS = "ods124"   # Physical flows on the Belgian high-voltage grid

    # Balancing
    IMBALANCE_PRICES_QH = "ods134"  # Imbalance prices per quarter-hour (Historical)
    IMBALANCE_PRICES_MIN = "ods133" # Imbalance price per minute (Historical)
    SYSTEM_IMBALANCE = "ods126"     # Current system imbalance (Historical)
    ACTIVATED_BALANCING_PRICES = "ods064" # Activated balancing energy prices per quarter hour (Historical)
    ACTIVATED_BALANCING_VOLUMES = "ods063" # Activated balancing energy volumes per quarter-hour (Historical)
    ACTIVATED_VOLUMES = "ods132"    # Activated Volumes in Belgium (Historical)
    AVAILABLE_BALANCING_PRICES = "ods153" # Available balancing energy prices per quarter hour in Belgium (Historical)
    AVAILABLE_BALANCING_VOLUMES = "ods152" # Available balancing energy volumes per quarter-hour (Historical)

    # Congestion Management
    REDISPATCH_INTERNAL = "ods071"  # Congestion management activations - Internal redispatching
    REDISPATCH_CROSSBORDER = "ods072" # Congestion management activations - Cross-border redispatching
    CONGESTION_COSTS = "ods074"      # Congestion management costs
    CONGESTION_RISKS = "ods076"      # Congestion risks 'Red Zones' per electrical zone
    CRI = "ods183"                   # Congestion Risk Indicator (CRI) per electrical zone

    # Capacity
    TRANSMISSION_CAPACITY = "ods006" # Quarter-ahead forecast net transfer capacity
    INSTALLED_CAPACITY = "ods036"    # Actual installed power - by unit and by fuel type

    # Bidding/Market
    INTRADAY_AVAILABLE_CAPACITY = "ods013" # Intraday available capacity at last closed gate - by border
    LONG_TERM_AVAILABLE_CAPACITY = "ods014" # Long term available capacity and use it or sell it allocated capacity - by border

    @classmethod
    def by_category(cls, category: DatasetCategory) -> list['Dataset']:
        """
        Get all datasets in a specific category.
        
        Args:
            category: The category to filter by
            
        Returns:
            List of datasets in that category
        """
        mappings = {
            DatasetCategory.CONSUMPTION: [
                cls.TOTAL_LOAD,
                cls.LOAD,
                cls.TOTAL_LOAD_NRT
            ],
            DatasetCategory.GENERATION: [
                cls.INSTALLED_POWER,
                cls.WIND_PRODUCTION,
                cls.PV_PRODUCTION,
                cls.PV_PRODUCTION_NRT,
                cls.CO2_INTENSITY,
                cls.CO2_INTENSITY_NRT
            ],
            DatasetCategory.TRANSMISSION: [
                cls.Q_AHEAD_NTC,
                cls.M_AHEAD_NTC,
                cls.WEEK_AHEAD_NTC,
                cls.DAY_AHEAD_NTC,
                cls.INTRADAY_NTC,
                cls.PHYSICAL_FLOWS
            ],
            DatasetCategory.BALANCING: [
                cls.IMBALANCE_PRICES_QH,
                cls.IMBALANCE_PRICES_MIN,
                cls.SYSTEM_IMBALANCE,
                cls.ACTIVATED_BALANCING_PRICES,
                cls.ACTIVATED_BALANCING_VOLUMES,
                cls.ACTIVATED_VOLUMES,
                cls.AVAILABLE_BALANCING_PRICES,
                cls.AVAILABLE_BALANCING_VOLUMES
            ],
            DatasetCategory.CONGESTION: [
                cls.REDISPATCH_INTERNAL,
                cls.REDISPATCH_CROSSBORDER,
                cls.CONGESTION_COSTS,
                cls.CONGESTION_RISKS,
                cls.CRI
            ],
            DatasetCategory.CAPACITY: [
                cls.TRANSMISSION_CAPACITY,
                cls.INSTALLED_CAPACITY
            ],
            DatasetCategory.BIDDING: [
                cls.INTRADAY_AVAILABLE_CAPACITY,
                cls.LONG_TERM_AVAILABLE_CAPACITY
            ]
        }
        return mappings.get(category, [])