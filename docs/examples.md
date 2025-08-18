# Examples

This section provides practical examples of using the Elia OpenData package for common tasks.

## Basic Data Retrieval

### Getting Current Values

Fetch the most recent data from any dataset:

```python
from elia_opendata import EliaDataProcessor
from elia_opendata.dataset_catalog import TOTAL_LOAD, PV_PRODUCTION, WIND_PRODUCTION

# Create processor with pandas output for analysis
processor = EliaDataProcessor(return_type="pandas")

# Get current total load
current_load = processor.fetch_current_value(TOTAL_LOAD)
print(f"Current total load: {current_load.iloc[0]['totalload']:.2f} MW")

# Get current renewable production
current_pv = processor.fetch_current_value(PV_PRODUCTION)
current_wind = processor.fetch_current_value(WIND_PRODUCTION)
print(f"Current PV production: {current_pv.iloc[0]['measured']:.2f} MW")
print(f"Current wind production: {current_wind.iloc[0]['measured']:.2f} MW")
```

### Historical Data Analysis

Analyze patterns in electricity consumption:

```python
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

# Get data for a specific month
start = datetime(2023, 6, 1)
end = datetime(2023, 6, 30)

june_load = processor.fetch_data_between(TOTAL_LOAD, start, end, export_data=True)

# Convert datetime column and set as index
june_load['datetime'] = pd.to_datetime(june_load['datetime'])
june_load.set_index('datetime', inplace=True)
june_load.sort_index(inplace=True)

# Basic statistics
print(f"Average load in June: {june_load['totalload'].mean():.2f} MW")
print(f"Peak load in June: {june_load['totalload'].max():.2f} MW")
print(f"Minimum load in June: {june_load['totalload'].min():.2f} MW")

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(june_load.index, june_load['totalload'])
plt.title('Total Load - June 2023')
plt.ylabel('Load (MW)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## Renewable Energy Analysis

### Solar Production Patterns

Analyze solar production with forecasts:

```python
from elia_opendata.dataset_catalog import PV_PRODUCTION

# Get solar data for analysis
processor = EliaDataProcessor(return_type="pandas")
solar_data = processor.fetch_data_between(
    PV_PRODUCTION,
    datetime(2023, 7, 1),
    datetime(2023, 7, 7)  # One week of data
)

# Analyze the solar production data structure
print("Solar data columns:", solar_data.columns.tolist())
print(f"Total records: {len(solar_data)}")
print(f"Unique regions: {solar_data['region'].unique()}")

# Filter for measured solar production values
measured_solar = solar_data[solar_data['measured'].notna()].copy()
print(f"Measured solar production records: {len(measured_solar)}")

# Also check forecast data - using the most recent forecast
forecast_solar = solar_data[solar_data['mostrecentforecast'].notna()].copy()
print(f"Most recent forecast records: {len(forecast_solar)}")

# Convert datetime and add time components
measured_solar['datetime'] = pd.to_datetime(measured_solar['datetime'])
measured_solar['date'] = measured_solar['datetime'].dt.date
measured_solar['hour'] = measured_solar['datetime'].dt.hour

# Aggregate by date and hour to get total production across all regions
daily_totals = measured_solar.groupby('date')['measured'].sum()
print("\nDaily solar production totals (MW):")
for date, total in daily_totals.items():
    print(f"{date}: {total:.2f} MW")

# Hourly pattern analysis
hourly_avg = measured_solar.groupby('hour')['measured'].mean()
print(f"\nPeak hour for solar production: {hourly_avg.idxmax()}:00 with average {hourly_avg.max():.2f} MW")
print(f"Average daily production: {measured_solar['measured'].mean():.2f} MW per region per 15-min interval")
```

### Renewable vs Total Load Comparison

Compare renewable production with total electricity demand:

```python
from elia_opendata.dataset_catalog import TOTAL_LOAD, WIND_PRODUCTION, PV_PRODUCTION

# Fetch all data for the same time period
start = datetime(2023, 8, 1)
end = datetime(2023, 8, 7)

total_load = processor.fetch_data_between(TOTAL_LOAD, start, end)
wind_prod = processor.fetch_data_between(WIND_PRODUCTION, start, end)
solar_prod = processor.fetch_data_between(PV_PRODUCTION, start, end)

print("Data structure check:")
print(f"Total load columns: {total_load.columns.tolist()}")
print(f"Wind production columns: {wind_prod.columns.tolist()}")
print(f"Solar production columns: {solar_prod.columns.tolist()}")

# Calculate renewable percentage using the correct column names
# For total load, use the appropriate column (likely 'totalload')
# For renewables, use 'measured' column and sum across regions
if not wind_prod.empty and not solar_prod.empty and not total_load.empty:
    # Sum renewable production across regions and time
    total_wind = wind_prod['measured'].sum()
    total_solar = solar_prod['measured'].sum()
    total_renewable = total_wind + total_solar
    
    # Sum total load (assuming 'totalload' column)
    if 'totalload' in total_load.columns:
        total_load_sum = total_load['totalload'].sum()
    else:
        # Fallback to other possible column names
        load_cols = [col for col in total_load.columns if 'load' in col.lower() or 'value' in col.lower()]
        if load_cols:
            total_load_sum = total_load[load_cols[0]].sum()
            print(f"Using column '{load_cols[0]}' for total load")
        else:
            print("Could not identify load column")
            total_load_sum = 0
    
    if total_load_sum > 0:
        renewable_percentage = (total_renewable / total_load_sum) * 100
        print(f"\nRenewable Energy Analysis:")
        print(f"Total wind production: {total_wind:.2f} MWh")
        print(f"Total solar production: {total_solar:.2f} MWh") 
        print(f"Total renewable: {total_renewable:.2f} MWh")
        print(f"Total load: {total_load_sum:.2f} MWh")
        print(f"Renewable share of total load: {renewable_percentage:.2f}%")
    else:
        print("Could not calculate renewable percentage - no load data found")
```

## Market Analysis

### Imbalance Price Analysis

Analyze electricity market imbalance prices:

```python
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_QH

# Get imbalance price data
imbalance_data = processor.fetch_data_between(
    IMBALANCE_PRICES_QH,
    datetime(2025, 1, 1),
    datetime(2025, 2, 1), 
    export_data=True
)

# Basic price statistics
prices = imbalance_data['systemimbalance']  # System imbalance price
print(f"Average imbalance price: {prices.mean():.2f} €/MWh")
print(f"Price volatility (std): {prices.std():.2f} €/MWh")
print(f"Maximum price: {prices.max():.2f} €/MWh")
print(f"Minimum price: {prices.min():.2f} €/MWh")

# Price distribution analysis
import numpy as np
positive_prices = prices[prices > 0]
negative_prices = prices[prices < 0]

print(f"Hours with positive prices: {len(positive_prices)} ({len(positive_prices)/len(prices)*100:.1f}%)")
print(f"Hours with negative prices: {len(negative_prices)} ({len(negative_prices)/len(prices)*100:.1f}%)")
```

## Data Export and Visualization

### Creating Dashboards

Simple dashboard-style analysis:

```python
def energy_dashboard(date_start, date_end):
    """Create a simple energy dashboard for a date range."""
    
    processor = EliaDataProcessor(return_type="pandas")
    
    # Fetch all major datasets
    load_data = processor.fetch_data_between(TOTAL_LOAD, date_start, date_end)
    wind_data = processor.fetch_data_between(WIND_PRODUCTION, date_start, date_end)
    solar_data = processor.fetch_data_between(PV_PRODUCTION, date_start, date_end)
    
    print(f"=== Energy Dashboard: {date_start} to {date_end} ===")
    
    # Total Load Analysis
    if not load_data.empty and 'totalload' in load_data.columns:
        print(f"Total Load:")
        print(f"  Average: {load_data['totalload'].mean():.2f} MW")
        print(f"  Peak: {load_data['totalload'].max():.2f} MW")
        print(f"  Records: {len(load_data)}")
        total_load_sum = load_data['totalload'].sum()
    else:
        print("Total Load: No data available")
        total_load_sum = 0
    
    # Wind Production Analysis  
    if not wind_data.empty and 'measured' in wind_data.columns:
        wind_measured = wind_data[wind_data['measured'].notna()]
        print(f"Wind Production:")
        print(f"  Average: {wind_measured['measured'].mean():.2f} MW per region")
        print(f"  Peak: {wind_measured['measured'].max():.2f} MW")
        print(f"  Records: {len(wind_measured)}")
        total_wind = wind_measured['measured'].sum()
    else:
        print("Wind Production: No data available")
        total_wind = 0
    
    # Solar Production Analysis
    if not solar_data.empty and 'measured' in solar_data.columns:
        solar_measured = solar_data[solar_data['measured'].notna()]
        print(f"Solar Production:")
        print(f"  Average: {solar_measured['measured'].mean():.2f} MW per region")
        print(f"  Peak: {solar_measured['measured'].max():.2f} MW")
        print(f"  Records: {len(solar_measured)}")
        total_solar = solar_measured['measured'].sum()
    else:
        print("Solar Production: No data available")
        total_solar = 0
    
    # Calculate renewable share
    if total_load_sum > 0:
        total_renewable = total_wind + total_solar
        renewable_share = (total_renewable / total_load_sum) * 100
        print(f"Renewable Share: {renewable_share:.2f}%")
    else:
        print("Renewable Share: Cannot calculate (no load data)")
        
    print("=" * 50)

# Use the dashboard
energy_dashboard(datetime(2023, 7, 1), datetime(2023, 7, 7))
```

These examples cover the most common use cases for the Elia OpenData package. For more specific scenarios, check the [API Reference](reference/client.md) for detailed parameter documentation.
