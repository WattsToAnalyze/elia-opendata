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
print(f"Current total load: {current_load.iloc[0]['value']:.2f} MW")

# Get current renewable production
current_pv = processor.fetch_current_value(PV_PRODUCTION)
current_wind = processor.fetch_current_value(WIND_PRODUCTION)

print(f"Current PV production: {current_pv.iloc[0]['value']:.2f} MW")
print(f"Current wind production: {current_wind.iloc[0]['value']:.2f} MW")
```

### Historical Data Analysis

Analyze patterns in electricity consumption:

```python
from datetime import datetime
import matplotlib.pyplot as plt

# Get data for a specific month
start = datetime(2023, 6, 1)
end = datetime(2023, 6, 30)

june_load = processor.fetch_data_between(TOTAL_LOAD, start, end)

# Convert datetime column and set as index
june_load['datetime'] = pd.to_datetime(june_load['datetime'])
june_load.set_index('datetime', inplace=True)

# Basic statistics
print(f"Average load in June: {june_load['value'].mean():.2f} MW")
print(f"Peak load in June: {june_load['value'].max():.2f} MW")
print(f"Minimum load in June: {june_load['value'].min():.2f} MW")

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(june_load.index, june_load['value'])
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

# Separate measured vs forecasted data
measured = solar_data[solar_data['type'] == 'measured']
forecast = solar_data[solar_data['type'] == 'forecast']

print(f"Measured solar production records: {len(measured)}")
print(f"Forecasted solar production records: {len(forecast)}")

# Daily peak analysis
solar_data['datetime'] = pd.to_datetime(solar_data['datetime'])
solar_data['date'] = solar_data['datetime'].dt.date
solar_data['hour'] = solar_data['datetime'].dt.hour

daily_peaks = measured.groupby('date')['value'].max()
print("Daily solar production peaks:")
for date, peak in daily_peaks.items():
    print(f"{date}: {peak:.2f} MW")
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

# Filter for measured values only
wind_measured = wind_prod[wind_prod['type'] == 'measured'].copy()
solar_measured = solar_prod[solar_prod['type'] == 'measured'].copy()
load_measured = total_load[total_load['type'] == 'measured'].copy()

# Calculate renewable percentage
if not wind_measured.empty and not solar_measured.empty:
    # Align timestamps and calculate renewable share
    wind_measured['datetime'] = pd.to_datetime(wind_measured['datetime'])
    solar_measured['datetime'] = pd.to_datetime(solar_measured['datetime'])
    load_measured['datetime'] = pd.to_datetime(load_measured['datetime'])
    
    # Sum renewable production
    renewable_total = wind_measured['value'].sum() + solar_measured['value'].sum()
    load_total = load_measured['value'].sum()
    
    renewable_percentage = (renewable_total / load_total) * 100
    print(f"Renewable share of total load: {renewable_percentage:.2f}%")
```

## Market Analysis

### Imbalance Price Analysis

Analyze electricity market imbalance prices:

```python
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_QH

# Get imbalance price data
imbalance_data = processor.fetch_data_between(
    IMBALANCE_PRICES_QH,
    datetime(2023, 9, 1),
    datetime(2023, 9, 30)
)

# Basic price statistics
prices = imbalance_data['systemimp']  # System imbalance price
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

## Cross-Border Analysis

### Physical Flow Analysis

Analyze electricity flows between countries:

```python
from elia_opendata.dataset_catalog import PHYSICAL_FLOWS

# Get physical flow data
flows = processor.fetch_data_between(
    PHYSICAL_FLOWS,
    datetime(2023, 10, 1),
    datetime(2023, 10, 7)
)

# Analyze flows by border
if 'border' in flows.columns:
    flow_by_border = flows.groupby('border')['value'].agg(['mean', 'std', 'min', 'max'])
    print("Physical flows by border (MW):")
    print(flow_by_border)
    
    # Net import/export analysis
    for border in flows['border'].unique():
        border_data = flows[flows['border'] == border]
        net_flow = border_data['value'].sum()
        direction = "import" if net_flow > 0 else "export"
        print(f"{border}: Net {direction} of {abs(net_flow):.2f} MW")
```

## Advanced Filtering

### Complex Date and Value Filters

Use advanced filtering for specific analysis needs:

```python
from elia_opendata import EliaClient

client = EliaClient()

# Filter for high-value periods during peak hours
peak_load_data = client.get_records(
    TOTAL_LOAD,
    where="datetime>='2023-07-01' AND datetime<'2023-08-01' AND value>10000",
    limit=1000,
    order_by="value desc"
)

print(f"Found {len(peak_load_data)} high-load periods in July 2023")

# Filter for weekend data only (assuming day of week info is available)
weekend_data = client.get_records(
    PV_PRODUCTION,
    where="datetime>='2023-06-01' AND datetime<'2023-07-01'",
    limit=5000
)

# Process to extract weekends (this would depend on your date processing)
import pandas as pd
df = pd.DataFrame(weekend_data)
df['datetime'] = pd.to_datetime(df['datetime'])
df['dayofweek'] = df['datetime'].dt.dayofweek
weekends = df[df['dayofweek'] >= 5]  # Saturday and Sunday
print(f"Weekend solar production records: {len(weekends)}")
```

## Data Export and Visualization

### Exporting Data

Save data for external analysis:

```python
# Fetch data and save as CSV
monthly_data = processor.fetch_data_between(
    TOTAL_LOAD,
    datetime(2023, 5, 1),
    datetime(2023, 5, 31)
)

# Save to CSV
monthly_data.to_csv('may_2023_load_data.csv', index=False)
print("Data exported to may_2023_load_data.csv")

# Save to Excel with multiple sheets
with pd.ExcelWriter('energy_analysis.xlsx') as writer:
    monthly_data.to_excel(writer, sheet_name='Load_Data', index=False)
    
    # Add renewable data to another sheet
    solar_data = processor.fetch_data_between(
        PV_PRODUCTION,
        datetime(2023, 5, 1),
        datetime(2023, 5, 31)
    )
    solar_data.to_excel(writer, sheet_name='Solar_Data', index=False)

print("Data exported to energy_analysis.xlsx")
```

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
    
    # Filter for measured values
    load_measured = load_data[load_data['type'] == 'measured']
    wind_measured = wind_data[wind_data['type'] == 'measured']
    solar_measured = solar_data[solar_data['type'] == 'measured']
    
    print(f"=== Energy Dashboard: {date_start} to {date_end} ===")
    print(f"Total Load:")
    print(f"  Average: {load_measured['value'].mean():.2f} MW")
    print(f"  Peak: {load_measured['value'].max():.2f} MW")
    
    print(f"Wind Production:")
    print(f"  Average: {wind_measured['value'].mean():.2f} MW")
    print(f"  Peak: {wind_measured['value'].max():.2f} MW")
    
    print(f"Solar Production:")
    print(f"  Average: {solar_measured['value'].mean():.2f} MW")
    print(f"  Peak: {solar_measured['value'].max():.2f} MW")
    
    # Calculate renewable share
    total_renewable = wind_measured['value'].sum() + solar_measured['value'].sum()
    total_load = load_measured['value'].sum()
    renewable_share = (total_renewable / total_load) * 100
    
    print(f"Renewable Share: {renewable_share:.2f}%")
    print("=" * 50)

# Use the dashboard
energy_dashboard(datetime(2023, 7, 1), datetime(2023, 7, 7))
```

## Error Handling Examples

### Robust Data Fetching

Handle various error scenarios gracefully:

```python
from elia_opendata.error import RateLimitError, APIError, EliaConnectionError
import time

def robust_data_fetch(dataset_id, max_retries=3, delay=60):
    """Fetch data with retry logic for rate limits."""
    
    client = EliaClient()
    
    for attempt in range(max_retries):
        try:
            data = client.get_records(dataset_id, limit=100)
            return data
            
        except RateLimitError:
            if attempt < max_retries - 1:
                print(f"Rate limit hit, waiting {delay} seconds...")
                time.sleep(delay)
                continue
            else:
                print("Max retries reached for rate limiting")
                raise
                
        except APIError as e:
            print(f"API Error: {e}")
            if "404" in str(e):
                print("Dataset not found")
                return None
            raise
            
        except EliaConnectionError:
            print(f"Connection failed, attempt {attempt + 1}")
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            raise

# Use robust fetching
try:
    data = robust_data_fetch(TOTAL_LOAD)
    if data:
        print(f"Successfully retrieved {len(data)} records")
except Exception as e:
    print(f"Failed to retrieve data: {e}")
```

These examples cover the most common use cases for the Elia OpenData package. For more specific scenarios, check the [API Reference](reference/client.md) for detailed parameter documentation.
