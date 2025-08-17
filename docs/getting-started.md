# Getting Started

This guide will help you get started with the Elia OpenData Python package.

## Installation

### Stable Release

Install the latest stable version from PyPI:

```bash
pip install elia-opendata
```

### Development Version

For the latest features and bug fixes, you can install the development version:

```bash
pip install git+https://github.com/WattsToAnalyze/elia-opendata.git@main
```

### Nightly/Pre-release Version

You can install the latest pre-release build directly from GitHub Releases:

1. Go to the [Releases page](https://github.com/WattsToAnalyze/elia-opendata/releases)
2. Find the most recent pre-release
3. Copy the link to the `.whl` file
4. Install with:

```bash
pip install https://github.com/WattsToAnalyze/elia-opendata/releases/download/<TAG>/<WHEEL_FILENAME>
```

## Basic Usage

### Creating a Client

The `EliaClient` is the main entry point for API access:

```python
from elia_opendata import EliaClient

# Create a client with default settings
client = EliaClient()

# Or with custom timeout
client = EliaClient(timeout=60)
```

### Fetching Data

Use the `get_records` method to fetch data from any dataset:

```python
from elia_opendata.dataset_catalog import TOTAL_LOAD

# Get the 10 most recent records
data = client.get_records(TOTAL_LOAD, limit=10)
print(f"Retrieved {len(data)} records")

# Each record is a dictionary
for record in data:
    print(f"Time: {record['datetime']}, Value: {record['value']}")
```

### Using the Data Processor

For more advanced operations, use the `EliaDataProcessor`:

```python
from elia_opendata import EliaDataProcessor

# Create processor (default returns JSON)
processor = EliaDataProcessor()

# Or with pandas DataFrame output
processor = EliaDataProcessor(return_type="pandas")

# Or with polars DataFrame output  
processor = EliaDataProcessor(return_type="polars")
```

## Understanding Datasets

### Dataset IDs

All datasets are identified by unique IDs like "ods001", "ods032", etc. Instead of memorizing these, use the constants from the dataset catalog:

```python
from elia_opendata.dataset_catalog import (
    TOTAL_LOAD,      # "ods001" - Total load data
    PV_PRODUCTION,   # "ods032" - Solar production  
    WIND_PRODUCTION, # "ods031" - Wind production
    IMBALANCE_PRICES_QH  # "ods134" - Imbalance prices
)
```

### Data Categories

The available datasets are organized into categories:

- **Load/Consumption**: Grid load and consumption data
- **Generation**: Renewable and conventional generation
- **Balancing**: Market balancing and imbalance pricing
- **Transmission**: Cross-border flows and capacity
- **Congestion Management**: Grid congestion data
- **Market Data**: Capacity and bidding information

## Common Patterns

### Get Current/Latest Values

```python
from elia_opendata import EliaDataProcessor
from elia_opendata.dataset_catalog import TOTAL_LOAD

processor = EliaDataProcessor(return_type="pandas")

# Get the most recent value
current = processor.fetch_current_value(TOTAL_LOAD)
print(current.iloc[0])
```

### Date Range Queries

```python
from datetime import datetime

# Define date range
start = datetime(2023, 1, 1)
end = datetime(2023, 1, 31)

# Fetch data for the entire month
monthly_data = processor.fetch_data_between(TOTAL_LOAD, start, end)
print(f"Retrieved {len(monthly_data)} records for January 2023")
```

### Filtering Data

```python
# Use OData-style filters
filtered_data = client.get_records(
    TOTAL_LOAD,
    where="datetime>='2023-01-01' AND value>5000",
    limit=100
)
```

## Error Handling

The package provides specific exceptions for different error scenarios:

```python
from elia_opendata import EliaClient
from elia_opendata.error import RateLimitError, AuthError, APIError

client = EliaClient()

try:
    data = client.get_records("invalid_dataset", limit=10)
except RateLimitError:
    print("Rate limit exceeded, please wait")
except AuthError:
    print("Authentication failed")
except APIError as e:
    print(f"API error: {e}")
```

## Next Steps

- Explore the [Examples](examples.md) section for common use cases
- Check the [API Reference](reference/client.md) for detailed documentation
- Browse available datasets in the [Dataset Catalog](reference/dataset_catalog.md)
