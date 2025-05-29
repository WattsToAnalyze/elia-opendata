![PyPI](https://img.shields.io/pypi/v/elia-opendata?style=flat&color=blue&logo=pypi&logoColor=white)
![Build Status](https://github.com/WattsToAnalyze/elia-opendata/actions/workflows/python-publish.yml/badge.svg)
![Latest dev release](https://img.shields.io/github/v/release/WattsToAnalyze/elia-opendata?include_prereleases&sort=semver&label=dev%20release&color=orange)
<!-- ![License](https://img.shields.io/github/license/WattsToAnalyze/elia-opendata) -->
# Elia OpenData Python Client

A Python client for accessing the Elia Open Data Portal API. This client provides a simple interface to access Elia's energy data with support for easy data conversion to popular data science formats.

## Installation
For stable releases, you can install the package from PyPI:

```bash
pip install elia-opendata
```

### Nightly/Pre-release Version

You can install the latest pre-release (nightly) build directly from GitHub Releases:

1. Go to the [Releases page](https://github.com/WattsToAnalyze/elia-opendata/releases) and find the most recent pre-release.
2. Copy the link to the `.whl` file attached to that release.
3. Install with:

```bash
pip install https://github.com/WattsToAnalyze/elia-opendata/releases/download/<TAG>/<WHEEL_FILENAME>
```

Or, if you have set up a "latest-nightly" tag as discussed, you can use:

```bash
pip install https://github.com/WattsToAnalyze/elia-opendata/releases/download/latest-nightly/elia_opendata-latest.whl
```

### Development Version (from source)

You can also install the development version directly from the main branch:

```bash
pip install git+https://github.com/WattsToAnalyze/elia-opendata.git@main
```


## Usage

### Basic Usage

```python
from elia_opendata import EliaClient, Dataset, DatasetCategory

# Initialize client
client = EliaClient()

# List all available datasets
catalog = client.get_catalog()
for entry in catalog:
    print(f"Dataset: {entry.title} (ID: {entry.id})")

# Get dataset metadata using enum
solar_metadata = client.get_dataset(Dataset.PV_PRODUCTION)
# print(f"Solar data fields: {solar_metadata.fields}")

# Get records from a dataset
solar_data = client.get_records(Dataset.PV_PRODUCTION, limit=100)
print(solar_data)
print("First 5 solar records:")
for record in getattr(solar_data, 'records', [])[:5]:
    print(record)

# Convert to different formats
df = solar_data.to_pandas()  # Convert to pandas DataFrame
np_array = solar_data.to_numpy()  # Convert to numpy array
pl_df = solar_data.to_polars()  # Convert to polars DataFrame
arrow_table = solar_data.to_arrow()  # Convert to Arrow table
```

### Exploring Available Datasets

```python
from elia_opendata import DatasetCategory

# Get all generation-related datasets
generation_datasets = Dataset.by_category(DatasetCategory.GENERATION)
for dataset in generation_datasets:
    print(f"Generation dataset: {dataset.value}")

# Available categories
print("Available categories:")
for category in DatasetCategory:
    print(f"- {category.value}")
```

### Handling Large Datasets

```python
# Iterate through large datasets in batches
for batch in client.iter_records(Dataset.ACTUAL_TOTAL_LOAD, batch_size=1000):
    df = batch.to_pandas()
    # Process your batch
```

### Dataset Categories

The client provides enums for easy access to different types of data:

- Consumption Data: Total load, day-ahead and week-ahead forecasts
- Generation Data: Solar and wind generation, offshore/onshore forecasts
- Transmission Data: Cross-border flows, scheduled exchanges
- Balancing Data: Imbalance prices, system imbalance
- Congestion Management: Redispatch measures, costs
- Capacity Data: Transmission and installed capacity
- Bidding Zone Data: Prices and cross-border capacity

## API Endpoints

The client maps directly to Elia's API endpoints:

- `get_catalog()`: List all available datasets (GET /api/v2/catalog/datasets)
- `get_dataset()`: Get dataset metadata (GET /api/v2/catalog/datasets/{dataset_id})
- `get_records()`: Get dataset records (GET /api/v2/catalog/datasets/{dataset_id}/records)
- `search_catalog()`: Search datasets (GET /api/v2/catalog/datasets/search)

## Dependencies

Core dependencies:
- requests

Optional dependencies for data conversion:
- pandas
- numpy
- polars
- pyarrow

### Advanced Data Processing

The library includes an `EliaDataProcessor` that makes it easy to work with large datasets and perform common data manipulation tasks:

```python
from elia_opendata import EliaClient, EliaDataProcessor, Dataset
from datetime import datetime, timedelta

# Initialize the data processor
processor = EliaDataProcessor()

# Fetch a complete dataset (automatically handles pagination)
solar_data = processor.fetch_complete_dataset(
    dataset=Dataset.PV_PRODUCTION,
    batch_size=100  # Number of records per API request (max 100)
)
print(f"Retrieved {solar_data.total_count} solar production records")

# Fetch data for a specific date range
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=7)

wind_data = processor.fetch_date_range(
    dataset=Dataset.WIND_PRODUCTION,
    start_date=start_date,
    end_date=end_date
)

# Aggregate data by a field
# For example, aggregate solar production by region
region_sum = processor.aggregate_by_field(
    solar_data,
    "region",
    {"measured": "sum", "datetime": "max"}
)
print(region_sum.to_pandas())

# Converting to different DataFrame formats
pandas_df = processor.to_dataframe(solar_data, output_format="pandas")
polars_df = processor.to_dataframe(solar_data, output_format="polars")
numpy_array = processor.to_dataframe(solar_data, output_format="numpy")
```

The `EliaDataProcessor` makes working with Elia OpenData more efficient by handling:
- Automatic pagination for large datasets
- Date filtering with optimized API calls
- Simplified data aggregation
- Format conversion between pandas, polars, and numpy

## License

MIT License
