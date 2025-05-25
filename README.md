# Elia OpenData Python Client

A Python client for accessing the Elia Open Data Portal API. This client provides a simple interface to access Elia's energy data with support for easy data conversion to popular data science formats.

## Installation

```bash
pip install elia_opendata
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

## License

MIT License