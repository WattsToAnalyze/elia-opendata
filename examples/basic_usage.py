from elia_opendata import EliaClient, Dataset, DatasetCategory
from pprint import pprint


# Initialize client
client = EliaClient()
dataset_id = Dataset.IMBALANCE_PRICES_QH

# List all available datasets
catalog = client.get_catalog()
for entry in catalog:
    print(f"Dataset: {entry.title} (ID: {entry.id})")

# Get dataset metadata using enum
metadata = client.get_dataset(dataset=dataset_id)
pprint(f"Metadata for dataset {dataset_id}:")
pprint(metadata.raw, indent=2)
print(f"--"* 10)

# Get records from a dataset
records = client.get_records(dataset=dataset_id, limit=100)
print("First 5 records:")
for record in getattr(records, 'records', [])[:5]:
    pprint(record, indent=2)

print(f"--"* 10)
print(f"--"* 10)
pprint(records.records[0], indent=2)
# Convert to different formats
df = records.to_pandas()  # Convert to pandas DataFrame
np_array = records.to_numpy()  # Convert to numpy array
pl_df = records.to_polars()  # Convert to polars DataFrame
arrow_table = records.to_arrow()  # Convert to Arrow table
