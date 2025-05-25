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
