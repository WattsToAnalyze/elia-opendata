#!/usr/bin/env python
# filepath: /Users/gargyagokhale/github/elia-open-data/examples/basic_usage_dataProcessor.py
"""
Basic usage examples for the EliaDataProcessor class.

The EliaDataProcessor provides advanced data manipulation capabilities
for working with Elia OpenData, including:
- Automated pagination for complete dataset retrieval
- Date range filtering
- Data aggregation
- Merging datasets
- Format conversion
"""
from datetime import datetime, timedelta
from elia_opendata import EliaClient, EliaDataProcessor, Dataset
from elia_opendata.models import Records

# Initialize the data processor (it will create a client automatically)
processor = EliaDataProcessor()

# Alternatively, you can pass an existing client
client = EliaClient()
processor_with_client = EliaDataProcessor(client)

# Example 1: Fetch a complete dataset (with pagination handled automatically)
print("\n=== Example 1: Fetch complete dataset ===")
solar_data = processor.fetch_complete_dataset(
    dataset=Dataset.PV_PRODUCTION,
    batch_size=100,  # Number of records per API request (max 100)
    max_batches=5    # Limit to 5 batches for demonstration
)
print(f"Retrieved {solar_data.total_count} solar production records")
print("First record:", solar_data.records[0]["record"]["fields"])

# Example 2: Fetch data for a specific date range
print("\n=== Example 2: Fetch data for a date range ===")
# Get data for the last 7 days
end_date = datetime.utcnow()
start_date = end_date - timedelta(days=7)
print(f"Fetching data from {start_date.isoformat()} to {end_date.isoformat()}")

wind_data = processor.fetch_date_range(
    dataset=Dataset.WIND_PRODUCTION,
    start_date=start_date,
    end_date=end_date,
    max_batches=2  # Limit to 2 batches for demonstration
)
print(f"Retrieved {wind_data.total_count} wind production records")

# Example 3: Merge multiple datasets
print("\n=== Example 3: Merge datasets ===")
# Get both solar and wind data for the past day
yesterday = datetime.utcnow() - timedelta(days=1)
solar_day = processor.fetch_date_range(
    dataset=Dataset.PV_PRODUCTION,
    start_date=yesterday,
    end_date=datetime.utcnow(),
    max_batches=2  # Limit to 2 batches for demonstration
)

wind_day = processor.fetch_date_range(
    dataset=Dataset.WIND_PRODUCTION,
    start_date=yesterday,
    end_date=datetime.utcnow(),
    max_batches=2  # Limit to 2 batches for demonstration
)

# Merge datasets - useful for analysis requiring multiple data sources
# Note: In a real application, you might want to handle schema differences
print(f"Solar records: {solar_day.total_count}, Wind records: {wind_day.total_count}")
combined = processor.merge_records([solar_day, wind_day])
print(f"Combined records: {combined.total_count}")

# Example 4: Aggregate data by a field
print("\n=== Example 4: Aggregate data ===")
# First convert to pandas DataFrame to check available fields
df = solar_day.to_pandas()
print("Available fields:", df.columns.tolist())

# For solar data, we have 'datetime' and 'measured' fields
# Aggregate data by region, summing the measured values
if 'datetime' in df.columns and 'measured' in df.columns and 'region' in df.columns:
    # Extract date from datetime for aggregation
    df['date'] = df['datetime'].str.split('T').str[0]
    
    # Convert back to Records for the aggregation
    solar_day_with_date = Records({
        "total_count": len(df),
        "records": [{"record": {"fields": row.to_dict()}} for _, row in df.iterrows()],
        "links": []
    })
    
    # Perform aggregation - sum measured values by region
    region_sum = processor.aggregate_by_field(
        solar_day_with_date,
        "region",
        {"measured": "sum", "datetime": "max"}
    )
    
    print("Regional aggregation results:")
    print(region_sum.to_pandas())
else:
    print("Required fields for aggregation not available in the sample data")

# Example 5: Converting to different DataFrame formats
print("\n=== Example 5: Convert to different formats ===")
# The processor can convert Records to pandas, polars, or numpy
pandas_df = processor.to_dataframe(solar_day, output_format="pandas")
print("Pandas DataFrame shape:", pandas_df.shape)

# You can also use Records methods directly:
numpy_array = solar_day.to_numpy()
print("NumPy array shape:", numpy_array.shape)

try:
    polars_df = solar_day.to_polars()
    print("Polars DataFrame shape:", polars_df.shape)
except ImportError:
    print("Polars not installed, skipping polars conversion")

print("\nDataProcessor makes working with Elia OpenData more efficient by handling:")
print("- Automatic pagination for large datasets")
print("- Date filtering with optimized API calls")
print("- Simplified data aggregation")
print("- Format conversion between pandas, polars, and numpy")
