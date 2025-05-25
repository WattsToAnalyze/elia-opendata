# Elia OpenData API Examples

This directory contains example scripts demonstrating real-world usage of the Elia OpenData API client.

## Demo API Usage

The `demo_api_usage.py` script showcases how to:
- Connect to the Elia OpenData API
- List available datasets in the catalog
- Fetch specific dataset details
- Retrieve recent solar generation data
- Get wind generation data
- Search the catalog for specific datasets
- Fetch actual total load data

### Running the Demo

To run the demo script:

```bash
python examples/demo_api_usage.py
```

The script will:
1. Connect to the Elia OpenData API
2. Fetch actual data from various datasets
3. Display the results with helpful logging information

If you encounter any errors, make sure:
- You have an active internet connection
- The API service is available (https://opendata.elia.be/api/explore/v2.1/)