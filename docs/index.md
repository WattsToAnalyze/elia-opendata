# Elia OpenData Python Package

![PyPI](https://img.shields.io/pypi/v/elia-opendata?style=flat&color=blue&logo=pypi&logoColor=white)
![Build Status](https://github.com/WattsToAnalyze/elia-opendata/actions/workflows/python-publish.yml/badge.svg)
![Latest dev release](https://img.shields.io/github/v/release/WattsToAnalyze/elia-opendata?include_prereleases&sort=semver&label=dev%20release&color=orange)

A comprehensive Python interface to the Elia OpenData API, providing easy access to Belgian electricity grid data including consumption, production, balancing, and market information.

## Features

- **Simple API Access**: Easy-to-use client for the Elia OpenData API
- **Multiple Output Formats**: Support for JSON, Pandas DataFrame, and Polars DataFrame
- **Comprehensive Dataset Catalog**: Pre-defined constants for all available datasets
- **Advanced Data Processing**: Built-in pagination, filtering, and date range queries
- **Robust Error Handling**: Specific exceptions for different error scenarios
- **Type Hints**: Full type annotation support for better IDE experience

## Quick Start

### Installation

```bash
pip install elia-opendata
```

### Basic Usage

```python
from elia_opendata import EliaClient
from elia_opendata.dataset_catalog import TOTAL_LOAD

# Create a client
client = EliaClient()

# Get recent data
recent_data = client.get_records(TOTAL_LOAD, limit=10)
print(f"Retrieved {len(recent_data)} records")
```

### Advanced Usage with Data Processor

```python
from elia_opendata import EliaDataProcessor
from elia_opendata.dataset_catalog import TOTAL_LOAD, PV_PRODUCTION
from datetime import datetime

# Create processor with pandas output
processor = EliaDataProcessor(return_type="pandas")

# Get current values
current_load = processor.fetch_current_value(TOTAL_LOAD)
current_pv = processor.fetch_current_value(PV_PRODUCTION)

# Get data for a specific date range
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 31)
january_data = processor.fetch_data_between(TOTAL_LOAD, start_date, end_date)
```

## Available Data

The Elia OpenData API provides access to various categories of electricity grid data:

- **Load/Consumption**: Total grid load, consumption patterns
- **Generation**: Wind, solar, and other renewable production data
- **Balancing**: Imbalance prices, activated reserves, system balance
- **Transmission**: Cross-border flows, net transfer capacity
- **Market Data**: Available capacity, congestion management

## Getting Help

- **Documentation**: Complete API reference and examples
- **GitHub Issues**: [Report bugs or request features](https://github.com/WattsToAnalyze/elia-opendata/issues)
- **Examples**: See the [Examples section](examples.md) for common use cases

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/WattsToAnalyze/elia-opendata/blob/main/LICENSE) file for details.
