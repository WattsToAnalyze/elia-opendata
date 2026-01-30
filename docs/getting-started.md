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

## Next Steps

- Explore the [Examples](examples.md) section for common use cases
- Check the [API Reference](reference/client.md) for detailed documentation
- Browse available datasets in the [Dataset Catalog](reference/dataset_catalog.md)

## MARI Transition Datasets (Pre/Post 2024-05-22)

Balancing datasets were split when MARI/ICAROS went live on 2024-05-22.
For these datasets, you can use a friendly `dataset_name` to automatically
select PRE/POST datasets (or merge across the transition).

```python
from datetime import datetime
from elia_opendata import EliaDataProcessor

processor = EliaDataProcessor()
data = processor.fetch_data_between(
	start_date=datetime(2024, 4, 1),
	end_date=datetime(2024, 6, 1),
	dataset_name="IMBALANCE_PRICES_QH",
)
```

If you prefer explicit datasets, use the `*_PRE_MARI` and `*_POST_MARI`
constants from the dataset catalog.
