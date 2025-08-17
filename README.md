![PyPI](https://img.shields.io/pypi/v/elia-opendata?style=flat&color=blue&logo=pypi&logoColor=white)
![Build Status](https://github.com/WattsToAnalyze/elia-opendata/actions/workflows/python-publish.yml/badge.svg)
![Latest dev release](https://img.shields.io/github/v/release/WattsToAnalyze/elia-opendata?include_prereleases&sort=semver&label=dev%20release&color=orange)
<!-- ![License](https://img.shields.io/github/license/WattsToAnalyze/elia-opendata) -->

# Elia OpenData Python Package

This package provides a Python interface to the Elia OpenData API, allowing users to easily access and process data related to electricity consumption, production, and other metrics from Elia, the Belgian transmission system operator.

The package includes functionality to fetch data for specific datasets, filter by date ranges, and handle the data in a flexible manner, allowing for both raw JSON based data and more user-friendly dataframees using `pandas` or `polars`.

The package is in active development and we would love your feedback! If you encounter any issues or have suggestions, please open an issue on our [GitHub repository](https://github.com/WattsToAnalyze/elia-opendata/issues).


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

## Documentation

Complete documentation is available at: <https://wattstoanalyze.github.io/elia-opendata/>

The documentation includes:

- **Getting Started Guide**: Installation and basic usage
- **Examples**: Practical examples for common use cases  
- **API Reference**: Complete documentation of all classes and methods
- **Dataset Catalog**: List of all available datasets with descriptions

### Local Documentation

To build the documentation locally:

```bash
# Install documentation dependencies
pip install -e ".[docs]"

# Serve the documentation
mkdocs serve
```

The documentation will be available at `http://127.0.0.1:8000`.
