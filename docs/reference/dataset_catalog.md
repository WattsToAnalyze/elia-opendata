# Dataset Catalog Reference

The dataset catalog provides constants for all available Elia OpenData datasets, organized by category.
For balancing datasets affected by the 2024-05-22 MARI/ICAROS transition,
use either the explicit `*_PRE_MARI`/`*_POST_MARI` constants or the
`dataset_name` parameter in `EliaDataProcessor.fetch_data_between` for automatic
pre/post selection and merging.

::: elia_opendata.dataset_catalog
