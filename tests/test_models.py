import pytest
from datetime import datetime
from elia_opendata.models import BaseModel, CatalogEntry, DatasetMetadata, Records

def test_base_model_conversions():
    # Test data
    test_data = {
        "key1": "value1",
        "key2": [1, 2, 3],
        "key3": {"nested": "value"}
    }
    model = BaseModel(test_data)
    
    # Test raw data access
    assert model.raw == test_data
    
    # Test dictionary conversion
    assert model.to_dict() == test_data
    
    # Test JSON conversion
    json_str = model.to_json()
    assert isinstance(json_str, str)
    assert '"key1":"value1"' in json_str.replace(" ", "")

def test_base_model_missing_dependencies():
    model = BaseModel({"test": "data"})
    
    # Test missing pandas
    with pytest.raises(ImportError) as exc_info:
        model.to_pandas()
    assert "pandas" in str(exc_info.value)
    
    # Test missing numpy
    with pytest.raises(ImportError) as exc_info:
        model.to_numpy()
    assert "numpy" in str(exc_info.value)
    
    # Test missing polars
    with pytest.raises(ImportError) as exc_info:
        model.to_polars()
    assert "polars" in str(exc_info.value)
    
    # Test missing pyarrow
    with pytest.raises(ImportError) as exc_info:
        model.to_arrow()
    assert "pyarrow" in str(exc_info.value)

def test_catalog_entry():
    # Test with complete data matching actual API structure
    data = {
        "dataset": {
            "dataset_id": "test_id",
            "metas": {
                "default": {
                    "title": "Test Title",
                    "description": "Test Description",
                    "theme": ["Test Theme"],
                    "modified": "2024-01-01T00:00:00Z",
                    "records_count": 1000
                }
            },
            "features": ["feature1", "feature2"],
            "fields": [{"name": "datetime"}, {"name": "measured"}]
        }
    }
    entry = CatalogEntry(data)
    
    assert entry.id == "test_id"
    assert entry.title == "Test Title"
    assert entry.description == "Test Description"
    assert entry.theme == ["Test Theme"]
    assert entry.features == ["feature1", "feature2"]
    assert isinstance(entry.modified, datetime)
    
    # Test with minimal data
    minimal_data = {
        "dataset": {
            "dataset_id": "test_id",
            "metas": {
                "default": {
                    "title": "Fallback Title"
                }
            }
        }
    }
    entry = CatalogEntry(minimal_data)
    assert entry.id == "test_id"
    assert entry.title == "Fallback Title"
    assert entry.modified is None

def test_dataset_metadata():
    # Test with nested dataset structure
    data = {
        "dataset": {
            "dataset_id": "test_id",
            "metas": {
                "default": {
                    "title": "Test Title",
                    "description": "Test Description",
                    "theme": ["Test Theme"],
                    "modified": "2024-01-01T00:00:00Z",
                    "records_count": 1000
                }
            },
            "features": ["feature1"],
            "fields": [{"name": "field1"}],
            "attachments": [{"id": "attach1"}]
        }
    }
    metadata = DatasetMetadata(data)
    
    assert metadata.id == "test_id"
    assert metadata.title == "Test Title"
    assert metadata.description == "Test Description"
    assert metadata.theme == ["Test Theme"]
    assert isinstance(metadata.modified, datetime)
    assert metadata.features == ["feature1"]
    assert metadata.fields == [{"name": "field1"}]
    assert metadata.attachments == [{"id": "attach1"}]
    
    # Test with flat structure
    flat_data = {
        "dataset": {
            "dataset_id": "test_id",
            "metas": {
                "default": {
                    "title": "Test Title",
                    "modified": "2024-01-01T00:00:00Z"
                }
            }
        }
    }
    metadata = DatasetMetadata(flat_data)
    assert metadata.id == "test_id"
    assert metadata.title == "Test Title"
    assert isinstance(metadata.modified, datetime)

def test_records():
    # Test with actual API response structure
    data = {
        "total_count": 100,
        "records": [
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/18d60852ec0ddb577e67cd8437471670ea6e20e1"},
                    {"rel": "datasets", "href": "https://opendata.elia.be/api/v2/catalog/datasets"},
                    {"rel": "dataset", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032"}
                ],
                "record": {
                    "id": "18d60852ec0ddb577e67cd8437471670ea6e20e1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 109,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "resolutioncode": "PT15M",
                        "region": "Namur",
                        "measured": 0.0,
                        "mostrecentforecast": 0.0,
                        "mostrecentconfidence10": 0.0,
                        "mostrecentconfidence90": 0.0,
                        "dayahead11hforecast": 0.0,
                        "dayahead11hconfidence10": 0.0,
                        "dayahead11hconfidence90": 0.0,
                        "dayaheadforecast": 0.0,
                        "dayaheadconfidence10": 0.0,
                        "dayaheadconfidence90": 0.0,
                        "weekaheadforecast": 0.0,
                        "weekaheadconfidence10": 0.0,
                        "weekaheadconfidence90": 0.0,
                        "monitoredcapacity": 358.348,
                        "loadfactor": 0.0
                    }
                }
            },
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/76995adf361eb810046c632c68f74cd89b6f4ed7"}
                ],
                "record": {
                    "id": "76995adf361eb810046c632c68f74cd89b6f4ed7",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "resolutioncode": "PT15M",
                        "region": "Wallonia",
                        "measured": 0.0,
                        "loadfactor": 0.0,
                        "monitoredcapacity": 2376.313
                    }
                }
            }
        ]
    }
    records = Records(data)
    
    assert records.total_count == 100
    assert len(records.records) == 2
    
    # Test first record structure matches actual API
    first_record = records.records[0]
    assert "links" in first_record
    assert "record" in first_record
    assert first_record["record"]["id"] == "18d60852ec0ddb577e67cd8437471670ea6e20e1"
    assert first_record["record"]["timestamp"] == "2025-05-25T03:47:05.518Z"
    assert first_record["record"]["size"] == 109
    
    # Test fields structure in first record
    fields = first_record["record"]["fields"]
    assert fields["datetime"] == "2024-10-28T18:15:00+00:00"
    assert fields["resolutioncode"] == "PT15M"
    assert fields["region"] == "Namur"
    assert fields["measured"] == 0.0
    assert fields["loadfactor"] == 0.0
    assert fields["monitoredcapacity"] == 358.348
    assert "mostrecentforecast" in fields
    assert "dayahead11hforecast" in fields
    assert "weekaheadforecast" in fields
    
    # Test to_dict only returns records
    dict_data = records.to_dict()
    assert list(dict_data.keys()) == ["records"]
    assert len(dict_data["records"]) == 2
    
    # Test without next link
    # Test without next link - using actual API structure
    data_no_next = {
        "total_count": 100,
        "records": [
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/9368be1bcec6a79e3cc75843f48d5bc51e5f54b6"}
                ],
                "record": {
                    "id": "9368be1bcec6a79e3cc75843f48d5bc51e5f54b6",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-10-28T18:00:00+00:00",
                        "resolutioncode": "PT15M",
                        "region": "Belgium",
                        "measured": 0.0,
                        "monitoredcapacity": 10395.707,
                        "loadfactor": 0.0
                    }
                }
            }
        ]
    }
    records_no_next = Records(data_no_next)
    assert records_no_next.has_next == False