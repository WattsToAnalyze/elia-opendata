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

    # Test DataFrame conversion
    df = model.to_pandas()
    assert "pandas.core.frame.DataFrame" in str(type(df))
    assert len(df) == 1
    assert all(key in df.columns for key in test_data.keys())

    # Test polars conversion
    pl_df = model.to_polars()
    assert "polars.dataframe.frame.DataFrame" in str(type(pl_df))
    assert len(pl_df) == 1
    assert all(key in pl_df.columns for key in test_data.keys())

    # Test numpy conversion
    np_array = model.to_numpy()
    assert "numpy.ndarray" in str(type(np_array))
    assert len(np_array) == 1

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
                    {"rel": "self", "href": "https://example.com/records/1"}
                ],
                "record": {
                    "id": "record1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 109,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "region": "Namur",
                        "measured": 0.0,
                        "monitoredcapacity": 358.348
                    }
                }
            },
            {
                "links": [
                    {"rel": "self", "href": "https://example.com/records/2"}
                ],
                "record": {
                    "id": "record2",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "region": "Wallonia",
                        "measured": 0.0,
                        "monitoredcapacity": 2376.313
                    }
                }
            }
        ]
    }
    records = Records(data)
    
    # Test basic attributes
    assert records.total_count == 100
    assert len(records.records) == 2
    
    # Test first record structure
    first_record = records.records[0]
    assert "links" in first_record
    assert "record" in first_record
    assert first_record["record"]["id"] == "record1"
    assert first_record["record"]["timestamp"] == "2025-05-25T03:47:05.518Z"
    assert first_record["record"]["size"] == 109
    
    # Test fields structure
    fields = first_record["record"]["fields"]
    assert fields["datetime"] == "2024-10-28T18:15:00+00:00"
    assert fields["region"] == "Namur"
    assert fields["measured"] == 0.0
    assert fields["monitoredcapacity"] == 358.348
    
    # Test pandas conversion
    df = records.to_pandas()
    assert "pandas.core.frame.DataFrame" in str(type(df))
    assert len(df) == 2
    assert "region" in df.columns
    assert "measured" in df.columns
    assert "monitoredcapacity" in df.columns
    assert df["region"].tolist() == ["Namur", "Wallonia"]
    
    # Test polars conversion
    pl_df = records.to_polars()
    assert "polars.dataframe.frame.DataFrame" in str(type(pl_df))
    assert len(pl_df) == 2
    assert "region" in pl_df.columns
    assert "measured" in pl_df.columns
    assert "monitoredcapacity" in pl_df.columns
    
    # Test numpy conversion
    np_array = records.to_numpy()
    assert "numpy.ndarray" in str(type(np_array))
    assert len(np_array) == 2
    
    # Test links
    assert not records.has_next
    
    # Test without next link
    data_no_next = {
        "total_count": 100,
        "records": [
            {
                "links": [
                    {"rel": "self", "href": "https://example.com/records/1"}
                ],
                "record": {
                    "id": "record1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-10-28T18:00:00+00:00",
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

def test_model_data_conversions():
    # Test with flattened record structure
    data = {
        "total_count": 2,
        "records": [
            {
                "record": {
                    "id": "record1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 109,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "region": "Namur",
                        "measured": 0.0,
                        "monitoredcapacity": 358.348
                    }
                }
            },
            {
                "record": {
                    "id": "record2",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-10-28T18:15:00+00:00",
                        "region": "Wallonia",
                        "measured": 0.0,
                        "monitoredcapacity": 2376.313
                    }
                }
            }
        ]
    }
    records = Records(data)
    
    # Test pandas conversion
    df = records.to_pandas()
    assert "pandas.core.frame.DataFrame" in str(type(df))
    assert len(df) == 2
    assert "id" in df.columns
    assert "region" in df.columns
    assert "measured" in df.columns
    assert df["region"].tolist() == ["Namur", "Wallonia"]
    assert df["monitoredcapacity"].tolist() == [358.348, 2376.313]
    
    # Test polars conversion
    pl_df = records.to_polars()
    assert "polars.dataframe.frame.DataFrame" in str(type(pl_df))
    assert len(pl_df) == 2
    assert "id" in pl_df.columns
    assert "region" in pl_df.columns
    assert "measured" in pl_df.columns
    
    # Test numpy conversion
    np_array = records.to_numpy()
    assert "numpy.ndarray" in str(type(np_array))
    assert len(np_array) == 2