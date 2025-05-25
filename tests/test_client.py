"""
Tests for the Elia OpenData API client.
"""
import pytest
import requests
import logging
import responses
from elia_opendata.client import EliaClient
from elia_opendata.datasets import Dataset
from elia_opendata.error import RateLimitError, AuthError, APIError, ConnectionError

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def client():
    """Create a test client instance"""
    logger.debug("Creating test client instance")
    return EliaClient(api_key="test_key")

@pytest.fixture
def mock_api():
    """Setup mock API responses"""
    with responses.RequestsMock() as rsps:
        yield rsps

def test_client_initialization():
    """Test client initialization with and without API key"""
    logger.info("Testing client initialization")
    
    # Test without API key
    client = EliaClient()
    assert client.api_key is None
    assert client.timeout == 30
    assert client.max_retries == 3
    logger.debug("Client initialized without API key")
    
    # Test with API key
    client = EliaClient(api_key="test_key", timeout=60, max_retries=5)
    assert client.api_key == "test_key"
    assert client.timeout == 60
    assert client.max_retries == 5
    assert client.session.headers.get("Authorization") == "Bearer test_key"
    logger.debug("Client initialized with API key")

@pytest.mark.usefixtures("mock_api")
def test_get_catalog(client, mock_api):
    """Test getting catalog entries"""
    logger.info("Testing get_catalog method")
    
    mock_response = [
        {
            "dataset": {
                "dataset_id": "ods032",
                "metas": {
                    "default": {
                        "title": "Test Dataset",
                        "description": "Test description",
                        "theme": ["Test Theme"],
                        "modified": "2024-01-01T00:00:00Z",
                        "records_count": 1000
                    }
                },
                "features": ["feature1"],
                "fields": [{"name": "datetime"}, {"name": "measured"}]
            }
        }
    ]
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets",
        json=mock_response,
        status=200
    )
    
    logger.debug("Making request to get catalog")
    catalog = client.get_catalog()
    
    # Test proper parsing of nested dataset structure
    entry = catalog[0]
    assert len(catalog) == 1
    assert entry.id == "ods032"
    assert entry.title == "Test Dataset"
    assert entry.description == "Test description"
    assert entry.theme == ["Test Theme"]
    assert len(entry.features) == 1
    assert len(entry.fields) == 2
    logger.debug("Successfully retrieved catalog entries")

@pytest.mark.usefixtures("mock_api")
def test_get_dataset(client, mock_api):
    """Test getting specific dataset metadata"""
    logger.info("Testing get_dataset method")
    
    dataset_id = Dataset.PV_PRODUCTION.value
    mock_response = {
        "dataset": {
            "dataset_id": dataset_id,
            "metas": {
                "default": {
                    "title": "Photovoltaic Generation",
                    "description": "Photovoltaic generation data",
                    "theme": ["Generation"],
                    "modified": "2024-01-01T00:00:00Z",
                    "records_count": 1000
                }
            },
            "features": ["daily", "monthly"],
            "fields": [
                {"name": "datetime"},
                {"name": "measured"},
                {"name": "loadfactor"},
                {"name": "region"}
            ]
        }
    }
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}",
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting metadata for dataset: {dataset_id}")
    metadata = client.get_dataset(Dataset.PV_PRODUCTION)
    
    # Test proper parsing of nested metadata structure
    assert metadata.id == dataset_id
    assert metadata.title == "Photovoltaic Generation"
    assert metadata.description == "Photovoltaic generation data"
    assert metadata.theme == ["Generation"]
    assert len(metadata.features) == 2
    assert len(metadata.fields) == 4
    logger.debug("Successfully retrieved dataset metadata")

@pytest.mark.usefixtures("mock_api")
def test_error_handling(client, mock_api):
    """Test error handling for different HTTP errors"""
    logger.info("Testing error handling")
    
    # Test rate limit error
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets",
        status=429
    )
    
    logger.warning("Testing rate limit error handling")
    with pytest.raises(RateLimitError):
        client.get_catalog()
    
    # Test authentication error
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets",
        status=401
    )
    
    logger.warning("Testing authentication error handling")
    with pytest.raises(AuthError):
        client.get_catalog()
    
    # Test generic API error
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets",
        status=500
    )
    
    logger.warning("Testing generic API error handling")
    with pytest.raises(APIError):
        client.get_catalog()

@pytest.mark.usefixtures("mock_api")
def test_get_records(client, mock_api):
    """Test getting records from a dataset"""
    logger.info("Testing get_records method")
    
    dataset_id = Dataset.PV_PRODUCTION.value
    mock_response = {
        "total_count": 2,
        "records": [
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/18d60852ec0ddb577e67cd8437471670ea6e20e1"}
                ],
                "record": {
                    "id": "record1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 109,
                    "fields": {
                        "datetime": "2024-01-01T00:00:00Z",
                        "resolutioncode": "PT15M",
                        "region": "BE",
                        "measured": 100,
                        "loadfactor": 25.5,
                        "monitoredcapacity": 358.348
                    }
                }
            },
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/76995adf361eb810046c632c68f74cd89b6f4ed7"}
                ],
                "record": {
                    "id": "record2",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-01-01T01:00:00Z",
                        "resolutioncode": "PT15M",
                        "region": "BE",
                        "measured": 200,
                        "loadfactor": 30.0,
                        "monitoredcapacity": 2376.313
                    }
                }
            }
        ],
        "links": [{"rel": "self", "href": "current_page"}],
        "has_next": False
    }
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting records for dataset: {dataset_id}")
    records = client.get_records(Dataset.PV_PRODUCTION, limit=2)
    
    assert records.total_count == 2
    assert len(records.records) == 2
    assert records.has_next is False
    logger.debug("Successfully retrieved dataset records")

@pytest.mark.usefixtures("mock_api")
def test_iter_records(client, mock_api):
    """Test iterating through records"""
    logger.info("Testing iter_records method")
    
    dataset_id = Dataset.PV_PRODUCTION.value
    mock_responses = [
        {
            "total_count": 4,
            "records": [
                {
                    "record": {
                        "fields": {
                            "datetime": "2024-01-01T00:00:00Z",
                            "measured": 100
                        }
                    }
                },
                {
                    "record": {
                        "fields": {
                            "datetime": "2024-01-01T01:00:00Z",
                            "measured": 200
                        }
                    }
                }
            ],
            "links": [{"rel": "next"}],
            "has_next": True
        },
        {
            "total_count": 4,
            "records": [
                {
                    "record": {
                        "fields": {
                            "datetime": "2024-01-01T02:00:00Z",
                            "measured": 300
                        }
                    }
                },
                {
                    "record": {
                        "fields": {
                            "datetime": "2024-01-01T03:00:00Z",
                            "measured": 400
                        }
                    }
                }
            ],
            "links": [],
            "has_next": False
        }
    ]
    
    # Mock first batch
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        json=mock_responses[0],
        status=200
    )
    
    # Mock second batch
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        json=mock_responses[1],
        status=200
    )
    
    logger.debug(f"Requesting record batches for dataset: {dataset_id}")
    all_records = []
    for batch in client.iter_records(Dataset.PV_PRODUCTION, batch_size=2):
        all_records.extend(batch.records)
        
    assert len(all_records) == 4
    assert [r["record"]["fields"]["measured"] for r in all_records] == [100, 200, 300, 400]
    logger.debug("Successfully retrieved all record batches")

@pytest.mark.usefixtures("mock_api")
def test_search_catalog(client, mock_api):
    """Test searching the catalog"""
    logger.info("Testing search_catalog method")
    
    mock_response = [
        {
            "dataset": {
                "dataset_id": "solar_test",
                "metas": {
                    "default": {
                        "title": "Solar Test Dataset",
                        "description": "Test solar data",
                        "theme": ["Generation"],
                        "modified": "2024-01-01T00:00:00Z"
                    }
                },
                "features": ["daily"],
                "fields": [{"name": "measured"}]
            }
        }
    ]
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/search",
        json=mock_response,
        status=200
    )
    
    logger.debug("Searching catalog with query: 'solar'")
    results = client.search_catalog("solar")
    
    assert len(results) == 1
    result = results[0]
    assert result.id == "solar_test"
    assert result.title == "Solar Test Dataset"
    assert result.theme == ["Generation"]
    assert len(result.features) == 1
    assert len(result.fields) == 1
    logger.debug("Successfully retrieved search results")

@pytest.mark.usefixtures("mock_api")
def test_connection_error(client, mock_api):
    """Test connection error handling"""
    logger.info("Testing connection error handling")
    
    # Simulate connection error
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets",
        body=requests.exceptions.ConnectionError()
    )
    
    logger.warning("Testing connection error scenario")
    with pytest.raises(ConnectionError) as exc_info:
        client.get_catalog()
    
    assert "Connection failed" in str(exc_info.value)
    logger.debug("Successfully caught connection error")

@pytest.mark.usefixtures("mock_api")
def test_get_dataset_between(client, mock_api):
    """Test getting dataset records between two dates"""
    logger.info("Testing get_dataset_between method")
    
    dataset_id = Dataset.PV_PRODUCTION.value
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    mock_response = {
        "total_count": 2,
        "records": [
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/date_test_1"}
                ],
                "record": {
                    "id": "date_test_1",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 109,
                    "fields": {
                        "datetime": "2024-01-15T12:00:00+00:00",
                        "resolutioncode": "PT15M",
                        "region": "BE",
                        "measured": 100,
                        "loadfactor": 25.5,
                        "monitoredcapacity": 358.348
                    }
                }
            },
            {
                "links": [
                    {"rel": "self", "href": "https://opendata.elia.be/api/v2/catalog/datasets/ods032/records/date_test_2"}
                ],
                "record": {
                    "id": "date_test_2",
                    "timestamp": "2025-05-25T03:47:05.518Z",
                    "size": 113,
                    "fields": {
                        "datetime": "2024-01-16T12:00:00+00:00",
                        "resolutioncode": "PT15M",
                        "region": "BE",
                        "measured": 200,
                        "loadfactor": 30.0,
                        "monitoredcapacity": 2376.313
                    }
                }
            }
        ],
        "has_next": False
    }
    
    # The expected where condition
    expected_where = f"datetime >= '{start_date}' AND datetime <= '{end_date}'"
    
    def match_query_params(request):
        from urllib.parse import parse_qs, urlparse
        query = urlparse(request.url).query
        params = parse_qs(query)
        valid = params.get('where', [None])[0] == expected_where
        reason = "where parameter does not match expected value" if not valid else ""
        return valid, reason
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        match=[match_query_params],
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting records for dataset {dataset_id} between {start_date} and {end_date}")
    records = client.get_dataset_between(
        Dataset.PV_PRODUCTION,
        start_date=start_date,
        end_date=end_date
    )
    
    assert records.total_count == 2
    assert len(records.records) == 2
    assert all('datetime' in record['record']['fields'] for record in records.records)
    assert all(start_date <= record['record']['fields']['datetime'].split('T')[0] <= end_date
              for record in records.records)
    logger.debug("Successfully retrieved date-filtered records")