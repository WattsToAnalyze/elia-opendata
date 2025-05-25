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
            "dataset_id": "test_dataset",
            "title": "Test Dataset",
            "description": "Test description"
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
    
    assert len(catalog) == 1
    assert catalog[0].id == "test_dataset"
    assert catalog[0].title == "Test Dataset"
    logger.debug("Successfully retrieved catalog entries")

@pytest.mark.usefixtures("mock_api")
def test_get_dataset(client, mock_api):
    """Test getting specific dataset metadata"""
    logger.info("Testing get_dataset method")
    
    dataset_id = Dataset.SOLAR_GENERATION.value
    mock_response = {
        "dataset_id": dataset_id,
        "title": "Solar Generation",
        "fields": []
    }
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}",
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting metadata for dataset: {dataset_id}")
    metadata = client.get_dataset(Dataset.SOLAR_GENERATION)
    
    assert metadata.id == dataset_id
    assert metadata.title == "Solar Generation"
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
    
    dataset_id = Dataset.SOLAR_GENERATION.value
    mock_response = {
        "total_count": 2,
        "records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200}
        ],
        "has_next": False
    }
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting records for dataset: {dataset_id}")
    records = client.get_records(Dataset.SOLAR_GENERATION, limit=2)
    
    assert records.total_count == 2
    assert len(records.records) == 2
    assert records.has_next is False
    logger.debug("Successfully retrieved dataset records")

@pytest.mark.usefixtures("mock_api")
def test_iter_records(client, mock_api):
    """Test iterating through records"""
    logger.info("Testing iter_records method")
    
    dataset_id = Dataset.SOLAR_GENERATION.value
    mock_responses = [
        {
            "total_count": 4,
            "records": [{"id": 1}, {"id": 2}],
            "links": [{"rel": "next"}],
            "has_next": True
        },
        {
            "total_count": 4,
            "records": [{"id": 3}, {"id": 4}],
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
    for batch in client.iter_records(Dataset.SOLAR_GENERATION, batch_size=2):
        all_records.extend(batch.records)
        
    assert len(all_records) == 4
    assert [r["id"] for r in all_records] == [1, 2, 3, 4]
    logger.debug("Successfully retrieved all record batches")

@pytest.mark.usefixtures("mock_api")
def test_search_catalog(client, mock_api):
    """Test searching the catalog"""
    logger.info("Testing search_catalog method")
    
    mock_response = [
        {
            "dataset_id": "solar_test",
            "title": "Solar Test Dataset",
            "description": "Test solar data"
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
    assert results[0].id == "solar_test"
    assert results[0].title == "Solar Test Dataset"
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
    
    dataset_id = Dataset.SOLAR_GENERATION.value
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    mock_response = {
        "total_count": 2,
        "records": [
            {"id": 1, "datetime": "2024-01-15T12:00:00", "value": 100},
            {"id": 2, "datetime": "2024-01-16T12:00:00", "value": 200}
        ],
        "has_next": False
    }
    
    # The expected where condition
    expected_where = f"datetime >= '{start_date}' AND datetime <= '{end_date}'"
    
    def match_query_params(request):
        params = requests.utils.parse_qs(request.url.split('?')[1])
        assert params.get('where', [None])[0] == expected_where
        return True
    
    mock_api.add(
        responses.GET,
        f"{EliaClient.BASE_URL}catalog/datasets/{dataset_id}/records",
        match=[match_query_params],
        json=mock_response,
        status=200
    )
    
    logger.debug(f"Requesting records for dataset {dataset_id} between {start_date} and {end_date}")
    records = client.get_dataset_between(
        Dataset.SOLAR_GENERATION,
        start_date=start_date,
        end_date=end_date
    )
    
    assert records.total_count == 2
    assert len(records.records) == 2
    assert all('datetime' in record for record in records.records)
    assert all(start_date <= record['datetime'].split('T')[0] <= end_date
              for record in records.records)
    logger.debug("Successfully retrieved date-filtered records")