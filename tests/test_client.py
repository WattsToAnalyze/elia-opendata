"""
Simple tests for the Elia OpenData API client.
"""
import responses
from elia_opendata.client import EliaClient
from elia_opendata.dataset_catalog import IMBALANCE_PRICES_REALTIME


def test_client_initialization():
    """Test client initialization."""
    # Test with default values
    client = EliaClient()
    assert client.timeout == 30

    # Test with custom timeout
    client = EliaClient(timeout=60)
    assert client.timeout == 60


@responses.activate
def test_get_records():
    """Test getting records from a predefined dataset."""
    # Mock response data matching actual API structure
    mock_response = {
        "total_count": 4,
        "results": [
            {
                "datetime": "2025-08-17T00:00:00+00:00",
                "resolutioncode": "PT1M",
                "quarterhour": "2025-08-17T00:00:00+00:00",
                "qualitystatus": "DataIssue",
                "ace": -22.072,
                "systemimbalance": 226.214,
                "alpha": 1.116,
                "alpha_prime": 0.0,
                "marginalincrementalprice": 129.6,
                "marginaldecrementalprice": 45.06,
                "imbalanceprice": 43.944
            },
            {
                "datetime": "2025-08-17T00:36:00+00:00",
                "resolutioncode": "PT1M",
                "quarterhour": "2025-08-17T00:30:00+00:00",
                "qualitystatus": "DataIssue",
                "ace": -4.387,
                "systemimbalance": 237.58,
                "alpha": 6.258,
                "alpha_prime": 0.0,
                "marginalincrementalprice": 127.3,
                "marginaldecrementalprice": -24.0,
                "imbalanceprice": -30.258
            }
        ]
    }

    # Mock the API endpoint
    base_url = EliaClient.BASE_URL
    endpoint = f"catalog/datasets/{IMBALANCE_PRICES_REALTIME}/records"
    dataset_url = f"{base_url}{endpoint}"
    responses.add(
        responses.GET,
        dataset_url,
        json=mock_response,
        status=200
    )

    # Test the get_records method
    client = EliaClient()
    result = client.get_records(IMBALANCE_PRICES_REALTIME, limit=2)
    # Verify the response structure matches actual API
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["datetime"] == "2025-08-17T00:00:00+00:00"
    assert result[0]["ace"] == -22.072
    assert result[1]["imbalanceprice"] == -30.258
