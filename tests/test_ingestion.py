import pytest
from unittest.mock import patch, MagicMock
from ingestion.fetcher import fetch_gtfs_realtime
from ingestion.producer import normalize_vehicle_position

def test_fetch_gtfs_realtime_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"gtfs_protobuf_bytes"

    with patch("requests.get", return_value=mock_response):
        data = fetch_gtfs_realtime("http://fake-feed.com/gtfs")
        assert data == b"gtfs_protobuf_bytes"

def test_fetch_gtfs_realtime_failure():
    mock_response = MagicMock()
    mock_response.status_code = 500

    with patch("requests.get", return_value=mock_response):
        data = fetch_gtfs_realtime("http://fake-feed.com/gtfs")
        assert data is None

def test_normalize_vehicle_position():
    raw_entity = {
        "id": "v101",
        "vehicle": {
            "trip": {"route_id": "M15"},
            "position": {"latitude": 40.7128, "longitude": -74.0060},
            "timestamp": 1700000000
        }
    }
    normalized = normalize_vehicle_position(raw_entity)
    
    assert normalized["vehicle_id"] == "v101"
    assert normalized["route_id"] == "M15"
    assert normalized["latitude"] == 40.7128
    assert normalized["longitude"] == -74.0060
    assert "ingested_at" in normalized