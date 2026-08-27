import pytest
import duckdb
from fastapi.testclient import TestClient
from serving.realtime.main import app

client = TestClient(app)

# --- Real-Time Serving API Tests ---
def test_fetch_vehicles_endpoint():
    response = client.get("/api/vehicles")
    assert response.status_code == 200
    assert "data" in response.json()
    assert isinstance(response.json()["data"], list)

def test_fetch_route_health_endpoint():
    response = client.get("/api/route-health")
    assert response.status_code == 200
    assert "data" in response.json()

# --- Batch DuckDB Query Tests ---
def test_duckdb_batch_view_execution(tmp_path):
    db_file = str(tmp_path / "test_gtfs.db")
    conn = duckdb.connect(db_file)
    
    # Create test schema and data
    conn.execute("""
        CREATE TABLE route_health_history (
            route_id VARCHAR,
            avg_delay_sec DOUBLE,
            timestamp TIMESTAMP
        );
        INSERT INTO route_health_history VALUES ('M15', 45.0, '2026-08-28 00:00:00');
    """)

    result = conn.execute("SELECT AVG(avg_delay_sec) FROM route_health_history WHERE route_id = 'M15'").fetchone()
    assert result[0] == 45.0
    conn.close()