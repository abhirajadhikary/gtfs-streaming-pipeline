import os
import duckdb

DB_PATH = os.getenv("BATCH_DB_PATH", "gtfs_batch.db")
QUERY_DB_PATH = os.getenv("BATCH_QUERY_DB_PATH", "gtfs_batch_read.db")

def create_batch_views(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE VIEW hourly_vehicle_speeds AS
        SELECT route_id, date_trunc('hour', timestamp) AS hour_bin,
               AVG(speed) AS avg_speed_kmh, MAX(speed) AS max_speed_kmh
        FROM vehicle_status_history
        GROUP BY route_id, hour_bin
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW hourly_route_performance AS
        SELECT route_id, date_trunc('hour', timestamp) AS hour_bin,
               AVG(avg_delay_sec) AS mean_delay_sec, MAX(avg_delay_sec) AS max_delay_sec
        FROM route_health_history
        GROUP BY route_id, hour_bin
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW daily_network_reliability AS
        SELECT date_trunc('day', timestamp) AS day_bin,
               AVG(on_time_pct) AS avg_on_time_percentage,
               AVG(total_vehicles) AS avg_active_vehicles
        FROM network_health_history
        GROUP BY day_bin
    """)


def open_batch_database() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(QUERY_DB_PATH, read_only=True)


def init_batch_views() -> None:
    conn = open_batch_database()
    try:
        missing = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_name IN (
                'hourly_vehicle_speeds', 'hourly_route_performance',
                'daily_network_reliability'
            )
        """).fetchall()
        if len(missing) != 3:
            raise RuntimeError(
                "Batch views are not initialized. Start serving.batch.consumer first."
            )
        conn.execute("SELECT 1")
    finally:
        conn.close()


if __name__ == "__main__":
    init_batch_views()
    print(f"Batch views are ready in {QUERY_DB_PATH}")