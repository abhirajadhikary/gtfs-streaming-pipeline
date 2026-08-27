import time
from streaming.utils import get_spark_session
from streaming.bronze import run_bronze_pipeline
from streaming.silver import run_silver_pipeline
from streaming.realtime_stream import run_realtime_producer

def run_all_streams():
    spark = get_spark_session("GTFS-Streaming-Pipeline")
    spark.sparkContext.setLogLevel("WARN")

    active_queries = []

    print("Starting Bronze Pipeline...")
    bronze_query = run_bronze_pipeline(spark)
    active_queries.append(bronze_query)

    print("Starting Silver Pipeline...")
    silver_query = run_silver_pipeline(spark)
    active_queries.extend(silver_query if isinstance(silver_query, list) else [silver_query])

    print("Starting Real-time Producer Pipeline...")
    realtime_query = run_realtime_producer(spark)
    active_queries.extend(realtime_query if isinstance(realtime_query, list) else [realtime_query])

    print(f"Successfully started {len(active_queries)} streaming queries on one unified JVM...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_all_streams()