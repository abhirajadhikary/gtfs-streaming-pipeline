import os
from pyspark.sql import SparkSession

DELTA_BUCKET = os.getenv("DELTA_BUCKET_NAME", "gtfs-lakehouse")
spark = (
    SparkSession.builder.appName("s3a-test")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.write.mode("overwrite").parquet(f"s3a://{DELTA_BUCKET}/test-write")
print("SUCCESS")