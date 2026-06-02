import os

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    dayofmonth,
    from_json,
    hour,
    lit,
    max,
    month,
    row_number,
    to_timestamp,
    year,
)
from pyspark.sql.types import IntegerType, StringType, StructType


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GOOGLE_TRENDS", "google_trends_stream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "google-trends-raw")
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_TRENDS", "google_trends")


def is_valid_mongo_uri(uri: str) -> bool:
    if not uri:
        return False
    lowered = uri.lower()
    return not any(token in lowered for token in ("<username>", "<password>", "xxxxx", "placeholder"))


def resolve_mongo_uri():
    if MONGO_BACKEND == "atlas" and is_valid_mongo_uri(MONGO_URI_ATLAS):
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL


def ensure_bucket_exists():
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' created.")


def create_spark_session():
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    ]

    spark = (
        SparkSession.builder.appName("GoogleTrendsBatchAnalytics")
        .config("spark.master", "spark://spark-master:7077")
        .config("spark.sql.streaming.minBatchesToRetain", "2")
        .config("spark.sql.streaming.checkpointFileManager.cleaner.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_mongo(dataframe, collection, mode="append"):
    dataframe.write.format("mongodb").mode(mode).option(
        "spark.mongodb.write.connection.uri", resolve_mongo_uri()
    ).option("spark.mongodb.write.database", MONGO_DB_NAME).option(
        "spark.mongodb.write.collection", collection
    ).save()


def process_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    raw_df = (
        batch_df.withColumn("ingested_at", current_timestamp())
        .withColumn("ingested_year", year("ingested_at"))
        .withColumn("ingested_month", month("ingested_at"))
        .withColumn("ingested_day", dayofmonth("ingested_at"))
        .withColumn("ingested_hour", hour("ingested_at"))
        .withColumn("timestamp_ts", col("timestamp"))
        .withColumn("collected_at_ts", col("collected_at"))
    )

    raw_path = f"s3a://{MINIO_BUCKET}/raw/google_trends"
    raw_df.drop("timestamp_ts", "collected_at_ts").write.mode("append").partitionBy(
        "ingested_year",
        "ingested_month",
        "ingested_day",
        "ingested_hour",
        "keyword",
    ).parquet(raw_path)

    # Zapisujemy historię (raw_df zawiera już rzutowane timestampy)
    history_df = raw_df.select("keyword", "timestamp", "interest", "timeframe", "geo", "collected_at")
    write_to_mongo(history_df, "google_trends_interest_over_time")

    latest_window = Window.partitionBy("keyword").orderBy(col("timestamp_ts").desc(), col("collected_at_ts").desc())
    latest_interest = (
        raw_df.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .select("keyword", col("interest").alias("last_interest"))
    )

    summary = (
        raw_df.groupBy("keyword")
        .agg(
            avg("interest").alias("avg_interest"),
            max("interest").alias("peak_interest"),
        )
        .join(latest_interest, on="keyword", how="inner")
        .withColumn("collected_at", current_timestamp())
        .withColumn("timeframe", lit(os.getenv("GOOGLE_TRENDS_TIMEFRAME", "now 7-d")))
        .withColumn("geo", lit(os.getenv("GOOGLE_TRENDS_GEO", "")))
    )

    write_to_mongo(summary, "google_trends_summary")
    print(f"Processed Google Trends batch {epoch_id} successfully.")


def main():
    ensure_bucket_exists()
    spark = create_spark_session()
    print("Spark Session created. Reading Google Trends events from Kafka...")

    schema = (
        StructType()
        .add("keyword", StringType())
        .add("timestamp", StringType())
        .add("interest", IntegerType())
        .add("timeframe", StringType())
        .add("geo", StringType())
        .add("collected_at", StringType())
        .add("source", StringType())
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("collected_at", to_timestamp("collected_at"))
    )

    query = (
        parsed_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "/app/spark_checkpoints/google_trends")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()