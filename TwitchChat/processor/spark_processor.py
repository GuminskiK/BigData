import os
import time
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from pymongo import MongoClient
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    current_timestamp,
    dayofmonth,
    from_json,
    hour,
    lit,
    max,
    min,
    month,
    sum as spark_sum,
    to_timestamp,
    udf,
    when,
    window,
    year,
)
from pyspark.sql.types import DoubleType, StringType, StructType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092").split(",")
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
MONGODB_DB = os.getenv("MONGO_DB_CHAT", "twitch_chat")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TWITCH_CHAT", "twitch_chat_stream")
WINDOW_MINUTES = int(os.getenv("SENTIMENT_WINDOW_MINUTES", "5"))
CHECKPOINT_VERSION = os.getenv("SPARK_CHECKPOINT_VERSION", "v1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "twitch-chat-raw")
KAFKA_STARTUP_RETRY_SECONDS = int(os.getenv("KAFKA_STARTUP_RETRY_SECONDS", "5"))
KAFKA_STARTUP_MAX_RETRIES = int(os.getenv("KAFKA_STARTUP_MAX_RETRIES", "12"))


def is_valid_mongo_uri(uri: str) -> bool:
    if not uri:
        return False
    lowered = uri.lower()
    return not any(token in lowered for token in ("<username>", "<password>", "xxxxx", "placeholder"))


def resolve_mongo_uri():
    if MONGO_BACKEND == "atlas" and is_valid_mongo_uri(MONGO_URI_ATLAS):
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL


mongo_client = MongoClient(resolve_mongo_uri())
mongo_db = mongo_client[MONGODB_DB]


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
    
    spark = (
            SparkSession.builder.appName("TwitchChatProcessor")
            # Optymalizacja pod mały RAM (8GB)
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")  # Kluczowe: mniej partycji = mniejszy overhead
            
            # Zarządzanie checkpointami i danymi
            .config("spark.sql.streaming.minBatchesToRetain", "2")
            .config("spark.sql.streaming.checkpointFileManager.cleaner.enabled", "true")
            .config("spark.cleaner.periodicGC.interval", "1min")
            
            .config("spark.mongodb.write.connection.uri", resolve_mongo_uri())
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
    


def wait_for_kafka_connection():
    for attempt in range(1, KAFKA_STARTUP_MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_SERVERS,
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
                metadata_max_age_ms=3000,
            )
            consumer.topics()
            consumer.close()
            print(f"Kafka is reachable on attempt {attempt}.")
            return
        except Exception as exc:
            print(
                f"Kafka not ready yet for TwitchChatProcessor (attempt {attempt}/{KAFKA_STARTUP_MAX_RETRIES}): {exc}"
            )
            time.sleep(KAFKA_STARTUP_RETRY_SECONDS)

    raise RuntimeError("Kafka did not become ready in time for TwitchChatProcessor.")


def analyze_sentiment(text, analyzer):
    if text:
        try:
            return analyzer.polarity_scores(text)["compound"]
        except Exception:
            return 0.0
    return 0.0


def to_docs(dataframe):
    return [row.asDict(recursive=True) for row in dataframe.collect()]


def insert_documents(collection_name, documents):
    if not documents:
        return

    mongo_db[collection_name].insert_many(documents)


def upsert_totals(collection_name, documents, key_fields):
    collection = mongo_db[collection_name]
    for document in documents:
        query = {field: document[field] for field in key_fields}
        inc_fields = {
            key: value
            for key, value in document.items()
            if key not in key_fields and key not in {"min_sentiment", "max_sentiment"}
        }
        update = {
            "$setOnInsert": {field: document[field] for field in key_fields},
            "$inc": inc_fields,
            "$min": {"min_sentiment": document.get("min_sentiment", 0.0)},
            "$max": {"max_sentiment": document.get("max_sentiment", 0.0)},
            "$set": {"updated_at": datetime.now(timezone.utc)},
        }
        collection.update_one(query, update, upsert=True)


def build_window_stats(batch_df, window_duration, window_minutes, collection_name):
    windowed_df = (
        batch_df.groupBy(window(col("timestamp"), window_duration), col("channel"))
        .agg(
            avg("sentiment_score").alias("avg_sentiment"),
            count("*").alias("message_count"),
            spark_sum(when(col("sentiment_score") < -0.3, 1).otherwise(0)).alias("negative_message_count"),
            approx_count_distinct("username").alias("unique_chatters"),
        )
        .withColumn("window_minutes", lit(window_minutes))
        .withColumn("snapshot_at", current_timestamp())
    )
    insert_documents(collection_name, to_docs(windowed_df))


def build_user_totals(batch_df):
    user_stats_df = (
        batch_df.withColumn("is_subscriber", col("badges.subscriber").isNotNull())
        .groupBy("channel", "username")
        .agg(
            count("*").alias("messages"),
            spark_sum(when(col("sentiment_score") < -0.3, 1).otherwise(0)).alias("negative_messages"),
            spark_sum(when(col("is_subscriber"), 1).otherwise(0)).alias("subscriber_messages"),
            spark_sum(when(~col("is_subscriber"), 1).otherwise(0)).alias("normal_messages"),
            spark_sum(when(col("is_subscriber"), col("sentiment_score")).otherwise(0.0)).alias("subscriber_sentiment_sum"),
            spark_sum(when(col("is_subscriber"), 1).otherwise(0)).alias("subscriber_sentiment_count"),
            spark_sum(when(~col("is_subscriber"), col("sentiment_score")).otherwise(0.0)).alias("normal_sentiment_sum"),
            spark_sum(when(~col("is_subscriber"), 1).otherwise(0)).alias("normal_sentiment_count"),
            spark_sum("sentiment_score").alias("sentiment_sum"),
            count("sentiment_score").alias("sentiment_count"),
            min("sentiment_score").alias("min_sentiment"),
            max("sentiment_score").alias("max_sentiment"),
        )
    )
    upsert_totals("chat_user_totals", to_docs(user_stats_df), ["channel", "username"])


def process_batch(batch_df, epoch_id):
    if batch_df.rdd.isEmpty():
        return

    raw_df = (
        batch_df.drop("sentiment_score")
        .withColumn("ingested_at", current_timestamp())
        .withColumn("ingested_year", year("ingested_at"))
        .withColumn("ingested_month", month("ingested_at"))
        .withColumn("ingested_day", dayofmonth("ingested_at"))
        .withColumn("ingested_hour", hour("ingested_at"))
    )
    raw_df.write.mode("append").partitionBy(
        "channel",
        "ingested_year",
        "ingested_month",
        "ingested_day",
        "ingested_hour",
    ).parquet(f"s3a://{MINIO_BUCKET}/raw/twitch_chat")

    build_window_stats(batch_df, "1 minute", 1, "chat_stats_1m")
    build_window_stats(batch_df, f"{WINDOW_MINUTES} minute", WINDOW_MINUTES, "channel_stats")
    build_window_stats(batch_df, "1 hour", 60, "chat_stats_1h")
    build_user_totals(batch_df)

    print(f"Processed Twitch chat batch {epoch_id} successfully.")


def main():
    ensure_bucket_exists()
    spark = create_spark_session()
    wait_for_kafka_connection()

    schema = (
        StructType()
        .add("timestamp", StringType())
        .add("channel", StringType())
        .add("username", StringType())
        .add("message", StringType())
        .add(
            "badges",
            StructType()
            .add("subscriber", StringType())
            .add("moderator", StringType()),
        )
    )

    analyzer = SentimentIntensityAnalyzer()
    sentiment_udf = udf(lambda text: analyze_sentiment(text, analyzer), DoubleType())

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(KAFKA_SERVERS))
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    processed_df = (
        df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("sentiment_score", sentiment_udf(col("message")))
    )

    while True:
        try:
            (
                processed_df.writeStream.foreachBatch(process_batch)
                .option("checkpointLocation", f"/app/spark_checkpoints/{CHECKPOINT_VERSION}/chat")
                .start()
            )
            break
        except Exception as exc:
            print(f"Failed to start Twitch chat stream, retrying in {KAFKA_STARTUP_RETRY_SECONDS} seconds: {exc}")
            time.sleep(KAFKA_STARTUP_RETRY_SECONDS)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()