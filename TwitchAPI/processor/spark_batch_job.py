import os
from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, dayofmonth, from_json, 
    hour, lit, max, month, row_number, sum, to_timestamp, year
)
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructType

import boto3
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_TWITCH_API", "twitch_api_stream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_ANALYTICS", os.getenv("MONGO_DB_NAME", "twitch_api_analytics"))


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
mongo_db = mongo_client[MONGO_DB_NAME]


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
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
    ]

    spark = (
        SparkSession.builder.appName("TwitchBatchAnalytics")
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


def to_docs(dataframe):
    return [row.asDict(recursive=True) for row in dataframe.collect()]


def insert_documents(collection_name, documents):
    if not documents:
        return
    mongo_db[collection_name].insert_many(documents)

def upsert_documents(collection_name, documents, keys):
    """
    Zamiast klasycznego upsertu, który nadpisuje dane, 
    robimy insert, aby zachować historię dla wykresów czasowych Grafany.
    Dodatkowo tworzymy indeksy na kluczach, aby API działało szybko.
    """
    if not documents:
        return
    
    collection = mongo_db[collection_name]
    
    # Opcjonalnie: upewnij się, że masz indeksy pod zapytania Grafany
    if keys:
        collection.create_index([(k, 1) for k in keys] + [("snapshot_at", -1)])
    
    # Wstawiamy nowe snapshoty (to pozwala na wykresy 'viewers over time')
    collection.insert_many(documents)
    print(f"Inserted {len(documents)} snapshots into {collection_name}")

def process_scope(scope_name, scope_df, collection_suffix=""):
    if scope_df.rdd.isEmpty():
        return

    # 1. Agregacja dla top gier w tym konkretnym momencie (batchu)
    top_games = scope_df.groupBy("game_name") \
        .agg(
            sum("viewer_count").alias("total_viewers"), 
            count("stream_id").alias("streams_count")
        ) \
        .withColumn("stream_scope", lit(scope_name)) \
        .withColumn("snapshot_at", current_timestamp())

    # 2. Agregacja dla streamerów (kto ma najwięcej widzów teraz)
    top_streamers = scope_df.groupBy("user_name", "game_name") \
        .agg(max("viewer_count").alias("peak_viewers")) \
        .withColumn("stream_scope", lit(scope_name)) \
        .withColumn("snapshot_at", current_timestamp())

    # 3. Szczegółowe statystyki twórców (tytuły, status live)
    # Wybieramy najświeższy rekord dla każdego streamu w batchu
    window_spec = Window.partitionBy("user_name").orderBy(col("collected_at_ts").desc())
    
    creator_snapshots = scope_df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select(
            "user_name", "game_name", 
            col("viewer_count").alias("current_viewers"),
            col("viewer_count").alias("peak_viewers"), # w tym snapshot'cie
            "title", "started_at", "collected_at", "is_live"
        ) \
        .withColumn("stream_scope", lit(scope_name)) \
        .withColumn("snapshot_at", current_timestamp())

    # Zapis do MongoDB
    upsert_documents(f"top_games{collection_suffix}", to_docs(top_games), ["game_name", "stream_scope"])
    upsert_documents(f"streamer_stats{collection_suffix}", to_docs(top_streamers), ["user_name", "stream_scope"])
    upsert_documents(f"creator_stats{collection_suffix}", to_docs(creator_snapshots), ["user_name", "stream_scope"])

def process_data():
    ensure_bucket_exists()
    spark = create_spark_session()
    print("Spark Session created. Reading Twitch API events from Kafka...")

    schema = (
        StructType()
        .add("stream_id", StringType())
        .add("user_id", StringType())
        .add("user_name", StringType())
        .add("game_id", StringType())
        .add("game_name", StringType())
        .add("title", StringType())
        .add("viewer_count", IntegerType())
        .add("language", StringType())
        .add("started_at", StringType())
        .add("collected_at", StringType())
        .add("stream_scope", StringType())
        .add("rank", IntegerType())
        .add("is_live", BooleanType())
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("viewer_count", col("viewer_count").cast("int"))
        .withColumn("rank", col("rank").cast("int"))
        .withColumn("is_live", col("is_live").cast("boolean"))
        .withColumn("collected_at_ts", to_timestamp("collected_at"))
    )

    def write_raw_to_minio(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        raw_df = (
            batch_df.withColumn("ingested_at", current_timestamp())
            .withColumn("ingested_year", year("ingested_at"))
            .withColumn("ingested_month", month("ingested_at"))
            .withColumn("ingested_day", dayofmonth("ingested_at"))
            .withColumn("ingested_hour", hour("ingested_at"))
        )

        raw_path = f"s3a://{MINIO_BUCKET}/raw/twitch_api"
        raw_df.write.mode("append").partitionBy(
            "stream_scope",
            "ingested_year",
            "ingested_month",
            "ingested_day",
            "ingested_hour",
        ).parquet(raw_path)
        print(f"Uploaded Twitch API batch {epoch_id} to MinIO raw storage.")

    def process_batch(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        write_raw_to_minio(batch_df, epoch_id)

        targeted_df = batch_df.filter(col("stream_scope") == "targeted")
        top100_df = batch_df.filter(col("stream_scope") == "top100")

        process_scope("targeted", targeted_df)
        process_scope("top100", top100_df, "_top100")

        print(f"Processed Twitch API batch {epoch_id} successfully.")

    query = (
        parsed_df.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "/app/spark_checkpoints/twitch_api")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    process_data()