import os
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, avg, max, count, lit, current_timestamp, row_number
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_ANALYTICS", os.getenv("MONGO_DB_NAME", "twitch_api_analytics"))
MINIO_WAIT_SECONDS = int(os.getenv("MINIO_WAIT_SECONDS", "10"))
BATCH_PROCESS_INTERVAL_MINUTES = int(os.getenv("BATCH_PROCESS_INTERVAL_MINUTES", "5"))


def resolve_mongo_uri():
    if MONGO_BACKEND == "atlas":
        if not MONGO_URI_ATLAS:
            raise RuntimeError("MONGO_URI is missing for Mongo Atlas mode.")
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL

def create_spark_session():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    ]
    
    spark = SparkSession.builder \
        .appName("TwitchBatchAnalytics") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_minio_input(spark):
    path = f"s3a://{MINIO_BUCKET}/raw/*/*/*/*/*.parquet"
    while True:
        try:
            return spark.read.parquet(path)
        except Exception as e:
            print(f"No data found in MinIO yet or connection error: {e}")
            print(f"Waiting {MINIO_WAIT_SECONDS} seconds before retrying...")
            time.sleep(MINIO_WAIT_SECONDS)

def process_data():
    spark = create_spark_session()
    print("Spark Session created. Reading from MinIO...")

    try:
        df = read_minio_input(spark)
        if "stream_scope" not in df.columns:
            df = df.withColumn("stream_scope", lit("targeted"))
        else:
            df = df.fillna({"stream_scope": "targeted"})

        df.cache()
        print(f"Total rows read: {df.count()}")

        def write_to_mongo(dataframe, collection, mode="append"):
            dataframe.write \
                .format("mongodb") \
                .mode(mode) \
                .option("spark.mongodb.write.connection.uri", resolve_mongo_uri()) \
                .option("spark.mongodb.write.database", MONGO_DB_NAME) \
                .option("spark.mongodb.write.collection", collection) \
                .save()

        def process_scope(scope_name, scope_df, collection_suffix=""):
            if scope_df.rdd.isEmpty():
                print(f"No rows found for scope '{scope_name}'. Skipping writes.")
                return

            latest_window = Window.partitionBy("user_name").orderBy(
                col("collected_at").desc(),
                col("viewer_count").desc(),
            )

            latest_streams = scope_df.withColumn("rn", row_number().over(latest_window)) \
                .filter(col("rn") == 1) \
                .drop("rn")

            peak_by_streamer = scope_df.groupBy("user_name") \
                .agg(max("viewer_count").alias("peak_viewers"))

            creator_snapshots = latest_streams.join(peak_by_streamer, on="user_name", how="inner") \
                .select(
                    "user_name",
                    "game_name",
                    col("viewer_count").alias("current_viewers"),
                    "peak_viewers",
                    "title",
                    "started_at",
                    "collected_at",
                    "is_live",
                ) \
                .withColumn("stream_scope", lit(scope_name)) \
                .withColumn("snapshot_at", current_timestamp())

            top_games = scope_df.groupBy("game_name") \
                .agg(sum("viewer_count").alias("total_viewers"), count("stream_id").alias("streams_count")) \
                .orderBy(col("total_viewers").desc()) \
                .withColumn("stream_scope", lit(scope_name)) \
                .withColumn("snapshot_at", current_timestamp())

            top_streamers = scope_df.groupBy("user_name", "game_name") \
                .agg(max("viewer_count").alias("peak_viewers")) \
                .orderBy(col("peak_viewers").desc()) \
                .withColumn("stream_scope", lit(scope_name)) \
                .withColumn("snapshot_at", current_timestamp())

            print(f"Writing {scope_name} analytics to MongoDB...")
            write_to_mongo(top_games, f"top_games{collection_suffix}")
            write_to_mongo(top_streamers, f"streamer_stats{collection_suffix}")
            write_to_mongo(creator_snapshots, f"creator_stats{collection_suffix}")

        targeted_df = df.filter(col("stream_scope") == "targeted")
        top100_df = df.filter(col("stream_scope") == "top100")

        process_scope("targeted", targeted_df)
        process_scope("top100", top100_df, "_top100")

        print("Batch processing finished successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    while True:
        process_data()
        print(f"Waiting {BATCH_PROCESS_INTERVAL_MINUTES} minutes before next batch run...")
        time.sleep(BATCH_PROCESS_INTERVAL_MINUTES * 60)