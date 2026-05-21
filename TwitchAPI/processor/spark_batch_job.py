import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, count
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_DOCKER")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_ANALYTICS", os.getenv("MONGO_DB_NAME", "twitch_api_analytics"))
MINIO_WAIT_SECONDS = int(os.getenv("MINIO_WAIT_SECONDS", "10"))
BATCH_PROCESS_INTERVAL_MINUTES = int(os.getenv("BATCH_PROCESS_INTERVAL_MINUTES", "5"))

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

        df.cache()
        print(f"Total rows read: {df.count()}")

        top_games = df.groupBy("game_name") \
            .agg(sum("viewer_count").alias("total_viewers"), count("stream_id").alias("streams_count")) \
            .orderBy(col("total_viewers").desc())

        top_streamers = df.groupBy("user_name", "game_name") \
            .agg(max("viewer_count").alias("peak_viewers")) \
            .orderBy(col("peak_viewers").desc())

        print("Writing to MongoDB...")
        
        def write_to_mongo(dataframe, collection):
            dataframe.write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("spark.mongodb.write.connection.uri", MONGO_URI) \
                .option("spark.mongodb.write.database", MONGO_DB_NAME) \
                .option("spark.mongodb.write.collection", collection) \
                .save()

        write_to_mongo(top_games, "top_games")
        write_to_mongo(top_streamers, "streamer_stats")

        print("Batch processing finished successfully.")
    finally:
        spark.stop()

if __name__ == "__main__":
    while True:
        process_data()
        print(f"Waiting {BATCH_PROCESS_INTERVAL_MINUTES} minutes before next batch run...")
        time.sleep(BATCH_PROCESS_INTERVAL_MINUTES * 60)