import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

KAFKA_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092")
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
MONGODB_DB = os.getenv("MONGODB_DB_CHAT", "twitch_chat")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TWITCH_CHAT", "twitch_chat_stream")
WINDOW_MINUTES = int(os.getenv("SENTIMENT_WINDOW_MINUTES", "1"))
CHECKPOINT_VERSION = os.getenv("SPARK_CHECKPOINT_VERSION", "v1")


def resolve_mongo_uri():
    if MONGO_BACKEND == "atlas":
        if not MONGO_URI_ATLAS:
            raise RuntimeError("MONGO_URI is missing for Mongo Atlas mode.")
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL

def main():
    spark = SparkSession.builder \
        .appName("TwitchChatProcessor") \
        .config("spark.mongodb.write.connection.uri", resolve_mongo_uri()) \
        .getOrCreate()

    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("channel", StringType()) \
        .add("username", StringType()) \
        .add("message", StringType()) \
        .add("badges", StructType() \
            .add("subscriber", StringType()) \
            .add("moderator", StringType()))

    analyzer = SentimentIntensityAnalyzer()

    def analyze_sentiment(text):
        if text:
            try:
                return analyzer.polarity_scores(text)['compound']
            except Exception:
                return 0.0
        return 0.0

    sentiment_udf = udf(analyze_sentiment, DoubleType())

    # 1. Odczyt z Kafki
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # 2. Parsowanie i dodanie sentymentu
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast("timestamp"))

    processed_df = parsed_df \
        .withColumn("sentiment_score", sentiment_udf(col("message")))

    # 3. Zapis surowych wiadomości do MongoDB (kolekcja raw_messages)
    def write_raw_to_mongo(batch_df, epoch_id):
        if batch_df.count() > 0:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("database", MONGODB_DB) \
                .option("collection", "raw_messages") \
                .save()

    raw_query = processed_df.writeStream \
        .foreachBatch(write_raw_to_mongo) \
        .option("checkpointLocation", f"/app/spark_checkpoints/{CHECKPOINT_VERSION}/raw") \
        .start()

    # 4. Agregacja okienkowa (średni sentyment na kanał)
    windowed_df = processed_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), f"{WINDOW_MINUTES} minute"),
            col("channel")
        ).agg(avg("sentiment_score").alias("avg_sentiment"))

    def write_stats_to_mongo(batch_df, epoch_id):
        if batch_df.count() > 0:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("database", MONGODB_DB) \
                .option("collection", "channel_stats") \
                .save()

    stats_query = windowed_df.writeStream \
        .foreachBatch(write_stats_to_mongo) \
        .option("checkpointLocation", f"/app/spark_checkpoints/{CHECKPOINT_VERSION}/stats") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
