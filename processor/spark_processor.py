import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def main():
    # Czysta inicjalizacja - parametry bierzemy z command w docker-compose
    spark = SparkSession.builder \
        .appName("TwitchChatProcessor") \
        .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
        .getOrCreate()

    # Schemat danych przychodzących z Kafka
    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("channel", StringType()) \
        .add("username", StringType()) \
        .add("message", StringType()) \
        .add("badges", StructType() \
            .add("subscriber", StringType()) \
            .add("moderator", StringType()))

    # Inicjalizacja analizatora sentymentu
    analyzer = SentimentIntensityAnalyzer()

    # UDF dla obliczania w locie wartości sentymentu wiadomości (od -1.0 negatywne do 1.0 pozytywne)
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
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "twitch-chat") \
        .load()

    # 2. Parsowanie i dodanie sentymentu
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast("timestamp"))

    processed_df = parsed_df \
        .withColumn("sentiment_score", sentiment_udf(col("message")))

    # Agregacja okienkowa (Średni sentyment na kanał co 1 minutę)
    windowed_df = processed_df \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("channel")
        ).agg(avg("sentiment_score").alias("avg_sentiment"))

    def write_to_mongo(batch_df, epoch_id):
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "twitch_db") \
            .option("collection", "channel_stats") \
            .save()

    query = windowed_df.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", "/app/spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
