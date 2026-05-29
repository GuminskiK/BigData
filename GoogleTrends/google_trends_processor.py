import json
import os
import random
import time
from datetime import datetime, timezone

import schedule
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
from pytrends.request import TrendReq

load_dotenv()

TWITCH_CHANNELS = [channel.strip() for channel in os.getenv("TWITCH_CHANNELS", "xqc").split(",") if channel.strip()]
GOOGLE_TRENDS_GEO = os.getenv("GOOGLE_TRENDS_GEO", "")
GOOGLE_TRENDS_TIMEFRAME = os.getenv("GOOGLE_TRENDS_TIMEFRAME", "now 7-d")
GOOGLE_TRENDS_INTERVAL_MINUTES = int(os.getenv("GOOGLE_TRENDS_INTERVAL_MINUTES", "360"))
GOOGLE_TRENDS_CHUNK_SIZE = int(os.getenv("GOOGLE_TRENDS_CHUNK_SIZE", "3"))
GOOGLE_TRENDS_PAUSE_SECONDS = int(os.getenv("GOOGLE_TRENDS_PAUSE_SECONDS", "30"))
GOOGLE_TRENDS_MAX_RETRIES = int(os.getenv("GOOGLE_TRENDS_MAX_RETRIES", "5"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GOOGLE_TRENDS", "google_trends_stream")


def create_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready at {KAFKA_BROKER}; retrying in 5 seconds...")
            time.sleep(5)


def create_topic_if_not_exists():
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
            topics = admin_client.list_topics()
            if KAFKA_TOPIC not in topics:
                topic = NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1)
                admin_client.create_topics([topic])
                print(f"Topic '{KAFKA_TOPIC}' created.")
            return
        except NoBrokersAvailable:
            print(f"Kafka admin not ready at {KAFKA_BROKER}; retrying in 5 seconds...")
            time.sleep(5)


def chunk_keywords(keywords, chunk_size=5):
    for index in range(0, len(keywords), chunk_size):
        yield keywords[index:index + chunk_size]


def is_rate_limit_error(exc):
    message = str(exc)
    return "429" in message or "Too Many Requests" in message or "method_whitelist" in message


def fetch_chunk_with_retry(pytrends, chunk):
    last_error = None
    for attempt in range(1, GOOGLE_TRENDS_MAX_RETRIES + 1):
        try:
            pytrends.build_payload(chunk, timeframe=GOOGLE_TRENDS_TIMEFRAME, geo=GOOGLE_TRENDS_GEO)
            return pytrends.interest_over_time()
        except Exception as exc:
            last_error = exc
            if not is_rate_limit_error(exc):
                raise

            wait_seconds = min(300, (2 ** attempt) * 15) + random.randint(0, 10)
            print(
                f"Google Trends rate limited for chunk {', '.join(chunk)}; "
                f"retry {attempt}/{GOOGLE_TRENDS_MAX_RETRIES} in {wait_seconds} seconds..."
            )
            time.sleep(wait_seconds)

    raise last_error


def fetch_interest_over_time():
    if not TWITCH_CHANNELS:
        print("No Twitch channels configured in TWITCH_CHANNELS.")
        return []

    pytrends = TrendReq(hl="en-US", tz=360)
    collected_at = datetime.now(timezone.utc)
    rows = []

    for chunk in chunk_keywords(TWITCH_CHANNELS, GOOGLE_TRENDS_CHUNK_SIZE):
        print(f"Fetching Google Trends for: {', '.join(chunk)}")
        interest = fetch_chunk_with_retry(pytrends, chunk)

        if interest.empty:
            print(f"No Google Trends data returned for chunk: {', '.join(chunk)}")
            continue

        if "isPartial" in interest.columns:
            interest = interest.drop(columns=["isPartial"])

        for timestamp, record in interest.iterrows():
            for keyword in chunk:
                value = int(record.get(keyword, 0))
                rows.append({
                    "keyword": keyword,
                    "timestamp": timestamp.to_pydatetime().replace(tzinfo=timezone.utc) if hasattr(timestamp, "to_pydatetime") else timestamp,
                    "interest": value,
                    "timeframe": GOOGLE_TRENDS_TIMEFRAME,
                    "geo": GOOGLE_TRENDS_GEO,
                    "collected_at": collected_at,
                    "source": "google_trends",
                })

        time.sleep(GOOGLE_TRENDS_PAUSE_SECONDS)

    return rows


def send_rows_to_kafka(rows, producer):
    if not rows:
        print("No Google Trends rows to send.")
        return

    for row in rows:
        producer.send(KAFKA_TOPIC, value=row)

    producer.flush()
    print(f"Sent {len(rows)} Google Trends rows to Kafka topic '{KAFKA_TOPIC}'.")


def run_job():
    print(f"[{datetime.now()}] Fetching Google Trends for: {', '.join(TWITCH_CHANNELS)}")
    try:
        rows = fetch_interest_over_time()
        if rows:
            producer = create_kafka_producer()
            send_rows_to_kafka(rows, producer)
    except Exception as exc:
        print(f"Google Trends job failed: {exc}")


if __name__ == "__main__":
    create_topic_if_not_exists()
    run_job()
    schedule.every(GOOGLE_TRENDS_INTERVAL_MINUTES).minutes.do(run_job)
    print(f"Google Trends processor running. Next run in {GOOGLE_TRENDS_INTERVAL_MINUTES} minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)