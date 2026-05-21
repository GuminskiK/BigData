import os
import json
import time
import requests
import schedule
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_TWITCH_API", "twitch_api_stream")
TWITCH_CHANNELS = [channel.strip() for channel in os.getenv("TWITCH_CHANNELS", "xqc").split(",") if channel.strip()]

def create_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready at {KAFKA_BROKER}; retrying in 5 seconds...")
            time.sleep(5)


producer = create_kafka_producer()

def create_topic_if_not_exists():
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
            topics = admin_client.list_topics()
            if TOPIC_NAME not in topics:
                topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
                admin_client.create_topics([topic])
                print(f"Topic '{TOPIC_NAME}' created.")
            return
        except NoBrokersAvailable:
            print(f"Kafka admin not ready at {KAFKA_BROKER}; retrying in 5 seconds...")
            time.sleep(5)

def get_twitch_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_and_send_streams():
    print(f"[{datetime.now()}] Fetching Twitch streams for: {', '.join(TWITCH_CHANNELS)}")
    try:
        token = get_twitch_token()
        headers = {
            "Client-ID": CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
        if not TWITCH_CHANNELS:
            print("No Twitch channels configured in TWITCH_CHANNELS.")
            return

        # Twitch Helix streams endpoint supports filtering by user_login.
        params = [("user_login", channel) for channel in TWITCH_CHANNELS]
        response = requests.get("https://api.twitch.tv/helix/streams", headers=headers, params=params)
        response.raise_for_status()
        streams = response.json()["data"]

        if not streams:
            print("No live streams found for the configured Twitch channels.")
            return

        collected_at = datetime.now(timezone.utc).isoformat()

        for stream in streams:
            data = {
                "stream_id": stream["id"],
                "user_id": stream["user_id"],
                "user_name": stream["user_name"],
                "game_id": stream["game_id"],
                "game_name": stream["game_name"],
                "title": stream["title"],
                "viewer_count": stream["viewer_count"],
                "language": stream["language"],
                "started_at": stream["started_at"],
                "collected_at": collected_at
            }
            producer.send(TOPIC_NAME, value=data)
        
        producer.flush()
        print(f"[{datetime.now()}] Successfully sent {len(streams)} streams to Kafka.")
        
    except Exception as e:
        print(f"Error fetching/sending data: {e}")

if __name__ == "__main__":
    create_topic_if_not_exists()
    
    fetch_and_send_streams()
    
    # Scheduling (5 minuts)
    schedule.every(5).minutes.do(fetch_and_send_streams)
    print("Producer is running. Waiting for scheduled jobs...")
    
    while True:
        schedule.run_pending()
        time.sleep(1)