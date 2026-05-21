import os
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_TWITCH_API", "twitch_api_stream")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "twitch-api-raw")

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name='us-east-1' 
)

def create_bucket_if_not_exists():
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket '{MINIO_BUCKET}' created.")

def upload_to_minio(df):
    if df.empty:
        return
        
    now = datetime.now(timezone.utc)
    s3_path = f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/streams_{int(now.timestamp())}.parquet"
    
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_path,
        Body=buf.getvalue()
    )
    print(f"Uploaded {len(df)} records to s3://{MINIO_BUCKET}/{s3_path}")

if __name__ == "__main__":
    create_bucket_if_not_exists()

    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='minio_writer_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            break
        except NoBrokersAvailable:
            print(f"Kafka not ready at {KAFKA_BROKER}; retrying in 5 seconds...")
            time.sleep(5)

    print("Consumer started. Listening to Kafka...")
    
    buffer = []
    last_flush_time = time.time()
    FLUSH_INTERVAL_SEC = 120
    BATCH_SIZE = 500 

    try:
        for message in consumer:
            buffer.append(message.value)
            
            current_time = time.time()
            if len(buffer) >= BATCH_SIZE or (current_time - last_flush_time) >= FLUSH_INTERVAL_SEC:
                df = pd.DataFrame(buffer)
                upload_to_minio(df)
                buffer = []
                last_flush_time = current_time
    except KeyboardInterrupt:
        print("Consumer stopped.")
        if buffer:
            upload_to_minio(pd.DataFrame(buffer))