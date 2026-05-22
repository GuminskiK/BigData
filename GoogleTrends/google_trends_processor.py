import os
import time
import random
from datetime import datetime, timezone

import pandas as pd
import schedule
from dotenv import load_dotenv
from pymongo import MongoClient
from pytrends.request import TrendReq

load_dotenv()

TWITCH_CHANNELS = [channel.strip() for channel in os.getenv("TWITCH_CHANNELS", "xqc").split(",") if channel.strip()]
GOOGLE_TRENDS_GEO = os.getenv("GOOGLE_TRENDS_GEO", "")
GOOGLE_TRENDS_TIMEFRAME = os.getenv("GOOGLE_TRENDS_TIMEFRAME", "now 7-d")
GOOGLE_TRENDS_INTERVAL_MINUTES = int(os.getenv("GOOGLE_TRENDS_INTERVAL_MINUTES", "360"))
GOOGLE_TRENDS_CHUNK_SIZE = int(os.getenv("GOOGLE_TRENDS_CHUNK_SIZE", "3"))
GOOGLE_TRENDS_PAUSE_SECONDS = int(os.getenv("GOOGLE_TRENDS_PAUSE_SECONDS", "30"))
GOOGLE_TRENDS_MAX_RETRIES = int(os.getenv("GOOGLE_TRENDS_MAX_RETRIES", "5"))
GOOGLE_TRENDS_DB_NAME = os.getenv("GOOGLE_TRENDS_DB_NAME", os.getenv("MONGO_DB_TRENDS", "google_trends"))
MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")


def resolve_mongo_uri():
    if MONGO_BACKEND == "atlas":
        if not MONGO_URI_ATLAS:
            raise RuntimeError("MONGO_URI is missing for Mongo Atlas mode.")
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL


def get_mongo_db():
    client = MongoClient(resolve_mongo_uri())
    return client[GOOGLE_TRENDS_DB_NAME]


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
                })

        time.sleep(GOOGLE_TRENDS_PAUSE_SECONDS)

    return rows


def write_google_trends_to_mongo(rows):
    if not rows:
        print("No Google Trends rows to write.")
        return

    db = get_mongo_db()
    collection = db["google_trends_interest_over_time"]
    result = collection.insert_many(rows)
    print(f"Inserted {len(result.inserted_ids)} Google Trends records into MongoDB.")


def summarize_rows(rows):
    if not rows:
        return []

    df = pd.DataFrame(rows)
    summary = df.groupby("keyword", as_index=False).agg(
        avg_interest=("interest", "mean"),
        peak_interest=("interest", "max"),
        last_interest=("interest", "last"),
    )
    summary["avg_interest"] = summary["avg_interest"].astype(float)
    summary["peak_interest"] = summary["peak_interest"].astype(int)
    summary["last_interest"] = summary["last_interest"].astype(int)
    summary["collected_at"] = datetime.now(timezone.utc)
    summary["timeframe"] = GOOGLE_TRENDS_TIMEFRAME
    summary["geo"] = GOOGLE_TRENDS_GEO
    return summary.to_dict(orient="records")


def write_summary_to_mongo(rows):
    summary_rows = summarize_rows(rows)
    if not summary_rows:
        print("No Google Trends summary rows to write.")
        return

    db = get_mongo_db()
    collection = db["google_trends_summary"]
    result = collection.insert_many(summary_rows)
    print(f"Inserted {len(result.inserted_ids)} Google Trends summary records into MongoDB.")


def run_job():
    print(f"[{datetime.now()}] Fetching Google Trends for: {', '.join(TWITCH_CHANNELS)}")
    try:
        rows = fetch_interest_over_time()
        write_google_trends_to_mongo(rows)
        write_summary_to_mongo(rows)
    except Exception as exc:
        print(f"Google Trends job failed: {exc}")


if __name__ == "__main__":
    run_job()
    schedule.every(GOOGLE_TRENDS_INTERVAL_MINUTES).minutes.do(run_job)
    print(f"Google Trends processor running. Next run in {GOOGLE_TRENDS_INTERVAL_MINUTES} minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)