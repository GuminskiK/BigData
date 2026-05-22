import os
from datetime import datetime
from typing import Any, Iterable

from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient, DESCENDING, ASCENDING


load_dotenv()

MONGO_BACKEND = os.getenv("MONGO_BACKEND", "local").lower()
MONGO_URI_LOCAL = os.getenv("MONGO_URI_LOCAL", "mongodb://mongodb:27017")
MONGO_URI_ATLAS = os.getenv("MONGO_URI", "")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
ANALYTICS_DB = os.getenv("MONGO_DB_ANALYTICS", "twitch_api_analytics")
CHAT_DB = os.getenv("MONGO_DB_CHAT", "twitch_chat")
TRENDS_DB = os.getenv("GOOGLE_TRENDS_DB_NAME", "google_trends")

def resolve_mongo_uri() -> str:
    if MONGO_BACKEND == "atlas":
        if not MONGO_URI_ATLAS:
            raise RuntimeError("MONGO_URI is missing for Mongo Atlas mode.")
        return MONGO_URI_ATLAS

    return MONGO_URI_LOCAL

client = MongoClient(resolve_mongo_uri())
db_analytics = client[ANALYTICS_DB]
db_chat = client[CHAT_DB]
db_trends = client[TRENDS_DB]

app = Flask(__name__)
CORS(app)


def as_iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def as_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def collect(cursor: Iterable[dict]) -> list[dict]:
    return list(cursor)


def top_streamers_response(collection_name: str, limit: int = 20):
    docs = collect(
        db_analytics[collection_name].find(
            {},
            {"_id": 0, "user_name": 1, "game_name": 1, "peak_viewers": 1}
        ).sort("peak_viewers", DESCENDING).limit(limit)
    )

    return jsonify([
        {
            "streamer": d.get("user_name", "unknown"),
            "game": d.get("game_name", "unknown"),
            "viewers": as_int(d.get("peak_viewers", 0)),
        }
        for d in docs
    ])


def top_games_response(collection_name: str, limit: int = 20):
    docs = collect(
        db_analytics[collection_name].find(
            {},
            {"_id": 0, "game_name": 1, "total_viewers": 1, "streams_count": 1}
        ).sort("total_viewers", DESCENDING).limit(limit)
    )

    return jsonify([
        {
            "game": d.get("game_name", "unknown"),
            "viewers": as_int(d.get("total_viewers", 0)),
            "streams": as_int(d.get("streams_count", 0)),
            "viewers_per_stream": round(
                as_int(d.get("total_viewers", 0)) / max(as_int(d.get("streams_count", 0)), 1),
                2,
            ),
        }
        for d in docs
    ])


def trends_summary_response():
    docs = collect(
        db_trends.google_trends_summary.find(
            {},
            {"_id": 0, "keyword": 1, "avg_interest": 1, "peak_interest": 1, "last_interest": 1, "collected_at": 1}
        ).sort([("peak_interest", DESCENDING), ("avg_interest", DESCENDING)])
    )

    return jsonify([
        {
            "keyword": d.get("keyword", "unknown"),
            "avg_interest": as_float(d.get("avg_interest", 0)),
            "peak_interest": as_int(d.get("peak_interest", 0)),
            "last_interest": as_int(d.get("last_interest", 0)),
            "collected_at": as_iso(d.get("collected_at")),
        }
        for d in docs
    ])


def trends_interest_response(limit: int = 500):
    docs = collect(
        db_trends.google_trends_interest_over_time.find(
            {},
            {"_id": 0, "keyword": 1, "timestamp": 1, "interest": 1, "timeframe": 1, "geo": 1, "collected_at": 1}
        ).sort("timestamp", ASCENDING).limit(limit)
    )

    return jsonify([
        {
            "keyword": d.get("keyword", "unknown"),
            "timestamp": as_iso(d.get("timestamp")),
            "interest": as_int(d.get("interest", 0)),
            "timeframe": d.get("timeframe", ""),
            "geo": d.get("geo", ""),
            "collected_at": as_iso(d.get("collected_at")),
        }
        for d in docs
    ])


@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "service": "BigData Mongo API",
        "databases": {
            ANALYTICS_DB: ["top_games", "streamer_stats", "top_games_top100", "streamer_stats_top100"],
            CHAT_DB: ["channel_stats", "raw_messages"],
            TRENDS_DB: ["google_trends_summary", "google_trends_interest_over_time"],
        },
        "endpoints": [
            "/top_streamers",
            "/top_streamers_top100",
            "/top_games",
            "/top_games_top100",
            "/sentiment_over_time",
            "/messages_per_minute",
            "/top_chatters",
            "/negative_messages",
            "/channel_summary",
            "/subscribers_vs_normal",
            "/google_trends_summary",
            "/google_trends_interest_over_time",
        ]
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/top_streamers")
def top_streamers():
    return top_streamers_response("streamer_stats")


@app.route("/top_streamers_top100")
def top_streamers_top100():
    return top_streamers_response("streamer_stats_top100")


@app.route("/top_games")
def top_games():
    return top_games_response("top_games")


@app.route("/top_games_top100")
def top_games_top100():
    return top_games_response("top_games_top100")


@app.route("/sentiment_over_time")
def sentiment_over_time():
    docs = collect(
        db_chat.channel_stats.find(
            {},
            {"_id": 0, "window.start": 1, "channel": 1, "avg_sentiment": 1}
        ).sort("window.start", ASCENDING)
    )

    result = []
    for d in docs:
        window = d.get("window", {})
        result.append({
            "time": as_iso(window.get("start")),
            "channel": d.get("channel", "unknown"),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
        })

    return jsonify(result)


@app.route("/messages_per_minute")
def messages_per_minute():
    pipeline = [
        {
            "$group": {
                "_id": {
                    "minute": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:00",
                            "date": "$timestamp"
                        }
                    },
                    "channel": "$channel"
                },
                "messages": {"$sum": 1}
            }
        },
        {"$sort": {"_id.minute": 1}}
    ]

    result = []
    for d in db_chat.raw_messages.aggregate(pipeline):
        result.append({
            "time": d["_id"]["minute"],
            "channel": d["_id"].get("channel", "unknown"),
            "messages": as_int(d.get("messages", 0)),
        })

    return jsonify(result)


@app.route("/top_chatters")
def top_chatters():
    pipeline = [
        {
            "$group": {
                "_id": "$username",
                "messages": {"$sum": 1},
                "avg_sentiment": {"$avg": "$sentiment_score"}
            }
        },
        {"$sort": {"messages": -1}},
        {"$limit": 20}
    ]

    result = []
    for d in db_chat.raw_messages.aggregate(pipeline):
        result.append({
            "username": d.get("_id", "unknown"),
            "messages": as_int(d.get("messages", 0)),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
        })

    return jsonify(result)


@app.route("/negative_messages")
def negative_messages():
    pipeline = [
        {"$match": {"sentiment_score": {"$lt": -0.3}}},
        {
            "$group": {
                "_id": {
                    "minute": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:00",
                            "date": "$timestamp"
                        }
                    },
                    "channel": "$channel"
                },
                "negative_messages": {"$sum": 1}
            }
        },
        {"$sort": {"_id.minute": 1}}
    ]

    result = []
    for d in db_chat.raw_messages.aggregate(pipeline):
        result.append({
            "time": d["_id"]["minute"],
            "channel": d["_id"].get("channel", "unknown"),
            "negative_messages": as_int(d.get("negative_messages", 0)),
        })

    return jsonify(result)


@app.route("/channel_summary")
def channel_summary():
    pipeline = [
        {
            "$group": {
                "_id": "$channel",
                "messages": {"$sum": 1},
                "unique_users": {"$addToSet": "$username"},
                "avg_sentiment": {"$avg": "$sentiment_score"},
                "min_sentiment": {"$min": "$sentiment_score"},
                "max_sentiment": {"$max": "$sentiment_score"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "channel": "$_id",
                "messages": 1,
                "unique_users": {"$size": "$unique_users"},
                "avg_sentiment": 1,
                "min_sentiment": 1,
                "max_sentiment": 1,
            }
        },
        {"$sort": {"messages": -1}}
    ]

    result = []
    for d in db_chat.raw_messages.aggregate(pipeline):
        result.append({
            "channel": d.get("channel", "unknown"),
            "messages": as_int(d.get("messages", 0)),
            "unique_users": as_int(d.get("unique_users", 0)),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
            "min_sentiment": as_float(d.get("min_sentiment", 0)),
            "max_sentiment": as_float(d.get("max_sentiment", 0)),
        })

    return jsonify(result)


@app.route("/subscribers_vs_normal")
def subscribers_vs_normal():
    pipeline = [
        {
            "$project": {
                "sentiment_score": 1,
                "user_type": {
                    "$cond": [
                        {"$ne": ["$badges.subscriber", None]},
                        "subscriber",
                        "normal_user"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$user_type",
                "messages": {"$sum": 1},
                "avg_sentiment": {"$avg": "$sentiment_score"}
            }
        }
    ]

    result = []
    for d in db_chat.raw_messages.aggregate(pipeline):
        result.append({
            "user_type": d.get("_id", "unknown"),
            "messages": as_int(d.get("messages", 0)),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
        })

    return jsonify(result)


@app.route("/google_trends_summary")
def google_trends_summary():
    return trends_summary_response()


@app.route("/google_trends_interest_over_time")
def google_trends_interest_over_time():
    return trends_interest_response()


if __name__ == "__main__":
    app.run(host=API_HOST, port=API_PORT, debug=False)