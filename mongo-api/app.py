import os
import re
from datetime import datetime
from typing import Any, Iterable
from collections import defaultdict

from dotenv import load_dotenv
from flask import Flask, jsonify, request
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
TRENDS_DB = os.getenv("MONGO_DB_TRENDS", "google_trends")


def is_valid_mongo_uri(uri: str) -> bool:
    if not uri:
        return False
    lowered = uri.lower()
    return not any(token in lowered for token in ("<username>", "<password>", "xxxxx", "placeholder"))

def resolve_mongo_uri() -> str:
    if MONGO_BACKEND == "atlas" and is_valid_mongo_uri(MONGO_URI_ATLAS):
        print("Using MongoDB Atlas backend")
        return MONGO_URI_ATLAS

    print(f"Using local MongoDB backend (Atlas URI valid: {is_valid_mongo_uri(MONGO_URI_ATLAS)})")
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


def dedupe_by_key(docs: list[dict], key_fn):
    seen = set()
    result = []
    for doc in docs:
        key = key_fn(doc)
        if key in seen:
            continue
        seen.add(key)
        result.append(doc)
    return result


def scope_for_collection(collection_name: str) -> str:
    return "top100" if collection_name.endswith("_top100") else "targeted"


def window_collection_name(window_minutes: int) -> str:
    return {
        1: "chat_stats_1m",
        5: "channel_stats",
        60: "chat_stats_1h",
    }.get(window_minutes, "channel_stats")


def latest_snapshot_time(collection_name: str, scope: str) -> datetime | None:
    doc = db_analytics[collection_name].find_one(
        {"stream_scope": scope, "snapshot_at": {"$exists": True}},
        {"_id": 0, "snapshot_at": 1},
        sort=[("snapshot_at", DESCENDING)]
    )
    if not doc:
        return None
    return doc.get("snapshot_at")


def latest_snapshot_docs(collection_name: str, label_field: str, value_field: str, scope: str, limit: int = 5) -> list[dict]:
    latest_snapshot = latest_snapshot_time(collection_name, scope)
    if latest_snapshot is None:
        return []

    return collect(
        db_analytics[collection_name].find(
            {"stream_scope": scope, "snapshot_at": latest_snapshot},
            {"_id": 0, label_field: 1, value_field: 1, "snapshot_at": 1, "stream_scope": 1}
        ).sort(value_field, DESCENDING).limit(limit)
    )


def latest_labels(collection_name: str, label_field: str, value_field: str, scope: str, limit: int = 5) -> list[str]:
    docs = latest_snapshot_docs(collection_name, label_field, value_field, scope, limit)
    return [str(d.get(label_field, "unknown")) for d in docs if d.get(label_field) is not None]


def history_timeseries_response(
    current_collection: str,
    label_field: str,
    value_field: str,
    scope: str,
    limit: int = 5,
):
    labels = latest_labels(current_collection, label_field, value_field, scope, limit)
    if not labels:
        return jsonify([])

    docs = collect(
        db_analytics[current_collection].find(
            {"stream_scope": scope, label_field: {"$in": labels}},
            {"_id": 0, label_field: 1, value_field: 1, "snapshot_at": 1, "stream_scope": 1}
        ).sort([("snapshot_at", ASCENDING), (label_field, ASCENDING)])
    )

    result = []
    for d in docs:
        result.append({
            "time": as_iso(d.get("snapshot_at")),
            "series": d.get(label_field, "unknown"),
            "value": as_int(d.get(value_field, 0)),
            label_field: d.get(label_field, "unknown"),
            value_field: as_int(d.get(value_field, 0)),
            "scope": d.get("stream_scope", scope),
        })

    return jsonify(result)


def messages_per_window_response(window_minutes: int = 60, negative_only: bool = False):
    collection_name = window_collection_name(window_minutes)
    field_name = "negative_message_count" if negative_only else "message_count"
    docs = collect(db_chat[collection_name].find(
        {},
        {
            "_id": 0,
            "window": 1,
            "channel": 1,
            "message_count": 1,
            "negative_message_count": 1,
            "snapshot_at": 1,
        },
    ).sort([("window.start", ASCENDING), ("channel", ASCENDING), ("snapshot_at", DESCENDING)]))
    docs = dedupe_by_key(docs, lambda d: (d.get("window", {}).get("start"), d.get("channel", "unknown")))

    result = []
    for d in docs:
        window_doc = d.get("window", {})
        result.append({
            "time": as_iso(window_doc.get("start")),
            "series": d.get("channel", "unknown"),
            "value": as_int(d.get(field_name, 0)),
            "channel": d.get("channel", "unknown"),
            field_name: as_int(d.get(field_name, 0)),
        })

    return jsonify(result)


def google_trends_comparison_response():
    docs = collect(
        db_trends.google_trends_interest_over_time.find(
            {},
            {"_id": 0, "keyword": 1, "timestamp": 1, "interest": 1}
        ).sort([("keyword", ASCENDING), ("timestamp", ASCENDING)])
    )

    grouped: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        keyword = d.get("keyword", "unknown")
        grouped[keyword].append(d)

    result = []
    for keyword, series in grouped.items():
        first_interest = as_int(series[0].get("interest", 0)) if series else 0
        last_interest = as_int(series[-1].get("interest", 0)) if series else 0
        peak_interest = max((as_int(item.get("interest", 0)) for item in series), default=0)
        avg_interest = round(sum(as_float(item.get("interest", 0)) for item in series) / max(len(series), 1), 2)
        change = last_interest - first_interest
        growth_pct = round((change / max(first_interest, 1)) * 100, 2) if series else 0.0

        result.append({
            "keyword": keyword,
            "avg_interest": avg_interest,
            "peak_interest": peak_interest,
            "first_interest": first_interest,
            "last_interest": last_interest,
            "interest_change": change,
            "interest_growth_pct": growth_pct,
        })

    return jsonify(sorted(result, key=lambda item: item["avg_interest"], reverse=True))


def top_streamers_response(collection_name: str, limit: int = 20):
    scope = scope_for_collection(collection_name)
    latest_snapshot = latest_snapshot_time(collection_name, scope)
    if latest_snapshot is None:
        return jsonify([])
    docs = collect(
        db_analytics[collection_name].find(
            {"stream_scope": scope, "snapshot_at": latest_snapshot},
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
    scope = scope_for_collection(collection_name)
    latest_snapshot = latest_snapshot_time(collection_name, scope)
    if latest_snapshot is None:
        return jsonify([])
    docs = collect(
        db_analytics[collection_name].find(
            {"stream_scope": scope, "snapshot_at": latest_snapshot},
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
    latest_collected_at = db_trends.google_trends_summary.find_one(
        {},
        {"_id": 0, "collected_at": 1},
        sort=[("collected_at", DESCENDING)]
    )
    if not latest_collected_at:
        return jsonify([])

    docs = collect(
        db_trends.google_trends_summary.find(
            {"collected_at": latest_collected_at.get("collected_at")},
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
        ).sort([("timestamp", ASCENDING), ("keyword", ASCENDING)])
    )

    return jsonify([
        {
            "keyword": d.get("keyword", "unknown"),
            "timestamp": as_iso(d.get("timestamp")),
            "interest": as_int(d.get("interest", 0)),
            "series": d.get("keyword", "unknown"),
            "value": as_int(d.get("interest", 0)),
            "timeframe": d.get("timeframe", ""),
            "geo": d.get("geo", ""),
            "collected_at": as_iso(d.get("collected_at")),
        }
        for d in docs
    ])


def creator_regex(value: str) -> dict:
    return {"$regex": f"^{re.escape(value.strip())}$", "$options": "i"}


def latest_creator_snapshot(creator: str, scope: str = "targeted") -> dict | None:
    return db_analytics.creator_stats.find_one(
        {"user_name": creator_regex(creator), "stream_scope": scope},
        sort=[("snapshot_at", DESCENDING)]
    )


def latest_channel_stats(channel: str) -> dict | None:
    return db_chat.channel_stats.find_one(
        {"channel": creator_regex(channel)},
        sort=[("window.start", DESCENDING)]
    )


def creator_names() -> list[str]:
    names = db_analytics.creator_stats.distinct("user_name", {"stream_scope": "targeted"})
    if not names:
        names = db_analytics.streamer_stats.distinct("user_name", {"stream_scope": "targeted"})
    return sorted({str(name) for name in names if name}, key=lambda item: item.lower())


def creator_summary_response(creator: str):
    snapshot = latest_creator_snapshot(creator)
    chat_stats = latest_channel_stats(creator)
    
    # Pobieramy rekordowy Peak Viewers z całej historii dla tego twórcy
    peak_doc = db_analytics.creator_stats.find_one(
        {"user_name": creator_regex(creator), "stream_scope": "targeted"},
        {"peak_viewers": 1},
        sort=[("peak_viewers", DESCENDING)]
    )

    trends_docs = collect(
        db_trends.google_trends_interest_over_time.find(
            {"keyword": creator_regex(creator)},
            {"_id": 0, "keyword": 1, "timestamp": 1, "interest": 1}
        ).sort([("timestamp", DESCENDING)]).limit(1)
    )
    latest_trend = trends_docs[0] if trends_docs else None

    return jsonify([
        {
            "creator": creator,
            "current_viewers": as_int(snapshot.get("current_viewers", 0)) if snapshot else 0,
            "peak_viewers": as_int(peak_doc.get("peak_viewers", 0)) if peak_doc else 0,
            "current_game": snapshot.get("game_name", "offline") if snapshot else "offline",
            "current_title": snapshot.get("title", "OFFLINE") if snapshot else "OFFLINE",
            "current_started_at": as_iso(snapshot.get("started_at")) if snapshot and snapshot.get("started_at") else None,
            "message_count_5m": as_int(chat_stats.get("message_count", 0)) if chat_stats else 0,
            "unique_chatters_5m": as_int(chat_stats.get("unique_chatters", 0)) if chat_stats else 0,
            "avg_sentiment_5m": as_float(chat_stats.get("avg_sentiment", 0)) if chat_stats else 0.0,
            "google_interest_latest": as_int(latest_trend.get("interest", 0)) if latest_trend else 0,
        }
    ])


def creator_viewers_history_response(creator: str):
    docs = collect(
        db_analytics.creator_stats.find(
            {"user_name": creator_regex(creator), "stream_scope": "targeted"},
            {"_id": 0, "user_name": 1, "game_name": 1, "current_viewers": 1, "peak_viewers": 1, "snapshot_at": 1}
        ).sort([("snapshot_at", ASCENDING)])
    )

    return jsonify([
        {
            "time": as_iso(d.get("snapshot_at")),
            "series": d.get("user_name", creator),
            "value": as_int(d.get("current_viewers", 0)),
            "game": d.get("game_name", "unknown"),
            "peak_viewers": as_int(d.get("peak_viewers", 0)),
        }
        for d in docs
    ])


def creator_recent_streams_response(creator: str, limit: int = 10):
    docs = collect(
        db_analytics.creator_stats.find(
            {"user_name": creator_regex(creator), "stream_scope": "targeted"},
            {"_id": 0, "user_name": 1, "game_name": 1, "title": 1, "started_at": 1, "current_viewers": 1, "peak_viewers": 1, "snapshot_at": 1}
        ).sort([("snapshot_at", DESCENDING)]).limit(limit)
    )

    return jsonify([
        {
            "streamer": d.get("user_name", creator),
            "game": d.get("game_name", "unknown"),
            "title": d.get("title", ""),
            "current_viewers": as_int(d.get("current_viewers", 0)),
            "peak_viewers": as_int(d.get("peak_viewers", 0)),
            "started_at": as_iso(d.get("started_at")) if d.get("started_at") else None,
            "snapshot_at": as_iso(d.get("snapshot_at")),
        }
        for d in docs
    ])


def creator_games_response(creator: str):
    docs = collect(
        db_analytics.creator_stats.find(
            {"user_name": creator_regex(creator), "stream_scope": "targeted"},
            {"_id": 0, "game_name": 1, "snapshot_at": 1}
        ).sort([("snapshot_at", DESCENDING)])
    )

    seen_games: set[str] = set()
    result = []
    for d in docs:
        game_name = d.get("game_name", "unknown")
        if game_name in seen_games:
            continue
        seen_games.add(game_name)
        result.append({
            "game": game_name,
            "last_seen": as_iso(d.get("snapshot_at")),
        })

    return jsonify(result)


def creator_chat_history_response(creator: str):
    docs = collect(
        db_chat.channel_stats.find(
            {"channel": creator_regex(creator)},
            {"_id": 0, "window.start": 1, "channel": 1, "avg_sentiment": 1, "message_count": 1, "unique_chatters": 1, "snapshot_at": 1}
        ).sort([("window.start", ASCENDING), ("snapshot_at", DESCENDING)])
    )
    docs = dedupe_by_key(docs, lambda d: (d.get("window", {}).get("start"), d.get("channel", creator)))

    return jsonify([
        {
            "time": as_iso(d.get("window", {}).get("start")),
            "series": d.get("channel", creator),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
            "message_count": as_int(d.get("message_count", 0)),
            "unique_chatters": as_int(d.get("unique_chatters", 0)),
        }
        for d in docs
    ])


def creator_top_chatters_response(creator: str, limit: int = 20):
    pipeline = [
        {"$match": {"channel": creator_regex(creator)}},
        {
            "$group": {
                "_id": "$username",
                "messages": {"$sum": "$messages"},
                "sentiment_sum": {"$sum": "$sentiment_sum"},
                "sentiment_count": {"$sum": "$sentiment_count"},
            }
        },
        {"$sort": {"messages": -1}},
        {"$limit": limit},
    ]
    docs = collect(db_chat.chat_user_totals.aggregate(pipeline))
    return jsonify([
        {
            "username": d.get("_id", "unknown"),
            "messages": as_int(d.get("messages", 0)),
            "avg_sentiment": round(
                as_float(d.get("sentiment_sum", 0)) / max(as_int(d.get("sentiment_count", 0)), 1),
                4,
            ),
        }
        for d in docs
    ])


def creator_trends_history_response(creator: str):
    docs = collect(
        db_trends.google_trends_interest_over_time.find(
            {"keyword": creator_regex(creator)},
            {"_id": 0, "keyword": 1, "timestamp": 1, "interest": 1, "collected_at": 1}
        ).sort([("timestamp", ASCENDING)])
    )

    return jsonify([
        {
            "time": as_iso(d.get("timestamp")),
            "series": d.get("keyword", creator),
            "value": as_int(d.get("interest", 0)),
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
            ANALYTICS_DB: ["top_games", "streamer_stats", "creator_stats", "top_games_top100", "streamer_stats_top100"],
            CHAT_DB: ["channel_stats", "chat_stats_1m", "chat_stats_1h", "chat_user_totals"],
            TRENDS_DB: ["google_trends_summary", "google_trends_interest_over_time"],
        },
        "endpoints": [
            "/top_streamers",
            "/top_streamers_top100",
            "/top_streamers_history",
            "/top_games",
            "/top_games_top100",
            "/top_games_history",
            "/sentiment_over_time",
            "/messages_per_minute",
            "/messages_per_hour",
            "/negative_messages_per_hour",
            "/top_chatters",
            "/negative_messages",
            "/channel_summary",
            "/subscribers_vs_normal",
            "/creators",
            "/creator_summary",
            "/creator_viewers_history",
            "/creator_recent_streams",
            "/creator_games",
            "/creator_chat_history",
            "/creator_top_chatters",
            "/creator_trends_history",
            "/google_trends_summary",
            "/google_trends_comparison",
            "/google_trends_interest_over_time",
        ]
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/creators")
def creators():
    return jsonify(creator_names())


@app.route("/creator_summary")
def creator_summary():
    creator = request.args.get("creator", "")
    return creator_summary_response(creator)


@app.route("/creator_viewers_history")
def creator_viewers_history():
    creator = request.args.get("creator", "")
    return creator_viewers_history_response(creator)


@app.route("/creator_recent_streams")
def creator_recent_streams():
    creator = request.args.get("creator", "")
    limit = int(request.args.get("limit", "10"))
    return creator_recent_streams_response(creator, limit=limit)


@app.route("/creator_games")
def creator_games():
    creator = request.args.get("creator", "")
    return creator_games_response(creator)


@app.route("/creator_chat_history")
def creator_chat_history():
    creator = request.args.get("creator", "")
    return creator_chat_history_response(creator)


@app.route("/creator_top_chatters")
def creator_top_chatters():
    creator = request.args.get("creator", "")
    limit = int(request.args.get("limit", "20"))
    return creator_top_chatters_response(creator, limit=limit)


@app.route("/creator_trends_history")
def creator_trends_history():
    creator = request.args.get("creator", "")
    return creator_trends_history_response(creator)


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


@app.route("/top_games_history")
def top_games_history():
    return history_timeseries_response("top_games", "game_name", "total_viewers", "targeted")


@app.route("/top_games_history_top100")
def top_games_history_top100():
    return history_timeseries_response("top_games_top100", "game_name", "total_viewers", "top100")


@app.route("/top_streamers_history")
def top_streamers_history():
    return history_timeseries_response("streamer_stats", "user_name", "peak_viewers", "targeted")


@app.route("/top_streamers_history_top100")
def top_streamers_history_top100():
    return history_timeseries_response("streamer_stats_top100", "user_name", "peak_viewers", "top100")


@app.route("/sentiment_over_time")
def sentiment_over_time():
    docs = collect(
        db_chat.channel_stats.find(
            {},
            {"_id": 0, "window.start": 1, "channel": 1, "avg_sentiment": 1, "snapshot_at": 1}
        ).sort([("window.start", ASCENDING), ("channel", ASCENDING), ("snapshot_at", DESCENDING)])
    )
    docs = dedupe_by_key(docs, lambda d: (d.get("window", {}).get("start"), d.get("channel", "unknown")))

    result = []
    for d in docs:
        window = d.get("window", {})
        result.append({
            "time": as_iso(window.get("start")),
            "channel": d.get("channel", "unknown"),
            "avg_sentiment": as_float(d.get("avg_sentiment", 0)),
            "series": d.get("channel", "unknown"),
            "value": as_float(d.get("avg_sentiment", 0)),
        })

    return jsonify(result)


@app.route("/messages_per_minute")
def messages_per_minute():
    return messages_per_window_response(window_minutes=1)


@app.route("/messages_per_hour")
def messages_per_hour():
    docs = collect(db_chat.chat_stats_1h.find(
        {},
        {"_id": 0, "window": 1, "channel": 1, "message_count": 1, "snapshot_at": 1},
    ).sort([("window.start", ASCENDING), ("channel", ASCENDING), ("snapshot_at", DESCENDING)]))
    docs = dedupe_by_key(docs, lambda d: (d.get("window", {}).get("start"), d.get("channel", "unknown")))

    result = []
    for d in docs:
        window_doc = d.get("window", {})
        result.append({
            "time": as_iso(window_doc.get("start")),
            "series": d.get("channel", "unknown"),
            "value": as_int(d.get("message_count", 0)),
            "channel": d.get("channel", "unknown"),
            "messages": as_int(d.get("message_count", 0)),
        })

    return jsonify(result)


@app.route("/top_chatters")
def top_chatters():
    pipeline = [
        {
            "$group": {
                "_id": "$username",
                "messages": {"$sum": "$messages"},
                "sentiment_sum": {"$sum": "$sentiment_sum"},
                "sentiment_count": {"$sum": "$sentiment_count"},
            }
        },
        {"$sort": {"messages": -1}},
        {"$limit": 20}
    ]

    result = []
    for d in db_chat.chat_user_totals.aggregate(pipeline):
        sentiment_count = as_int(d.get("sentiment_count", 0))
        result.append({
            "username": d.get("_id", "unknown"),
            "messages": as_int(d.get("messages", 0)),
            "avg_sentiment": round(as_float(d.get("sentiment_sum", 0)) / max(sentiment_count, 1), 4),
        })

    return jsonify(result)


@app.route("/unique_chat_users")
def unique_chat_users():
    pipeline = [
        {"$group": {"_id": "$username"}},
        {"$count": "unique_users"},
    ]

    result = list(db_chat.chat_user_totals.aggregate(pipeline))
    if not result:
        return jsonify([{"unique_users": 0}])

    return jsonify(result)


@app.route("/negative_messages")
def negative_messages():
    return messages_per_window_response(window_minutes=1, negative_only=True)


@app.route("/negative_messages_per_hour")
def negative_messages_per_hour():
    docs = collect(db_chat.chat_stats_1h.find(
        {},
        {"_id": 0, "window": 1, "channel": 1, "negative_message_count": 1, "snapshot_at": 1},
    ).sort([("window.start", ASCENDING), ("channel", ASCENDING), ("snapshot_at", DESCENDING)]))
    docs = dedupe_by_key(docs, lambda d: (d.get("window", {}).get("start"), d.get("channel", "unknown")))

    result = []
    for d in docs:
        window_doc = d.get("window", {})
        result.append({
            "time": as_iso(window_doc.get("start")),
            "series": d.get("channel", "unknown"),
            "value": as_int(d.get("negative_message_count", 0)),
            "channel": d.get("channel", "unknown"),
            "negative_messages": as_int(d.get("negative_message_count", 0)),
        })

    return jsonify(result)


@app.route("/channel_summary")
def channel_summary():
    channel = request.args.get("channel", "").strip()
    pipeline = [
        *([{"$match": {"channel": creator_regex(channel)}}] if channel else []),
        {
            "$group": {
                "_id": "$channel",
                "messages": {"$sum": "$messages"},
                "unique_users": {"$sum": 1},
                "sentiment_sum": {"$sum": "$sentiment_sum"},
                "sentiment_count": {"$sum": "$sentiment_count"},
                "min_sentiment": {"$min": "$min_sentiment"},
                "max_sentiment": {"$max": "$max_sentiment"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "channel": "$_id",
                "messages": 1,
                "unique_users": 1,
                "avg_sentiment": {
                    "$cond": [
                        {"$gt": ["$sentiment_count", 0]},
                        {"$divide": ["$sentiment_sum", "$sentiment_count"]},
                        0,
                    ]
                },
                "min_sentiment": 1,
                "max_sentiment": 1,
            }
        },
        {"$sort": {"messages": -1}}
    ]

    result = []
    for d in db_chat.chat_user_totals.aggregate(pipeline):
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
    channel = request.args.get("channel", "").strip()
    pipeline = [
        *([{"$match": {"channel": creator_regex(channel)}}] if channel else []),
        {
            "$group": {
                "_id": None,
                "subscriber_messages": {"$sum": "$subscriber_messages"},
                "normal_messages": {"$sum": "$normal_messages"},
                "subscriber_sentiment_sum": {"$sum": "$subscriber_sentiment_sum"},
                "subscriber_sentiment_count": {"$sum": "$subscriber_sentiment_count"},
                "normal_sentiment_sum": {"$sum": "$normal_sentiment_sum"},
                "normal_sentiment_count": {"$sum": "$normal_sentiment_count"},
            }
        }
    ]

    docs = collect(db_chat.chat_user_totals.aggregate(pipeline))
    if not docs:
        return jsonify([])

    d = docs[0]
    subscriber_count = as_int(d.get("subscriber_sentiment_count", 0))
    normal_count = as_int(d.get("normal_sentiment_count", 0))

    return jsonify([
        {
            "user_type": "subscriber",
            "messages": as_int(d.get("subscriber_messages", 0)),
            "avg_sentiment": round(as_float(d.get("subscriber_sentiment_sum", 0)) / max(subscriber_count, 1), 4),
        },
        {
            "user_type": "normal_user",
            "messages": as_int(d.get("normal_messages", 0)),
            "avg_sentiment": round(as_float(d.get("normal_sentiment_sum", 0)) / max(normal_count, 1), 4),
        },
    ])


@app.route("/google_trends_summary")
def google_trends_summary():
    return trends_summary_response()


@app.route("/google_trends_comparison")
def google_trends_comparison():
    return google_trends_comparison_response()


@app.route("/google_trends_interest_over_time")
def google_trends_interest_over_time():
    return trends_interest_response()


if __name__ == "__main__":
    app.run(host=API_HOST, port=API_PORT, debug=False)