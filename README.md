# BigData

Local data platform for Twitch streams, Twitch chat, and Google Trends.

## What runs locally

- Kafka for event transport.
- MongoDB for analytics storage.
- MinIO for RAW parquet snapshots from Twitch API, Twitch Chat, and Google Trends.
- TwitchChat pipeline: IRC producer -> Kafka -> Spark streaming -> MinIO raw + MongoDB.
- TwitchAPI pipeline: Twitch API producer -> Kafka -> Spark streaming -> MinIO raw + MongoDB.
- Google Trends pipeline: Google Trends -> Kafka -> Spark batch -> MinIO raw + MongoDB.
- Mongo API: lightweight Flask API used by Grafana Infinity.
- Grafana: optional dashboard layer.

## Start the core stack

```bash
docker compose up -d --build
```

This starts everything except Grafana.

## Start Grafana too

```bash
docker compose --profile viz up -d --build
```

Grafana is exposed at:

```text
http://localhost:3000
```

Default login:

```text
admin / admin
```

Grafana starts with one dashboard called `BigData Overview`. It is defined in `grafana/dashboards/bigdata-visuals.json` and is loaded automatically by Grafana provisioning.

The dashboard is chart-focused by default and assumes append-only Mongo data. Google Trends uses bar and time-series panels, Twitch stream analytics use pie charts and trend lines, and chat analytics use time-series and summary charts instead of a table-first layout. The latest visible snapshot is selected from the newest `snapshot_at`, `timestamp`, or `collected_at` in the same collections that store the full history.

## How Grafana fits together

Grafana does not read MongoDB directly in this setup. It queries the local Flask API through the Infinity datasource, and that API reads the Mongo collections.

The chain is:

`MongoDB -> mongo-api -> Grafana Infinity -> Grafana dashboard`

So if a panel is empty, the first thing to check is the API endpoint in a browser, not Grafana itself.

## What this dashboard covers right now

- Targeted Twitch stream analytics.
- Top 100 Twitch stream analytics.
- Per-creator Twitch analytics with a `creator` Grafana variable.
- Google Trends summaries.
- Google Trends interest over time.

The dashboard is intentionally simple. It is a starter that proves the full path works; you can add more panels later without changing the backend wiring.

The creator view uses these API endpoints:

- `/creators`
- `/creator_summary`
- `/creator_viewers_history`
- `/creator_chat_history`
- `/creator_top_chatters`
- `/creator_trends_history`
- `/creator_recent_streams`
- `/creator_games`

The creator panels are append-only too, and the dashboard selects the latest snapshot per creator from MongoDB.

## Useful URLs

- Kafka UI: `http://localhost:8080`
- Spark master UI: `http://localhost:8081`
- MinIO console: `http://localhost:9001`
- Mongo API: `http://localhost:5000`

## Mongo API endpoints

- `/top_streamers`
- `/top_streamers_top100`
- `/top_streamers_history`
- `/top_streamers_history_top100`
- `/top_games`
- `/top_games_top100`
- `/top_games_history`
- `/top_games_history_top100`
- `/sentiment_over_time`
- `/messages_per_minute`
- `/messages_per_hour`
- `/negative_messages_per_hour`
- `/top_chatters`
- `/negative_messages`
- `/channel_summary`
- `/subscribers_vs_normal`
- `/unique_chat_users`
- `/creators`
- `/creator_summary`
- `/creator_viewers_history`
- `/creator_chat_history`
- `/creator_top_chatters`
- `/creator_trends_history`
- `/creator_recent_streams`
- `/creator_games`
- `/google_trends_summary`
- `/google_trends_comparison`
- `/google_trends_interest_over_time`

## Data layout

- TwitchChat writes RAW chat parquet into MinIO and append-only chat aggregates into `twitch_chat.chat_stats_1m`, `twitch_chat.channel_stats`, `twitch_chat.chat_stats_1h`, and `twitch_chat.chat_user_totals`.
- TwitchChat no longer stores raw chat messages in MongoDB.
- TwitchAPI writes RAW stream parquet into MinIO and append-only targeted snapshots into `twitch_api_analytics.top_games` and `twitch_api_analytics.streamer_stats`.
- TwitchAPI writes append-only top 100 snapshots into `twitch_api_analytics.top_games_top100` and `twitch_api_analytics.streamer_stats_top100`.
- Google Trends writes RAW parquet into MinIO and append-only keyword time series plus summaries into `google_trends.google_trends_interest_over_time` and `google_trends.google_trends_summary`.

In all of those collections, the latest state is determined by `snapshot_at`, `timestamp`, or `collected_at`, depending on the dataset.

## Notes

- `mongo-api` is the preferred integration point for Grafana Infinity.
- Grafana is kept optional through the `viz` profile so the core pipeline stays lightweight.
- The follower count panel was removed on purpose; creator views do not require Twitch auth.
- Google Trends uses `pytrends`, so Google rate limiting may still happen occasionally.
- Switch between local MongoDB and Atlas by changing `MONGO_BACKEND` in `.env` from `local` to `atlas`.
- If a Spark streaming checkpoint gets stale, bump `SPARK_CHECKPOINT_VERSION` instead of deleting all volumes.
- If Kafka logs `InconsistentClusterIdException`, recreate the Kafka data volume or bump its name in `docker-compose.yml` so it starts against the current ZooKeeper metadata.
