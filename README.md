# BigData

Local data platform for Twitch streams, Twitch chat, and Google Trends.

## What runs locally

- Kafka for event transport.
- MongoDB for analytics storage.
- MinIO for Twitch API raw parquet snapshots.
- TwitchChat pipeline: IRC producer -> Spark streaming -> MongoDB.
- TwitchAPI pipeline: Twitch API producer -> Kafka -> MinIO -> Spark batch -> MongoDB.
- Google Trends batch: Google Trends -> MongoDB.
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

Grafana starts with one dashboard called `BigData Overview`. It is defined in `grafana/dashboards/bigdata-overview.json` and is loaded automatically by Grafana provisioning.

## How Grafana fits together

Grafana does not read MongoDB directly in this setup. It queries the local Flask API through the Infinity datasource, and that API reads the Mongo collections.

The chain is:

`MongoDB -> mongo-api -> Grafana Infinity -> Grafana dashboard`

So if a panel is empty, the first thing to check is the API endpoint in a browser, not Grafana itself.

## What this dashboard covers right now

- Targeted Twitch stream analytics.
- Top 100 Twitch stream analytics.
- Google Trends summaries.
- Google Trends interest over time.

The dashboard is intentionally simple. It is a starter that proves the full path works; you can add more panels later without changing the backend wiring.

## Useful URLs

- Kafka UI: `http://localhost:8080`
- Spark master UI: `http://localhost:8081`
- MinIO console: `http://localhost:9001`
- Mongo API: `http://localhost:5000`

## Mongo API endpoints

- `/top_streamers`
- `/top_streamers_top100`
- `/top_games`
- `/top_games_top100`
- `/sentiment_over_time`
- `/messages_per_minute`
- `/top_chatters`
- `/negative_messages`
- `/channel_summary`
- `/subscribers_vs_normal`
- `/google_trends_summary`
- `/google_trends_interest_over_time`

## Data layout

- TwitchChat writes chat analytics into `twitch_chat`.
- TwitchAPI writes targeted stream analytics into `twitch_api_analytics`.
- TwitchAPI also stores top 100 stream analytics in separate collections with the `_top100` suffix.
- Google Trends writes into `google_trends`.

## Notes

- `mongo-api` is the preferred integration point for Grafana Infinity.
- Grafana is kept optional through the `viz` profile so the core pipeline stays lightweight.
- Google Trends uses `pytrends`, so Google rate limiting may still happen occasionally.
- Switch between local MongoDB and Atlas by changing `MONGO_BACKEND` in `.env` from `local` to `atlas`.
- If a Spark streaming checkpoint gets stale, bump `SPARK_CHECKPOINT_VERSION` instead of deleting all volumes.
