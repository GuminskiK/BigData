import json
import os
import asyncio
from datetime import datetime, timezone

import twitchio
from twitchio.ext import commands
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092").split(",")
KAFKA_TOPIC = os.getenv("TWITCH_KAFKA_TOPIC", "twitch_chat_stream")
TWITCH_TOKEN = os.getenv("TWITCH_TOKEN")
TWITCH_NICK = os.getenv("TWITCH_NICK")
TWITCH_CHANNELS = os.getenv("TWITCH_CHANNELS", "xqc").split(",")


def get_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(0, 10, 1),
            )
            print(f"Połączono z Kafką: {KAFKA_SERVERS}")
            return p
        except NoBrokersAvailable:
            print("Kafka nie jest jeszcze gotowa... ponawiam za 5 sekund.")
            import time; time.sleep(5)


kafka_producer = None


def on_send_error(exc):
    print(f"[BŁĄD] Nie udało się wysłać wiadomości: {exc}")


class TwitchBot(commands.Bot):
    def __init__(self):
        super().__init__(
            token=TWITCH_TOKEN,
            nick=TWITCH_NICK,
            prefix="!",
            initial_channels=TWITCH_CHANNELS,
        )

    async def event_ready(self):
        print(f"Zalogowano jako: {self.nick}")
        print(f"Obserwowane kanały: {TWITCH_CHANNELS}")

    async def event_message(self, message: twitchio.Message):
        if message.echo:
            return

        badges = {}
        if message.author.badges:
            badges = {k: str(v) for k, v in message.author.badges.items()}

        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": message.channel.name,
            "username": message.author.name,
            "message": message.content,
            "badges": badges,
        }

        kafka_producer.send(KAFKA_TOPIC, data).add_errback(on_send_error)
        print(f"[{data['channel']}] {data['username']}: {data['message']}")

    async def event_error(self, error: Exception, data=None):
        print(f"[BŁĄD IRC] {error}")


if __name__ == "__main__":
    if not TWITCH_TOKEN or TWITCH_TOKEN == "oauth:WKLEJ_NOWY_TOKEN_TUTAJ":
        raise ValueError("Ustaw TWITCH_TOKEN w .env!")
    if not TWITCH_NICK:
        raise ValueError("Ustaw TWITCH_NICK w .env!")

    kafka_producer = get_producer()

    bot = TwitchBot()
    try:
        bot.run()
    finally:
        kafka_producer.flush()
        kafka_producer.close()
