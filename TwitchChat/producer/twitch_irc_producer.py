import os
import json
import asyncio
from datetime import datetime, timezone

import twitchio
from twitchio.ext import commands
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Konfiguracja z ENV
KAFKA_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TWITCH_CHAT", "twitch_chat_stream")
TWITCH_TOKEN = os.getenv("TWITCH_TOKEN")
TWITCH_NICK = os.getenv("TWITCH_NICK")
TWITCH_CHANNELS = os.getenv("TWITCH_CHANNELS", "xqc").split(",")

class TwitchBot(commands.Bot):
    def __init__(self, kafka_producer):
        self.kafka_producer = kafka_producer
        # Inicjalizacja bota TwitchIO
        super().__init__(
            token=TWITCH_TOKEN,
            nick=TWITCH_NICK,
            prefix="!",
            initial_channels=TWITCH_CHANNELS,
        )

    async def event_ready(self):
        print(f"--- [IRC READY] Zalogowano jako: {self.nick} ---")
        print(f"--- Obserwowane kanały: {TWITCH_CHANNELS} ---")

    async def event_message(self, message: twitchio.Message):
        # Ignoruj wiadomości wysłane przez samego bota
        if message.echo:
            return

        # Przygotowanie metadanych (badges)
        badges = {}
        if message.author.badges:
            badges = {k: str(v) for k, v in message.author.badges.items()}

        # Tworzenie struktury JSON
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": message.channel.name,
            "username": message.author.name,
            "message": message.content,
            "badges": badges,
        }

        # ASYNCHRONICZNA wysyłka do Kafki
        try:
            payload = json.dumps(data).encode("utf-8")
            # send_and_wait zapewnia, że wiadomość faktycznie dotarła do brokera
            await self.kafka_producer.send(KAFKA_TOPIC, payload)
            # Opcjonalny log (zakomentuj przy bardzo dużym ruchu, żeby nie zapchać konsoli)
            # print(f"[{data['channel']}] {data['username']}: {data['message']}")
        except Exception as e:
            print(f"[BŁĄD KAFKA] Nie udało się wysłać: {e}")

    async def event_error(self, error: Exception, data=None):
        print(f"[BŁĄD IRC] Wystąpił błąd w pętli bota: {error}")

async def run_producer():
    print("Inicjalizacja producenta Kafki...")
    
    # Konfiguracja asynchronicznego producenta
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        # Automatyczne ponawianie prób w razie awarii sieci
        retry_backoff_ms=500,
        # api_version="0.11" # Możesz odkomentować, jeśli masz starą wersję Kafki
    )

    # Czekaj na połączenie z Kafką przed startem bota
    connected = False
    while not connected:
        try:
            await producer.start()
            connected = True
            print(f"Połączono z Kafką: {KAFKA_SERVERS}")
        except Exception as e:
            print(f"Kafka niegotowa ({e}). Ponawiam za 5 sekund...")
            await asyncio.sleep(5)

    bot = TwitchBot(kafka_producer=producer)

    try:
        # Start bota (to blokuje pętlę do momentu zamknięcia)
        await bot.start()
    except KeyboardInterrupt:
        print("Zamykanie producenta...")
    finally:
        # Pamiętaj o zamknięciu producenta, żeby wysłać pozostałe wiadomości z bufora
        await producer.stop()
        print("Producent zamknięty.")

if __name__ == "__main__":
    if not TWITCH_TOKEN or "oauth:" not in TWITCH_TOKEN:
        print("BŁĄD: Brak poprawnego TWITCH_TOKEN w .env!")
    else:
        # Uruchomienie pętli asynchronicznej
        asyncio.run(run_producer())