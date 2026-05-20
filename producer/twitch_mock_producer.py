# twitch_mock_producer.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Inicjalizacja producenta Kafki
def get_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # api_version pomaga uniknąć błędów przy autodetekcji wersji podczas startu
                api_version=(0, 10, 1)
            )
            return p
        except NoBrokersAvailable:
            print("Kafka nie jest jeszcze gotowa... ponawiam za 5 sekund.")
            time.sleep(5)

producer = get_producer()

# Przykładowe dane do losowania (pozytywne, negatywne, neutralne, emotki)
MOCK_MESSAGES = [
    "OMG, what an incredible play! PogChamp",
    "This is so boring, change the game... LUL",
    "Hello everyone! What are we playing today?",
    "What is he doing?! Facepalm",
    "Best streamer on Twitch <3",
    "This gameplay is tragic, I can't watch this.",
    "Kappa what a lucky shot, I don't believe it!",
    "HE IS THE GOAT!!",
    "Worst stream ever.",
    "I love this community so much!"
]

STREAMERS = ["izakoo", "ewroon", "nitro"]
USERS = ["widz_1", "moderator_pl", "fan_gaming", "hejter99", "sub_od_roku"]

print("Uruchomiono symulator czatu Twitch... Wysyłam dane do Kafki.")

try:
    while True:
        mock_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "channel": random.choice(STREAMERS),
            "username": random.choice(USERS),
            "message": random.choice(MOCK_MESSAGES),
            "badges": {
                "subscriber": str(random.choice([0, 1, 6, 12])),
                "moderator": str(random.choice([0, 0, 0, 1])) # rzadziej moderator
            }
        }
        
        # Wysyłanie do tematu 'twitch-chat'
        producer.send('twitch-chat', mock_data)
        print(f"Wysłano: [{mock_data['channel']}] {mock_data['username']}: {mock_data['message']}")
        
        # Czekamy 0.5 - 1.5 sekundy (symulacja ruchu na czacie)
        time.sleep(random.uniform(0.5, 1.5))

except KeyboardInterrupt:
    print("Zatrzymano symulator.")
