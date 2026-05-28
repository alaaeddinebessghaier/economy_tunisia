import os
import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone

load_dotenv()  # loads .env from project root

# -------------------------
# 1. FETCH DATA FROM API
# -------------------------
def fetch_data():
    api_key = os.getenv("EXCHANGERATE_API")
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/TND"

    response = requests.get(url)
    response.raise_for_status()  # raises error on 4xx/5xx

    data = response.json()

    if "conversion_rates" not in data:
        raise ValueError(f"Unexpected API response: {data}")

    return data


# -------------------------
# 2. CLEAN / TRANSFORM
# -------------------------
def clean_data(data):
    currency = []
    rates = data["conversion_rates"]

    for cur, rate in rates.items():
        currency.append({
            "base": data["base_code"],
            "currency": cur,
            "rate": rate,
            "metadata": {
                "source": "exchangerate-api",
                "pipeline": "fx_producer",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }
        })

    return currency


# -------------------------
# 3. KAFKA PRODUCER
# -------------------------
def send_data(events):
    producer = KafkaProducer(  # moved here, not global
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    try:
        for event in events:
            producer.send("currency_rate", value=event)
        producer.flush()
        print(f"✅ Sent {len(events)} currency rates to Kafka")
    finally:
        producer.close()  # always close


# -------------------------
# 4. MAIN
# -------------------------
def main():
    data = fetch_data()
    events = clean_data(data)
    send_data(events)


if __name__ == "__main__":
    main()