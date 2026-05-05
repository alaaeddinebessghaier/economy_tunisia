import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone


def get_data():
    api_key = "b73011193c264958ae2418f4e1bb3902"
    url = f"https://newsapi.org/v2/everything?q=economy+tunisia&apiKey={api_key}"

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    if "articles" not in data:
        raise ValueError(f"Unexpected API response: {data}")

    return data


def clean_data(data):
    news = []
    for article in data["articles"]:
        news.append({
            "source": article["source"]["id"],
            "title": article["title"],
            "description": article["description"],  # fixed, was url before
            "url": article["url"],
            "content": article["content"],
            "metadata": {
                "source": "news-api",
                "pipeline": "news_producer",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }
        })
    return news


def send_data(events):
    producer = KafkaProducer(  
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    try:
        for event in events:
            producer.send("news_repos", value=event) 
        producer.flush()
        print(f"✅ Sent {len(events)} articles to Kafka")
    finally:
        producer.close()



def main():
    data = get_data()
    events = clean_data(data)
    send_data(events)


if __name__ == "__main__":
    main()