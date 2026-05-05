import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone


GITHUB_API_URL = "https://api.github.com/search/repositories"
HEADERS = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "DataEngineering-Project",
    # "Authorization": "Bearer YOUR_TOKEN"
}


def get_data():
    response = requests.get(
        GITHUB_API_URL,
        headers=HEADERS,
        params={"q": "language:python", "per_page": 100}
    )
    response.raise_for_status()
    data = response.json()

    if "items" not in data:
        raise ValueError(f"Unexpected GitHub API response: {data}")

    return data


def clean_data(response):
    events = []
    for repo in response["items"]:
        events.append({
            "id": repo["id"],
            "full_name": repo["full_name"],
            "language": repo["language"],
            "stargazers_count": repo["stargazers_count"],
            "topics": repo["topics"],
            "metadata": {
                "source": "github-api",
                "pipeline": "github_producer",
                "query": "language:python",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }
        })
    return events


def send_data(events):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        for event in events:
            producer.send("github_repos", value=event)
        producer.flush()
        print(f"Done sending {len(events)} events to Kafka")
    finally:
        producer.close()


def main():
    data = get_data()
    events = clean_data(data)
    send_data(events)


if __name__ == "__main__":
    main()