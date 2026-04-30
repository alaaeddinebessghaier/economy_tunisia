import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone


def get_data(sk):
    GITHUB_API_URL = "https://api.github.com/search/repositories"
    HEADERS = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "DataEngineering-Project"
    }
    QUERY = f"language:{sk}"

    response = requests.get(GITHUB_API_URL, headers=HEADERS, params={"q": QUERY})
    #print(json.dumps(response.json(), indent=2))
    return response.json()

def clean_data(response, sk):
    events = []
    for repo in response["items"]:
        events.append({
        "id" : repo["id"],
        "full_name" : repo["full_name"],
        "language" : repo["language"],
        "stargazers_count" : repo["stargazers_count"],
        "topics" : repo["topics"],
        "metadata": {
                "source": "github-api",
                "pipeline": "github_producer",
                "query": f"language:{sk}",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }
        })
    return events




########### kafka connection

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_data(events):
    for event in events:
        producer.send("github_repos", value=event)
    producer.flush()
    print("done sending to kafka")


data = get_data(sk = "python")
events = clean_data(data,"python")
send_data(events)