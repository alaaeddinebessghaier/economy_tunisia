import os
import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone

load_dotenv()

# -------------------------
# 1. FETCH DATA FROM API
# -------------------------
def get_jobs(query):
    url = "https://jsearch.p.rapidapi.com/search"

    headers = {
        "x-rapidapi-key": os.getenv("RAPID_API"),  # store in env variable
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }

    params = {
        "query": query,
        "page": 1,
        "num_pages": 1,
        "country": "TN",
        "date_posted": "all"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # raises error on 4xx/5xx

    data = response.json()

    if "data" not in data:
        raise ValueError(f"Unexpected API response: {data}")

    return data


# -------------------------
# 2. CLEAN / TRANSFORM
# -------------------------
def clean_jobs(data, query):
    events = []

    for job in data.get("data", []):
        events.append({
            "job_id": job.get("job_id"),
            "title": job.get("job_title"),
            "company": job.get("employer_name"),
            "location": job.get("job_city"),
            "salary": job.get("job_min_salary"),
            "metadata": {
                "source": "jsearch-api",
                "pipeline": "jobs_producer",
                "query": query,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }
        })

    return events


# -------------------------
# 3. KAFKA PRODUCER
# -------------------------
def send_to_kafka(events):
    producer = KafkaProducer(  # moved here, not global
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    try:
        for event in events:
            producer.send("job_repos", value=event)
        producer.flush()
        print(f"✅ Sent {len(events)} events to Kafka successfully")
    finally:
        producer.close()  # always close


# -------------------------
# 4. MAIN
# -------------------------
def main():
    query = "data engineer tunisia"
    raw_data = get_jobs(query)
    events = clean_jobs(raw_data, query)
    send_to_kafka(events)


if __name__ == "__main__":
    main()











    