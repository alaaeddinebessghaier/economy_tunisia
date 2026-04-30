import requests
import json
from kafka import KafkaProducer
from datetime import datetime, timezone


# -------------------------
# 1. FETCH DATA FROM API
# -------------------------
def get_jobs(query):
    url = "https://jsearch.p.rapidapi.com/search"

    headers = {
        "x-rapidapi-key": "8e090c8da6mshad59666debd7febp14c265jsn62a10488af38",
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

    if response.status_code != 200:
        print("API Error:", response.text)
        return {}

    return response.json()



# -------------------------
# 2. CLEAN / TRANSFORM
# -------------------------
def clean_jobs(data, query):
    events = []

    for job in data.get("data", []):  # JSearch uses "data"
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
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_to_kafka(events):
    for event in events:
        producer.send("job_repos", value=event)

    producer.flush()
    print("✅ Sent to Kafka successfully")






query = "data engineer tunisia"
raw_data = get_jobs(query)
events = clean_jobs(raw_data, query)
send_to_kafka(events)