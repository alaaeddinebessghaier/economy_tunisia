import requests 
import json 
from kafka import KafkaProducer
from datetime import datetime, timezone

def get_data():
    api_key = "b73011193c264958ae2418f4e1bb3902"
    url = f'https://newsapi.org/v2/everything?q=economy+tunisia&apiKey={api_key}'


    response = requests.get(url)
    data = response.json()

    return data




def clean_data(data):
    news = []
    for article in data["articles"]:
        news.append({
            "source": article['source']['id'],
            "title": article['title'],
            "description":article['url'],
            "content": article['content'],
            "metadata": {
                "source": "news-api",
                "pipeline": "news_producer",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "environment": "dev"
            }

        })
    return news


producer_news = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_data(data):
    producer_rate.send("news_repos",value=data)
    producer_rate.flush()
    print("done sending to kafka")


"""
data = get_data()

print(json.dumps(data,indent=2))
"""