import requests 
import json 
from kafka import KafkaProducer
from datetime import datetime, timezone



def fetch_data():
    api_key = "e2a842c273392049533aef62"
    url = 'https://v6.exchangerate-api.com/v6/e2a842c273392049533aef62/latest/TND'


    response = requests.get(url)
    data = response.json()
    #print(json.dumps(data ,indent=2))
    return data


def clean_data(data):
    currency = []
    rates = data["conversion_rates"]
    for cur , rate in rates.items():
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




##### kafka connection 
producer_rate = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_data(data):
    producer_rate.send("currency_rate",value=data)
    producer_rate.flush()
    print("done sending to kafka")






data = fetch_data()
curr = clean_data(data)
send_data(curr)

