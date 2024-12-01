from kafka import KafkaProducer
import requests
import time
from json import dumps
from config.config import API_URL
from config.private_config import API_KEY

KAFKA_BROKER = 'localhost:9092'
OUTPUT_TOPIC = 'velib-stations'

params = {'apiKey': API_KEY}
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def query_api():
    response = requests.get(API_URL, params=params)
    data = response.json()
    producer.send(OUTPUT_TOPIC, data)

while True:
    query_api()
    time.sleep(2)