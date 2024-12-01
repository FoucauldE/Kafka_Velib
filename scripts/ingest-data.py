from kafka import KafkaProducer
import requests
import time
import json
from bson import json_util
from config.config import API_URL
from config.private_config import API_KEY

producer = KafkaProducer(bootstrap_servers='localhost:9092')
params = {'apiKey': API_KEY}

def query_api():
    response = requests.get(API_URL, params=params)
    data = response.json()
    producer.send('velib-stations', json.dumps(data, default=json_util.default).encode('utf-8'))
    print('Data sent')

while True:
    query_api()
    time.sleep(2)