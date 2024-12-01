from kafka import KafkaProducer, KafkaConsumer
import requests
import time
import json
from json import loads
from bson import json_util
from config.private_config import API_KEY

params = {'apiKey': API_KEY}

consumer = KafkaConsumer('velib-stations',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='latest',
                         group_id='test-consumer-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

def process_data(data):
     for station in data:
          print(station.keys())
        # print(f"Station ID: {station['station_id']}")
        # print(f"Available bikes: {station['available_bikes']}")
        # print(f"Free slots: {station['available_bike_stands']}")

while True:
     for message in consumer:
          data = message.value
          # print(data)
          process_data(data)
     time.sleep(2)

