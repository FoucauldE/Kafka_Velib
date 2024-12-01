from kafka import KafkaConsumer
import time
from json import loads
from config.private_config import API_KEY

KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'empty-stations'

params = {'apiKey': API_KEY}

consumer = KafkaConsumer(INPUT_TOPIC,
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='latest',
                         group_id='empty-stations-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))


last_emptiness_state = {}

def process_station(station):

    station_unique_id = station['station_id']
    city = station['city']
    address = station['address']
    emptiness_status = station['emptiness_status']

    if emptiness_status == "BECAME_EMPTY":
        print(f'Station {station_unique_id} ({address}, {city}: {emptiness_status})')


while True:
     for message in consumer:
          station_emptiness_status = message.value
          process_station(station_emptiness_status)
          # for station in stations_emptiness_status:
     time.sleep(2)