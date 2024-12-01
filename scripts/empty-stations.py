from kafka import KafkaProducer, KafkaConsumer
import time
import json
from json import loads, dumps
from config.private_config import API_KEY

KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'stations-status'
OUTPUT_TOPIC = 'empty-stations'

params = {'apiKey': API_KEY}

consumer = KafkaConsumer(INPUT_TOPIC,
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='latest',
                         group_id='empty-stations-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


last_emptiness_state = {}

def process_station(station):

    station_nb = station['number']
    contract_name = station['contract_name']
    available_bikes = station['available_bikes']
    address = station['address']

    station_unique_id = f'{contract_name}_{station_nb}' # station_nb is only unique within a contract

    is_empty = available_bikes == 0

    if station_unique_id in last_emptiness_state:
        # compare new state with previously stored
        was_empty = last_emptiness_state[station_unique_id]

        if is_empty != was_empty:
            info_to_send = {
                'station_id': station_unique_id,
                'city': contract_name, # Closest information from city ?
                'address': address,
                'emptiness_status': "BECAME_EMPTY" if is_empty else "NO_LONGER_EMPTY"
            }
            producer.send(OUTPUT_TOPIC, info_to_send)
            last_emptiness_state[station_unique_id] = is_empty


    else:
        # starts monitoring station
        info_to_send = {
                'station_id': station_unique_id,
                'city': contract_name, # Closest information from city ?
                'address': address,
                'emptiness_status': "BECAME_EMPTY" if is_empty else "NO_LONGER_EMPTY"
            }
        producer.send(OUTPUT_TOPIC, info_to_send)
        last_emptiness_state[station_unique_id] = is_empty


while True:
     for message in consumer:
          station_status = message.value
          # print('station status:', station_status, type(station_status))
          if isinstance(station_status, str):
            try:
                station_status = json.loads(station_status)
            except json.JSONDecodeError:
                continue
          process_station(station_status)
          # for station in stations_status:
     time.sleep(2)