from kafka import KafkaProducer, KafkaConsumer
import time
from json import loads, dumps
from config.config import KAFKA_BROKER
from config.private_config import API_KEY, WAIT_TIME

INPUT_TOPIC = 'stations-status'
OUTPUT_TOPIC = 'empty-stations'

params = {'apiKey': API_KEY}

consumer = KafkaConsumer(INPUT_TOPIC,
                        bootstrap_servers=KAFKA_BROKER,
                        auto_offset_reset='latest',
                        group_id='stations-activity-group',
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

        was_empty = last_emptiness_state[station_unique_id]

        if is_empty != was_empty:
            info_to_send = {
                'station_id': station_unique_id,
                'city': contract_name, # Closest information from city
                'address': address,
                'emptiness_status': "BECAME_EMPTY" if is_empty else "NO_LONGER_EMPTY"
            }
            producer.send(OUTPUT_TOPIC, info_to_send)
            last_emptiness_state[station_unique_id] = is_empty
    
    else : # the station is new
        info_to_send = {
                'station_id': station_unique_id,
                'city': contract_name, # Closest information from city
                'address': address,
                'emptiness_status': "BECAME_EMPTY" if is_empty else "NO_LONGER_EMPTY"
            }
        producer.send(OUTPUT_TOPIC, info_to_send)
        last_emptiness_state[station_unique_id] = is_empty


try:
    print("Sending message if emptiness changed...")
    while True:
        for message in consumer:
            station = message.value
            process_station(station)

        time.sleep(WAIT_TIME)

except KeyboardInterrupt:
    print("Interrumpting...")
finally:
    producer.close()
    print('Producer closed.')

