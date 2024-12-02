from kafka import KafkaProducer, KafkaConsumer
import time
from json import loads, dumps
from config.config import KAFKA_BROKER
from config.private_config import API_KEY

INPUT_TOPIC = 'velib-stations'
OUTPUT_TOPIC = 'stations-status'

params = {'apiKey': API_KEY}

consumer = KafkaConsumer(INPUT_TOPIC,
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='latest',
                         group_id='stations-activity-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


last_stations_states = {}

def process_station(station):

     station_nb = station['number']
     contract_name = station['contract_name']
     available_bikes = station['available_bikes']
     free_spaces = station['available_bike_stands']
     status = station['status']

     station_unique_id = f'{contract_name}_{station_nb}' # station_nb is only unique within a contract

     if station_unique_id in last_stations_states:
          # compare new state with previously stored
          previous_state = last_stations_states[station_unique_id]

          if (previous_state['bikes'] != available_bikes or
              previous_state['slots'] != free_spaces or
              previous_state['status'] != status):
               
               producer.send(OUTPUT_TOPIC, station)
               last_stations_states[station_unique_id] = {
                    'bikes': available_bikes,
                    'slots': free_spaces,
                    'status': status
               }
     
     else:
          # starts monitoring new station
          producer.send(OUTPUT_TOPIC, station)
          last_stations_states[station_unique_id] = {
                    'bikes': available_bikes,
                    'slots': free_spaces,
                    'status': status
               }

try:
     while True:
          for message in consumer:
               data = message.value
               for station in data:
                    process_station(station)
          time.sleep(2)

except KeyboardInterrupt:
    print("Interrumpting...")
finally:
    producer.close()
    print('Producer closed.')