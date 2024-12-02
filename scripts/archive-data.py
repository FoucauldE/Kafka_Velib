from kafka import KafkaConsumer
import time
import os
import json
from json import loads
from helper.tools import convert_timestamp
from config.private_config import API_KEY, WAIT_TIME

KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'velib-stations'
ARCHIVE_DIR = './archives'

os.makedirs(ARCHIVE_DIR, exist_ok=True)
params = {'apiKey': API_KEY}


def get_file_name(last_update_timestamp):
    date = convert_timestamp(last_update_timestamp)
    return f"velib-stations-{date}.txt"

current_date = None
current_file = None
file = None

consumer = KafkaConsumer(INPUT_TOPIC,
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='latest',
                         group_id='archive-group',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

try:
    print("Archiving the input data")
    while True:
        for message in consumer:
            data = message.value
            new_file = None

            for station in data:
                last_update_timestamp = station['last_update']
                station_date = convert_timestamp(last_update_timestamp)

                if new_file is None:
                    new_file = f"velib-stations-{station_date}.txt"

                if station_date != current_date:
                    new_file = f"velib-stations-{station_date}.txt"
                    break # no need to iterate through the other stations once new day detected

            if new_file != current_file:
                if file:
                    file.close()
                current_file = new_file
                file = open(os.path.join(ARCHIVE_DIR, current_file), 'a')

            file.write(json.dumps(data) + '\n')
            file.flush() # write data as we go along
        
        time.sleep(WAIT_TIME)

except KeyboardInterrupt:
    print("Process interrupted")
finally:
    if file and not file.closed:
        file.close()
    print('Files are archived in the "archives/" folder.')