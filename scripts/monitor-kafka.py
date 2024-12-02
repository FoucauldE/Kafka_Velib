from kafka import KafkaConsumer
import time
from json import loads
from helper.tools import convert_timestamp
from config.private_config import API_KEY, WAIT_TIME

KAFKA_BROKER = 'localhost:9092'
TOPICS_PATTERN = '.*stations.*'

params = {'apiKey': API_KEY}

consumer = KafkaConsumer(# INPUT_TOPIC,
                         bootstrap_servers=KAFKA_BROKER,
                         auto_offset_reset='latest',
                         group_id='monitor-group',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer.subscribe(pattern=TOPICS_PATTERN)


try:
     while True:
        for message in consumer:
            topic = message.topic
            partition = message.partition
            offset = message.offset
            timestamp = convert_timestamp(message.timestamp, include_time=True)

            print(f"Topic: {topic}, Partition: {partition}, Offset: {offset}, Timestamp: {timestamp}")

        time.sleep(WAIT_TIME)

except KeyboardInterrupt:
    print("Monitoring interrupted.")

finally:
    consumer.close()
    print("Kafka monitoring stopped.")