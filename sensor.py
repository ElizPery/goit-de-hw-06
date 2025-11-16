from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import datetime
import time
import random

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Name of the topic
my_name = "eli"
topic_name = f'{my_name}_building_sensors_1'
sensor_id = random.randint(1, 10000)

for i in range(30):
    # Send data from the sensor in topic
    try:
        data = {
            "timestamp": datetime.datetime.now().isoformat(),  # Time stamp
            "sensor_id": sensor_id,
            "temperature_val": random.randint(0, 50),
            "humidity_val": random.randint(0, 100)
        }
        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Waiting, until all messages would be sent
        print(f"Data from {data['timestamp']} sent to topic '{topic_name}' successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Close producer