from kafka import KafkaConsumer
from configs import kafka_config
import json

# Create Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8') if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_2'
)

# Name of the topic
my_name = "eli"
topic_name_1 = f'{my_name}_temperature_alerts_1'
topic_name_2 = f'{my_name}_humidity_alerts_1'

# Subscription to topic
consumer.subscribe([topic_name_1, topic_name_2])

print(f"Subscribed to topic '{topic_name_1, topic_name_2}'")

# Processing messages from the topic
try:
    for message in consumer:
        # print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        print(f"Received message: {message.value} from topic {message.topic}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Close consumer