import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

ORDER_KAFKA_TOPIC = 'order_details'
NOTIFICATION_KAFKA_TOPIC = 'notification'
ORDER_LIMIT = 10

# Configure the producer
producer = KafkaProducer(
    bootstrap_servers='localhost:29092'
)

def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Error while sending message: {excp}")

# for i in range(1, ORDER_LIMIT+1):
# time.sleep(1)
while True:
    i = input(f'Enter: ')
    data = {
        'order_id': i,
        'user_id': f'Houch_{i}',
        'total_cost': i * 2,
        'items': 'burger, sandwich'
    }
    string = f'order_details from order_id: {i}'
    producer.send(
        NOTIFICATION_KAFKA_TOPIC,
        string.encode('utf-8')
    )
    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode('utf-8')
    ).add_callback(on_send_success).add_errback(on_send_error)
    producer.flush()
    print(f'Done sending..{i}')

