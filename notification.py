from kafka import KafkaConsumer

NOTIFICATION_KAFKA_TOPIC = 'notification'
ORDER_KAFKA_TOPIC = 'order_details'

consumer = KafkaConsumer(
    NOTIFICATION_KAFKA_TOPIC,
    bootstrap_servers='localhost:29092'
)
print('Notification listening...')

while True:
    for message in consumer:
        print(f'Notification from {message.value.decode()}')
