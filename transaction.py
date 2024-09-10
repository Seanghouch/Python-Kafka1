import json
from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = 'order_details'
ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'
NOTIFICATION_KAFKA_TOPIC = 'notification'

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers='localhost:29092'
)
producer = KafkaProducer(
    bootstrap_servers='localhost:29092'
)

print('Gonna start listening..')

while True:
    for message in consumer:
        print('Ongoing transaction...')
        consumer_message = json.loads(message.value.decode())
        user_id = consumer_message['user_id']
        total_cost = consumer_message['total_cost']
        data = {
            'customer_id': user_id,
            'customer_email': f'{user_id}@gmail.com',
            'total_cost': total_cost
        }
        print(json.dumps(data).encode())
        print('Successful transaction...')
        confirmed_order = producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode('utf-8')
        )
        if confirmed_order:
            string = f'transaction from user_id: {user_id}'
            producer.send(
                NOTIFICATION_KAFKA_TOPIC,
                string.encode('utf-8')
            )
