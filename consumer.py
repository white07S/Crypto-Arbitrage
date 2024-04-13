from confluent_kafka import Consumer, KafkaError

# Configuration for Kafka Consumer
conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "binance-group",
    'auto.offset.reset': 'earliest'
}

# Create a Kafka Consumer instance
consumer = Consumer(**conf)
consumer.subscribe(['binance_ETH_v1'])

def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            print('Received message: {}'.format(msg.value().decode('utf-8')))
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# Run the function
consume_messages()
