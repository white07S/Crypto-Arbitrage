import json
import time
from confluent_kafka import Producer

def create_producer():
    """Create and configure a Kafka producer."""
    config = {'bootstrap.servers': 'localhost:9092'}
    return Producer(config)

def produce_messages(producer, topic, num_messages):
    """Produce a large number of messages to Kafka topic."""
    start_time = time.time()
    for i in range(num_messages):
        message = json.dumps({'number': i})
        producer.produce(topic, message.encode('utf-8'))
        if i % 1000 == 0:  # Adjust batching to your needs
            producer.flush()
    producer.flush()
    end_time = time.time()
    print(f"Finished producing messages. Total time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    producer = create_producer()
    produce_messages(producer, 'test_topic', 1000000)  # Adjust the number of messages based on test requirements
