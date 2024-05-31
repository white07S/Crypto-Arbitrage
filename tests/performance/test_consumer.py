from confluent_kafka import Consumer, KafkaError
import time

def create_consumer():
    """Create and configure a Kafka consumer."""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(config)

def consume_messages(consumer, topic, idle_timeout=30):
    """Consume messages and measure performance excluding idle time."""
    consumer.subscribe([topic])
    total_messages = 0
    start_time = time.time()
    last_received_time = start_time  # Initialize last received time
    active_processing_time = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                current_time = time.time()
                if current_time - last_received_time > idle_timeout:
                    print("Idle timeout reached. Exiting...")
                    break
                print("Waiting for message or end of partition...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Reached end of partition.")
                    break
                elif msg.error().code() == KafkaError._ALL_BROKERS_DOWN:
                    print("All brokers are down!")
                    break
                else:
                    print(f"Error: {msg.error()}")
                    break
            # Calculate active processing time
            processing_start = time.time()
            last_received_time = processing_start
            total_messages += 1
            active_processing_time += time.time() - processing_start
            if total_messages % 1000 == 0:
                print(f"Processed {total_messages} messages so far.")

    finally:
        end_time = time.time()
        consumer.close()
        total_elapsed_time = end_time - start_time
        print(f"Finished consuming messages. Processed {total_messages} messages in total elapsed time: {total_elapsed_time:.2f} seconds.")
        print(f"Active processing time: {active_processing_time:.2f} seconds.")
        print("Consumer closed.")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_messages(consumer, 'test_topic')
