import ccxt
import time
import json
import threading
from confluent_kafka import Producer

# Load configuration from JSON file
with open('appsettings.json', 'r') as file:
    config_data = json.load(file)

# Configuration for Kafka Producer
kafka_config = {
    'bootstrap.servers': "localhost:9092"  # Assuming Kafka is running on localhost
}

# Create a Kafka Producer instance
producer = Producer(**kafka_config)

def delivery_report(err, msg):
    """Callback to report message delivery status"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_and_produce(coin, provider):
    exchange = getattr(ccxt, provider)({
        'rateLimit': 1200,
        'enableRateLimit': True
    })
    
    topic = f'{provider}_{coin.replace("/", "")}'
    
    while True:
        try:
            # Fetch the ticker data for the coin
            ticker = exchange.fetch_ticker(coin)
            # Serialize the ticker data to JSON format
            message = json.dumps(ticker)
            # Produce the message to the Kafka topic
            producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
            # Wait for any outstanding messages to be delivered
            producer.flush()
        except ccxt.BaseError as error:
            print(f"An error occurred: {error}")
            break  # Exit the while loop to prevent infinite loops on errors
        # Sleep to respect rate limits and manage message flow
        time.sleep(config_data['time_sleep'])

def start_threads():
    threads = []
    for provider in config_data['providers']:
        for coin in config_data['coins']:
            t = threading.Thread(target=fetch_and_produce, args=(coin, provider))
            t.start()
            threads.append(t)

    for thread in threads:
        thread.join()  # Wait for all threads to complete

if __name__ == "__main__":
    start_threads()
