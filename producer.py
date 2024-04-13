import ccxt
import time
import json  # Import json module for serialization
from confluent_kafka import Producer

# Configuration for Kafka Producer
conf = {
    'bootstrap.servers': "localhost:9092"  # Assuming Kafka is running on localhost
}

# Create a Kafka Producer instance
producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_and_produce():
    # Create an instance of the Binance exchange
    binance = ccxt.binance({
        'rateLimit': 1200,
        'enableRateLimit': True
    })
    symbol = 'ETH/USDT'
    topic = 'binance_ETH_v1'

    while True:
        try:
            # Fetch the ticker data for ETH/USDT
            ticker = binance.fetch_ticker(symbol)
            # Serialize the ticker data to JSON format
            message = json.dumps(ticker)
            # Produce the message to the Kafka topic
            producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
            # Wait for any outstanding messages to be delivered
            producer.flush()
        except ccxt.BaseError as error:
            print(f"An error occurred: {error}")
        # Sleep to respect rate limits and manage message flow
        time.sleep(1)

# Run the function
fetch_and_produce()
