import json
import threading
import os
from confluent_kafka import Consumer, KafkaError

# Load configuration from a JSON file
def load_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(current_dir, 'appsettings.json')
    with open(config_file_path, 'r') as file:
        return json.load(file)

config_data = load_config()

# Create a Kafka consumer for a specific provider
def create_consumer(provider):
    return Consumer({
        'bootstrap.servers': config_data['bootstrap_servers'],
        'group.id': f"{provider}-group",
        'auto.offset.reset': 'earliest'
    })

# Simple class to store prices and compute SMA
class PriceCache:
    def __init__(self):
        self.prices = []

    def add_price(self, price):
        self.prices.append(price)
        if len(self.prices) > 200:
            self.prices.pop(0)

    def calculate_200sma(self):
        return sum(self.prices) / len(self.prices) if self.prices else 0

def print_price_difference(price1, price2, coin):
    print(f"Difference for {coin}: {abs(price1 - price2)}")

latest_prices = {}

# Consuming messages from Kafka
def consume_messages(provider, coins):
    consumer = create_consumer(provider)
    topic_list = [f"{provider}_{coin.replace('/', '')}" for coin in coins]
    consumer.subscribe(topic_list)
    caches = {coin: PriceCache() for coin in coins}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError._ALL_BROKERS_DOWN:
                    print(f"Critical error: {msg.error()}")
                    raise SystemExit
                else:
                    print(msg.error())
                    break
            process_message(msg, caches, provider)
    finally:
        consumer.close()

def process_message(msg, caches, provider):
    data = json.loads(msg.value().decode('utf-8'))
    coin = data['symbol']
    cache = caches[coin]
    cache.add_price(data['close'])
    sma200 = cache.calculate_200sma()
    signal = 'B' if data['close'] > sma200 else 'S'
    print(f"{provider} - {coin} - {data['datetime']} - {data['percentage']}% - {data['close']} - Signal: {signal}")

    latest_prices[(provider, coin)] = (data['close'], data['datetime'], signal)

def start_consumers():
    providers = config_data['providers']
    coins = config_data['coins']
    threads = []

    for provider in providers:
        t = threading.Thread(target=consume_messages, args=(provider, coins))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()  # Wait for all threads to complete

if __name__ == "__main__":
    start_consumers()