import json
import threading
from confluent_kafka import Consumer, KafkaError

# Load configuration from JSON file
with open('appsettings.json', 'r') as file:
    config_data = json.load(file)


# Configuration for Kafka Consumer
def create_consumer(provider):
    return Consumer({
        'bootstrap.servers': "localhost:9092",
        'group.id': f"{provider}-group",
        'auto.offset.reset': 'earliest'
    })

# Cache class to store last 200 values
class PriceCache:
    def __init__(self):
        self.prices = []
        
    def add_price(self, price):
        self.prices.append(price)
        if len(self.prices) > 200:
            self.prices.pop(0)
        
    def calculate_200sma(self):
        return sum(self.prices) / len(self.prices) if self.prices else 0

# Price difference calculation
def print_price_difference(price1, price2, coin):
    print(f"Difference for {coin}: {abs(price1 - price2)}")

latest_prices = {}

def consume_messages(provider, coins):
    consumer = create_consumer(provider)
    topic_list = [f"{provider}_{coin.replace('/', '')}" for coin in coins]
    consumer.subscribe(topic_list)

    # Caches for each coin
    caches = {coin: PriceCache() for coin in coins}
    
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
            data = json.loads(msg.value().decode('utf-8'))
            coin = data['symbol']
            cache = caches[coin]
            cache.add_price(data['close'])
            sma200 = cache.calculate_200sma()
            signal = 'B' if data['close'] > sma200 else 'S'
            print(f"{provider} - {coin} - {data['datetime']} - {data['percentage']}% - {data['close']} - Signal: {signal}")

            # Store the latest price
            latest_prices[(provider, coin)] = data['close']

            # Compare if both provider prices are available
            if ('binance', coin) in latest_prices and ('bingx', coin) in latest_prices:
                print_price_difference(latest_prices[('binance', coin)], latest_prices[('bingx', coin)], coin)
    finally:
        consumer.close()

# Threads for each provider
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

