import signal
from confluent_kafka import Producer
import ccxt
import json
import threading
import os
import time
# Global flag to control the running of threads
running = True

def signal_handler(signum, frame):
    """Signal handler to stop the producer loop."""
    global running
    running = False
    print("Shutdown signal received. Exiting threads...")

def load_config():
    """Load the configuration file."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(current_dir, 'appsettings.json')
    with open(config_file_path, 'r') as file:
        return json.load(file)

def create_producer(config):
    """Create and return a Kafka producer."""
    kafka_config = {'bootstrap.servers': config['bootstrap_servers']}
    return Producer(**kafka_config)

def fetch_ticker_data(exchange, coin):
    """Fetch ticker data from the exchange."""
    return exchange.fetch_ticker(coin)

def produce_message(producer, topic, message):
    """Produce a message to the Kafka topic."""
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()

def delivery_report(err, msg):
    """Callback to report message delivery status."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def run_producer_loop(coin, provider, config):
    """Run the main loop to fetch and produce data."""
    global running
    exchange = getattr(ccxt, provider)({
        'rateLimit': 1200,
        'enableRateLimit': True
    })
    topic = f'{provider}_{coin.replace("/", "")}'
    producer = create_producer(config)

    while running:
        try:
            ticker = fetch_ticker_data(exchange, coin)
            message = json.dumps(ticker)
            produce_message(producer, topic, message)
            time.sleep(config['time_sleep'])  # Sleep for some time defined by your rate limit or processing needs
        except ccxt.BaseError as error:
            print(f"An error occurred: {error}")
        except KeyboardInterrupt:
            print("Producer loop interrupted")
            break

def start_threads(config):
    threads = []
    for provider in config['providers']:
        for coin in config['coins']:
            t = threading.Thread(target=run_producer_loop, args=(coin, provider, config))
            t.start()
            threads.append(t)
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    config = load_config()
    start_threads(config)
