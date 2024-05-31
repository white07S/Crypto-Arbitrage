import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import json
from confluent_kafka import KafkaError

# Ensure the root directory of your project is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Now attempt to import the consumer module
from app.real_time import consumer

class TestConsumer(unittest.TestCase):
    def test_load_config(self):
        with patch('builtins.open', unittest.mock.mock_open(read_data='{"providers":["binance"],"coins":["BTC/USD"]}')), \
             patch('json.load', return_value={'providers': ['binance'], 'coins': ['BTC/USD']}):
            config = consumer.load_config()
            self.assertEqual(config['providers'], ['binance'])

    def test_create_consumer(self):
        with patch('app.real_time.consumer.Consumer') as mock_consumer:
            consumer_instance = consumer.create_consumer('binance')
            mock_consumer.assert_called_with({
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'binance-group',
                'auto.offset.reset': 'earliest'
            })

    def test_price_cache(self):
        cache = consumer.PriceCache()
        for i in range(205):  # Add more than 200 prices to test the rolling window
            cache.add_price(i)
        expected_average = sum(range(5, 205)) / 200  # Correct range for last 200 elements
        self.assertEqual(len(cache.prices), 200)
        self.assertAlmostEqual(cache.calculate_200sma(), expected_average)

    @patch('app.real_time.consumer.Consumer')
    def test_consume_messages(self, mock_consumer_class):
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.poll.return_value = MagicMock(value=json.dumps({'symbol': 'BTC/USD', 'close': 50000, 'datetime': 'now', 'percentage': 0.1}).encode('utf-8'))
        mock_consumer_class.return_value = mock_consumer_instance
        
        consumer.consume_messages('binance', ['BTC/USD'])
        mock_consumer_instance.poll.assert_called()

    @patch('app.real_time.consumer.Consumer')
    def test_handle_kafka_errors(self, mock_consumer_class):
        mock_consumer_instance = MagicMock()
        # Ensure the first call simulates ALL_BROKERS_DOWN error and subsequent call is normal
        error_message = MagicMock()
        error_message.error.return_value = MagicMock(code=lambda: KafkaError._ALL_BROKERS_DOWN)
        mock_consumer_instance.poll.side_effect = [error_message, MagicMock()]
        mock_consumer_class.return_value = mock_consumer_instance
    
        with self.assertRaises(SystemExit):
            consumer.consume_messages('binance', ['BTC/USD'])


if __name__ == '__main__':
    unittest.main()
