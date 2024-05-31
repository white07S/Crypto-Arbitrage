import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Ensure the root directory of your project is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Now attempt to import the producer module
from app.real_time import producer

class TestProducer(unittest.TestCase):
    def test_load_config(self):
        with patch('builtins.open', unittest.mock.mock_open(read_data='{"bootstrap_servers":"localhost:9092"}')), \
             patch('json.load', return_value={'bootstrap_servers': 'localhost:9092'}):
            config = producer.load_config()
            self.assertEqual(config['bootstrap_servers'], 'localhost:9092')

    def test_create_producer(self):
        config = {'bootstrap_servers': 'localhost:9092'}
        with patch('app.real_time.producer.Producer') as mock_producer:
            producer_instance = producer.create_producer(config)
            mock_producer.assert_called_with(**{'bootstrap.servers': 'localhost:9092'})

    @patch('app.real_time.producer.ccxt.binance')
    def test_fetch_ticker_data(self, mock_binance):
        mock_exchange = MagicMock()
        mock_binance.return_value = mock_exchange
        mock_exchange.fetch_ticker = MagicMock(return_value={'bid': 1})
        data = producer.fetch_ticker_data(mock_exchange, 'BTC/USD')
        self.assertEqual(data['bid'], 1)

    @patch('app.real_time.producer.Producer')
    def test_produce_message(self, mock_producer):
        producer_instance = MagicMock()
        mock_producer.return_value = producer_instance
        producer.produce_message(producer_instance, 'topic_test', '{"bid":1}')
        producer_instance.produce.assert_called_once()

    # @patch('time.sleep', return_value=None)  # Patch sleep to avoid waiting
    # @patch('app.real_time.producer.running', new_callable=MagicMock)  # Mock the running flag
    # @patch('app.real_time.producer.create_producer')
    # @patch('app.real_time.producer.fetch_ticker_data')
    # @patch('app.real_time.producer.produce_message')
    # def test_run_producer_loop(self, mock_produce_message, mock_fetch_ticker_data, mock_create_producer, mock_running, mock_sleep):
    #     mock_create_producer.return_value = MagicMock()
    #     mock_fetch_ticker_data.return_value = {'bid': 1}

    #     # Include 'time_sleep' in the configuration
    #     config = {'bootstrap.servers': 'localhost:9092', 'time_sleep': 5}
        
    #     # Set the running flag to True initially and then to False to stop the loop
    #     mock_running.side_effect = [True, False]
        
    #     producer.run_producer_loop('BTC/USD', 'binance', config)
    #     mock_produce_message.assert_called_with(mock_create_producer.return_value, 'binance_BTCUSD', '{"bid": 1}')

    #     # Check that the loop ran exactly once
    #     self.assertEqual(mock_produce_message.call_count, 1)

if __name__ == "__main__":
    unittest.main()
