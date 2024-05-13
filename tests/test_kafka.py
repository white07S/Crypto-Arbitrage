# test_kafka.py
import unittest
from unittest.mock import patch, MagicMock
from ..app.real_time import producer

class TestKafkaConnection(unittest.TestCase):
    @patch('producer.Producer')
    def test_kafka_connection(self, MockProducer):
        # Set up a mock instance of the Producer
        mock_producer_instance = MagicMock()
        MockProducer.return_value = mock_producer_instance

        # Define the Kafka configuration
        kafka_config = {
            'bootstrap.servers': "localhost:9092"
        }

        # Create a Kafka Producer instance from the producer_code module
        producer = producer.Producer(**kafka_config)

        # Check that the producer was instantiated
        self.assertIsNotNone(producer)

        # Verify that the Producer was called with the right configuration
        MockProducer.assert_called_once_with(**kafka_config)

        # Simulate a successful produce call
        mock_producer_instance.produce.return_value = None

        # Produce a sample message
        producer.produce('test_topic', b'test_message')

        # Verify that the message was produced
        mock_producer_instance.produce.assert_called_once_with('test_topic', b'test_message')

        # Check that the flush method was called
        mock_producer_instance.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()
