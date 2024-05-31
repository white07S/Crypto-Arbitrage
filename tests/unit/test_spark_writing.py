import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import sys
import os

# Ensure the path is correctly added; consider printing sys.path to debug
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from app.batch_processing.session_manager import read_and_write_data, process_all_coins

class TestCryptoDataProcessing(unittest.TestCase):
    def setUp(self):
        # Setup a mock Spark session
        self.spark = MagicMock()
        self.spark.read.csv.return_value = MagicMock(name='DataFrame')
        self.base_path = os.path.join(os.getcwd(), 'app', 'data')

    def tearDown(self):
        pass  # In case the Spark session stopping is also mocked, or unnecessary in the mock setup

    @patch('os.listdir')
    @patch('os.path.join', side_effect=os.path.join)
    @patch('os.path.exists', return_value=True)
    @patch('builtins.print')
    def test_read_and_write_data(self, mock_print, mock_exists, mock_join, mock_listdir):
        mock_listdir.return_value = ['BTCUSDT_2020.csv', 'BTCUSDT_2021.csv']

        # Execute the test function
        read_and_write_data(self.spark, self.base_path, 'BTC')

        # Assertions to check if files are processed as expected
        self.assertEqual(self.spark.read.csv.call_count, 2)  # Check how many times csv reader was called
        self.assertEqual(self.spark.read.csv.return_value.write.mode().parquet.call_count, 2)
        mock_print.assert_called_with('Data written to hdfs://localhost:9000/user/hadoop/data/BTC/2021.parquet for BTC of year 2021')

    @patch('app.batch_processing.session_manager.read_and_write_data')
    def test_process_all_coins(self, mock_read_and_write):
        process_all_coins(self.spark, self.base_path)

        self.assertEqual(mock_read_and_write.call_count, 7)
        mock_read_and_write.assert_any_call(self.spark, self.base_path, 'BNB')

# Run the tests
if __name__ == "__main__":
    unittest.main()
