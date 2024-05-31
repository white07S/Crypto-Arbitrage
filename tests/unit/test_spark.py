import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))



# Import your module here, assuming the functions are in a module named spark_utils
from app.batch_processing.spark_utils import create_spark_session, read_data

class TestSparkSession(unittest.TestCase):
    def setUp(self):
        # Setting up a SparkSession for testing
        self.spark = SparkSession.builder \
            .appName("TestApp") \
            .master("local[2]") \
            .getOrCreate()

    def tearDown(self):
        # Tear down SparkSession after each test
        self.spark.stop()

    def test_create_spark_session(self):
        # Testing creation of SparkSession
        with patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=self.spark):
            session = create_spark_session("TestApp")
            self.assertEqual(session.conf.get("spark.app.name"), "TestApp")
            self.assertTrue(session.conf.get("spark.sql.shuffle.partitions"), "200")
            self.assertTrue(session.conf.get("spark.sql.catalogImplementation"), "hive")

    def test_read_data(self):
        # Testing reading data
        data_path = "hdfs://localhost:9000/user/hadoop/data/BTC/2020.parquet"
        # Assuming the data path and format are correct for the environment
        df = read_data(self.spark, data_path)
        # You may want to add more checks here depending on the expected content of the data
        self.assertIsNotNone(df)
        self.assertTrue(hasattr(df, "dtypes"))  # Checking if DataFrame is valid

# Run the tests
if __name__ == "__main__":
    unittest.main()
