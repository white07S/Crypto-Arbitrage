import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.batch_processing.correlation import process_year_data, load_and_aggregate_data

@pytest.fixture(scope="module")
def spark_session():
    """Fixture for creating a Spark session."""
    spark = SparkSession.builder \
        .appName("SparkTestSession") \
        .master("local[4]") \
        .getOrCreate()
    yield spark
    spark.stop()

def create_test_data(spark, num_records, coins):
    """Generate a list of dummy DataFrames similar to how they would be read from disk."""
    from pyspark.sql.functions import expr
    data = []
    for coin in coins:
        df = spark.range(num_records).withColumn("timestamp", (rand() * 1000000000).cast("timestamp"))\
                                     .withColumn("close", (randn() * 1000 + 10000))\
                                     .withColumn("open", (randn() * 1000 + 10000))\
                                     .withColumn("high", (randn() * 1000 + 10000))\
                                     .withColumn("low", (randn() * 1000 + 10000))\
                                     .withColumn("coin", expr(f"'{coin}'"))
        data.append(df)
    return data

def test_process_year_data_performance(spark_session):
    num_records = 100000  # Large number of records
    coins = ['BNB', 'BTC', 'DOGE', 'ETH', 'SOL', 'USDC', 'XRP']
    create_test_data(spark_session, num_records, coins)  # This function call is just to ensure the data can be created correctly.
    
    import time
    start_time = time.time()
    process_year_data(spark_session, "2022", "hdfs://localhost:9000/user/hadoop/data", coins)
    end_time = time.time()
    print(f"Execution time for processing {num_records * len(coins)} records across {len(coins)} coins: {end_time - start_time} seconds")

    assert (end_time - start_time) < 60  # Assert that the processing time is reasonable, adjust accordingly

