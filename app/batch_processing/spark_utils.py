from pyspark.sql import SparkSession

def create_spark_session(app_name):
    """
    Create or retrieve a SparkSession for the application.
    
    Args:
    app_name (str): The name of the application.

    Returns:
    SparkSession: The Spark session instance.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \
        .getOrCreate()

def read_data(spark, data_path):
    """
    Reads data from a specified path using Spark.

    Args:
    spark (SparkSession): The Spark session.
    data_path (str): Path to the data file.

    Returns:
    DataFrame: Spark DataFrame containing the data.
    """
    return spark.read.parquet(data_path)
