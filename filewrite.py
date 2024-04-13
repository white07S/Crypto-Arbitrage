from pyspark.sql import SparkSession

# Create a SparkSession
sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

# Read a CSV file from a local directory specifying that the first row contains headers and to infer the data types
df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("ETHUSDT_2023.csv")

# Show the DataFrame to confirm it's loaded correctly with headers
print(df.show())

# Write DataFrame to HDFS in Parquet format
df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/eth_usdt.parquet")
