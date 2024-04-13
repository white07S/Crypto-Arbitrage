from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

# Read the Parquet file back into a Spark DataFrame from HDFS
df_load = sparkSession.read.parquet("hdfs://localhost:9000/user/hadoop/eth_usdt.parquet")

# Show the DataFrame to confirm it's loaded correctly from HDFS
print(df_load.show())