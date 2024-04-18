# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lag, avg, when, monotonically_increasing_id
# from pyspark.sql.window import Window
# import time

# # Create a SparkSession
# sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

# # Read the Parquet file
# df_load = sparkSession.read.parquet("hdfs://localhost:9000/user/hadoop/eth_usdt.parquet")

# # Convert the timestamp to TimestampType and repartition by hour for time series analysis
# df_time_partitioned = df_load.withColumn("timestamp", col("timestamp").cast("timestamp")) \
#                              .repartition("timestamp")

# # Define window specification for moving averages
# windowSpec200 = Window.orderBy("timestamp").rowsBetween(-199, 0)
# windowSpec50 = Window.orderBy("timestamp").rowsBetween(-49, 0)

# # Calculate moving averages
# df_with_sma = df_time_partitioned.withColumn("sma200", avg(col("close")).over(windowSpec200)) \
#                                  .withColumn("sma50", avg(col("close")).over(windowSpec50))

# # Identify crossover points for trading signals
# df_signals = df_with_sma.withColumn("signal", 
#                                     when(col("sma50") > col("sma200"), 1)
#                                     .when(col("sma50") < col("sma200"), -1)
#                                     .otherwise(0))

# def calculate_win_rate(df):
#     # Create lagged SMA columns to detect crossovers
#     df = df.withColumn("prev_sma200", lag(col("sma200"), 1).over(Window.orderBy("timestamp")))
#     df = df.withColumn("prev_sma50", lag(col("sma50"), 1).over(Window.orderBy("timestamp")))
    
#     # Define DataFrames for buy and sell actions
#     buy_signals = df.filter(col("signal") == 1).withColumn("row_id", monotonically_increasing_id())
#     sell_signals = df.filter(col("signal") == -1).withColumn("row_id", monotonically_increasing_id())
    
#     # Alias for clarity in join
#     buy_aliased = buy_signals.alias("buy")
#     sell_aliased = sell_signals.alias("sell")
    
#     # Join on row_id with conditions to match each buy with the next sell
#     trades = buy_aliased.join(sell_aliased, 
#                               (col("buy.row_id") < col("sell.row_id")) & 
#                               (col("buy.timestamp") < col("sell.timestamp")), 
#                               "left_outer") \
#                         .select(col("buy.close").alias("buy_price"), 
#                                 col("sell.close").alias("sell_price"))
    
#     # Calculate profit for each trade
#     trades = trades.withColumn("profit", col("sell_price") - col("buy_price"))
    
#     # Calculate win rate based on profit
#     win_trades = trades.filter(col("profit") > 0).count()
#     total_trades = trades.count()
    
#     return win_trades / total_trades if total_trades != 0 else 0

# # Process partitions and calculate average win rate
# start_time = time.time()
# win_rates = df_signals.randomSplit([1.0]*10)  # Split into 10 partitions
# avg_win_rate = sum([calculate_win_rate(partition) for partition in win_rates]) / len(win_rates)

# # Print the average win rate and the processing time
# print(f"Average Win Rate: {avg_win_rate}")
# print(f"Time taken: {time.time() - start_time} seconds")

# # Stop the Spark session
# sparkSession.stop()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lag, avg, when, broadcast
# from pyspark.sql.window import Window
# import time

# # Initialize SparkSession with optimized configuration
# sparkSession = SparkSession.builder \
#     .appName("Optimized PySpark Processing") \
#     .config("spark.executor.instances", "10") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.executor.cores", "4") \
#     .config("spark.sql.shuffle.partitions", "200") \
#     .getOrCreate()

# # Read the Parquet file with optimal partitions
# df_load = sparkSession.read.parquet("hdfs://localhost:9000/user/hadoop/eth_usdt.parquet")

# # Convert the timestamp to TimestampType
# df_load = df_load.withColumn("timestamp", col("timestamp").cast("timestamp"))

# # Cache the DataFrame as it's used multiple times
# df_load.cache()

# # Define window specification for moving averages
# windowSpec200 = Window.orderBy("timestamp").rowsBetween(-199, 0)
# windowSpec50 = Window.orderBy("timestamp").rowsBetween(-49, 0)

# # Calculate moving averages
# df_with_sma = df_load.withColumn("sma200", avg(col("close")).over(windowSpec200)) \
#                      .withColumn("sma50", avg(col("close")).over(windowSpec50))

# # Identify crossover points for trading signals
# df_signals = df_with_sma.withColumn("signal", 
#                                     when(col("sma50") > col("sma200"), 1)
#                                     .when(col("sma50") < col("sma200"), -1)
#                                     .otherwise(0))

# # Cache the signals DataFrame
# df_signals.cache()

# def calculate_win_rate(df):
#     # Ensure calculations are distributed
#     df = df.withColumn("prev_sma200", lag(col("sma200"), 1).over(Window.orderBy("timestamp")))
#     df = df.withColumn("prev_sma50", lag(col("sma50"), 1).over(Window.orderBy("timestamp")))
    
#     # Calculate profits and determine win rate
#     df = df.withColumn("profit", col("close") - lag(col("close"), 1).over(Window.orderBy("timestamp")))
#     win_count = df.filter((col("profit") > 0) & (col("signal") == 1)).count()
#     total_count = df.filter(col("signal") == 1).count()
    
#     return win_count / total_count if total_count != 0 else 0

# # Process partitions and calculate average win rate
# start_time = time.time()
# avg_win_rate = calculate_win_rate(df_signals)

# # Print the average win rate and the processing time
# print(f"Average Win Rate: {avg_win_rate}")
# print(f"Time taken: {time.time() - start_time} seconds")

# # Stop the Spark session
# sparkSession.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, when
from pyspark.sql.window import Window
import time

def initialize_spark_session():
    # Initialize SparkSession with optimized configuration
    sparkSession = SparkSession.builder \
        .appName("Optimized PySpark Processing") \
        .config("spark.executor.instances", "10") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return sparkSession

def process_data(sparkSession):
    # Read the Parquet file with optimal partitions
    df_load = sparkSession.read.parquet("hdfs://localhost:9000/user/hadoop/eth_usdt.parquet")
    df_load = df_load.withColumn("timestamp", col("timestamp").cast("timestamp"))
    df_load.cache()  # Cache the DataFrame as it's used multiple times
    
    # Define window specification for moving averages
    windowSpec200 = Window.orderBy("timestamp").rowsBetween(-199, 0)
    windowSpec50 = Window.orderBy("timestamp").rowsBetween(-49, 0)
    
    # Calculate moving averages
    df_with_sma = df_load.withColumn("sma200", avg(col("close")).over(windowSpec200)) \
                         .withColumn("sma50", avg(col("close")).over(windowSpec50))
    df_signals = df_with_sma.withColumn("signal", 
                                        when(col("sma50") > col("sma200"), 1)
                                        .when(col("sma50") < col("sma200"), -1)
                                        .otherwise(0))
    df_signals.cache()
    return df_signals

def calculate_win_rate(df):
    df = df.withColumn("prev_sma200", lag(col("sma200"), 1).over(Window.orderBy("timestamp")))
    df = df.withColumn("prev_sma50", lag(col("sma50"), 1).over(Window.orderBy("timestamp")))
    df = df.withColumn("profit", col("close") - lag(col("close"), 1).over(Window.orderBy("timestamp")))
    win_count = df.filter((col("profit") > 0) & (col("signal") == 1)).count()
    total_count = df.filter(col("signal") == 1).count()
    return win_count / total_count if total_count != 0 else 0

def stress_test(iterations):
    sparkSession = initialize_spark_session()
    start_time = time.time()
    
    for i in range(iterations):
        df_signals = process_data(sparkSession)
        win_rate = calculate_win_rate(df_signals)
        print(f"Iteration {i+1}: Win Rate: {win_rate}")
    
    print(f"Total Time Taken: {time.time() - start_time} seconds")
    sparkSession.stop()

# Run the stress test for 200 iterations
stress_test(10)
