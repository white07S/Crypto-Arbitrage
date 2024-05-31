import argparse
from pyspark.sql.functions import col, to_date, avg, lit
import os
import sys

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(module_path)

from app.batch_processing.spark_utils import create_spark_session, read_data

def load_and_aggregate_data(spark, year, base_path, coin):
    path = f"{base_path}/{coin}/{year}.parquet"
    df = read_data(spark, path)
    df = df.withColumn("date", to_date(col("timestamp")))
    daily_df = df.groupBy("date").agg(avg("close").alias("daily_close"))
    daily_df = daily_df.withColumn("coin", lit(coin))
    return daily_df

def calculate_correlations(spark, combined_df):
    coins = combined_df.select("coin").distinct().rdd.flatMap(lambda x: x).collect()
    results = []
    for i in range(len(coins)):
        for j in range(i + 1, len(coins)):
            df1 = combined_df.filter(col("coin") == coins[i])
            df2 = combined_df.filter(col("coin") == coins[j])
            df1 = df1.withColumnRenamed("daily_close", f"{coins[i]}_close")
            df2 = df2.withColumnRenamed("daily_close", f"{coins[j]}_close")
            combined = df1.join(df2, "date")
            correlation = combined.stat.corr(f"{coins[i]}_close", f"{coins[j]}_close")
            results.append((coins[i], coins[j], correlation))
    for result in results:
        print(f"Correlation between {result[0]} and {result[1]}: {result[2]}")

def process_year_data(spark, year, base_path, coins):
    daily_dfs = []
    for coin in coins:
        daily_df = load_and_aggregate_data(spark, year, base_path, coin)
        if daily_df is not None:
            daily_dfs.append(daily_df)
    if daily_dfs:
        combined_df = daily_dfs[0]
        for df in daily_dfs[1:]:
            combined_df = combined_df.union(df)
        calculate_correlations(spark, combined_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process and analyze cryptocurrency data.')
    parser.add_argument('year', type=str, help='The year of the data to process (e.g., 2022)')
    parser.add_argument('base_path', type=str, help='The base path of the data in HDFS (e.g., hdfs://localhost:9000/user/hadoop/data)')
    parser.add_argument('coins', type=str, nargs='+', help='A list of coins to process (e.g., BNB BTC DOGE ETH SOL USDC XRP)')

    args = parser.parse_args()

    spark = create_spark_session("CryptoCorrelationAnalysis")
    process_year_data(spark, args.year, args.base_path, args.coins)

'python app/batch_processing/correlation.py 2022 hdfs://localhost:9000/user/hadoop/data BNB BTC DOGE ETH SOL USDC XRP'