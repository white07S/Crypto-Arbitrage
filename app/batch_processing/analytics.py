import argparse
from pyspark.sql.functions import col, unix_timestamp, from_unixtime, max, min, abs as abs_col, stddev_pop, lag
from pyspark.sql.window import Window
from spark_utils import create_spark_session, read_data

def resample_data(df):
    df = df.withColumn("timestamp", unix_timestamp("timestamp"))
    df = df.withColumn("date", from_unixtime(col("timestamp"), 'yyyy-MM-dd'))
    daily_df = df.groupBy("date").agg(
        max("close").alias("max_price"),
        min("close").alias("min_price"),
        stddev_pop("close").alias("volatility")
    )
    return daily_df

def calculate_daily_changes(df):
    windowSpec = Window.orderBy("date")
    df = df.withColumn("prev_max_price", lag("max_price").over(windowSpec))
    df = df.withColumn("daily_change", abs_col(col("max_price") - col("prev_max_price")))
    return df

def display_analysis_results(df):
    max_price_row = df.orderBy(col("max_price").desc()).first()
    min_price_row = df.orderBy(col("min_price").asc()).first()
    max_change_row = df.orderBy(col("daily_change").desc()).first()
    max_volatility_row = df.orderBy(col("volatility").desc()).first()

    print(f"Maximum Price in the Year: {max_price_row['date']} with ${max_price_row['max_price']}")
    print(f"Minimum Price in the Year: {min_price_row['date']} with ${min_price_row['min_price']}")
    print(f"Maximum Daily Change: {max_change_row['date']} with a change of ${max_change_row['daily_change']}")
    print(f"Highest Volatility Day: {max_volatility_row['date']} with a volatility of {max_volatility_row['volatility']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze cryptocurrency daily data.')
    parser.add_argument('coin_name', type=str, help='The name of the cryptocurrency coin (e.g., BTC)')
    parser.add_argument('year', type=str, help='The year of the data to process (e.g., 2020)')

    args = parser.parse_args()

    spark = create_spark_session("CryptoDailyAnalysis")
    df = read_data(spark, f'hdfs://localhost:9000/user/hadoop/data/{args.coin_name}/{args.year}.parquet')
    daily_df = resample_data(df)
    daily_changes_df = calculate_daily_changes(daily_df)
    display_analysis_results(daily_changes_df)
