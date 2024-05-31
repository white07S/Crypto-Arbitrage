import argparse
from pyspark.sql.functions import col, lag, unix_timestamp, from_unixtime, avg
from pyspark.sql.window import Window
from spark_utils import create_spark_session, read_data

def resample_data(df, timeframe):
    seconds_map = {
        '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '3h': 10800, '1d': 86400
    }
    seconds = seconds_map.get(timeframe, 86400)  # default to '1d'
    
    df = df.withColumn("timestamp", unix_timestamp("timestamp"))
    df = df.withColumn("time_window", (col("timestamp") / seconds).cast("integer") * seconds)
    df = df.groupBy("time_window").agg(avg("close").alias("close"))
    df = df.withColumn("time_window", from_unixtime("time_window"))
    return df

def calculate_returns_and_detect_anomalies(df, anomaly_percent):
    windowSpec = Window.orderBy("time_window")
    df = df.withColumn("prev_close", lag("close").over(windowSpec))
    df = df.withColumn("return", (col("close") - col("prev_close")) / col("prev_close"))

    threshold = anomaly_percent / 100
    anomalies = df.filter((col("return") > threshold) | (col("return") < -threshold))
    return anomalies.count()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Detect anomalies in cryptocurrency data.')
    parser.add_argument('coin_name', type=str, help='The name of the cryptocurrency coin (e.g., BTC)')
    parser.add_argument('year', type=str, help='The year of the data to process (e.g., 2020)')
    parser.add_argument('anomaly_percent', type=float, help='The percentage threshold for anomaly detection (e.g., 0.01)')
    parser.add_argument('timeframe', type=str, help='The timeframe for resampling (e.g., 1d, 1h, 5m)')

    args = parser.parse_args()

    spark = create_spark_session("CryptoDataAnomalyDetection")
    df = read_data(spark, f'hdfs://localhost:9000/user/hadoop/data/{args.coin_name}/{args.year}.parquet')
    df_resampled = resample_data(df, args.timeframe)
    num_anomalies = calculate_returns_and_detect_anomalies(df_resampled, args.anomaly_percent)
    print(f"Number of anomalies detected: {num_anomalies}")

'python app/batch_processing/anomaly_detection.py BTC 2020 0.01 1d'