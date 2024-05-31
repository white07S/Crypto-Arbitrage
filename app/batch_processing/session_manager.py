import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from app.batch_processing.spark_utils import create_spark_session, read_data
import os

def read_and_write_data(spark, base_path, coin_name):
    data_path = os.path.join(base_path, coin_name)
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            year = filename.split('_')[-1].split('.')[0]
            df = spark.read.csv(os.path.join(data_path, filename), header=True, inferSchema=True)
            output_path = f'hdfs://localhost:9000/user/hadoop/data/{coin_name}/{year}.parquet'
            df.write.mode('overwrite').parquet(output_path)
            print(f"Data written to {output_path} for {coin_name} of year {year}")

def process_all_coins(spark, base_path):
    coins = ['BNB', 'BTC', 'DOGE', 'ETH', 'SOL', 'USDC', 'XRP']
    for coin in coins:
        read_and_write_data(spark, base_path, coin)

if __name__ == "__main__":
    spark = create_spark_session("CryptoDataProcessing")
    current_dir = os.getcwd()
    base_path = os.path.join(current_dir, "app", "data")
    process_all_coins(spark, base_path)
