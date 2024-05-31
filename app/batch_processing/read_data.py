import argparse
from spark_utils import create_spark_session, read_data

def read_crypto_data(spark, coin_name, year):
    data_path = f'hdfs://localhost:9000/user/hadoop/data/{coin_name}/{year}.parquet'
    try:
        df = read_data(spark, data_path)
        print(f"Data successfully read from {data_path}")
        return df
    except Exception as e:
        print(f"Failed to read data from {data_path}: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read cryptocurrency data.')
    parser.add_argument('coin_name', type=str, help='The name of the cryptocurrency coin (e.g., BTC)')
    parser.add_argument('year', type=str, help='The year of the data to read (e.g., 2020)')

    args = parser.parse_args()

    spark = create_spark_session("CryptoDataRead")
    df = read_crypto_data(spark, args.coin_name, args.year)
    if df is not None:
        df.show()
