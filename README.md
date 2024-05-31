# Crypto-Arbitrage: Real-Time Crypto Analysis and Recommendation Platform

## Introduction

Crypto-Arbitrage is a real-time cryptocurrency analysis and recommendation platform designed to detect arbitrage opportunities. The project is divided into two main parts: real-time processing using Kafka and batch processing using PySpark.

## Prerequisites

Ensure you have the following installed:
- Docker
- Docker Compose
- Python 3.8 or later

Clone the repository:

```bash
git clone https://github.com/white07S/Crypto-Arbitrage.git
cd Crypto-Arbitrage
```

Start the Docker containers:

```bash
docker-compose up -d
```

## Project Structure

The project has two main components:
1. Real-time processing using Kafka
2. Batch processing using PySpark

## Part 1: Real-Time Processing Using Kafka

### Configuration

Define the configuration for real-time processing:

```json
{
    "providers": ["binance", "bingx"],
    "coins": ["ETH/USDT", "BTC/USDT", "SOL/USDT"],
    "time_sleep": 5
}
```

- `providers`: List of crypto providers to check for arbitrage opportunities.
- `coins`: List of coin pairs to monitor.
- `time_sleep`: Time interval (in seconds) between data fetches.

### Running the Real-Time Setup

1. **Kafka Producer**

   In the first terminal, start the Kafka producer to generate tick data:

   ```bash
   python app/real_time/producer.py
   ```

2. **Kafka Consumer**

   In the second terminal, start the Kafka consumer to analyze data and detect arbitrage opportunities:

   ```bash
   python app/real_time/consumer.py
   ```

### Consumer Functionality

- **200 EMA Cache**: The consumer creates a 200 EMA cache and checks the price above or below to decide Buy or Sell.
- **Arbitrage Detection**: It checks the change and difference between two providers for the same coin.

## Part 2: Batch Processing Using PySpark

### Data Download

Download historical data for the coins listed below. Note that this process is time-consuming and should not be repeated frequently.

Coins: `['BNB', 'BTC', 'DOGE', 'ETH', 'SOL', 'USDC', 'XRP']`

### Writing Data to HDFS

Create a Spark session and write the data into HDFS:

```bash
python app/batch_processing/session_manager.py
```

Data will be written to HDFS in the format: `hadoop/data/coin/year.parquet`.

### Batch Processing Options

1. **Analytics**

   Run analytics for a specific coin and year:

   ```bash
   python app/batch_processing/analytics.py BTC 2020
   ```

   Example output:

   ```
   Maximum Price in the Year: 2021-01-01 with $29470.0
   Minimum Price in the Year: 2020-03-13 with $3810.78
   Maximum Daily Change: 2020-12-17 with a change of $2258.329999999998
   Highest Volatility Day: 2020-03-12 with a volatility of 857.492124890834
   ```

2. **Anomaly Detection**

   Detect anomalies in the data:

   ```bash
   python app/batch_processing/anomaly_detection.py BTC 2020 0.01 1d
   ```

   - `0.01`: Threshold for detecting anomalies (1% change).
   - `1d`: Resampling interval (can be changed to 1h, 30min, 15min, or 5min).

3. **Correlation Analysis**

   Analyze correlations between multiple coins:

   ```bash
   python app/batch_processing/correlation.py 2022 hdfs://localhost:9000/user/hadoop/data BNB BTC DOGE ETH SOL USDC XRP
   ```

   Example output:

   ```
   Correlation between BNB and BTC: 0.9095010969615606
   Correlation between BNB and DOGE: 0.9292182337933099
   Correlation between BNB and ETH: 0.9602063533921824
   Correlation between BNB and SOL: 0.9285391870972846
   ```

4. **Read Data**

   Read and display the first 20 rows of data for a specific coin and year:

   ```bash
   python app/batch_processing/read_data.py BTC 2020
   ```

## Testing

### Unit Tests

Unit tests are provided for the following components:
- Kafka Consumer
- Kafka Producer
- Spark Session
- Spark Writing

Run unit tests using `pytest`:

```bash
pytest tests/unit
```

### Performance Tests

Performance tests for the Kafka producer and consumer are included to check the processing time for 1 million messages.

Run performance tests in separate terminals:

1. Producer:

   ```bash
   python tests/performance/test_producer.py
   ```

2. Consumer:

   ```bash
   python tests/performance/test_consumer.py
   ```

Additionally, a Spark correlation test checks if the processing is done under 60 seconds.

## Further Improvements

- Implement MLlib for creating a pipeline for training and real-time ML-based recommendations.
- Add more tests for PySpark performance analysis.

## Stopping and Removing Docker Containers

To stop and remove all running Docker containers with one command:

```bash
docker stop $(docker ps -aq) && docker rm -f $(docker ps -aq)
```
