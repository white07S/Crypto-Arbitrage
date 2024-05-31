import ccxt
import pandas as pd
from datetime import datetime

# Function to fetch data for a specific year
def fetch_yearly_data(exchange, symbol, year):
    since = exchange.parse8601(f'{year}-01-01T00:00:00Z')
    end = exchange.parse8601(f'{year+1}-01-01T00:00:00Z')
    all_data = []

    while since < end:
        try:
            data = exchange.fetch_ohlcv(symbol, '1m', since)
            if not data:
                break
            since = data[-1][0] + 60 * 1000  # increment by one minute in milliseconds
            all_data.extend(data)
        except ccxt.NetworkError as e:
            print(f'Network error occurred: {e}')
        except ccxt.ExchangeError as e:
            print(f'Exchange error occurred: {e}')
        except Exception as e:
            print(f'An error occurred: {e}')

    df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    return df

# Setup exchange
exchange = ccxt.binance({
    'rateLimit': 1200,
    'enableRateLimit': True
})

symbol = 'DOGE/USDT'

current_year = datetime.utcnow().year
start_year = current_year - 5

# Fetch data year by year
for year in range(start_year, current_year):
    print(f'Fetching data for year: {year}')
    df_year = fetch_yearly_data(exchange, symbol, year)
    df_year.to_csv(f'DOGEUSDT_{year}.csv')
    print(f'Data for year {year} saved successfully.')

print('All data fetched and saved.')

