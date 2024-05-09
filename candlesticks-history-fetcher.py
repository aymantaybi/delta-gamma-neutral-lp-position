import time
import requests
import datetime
from dotenv import load_dotenv
import os
import psycopg2
import concurrent.futures

load_dotenv()

connection = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
)

cursor = connection.cursor()

insert_query = """
INSERT INTO candlesticks (timestamp, open_price, highest_price, lowest_price, close_price, volume, volume_currency, volume_currency_quote, confirm) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (timestamp)
DO NOTHING;
"""


def fetch_candlestick_data(from_timestamp):
    before = from_timestamp
    after = before + 101 * 1000
    print(before, after)
    base_url = "https://www.okx.com/api/v5/market/history-candles"
    params = {
        "instId": "RON-USDT-SWAP",
        "before": before,
        "after": after,
        "bar": "1s",
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data: Status code", response.status_code)
        return None


start_timestamp = 1704908200000  # 1702339200000

stop_timestamp = 1705017600000

step = 100_000

count = 10

for timestamp in range(start_timestamp, stop_timestamp, step * count):
    from_timestamps = [timestamp + step * i for i in range(count)]
    print(from_timestamps)
    time.sleep(1)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        records = []
        for from_timestamp in from_timestamps:
            futures.append(
                executor.submit(fetch_candlestick_data, from_timestamp=from_timestamp)
            )
        for future in concurrent.futures.as_completed(futures):
            response_data = future.result()
            data = response_data["data"]
            records = records + data
        cursor.executemany(insert_query, records)
        connection.commit()
        print("inserted data", len(records))
