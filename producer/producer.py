import time
import json
import requests
from kafka import KafkaProducer

Api_key="d80d3hpr01qq9ln3ect0d80d3hpr01qq9ln3ectg"
Base_url="https://finnhub.io/api/v1/quote"
symbols=["AAPL","GOOGL","MSFT","AMZN","TSLA"]

producer = KafkaProducer(bootstrap_servers=['host.docker.internal:29092']
                          ,value_serializer=lambda  x: json.dumps(x).encode('utf-8'))
def get_stock_price(symbol):
    url = f"{Base_url}?symbol={symbol}&token={Api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            data['symbol'] = symbol  # Add the symbol to the data
            data['fetched_at'] = int(time.time())  # Add the timestamp to the data
            return data   # 'c' is the current price
        else:
            print(f"Error fetching data for {symbol}: {response.status_code}")
            return None
    except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None
while True:
    for symbol in symbols:
        stock_data = get_stock_price(symbol)
        if stock_data is not None:
            print(f"Processing data for {symbol}: {stock_data}")
            producer.send('stock_prices', value=stock_data)
            print(f"Sent data for {symbol}: {stock_data}")
    time.sleep(6)  # Wait for 6 seconds before fetching the next set of prices