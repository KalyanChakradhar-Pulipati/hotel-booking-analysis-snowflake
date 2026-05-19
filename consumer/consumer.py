import time
import json
import boto3
from kafka import KafkaConsumer

consumer = KafkaConsumer('stock_prices',
                         bootstrap_servers=['host.docker.internal:29092'], 
                         enable_auto_commit=True,
                         auto_offset_reset='earliest',  
                         group_id='bronze-consumers', 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
s3_client = boto3.client('s3', endpoint_url='http://localhost:9002', aws_access_key_id='admin', aws_secret_access_key='password123')
bucket_name = 'bronze-stock-data-bucket'

print("Consumerstreaming and saving data to MinIO....")

#Main loop to consume messages from Kafka and save to MinIO
for message in consumer:
    record = message.value
    symbol = record.get('symbol')
    ts=record.get('fetched_at', int(time.time()))
    key = f"{symbol}/{ts}.json"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(record),ContentType='application/json')
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")    

