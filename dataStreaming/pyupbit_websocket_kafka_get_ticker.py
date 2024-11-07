import multiprocessing as mp
import pyupbit
import datetime
import os
import json
from kafka import KafkaProducer

KAFKA_TOPIC = 'upbit-ticker-data'
KAFKA_BROKER = 'spark-worker-panda-01:9092, spark-worker-panda-02:9092, spark-worker-panda-03:9092, spark-worker-panda-04:9092'

def default_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=default_converter).encode('utf-8'),
    retries=5,
    acks='all'
)

def on_send_success(record_metadata):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Message sent to {record_metadata.topic} partition {record_metadata.partition}")

def on_send_error(excp):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Error sending message: {excp}")

def get_market_codes():
    try:
        return pyupbit.get_tickers()
    except Exception as e:
        print(f"Error fetching market codes: {e}")
        return []

def collect_save_data(queue):
    while True:
        data = queue.get()

        producer.send(KAFKA_TOPIC, data).add_callback(on_send_success).add_errback(on_send_error)

if __name__ == "__main__":
    queue = mp.Queue()

    market_codes = get_market_codes()
    if not market_codes:
        print("No market codes available.")
        exit(1)

    proc = mp.Process(
        target=pyupbit.WebSocketClient,
        args=('ticker', market_codes, queue),
        daemon=True
    )
    proc.start()


    collect_save_data(queue)

    producer.flush()
    producer.close()
