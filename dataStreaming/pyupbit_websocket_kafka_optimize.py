from kafka import KafkaProducer
import json
import multiprocessing as mp
import pyupbit
import datetime

KAFKA_TOPIC = 'upbit-ticker-data'
KAFKA_BROKER = 'spark-worker-panda-01:9092, spark-worker-panda-02:9092, spark-worker-panda-03:9092, spark-worker-panda-04:9092'

def get_market_codes():
    try:
        return pyupbit.get_tickers()
    except Exception as e:
        print(f"Error fetching market codes: {e}")
        return []

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=0,
    batch_size=16384,
    buffer_memory=33554432,
    retries=5,
    acks='all'
)

def on_send_success(record_metadata):
    print(f"[SUCCESS] Sent to {record_metadata.topic} partition {record_metadata.partition}")

def on_send_error(excp):
    print(f"[ERROR] Failed to send message: {excp}")

def websocket_producer_worker(market_codes):
    queue = mp.Queue()
    proc = mp.Process(target=pyupbit.WebSocketClient, args=('ticker', market_codes, queue), daemon=True)
    proc.start()

    while True:
        data = queue.get()
        producer.send(KAFKA_TOPIC, data).add_callback(on_send_success).add_errback(on_send_error)

if __name__ == "__main__":
    market_codes = get_market_codes()
    websocket_producer_worker(market_codes)
    producer.flush()
    producer.close()
