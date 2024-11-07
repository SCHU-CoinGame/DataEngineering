import multiprocessing as mp
import pyupbit
import datetime
import os
import json
from kafka import KafkaProducer
import psycopg2

KAFKA_TOPIC = 'upbit-ticker-data'
KAFKA_BROKER = 'spark-worker-panda-01:9092, spark-worker-panda-02:9092, spark-worker-panda-03:9092, spark-worker-panda-04:9092'

def default_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=default_converter).encode('utf-8'),
    retries=5,  # 재시도 횟수 설정
    acks='all'  # 모든 복제본에 메시지 도달 시 성공으로 처리
)

def get_market_codes():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="upbit_db",
            user="ilhan",
            password="000411"
        )
        cur = conn.cursor()

        cur.execute("SELECT market FROM market_codes")
        rows = cur.fetchall()

        market_codes = [row[0] for row in rows]

        cur.close()
        conn.close()

        return market_codes

    except Exception as e:
        print(f"Error fetching market codes: {e}")
        return []

def on_send_error(excp):
    print(f"Error sending message: {excp}")

def collect_save_data(queue):
    while True:
        data = queue.get()
        data['event_time'] = datetime.datetime.now()

        producer.send(KAFKA_TOPIC, data).add_errback(on_send_error)


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
