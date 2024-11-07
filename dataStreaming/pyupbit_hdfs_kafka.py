import multiprocessing as mp
import pyupbit
import datetime
import os
import json
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import psycopg2

os.environ['SPARK_HOME'] = '/home/spark/spark-3.1.2-bin-hadoop3.2'
os.environ['HADOOP_CONF_DIR'] = '/home/spark/spark-3.1.2-bin-hadoop3.2/conf2'
os.environ['YARN_CONF_DIR'] = '/home/spark/spark-3.1.2-bin-hadoop3.2/conf2'

spark = SparkSession.builder \
    .appName("UpbitWebSocketToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://spark-master-panda-01:9000") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.driver.memory", "2g") \
    .config("spark.master", "yarn") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "spark-master-panda-01") \
    .getOrCreate()

schema = "type STRING, code STRING, opening_price DOUBLE, high_price DOUBLE, low_price DOUBLE, trade_price DOUBLE, prev_closing_price DOUBLE," \
         "change STRING, change_price DOUBLE, signed_change_price DOUBLE, change_rate DOUBLE, signed_change_rate DOUBLE, trade_volume DOUBLE," \
         "acc_trade_price DOUBLE, acc_trade_price_24h DOUBLE, acc_trade_volume DOUBLE, acc_trade_volume_24h DOUBLE, trade_date STRING, trade_time STRING, trade_timestamp LONG, ask_bid STRING," \
         "acc_ask_volume DOUBLE, acc_bid_volume DOUBLE, highest_52_week_price DOUBLE, hightes_52_week_date STRING, lowest_52_week_price DOUBLE, lowest_52_week_date STRING," \
         "trade_status STRING, market_state STRING, market_state_for_ios STRING, is_trading_suspended BOOLEAN, delisting_date DATE, timestamp LONG, stream_type STRING, event_time TIMESTAMP"

KAFKA_TOPIC = 'upbit-ticker-data'
KAFKA_BROKER = 'spark-worker-panda-01:9092, spark-worker-panda-02:9092, spark-worker-panda-03:9092, spark-worker-panda-04:9092'


def default_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()


# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v, default=default_converter).encode('utf-8')
# )

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


def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")

def on_send_error(excp):
    print(f"Error sending message: {excp}")


def save_to_hdfs(collected_data):
    df = spark.createDataFrame(collected_data, schema=schema)
    df.show()

    try:
        df.write.mode("append").parquet("hdfs://spark-master-panda-01:9000/finVizor/upbit/ticker_data/")
        print("Data successfully written to HDFS.")
        return True  # 성공적으로 저장했음을 나타냄
    except Exception as e:
        print(f"Error writing to HDFS: {e}")
        return False  # 오류 발생 시 False 반환

def collect_save_data(queue, batch_size):
    collected_data = []

    while True:
        data = queue.get()
        data['event_time'] = datetime.datetime.now()
        collected_data.append(data)

        producer.send(KAFKA_TOPIC, data).add_callback(on_send_success).add_errback(on_send_error)

        if len(collected_data) >= batch_size:
            if save_to_hdfs(collected_data):
                collected_data = []  # 성공적으로 저장했을 때만 리스트 초기화


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

    collect_save_data(queue, 20)

    producer.flush()
    producer.close()
