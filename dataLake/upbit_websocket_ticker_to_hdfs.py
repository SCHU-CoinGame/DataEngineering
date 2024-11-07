import datetime
import os

import json
from websocket import create_connection, WebSocketConnectionClosedException
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from kafka import KafkaProducer
import psycopg2
import time

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

schema = "type STRING, code STRING, opening_price DOUBLE, high_price DOUBLE, low_price DOUBLE, trade_price DOUBLE, prev_closing_price DOUBLE,"\
        "change STRING, change_price DOUBLE, signed_change_price DOUBLE, change_rate DOUBLE, signed_change_rate DOUBLE, trade_volume DOUBLE,"\
        "acc_trade_price DOUBLE, acc_trade_price_24h DOUBLE, acc_trade_volume DOUBLE, acc_trade_volume_24h DOUBLE, trade_date STRING, trade_time STRING, trade_timestamp LONG, ask_bid STRING,"\
        "acc_ask_volume DOUBLE, acc_bid_volume DOUBLE, highest_52_week_price DOUBLE, hightes_52_week_date STRING, lowest_52_week_price DOUBLE, lowest_52_week_date STRING,"\
        "trade_status STRING, market_state STRING, market_state_for_ios STRING, is_trading_suspended BOOLEAN, delisting_date DATE, timestamp LONG, stream_type STRING, event_time TIMESTAMP"

KAFKA_TOPIC = 'upbit-ticker-data'
KAFKA_BROKER = 'spark-worker-panda-01:9092, spark-worker-panda-02:9092, spark-worker-panda-03:9092, spark-worker-panda-04:9092'

def default_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=default_converter).encode('utf-8')
)

# PostgreSQL에 연결하여 market_codes 테이블에서 데이터를 가져오는 함수
def get_market_codes():
    try:
        # PostgreSQL 연결 설정
        conn = psycopg2.connect(
            host="localhost",
            database="upbit_db",
            user="ilhan",
            password="000411"
        )
        cur = conn.cursor()

        # market_codes 테이블에서 모든 마켓 코드를 가져오는 쿼리
        cur.execute("SELECT market FROM market_codes")
        rows = cur.fetchall()

        # 마켓코드를 리스트로 변환
        market_codes = [row[0] for row in rows]

        cur.close()
        conn.close()

        return market_codes

    except Exception as e:
        print(f"Error fetching market codes: {e}")
        return []

# UPBIT websocket을 통해 암호화폐 ticker - 현재가 데이터를 가져오는 함수
def collect_save_data(batch_size):
    market_codes = get_market_codes()
    if not market_codes:
        print("no Market codes available.")
        return
    try:
        ws = create_connection("wss://api.upbit.com/websocket/v1")
        ws.send(json.dumps([
            {"ticket": "ticker-test"},
            {"type": "ticker", "codes": market_codes, "format": "SIMPLE"} # real-time
        ]))
    except Exception as e:
        print(f"Error connection to WebSocket: {e}")
        return

    collected_data = []

    while True:
        result = ws.recv()
        data = json.loads(result)
        fields_to_convert = ["trade_price", "change_rate", "signed_change_rate", "acc_bid_volume", "acc_ask_volume"]

        for key in fields_to_convert:
            if key in data and isinstance(data[key], int):
                data[key] = float(data[key])

        data['event_time'] = datetime.datetime.now()
        collected_data.append(data)

        producer.send(KAFKA_TOPIC, data)

        if len(collected_data) >= batch_size:
            df = spark.createDataFrame(collected_data, schema=schema)
            df.show()
            try:
                df.write.mode("append").parquet("hdfs://spark-master-panda-01:9000/finVizor/upbit/ticker_data/")
                collected_data = []
            except Exception as e:
                print(f"Error writing to HDFS: {e}")

if __name__ == "__main__":
    collect_save_data(20)

    producer.flush()
    producer.close()