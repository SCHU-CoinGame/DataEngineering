from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, FloatType, BooleanType
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

influxdb_url = "http://spark-master-panda-01:8086"
influxdb_token = "jCKAgbR5JWKVOoJCBRNMmNyUcTwNgFf_r0hLEeyHCTegaCqIkjRWRu41---uvwhbnCpn1rK2Jr4oi8BLJfLowA=="
influxdb_org = "spark"
influxdb_bucket = "spark"

client = InfluxDBClient(
    url=influxdb_url,
    token=influxdb_token,
    org=influxdb_org
)

write_api = client.write_api(write_options=SYNCHRONOUS)

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingToInfluxDB") \
    .config("spark.jars", "/home/spark/spark-3.1.2-bin-hadoop3.2/jars/commons-pool2-2.11.1.jar") \
    .config("spark.executor.memory", "3g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "3") \
    .config("spark.driver.memory", "1g") \
    .config("spark.master", "yarn") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "spark-master-panda-01") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

kafka_bootstrap_servers = "spark-worker-panda-01:9092,spark-worker-panda-02:9092,spark-worker-panda-03:9092,spark-worker-panda-04:9092"
kafka_topic = "upbit-ticker-data"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType() \
    .add("type", StringType()) \
    .add("code", StringType()) \
    .add("opening_price", DoubleType()) \
    .add("high_price", DoubleType()) \
    .add("low_price", DoubleType()) \
    .add("trade_price", DoubleType()) \
    .add("prev_closing_price", DoubleType()) \
    .add("change", StringType()) \
    .add("change_price", DoubleType()) \
    .add("signed_change_price", DoubleType()) \
    .add("change_rate", DoubleType()) \
    .add("signed_change_rate", DoubleType()) \
    .add("trade_volume", DoubleType()) \
    .add("acc_trade_price", DoubleType()) \
    .add("acc_trade_price_24h", DoubleType()) \
    .add("acc_trade_volume", DoubleType()) \
    .add("acc_trade_volume_24h", DoubleType()) \
    .add("highest_52_week_price", DoubleType()) \
    .add("lowest_52_week_price", DoubleType()) \
    .add("market_state", StringType()) \
    .add("trade_timestamp", LongType()) \
    .add("is_trading_suspended", BooleanType())

json_df = df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), schema).alias("data")).select("data.*")

def write_to_influxdb(batch_df, batch_id):
    records = batch_df.collect()
    for record in records:
        point = Point("upbit_ticker") \
            .tag("type", record["type"]) \
            .tag("code", record["code"]) \
            .tag("market_state", record["market_state"]) \
            .tag("change", record["change"]) \
            .field("opening_price", record["opening_price"]) \
            .field("high_price", record["high_price"]) \
            .field("low_price", record["low_price"]) \
            .field("trade_price", record["trade_price"]) \
            .field("prev_closing_price", record["prev_closing_price"]) \
            .field("change_price", record["change_price"]) \
            .field("signed_change_price", record["signed_change_price"]) \
            .field("change_rate", record["change_rate"]) \
            .field("signed_change_rate", record["signed_change_rate"]) \
            .field("trade_volume", record["trade_volume"]) \
            .field("acc_trade_price", record["acc_trade_price"]) \
            .field("acc_trade_price_24h", record["acc_trade_price_24h"]) \
            .field("acc_trade_volume", record["acc_trade_volume"]) \
            .field("acc_trade_volume_24h", record["acc_trade_volume_24h"]) \
            .field("highest_52_week_price", record["highest_52_week_price"]) \
            .field("lowest_52_week_price", record["lowest_52_week_price"]) \
            .field("is_trading_suspended", record["is_trading_suspended"]) \
            .time(record["trade_timestamp"], WritePrecision.MS)

        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)


query = json_df.writeStream \
    .foreachBatch(write_to_influxdb) \
    .outputMode("append") \
    .start()

query.awaitTermination()