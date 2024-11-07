from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType, TimestampType, StructField
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
    .appName("KafkaSparkStreamingToInfluxAndHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://spark-master-panda-01:9000") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "3") \
    .config("spark.driver.memory", "1g") \
    .config("spark.master", "yarn") \
    .getOrCreate()

kafka_bootstrap_servers = "spark-worker-panda-01:9092,spark-worker-panda-02:9092,spark-worker-panda-03:9092,spark-worker-panda-04:9092"
kafka_topic = "upbit-ticker-data"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("type", StringType(), True),
    StructField("code", StringType(), True),
    StructField("opening_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("trade_price", DoubleType(), True),
    StructField("prev_closing_price", DoubleType(), True),
    StructField("change", StringType(), True),
    StructField("change_price", DoubleType(), True),
    StructField("signed_change_price", DoubleType(), True),
    StructField("change_rate", DoubleType(), True),
    StructField("signed_change_rate", DoubleType(), True),
    StructField("trade_volume", DoubleType(), True),
    StructField("acc_trade_price", DoubleType(), True),
    StructField("acc_trade_price_24h", DoubleType(), True),
    StructField("acc_trade_volume", DoubleType(), True),
    StructField("acc_trade_volume_24h", DoubleType(), True),
    StructField("highest_52_week_price", DoubleType(), True),
    StructField("lowest_52_week_price", DoubleType(), True),
    StructField("market_state", StringType(), True),
    StructField("trade_timestamp", LongType(), True),
    StructField("is_trading_suspended", BooleanType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

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

query_hdfs = json_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://spark-master-panda-01:9000/finVizor/upbit/ticker_data/") \
    .option("checkpointLocation", "hdfs://spark-master-panda-01:9000/checkpoints/upbit") \
    .start()

query_influxdb = json_df.writeStream \
    .foreachBatch(write_to_influxdb) \
    .outputMode("append") \
    .start()

query_influxdb.awaitTermination()
query_hdfs.awaitTermination()

