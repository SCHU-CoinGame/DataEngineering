import boto3
import decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('dynamoDB_upbit_table')

spark = SparkSession.builder \
    .appName("KafkaSparkStreamingToDynamoDBAndS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "4") \
    .config("spark.streaming.concurrentJobs", "4") \
    .config("spark.driver.memory", "4g") \
    .config("spark.master", "yarn") \
    .getOrCreate()

kafka_bootstrap_servers = "spark-worker-panda-01:9092,spark-worker-panda-02:9092,spark-worker-panda-03:9092,spark-worker-panda-04:9092"
kafka_topic = "upbit-ticker-data"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("group.id", "upbit-consumer-group") \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType([
    StructField("code", StringType(), True),
    StructField("trade_date", StringType(), True),
    StructField("trade_time", StringType(), True),
    StructField("trade_timestamp", LongType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("trade_price", DoubleType(), True),
    StructField("change", StringType(), True),
    StructField("change_price", DoubleType(), True),
    StructField("change_rate", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

def write_to_dynamodb(batch_df, batch_id):
    selected_df = batch_df.select(
        col("code"),
        col("trade_timestamp"),
        col("timestamp"),
        col("high_price"),
        col("low_price"),
        col("trade_price"),
        col("change"),
        col("change_price"),
        col("change_rate")
    )

    records = selected_df.collect()

    for record in records:
        try:
            item = {
                "code": record["code"],
                "trade_timestamp": record["trade_timestamp"],
                "timestamp": record["timestamp"],
                "high_price": decimal.Decimal(str(record["high_price"])),
                "low_price": decimal.Decimal(str(record["low_price"])),
                "trade_price": decimal.Decimal(str(record["trade_price"])),
                "change": record["change"],
                "change_price": decimal.Decimal(str(record["change_price"])),
                "change_rate": decimal.Decimal(str(record["change_rate"]))
            }

            table.put_item(Item=item)
        except Exception as e:
            print(f"Failed to write record to DynamoDB: {e}")

query_dynamodb = json_df.writeStream \
    .foreachBatch(write_to_dynamodb) \
    .outputMode("append") \
    .start()

query_dynamodb.awaitTermination()

