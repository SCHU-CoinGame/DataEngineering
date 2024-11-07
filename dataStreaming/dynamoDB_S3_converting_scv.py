import boto3
import decimal
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder \
    .appName("DynamoDBToS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.driver.memory", "4g") \
    .config("spark.master", "yarn") \
    .getOrCreate()

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('dynamoDB_upbit_table')

def get_dynamodb_data():
    segment_count = 10
    all_data = []

    for segment in range(segment_count):
        response = table.scan(Segment=segment, TotalSegments=segment_count)
        data = response['Items']
        all_data.extend(data)

        while 'LastEvaluatedKey' in response:
            response = table.scan(Segment=segment, TotalSegments=segment_count, ExclusiveStartKey=response['LastEvaluatedKey'])
            all_data.extend(response['Items'])

    return all_data

def dynamodb_to_spark_df(data):
    # 필요한 스키마 정의
    schema = StructType([
        StructField("code", StringType(), True),
        StructField("trade_timestamp", LongType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("trade_price", DoubleType(), True),
        StructField("change", StringType(), True),
        StructField("change_price", DoubleType(), True),
        StructField("change_rate", DoubleType(), True)
    ])

    rows = [Row(
        code=item["code"],
        trade_timestamp=int(item["trade_timestamp"]),
        high_price=float(item["high_price"]),
        low_price=float(item["low_price"]),
        trade_price=float(item["trade_price"]),
        change=item["change"],
        change_price=float(item["change_price"]),
        change_rate=float(item["change_rate"])
    ) for item in data]

    return spark.createDataFrame(rows, schema)

dynamodb_data = get_dynamodb_data()

df = dynamodb_to_spark_df(dynamodb_data)
s3_output_path = "s3a://aws-s3-bucket-fastcampus/dynamoDB_upbit_data/"

df.write.mode("overwrite").csv(s3_output_path)

print("Data has been successfully written to S3")
