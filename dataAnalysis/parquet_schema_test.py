from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# S3 bucket and path
RAW_DATA_BUCKET = "aws-s3-bucket-fastcampus"
UNPROCESSED_PREFIX = "ClickStream/unprocessed/"

# Define schema for deeplearningData
deeplearning_schema = StructType([
    StructField("largest_rise", BooleanType(), True),
    StructField("code", StringType(), True),
    StructField("largest_spike", BooleanType(), True),
    StructField("fastest_growth", BooleanType(), True),
    StructField("most_volatile", BooleanType(), True),
    StructField("percentage", DoubleType(), True),
    StructField("largest_drop", BooleanType(), True),
    StructField("fastest_decline", BooleanType(), True),
    StructField("least_volatile", BooleanType(), True),
    StructField("rank", IntegerType(), True)
])

# Define schema for userBuyCoinMoney and userSellTime
coin_money_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("money", StringType(), True)
])

sell_time_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("time", IntegerType(), True)
])

# Define main schema (Corrected types)
schema = StructType([
    StructField("userName", StringType(), True),
    StructField("userAffiliation", StringType(), True),
    StructField("userNickname", StringType(), True),
    StructField("remainingTime_1", LongType(), True),
    StructField("remainingTime_2", LongType(), True),
    StructField("remainingTime_3", LongType(), True),
    StructField("coins", StringType(), True),
    StructField("deeplearningData", StringType(), True),
    StructField("aiRecommend_1", BooleanType(), True), 
    StructField("aiRecommend_2", BooleanType(), True),
    StructField("leverage", LongType(), True),
    StructField("userBuyCoinMoney_1", StringType(), True),
    StructField("userBuyCoinMoney_2", StringType(), True),
    StructField("userBuyCoinMoney_3", StringType(), True),
    StructField("userSellTime_1", StringType(), True),
    StructField("userSellTime_2", StringType(), True),
    StructField("userSellTime_3", StringType(), True),
    StructField("balance", DoubleType(), True)
])


# Create Spark session
spark = SparkSession.builder \
    .appName("Click StreamData schema test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.master", "local") \
    .getOrCreate()

# Load Parquet data with schema
s3_path = f"s3a://{RAW_DATA_BUCKET}/{UNPROCESSED_PREFIX}/part-d5df92c6-d0b6-4916-9ece-2187406b0605-20241220215202481.parquet"

temp_df = spark.read.parquet(s3_path)
temp_df.printSchema()

df = spark.read.schema(schema).parquet(s3_path)

# Parse JSON fields
df = df.withColumn("coins", from_json(col("coins"), ArrayType(StringType())))
df = df.withColumn("deeplearningData", from_json(col("deeplearningData"), ArrayType(deeplearning_schema)))
df = df.withColumn("userBuyCoinMoney_1", from_json(col("userBuyCoinMoney_1"), coin_money_schema))
df = df.withColumn("userBuyCoinMoney_2", from_json(col("userBuyCoinMoney_2"), coin_money_schema))
df = df.withColumn("userBuyCoinMoney_3", from_json(col("userBuyCoinMoney_3"), coin_money_schema))
df = df.withColumn("userSellTime_1", from_json(col("userSellTime_1"), sell_time_schema))
df = df.withColumn("userSellTime_2", from_json(col("userSellTime_2"), sell_time_schema))
df = df.withColumn("userSellTime_3", from_json(col("userSellTime_3"), sell_time_schema))


# Show data for testing
df.show(truncate=False)

# Stop Spark session
spark.stop()
