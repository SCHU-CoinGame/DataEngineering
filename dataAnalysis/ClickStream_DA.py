from pyspark.sql import SparkSession
import boto3
import time
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, BooleanType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, explode, lit

RAW_DATA_BUCKET = "aws-s3-bucket-fastcampus"
UNPROCESSED_PREFIX = "ClickStream/unprocessed/"
PROCESSED_PREFIX = "ClickStream/processed/"
RESULTS_PATH = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/statistics.parquet"
COIN_SELECTION_STATS_PATH = f"s3a://{RAW_DATA_BUCKET}/ClickStream/analytics_results/coin_selection_statistics.parquet"

spark = SparkSession.builder \
    .appName("ClickStreamAnalysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.driver.memory", "3g") \
    .config("spark.master", "yarn") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir(f"s3a://{RAW_DATA_BUCKET}/ClickStream/checkpoints")

s3_client = boto3.client('s3')

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

coin_money_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("money", StringType(), True)
])

sell_time_schema = StructType([
    StructField("coin", StringType(), True),
    StructField("time", IntegerType(), True)
])

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

def list_unprocessed_files(bucket, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', [])] if 'Contents' in response else []

def move_to_processed(bucket, file_key):
    processed_key = file_key.replace(UNPROCESSED_PREFIX, PROCESSED_PREFIX)
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': file_key},
        Key=processed_key
    )
    s3_client.delete_object(Bucket=bucket, Key=file_key)
    print(f"Moved {file_key} to {processed_key}")

def analyze_coin_selection_ratio(spark, bucket, file_key):
    s3_path = f"s3a://{bucket}/{file_key}"
    print(f"Reading data from {s3_path}")

    df = spark.read.schema(schema).parquet(s3_path)

    df = df.withColumn("coins", from_json(col("coins"), ArrayType(StringType())))
    df = df.withColumn("deeplearningData", from_json(col("deeplearningData"), ArrayType(deeplearning_schema)))
    df = df.withColumn("userBuyCoinMoney_1", from_json(col("userBuyCoinMoney_1"), coin_money_schema))
    df = df.withColumn("userBuyCoinMoney_2", from_json(col("userBuyCoinMoney_2"), coin_money_schema))
    df = df.withColumn("userBuyCoinMoney_3", from_json(col("userBuyCoinMoney_3"), coin_money_schema))
    df = df.withColumn("userSellTime_1", from_json(col("userSellTime_1"), sell_time_schema))
    df = df.withColumn("userSellTime_2", from_json(col("userSellTime_2"), sell_time_schema))
    df = df.withColumn("userSellTime_3", from_json(col("userSellTime_3"), sell_time_schema))

    coin_counts = df.withColumn("coin", explode(col("coins"))) \
                    .groupBy("coin") \
                    .count() \
                    .withColumnRenamed("count", "new_count")

    try:
        existing_stats = spark.read.parquet(COIN_SELECTION_STATS_PATH)
        print(f"Loaded existing coin selection statistics from {COIN_SELECTION_STATS_PATH}")
    except Exception as e:
        print("No existing statistics found, starting fresh.")
        existing_stats = spark.createDataFrame([], schema=coin_counts.schema)

    updated_stats = existing_stats.union(coin_counts) \
        .groupBy("coin").sum("new_count") \
        .withColumnRenamed("sum(new_count)", "total_count")

    total_selections = updated_stats.agg({"total_count": "sum"}).collect()[0][0]

    selection_ratio = updated_stats.withColumn(
        "selection_ratio",
        (updated_stats["total_count"] / total_selections * 100)
    )

    selection_ratio.write.mode("overwrite").parquet(COIN_SELECTION_STATS_PATH)
    print(f"Coin selection statistics saved to {COIN_SELECTION_STATS_PATH}")

    return selection_ratio

def main():
    while True:
        unprocessed_files = list_unprocessed_files(RAW_DATA_BUCKET, UNPROCESSED_PREFIX)

        if not unprocessed_files:
            print("No unprocessed files found.")
        else:
            for file_key in unprocessed_files:
                try:
                    analyze_coin_selection_ratio(spark, RAW_DATA_BUCKET, file_key)
                    move_to_processed(RAW_DATA_BUCKET, file_key)
                except Exception as e:
                    print(f"Error processing {file_key}: {e}")

        time.sleep(15)

if __name__ == "__main__":
    main()
